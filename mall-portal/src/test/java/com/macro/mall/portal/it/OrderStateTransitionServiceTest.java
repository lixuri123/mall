package com.macro.mall.portal.it;

import com.macro.mall.portal.domain.CancelCommand;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PayResult;
import com.macro.mall.portal.exception.OrderConsistencyException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A 阶段：订单状态机的真实 MySQL 集成测试。
 * 覆盖重复通知、重复取消、支付与取消竞争、多 SKU 回滚、用户权限、
 * 支付晚到异常记录、同 SKU 汇总、积分与优惠券恢复。
 */
class OrderStateTransitionServiceTest extends AbstractOrderIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("100.00");

    @Test
    void paySuccess_deductsStockExactlyOnce() {
        long skuA = createSku(10, 3);
        long skuB = createSku(8, 2);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, skuA, 3);
        addOrderItem(orderId, skuB, 2);

        PayResult first = orderStateTransitionService.paySuccess(PayCommand.internal(orderId, 1));
        assertEquals(PayResult.Status.SUCCESS, first.getStatus());
        assertEquals(2, first.getStockRows());
        assertEquals(1, statusOf(orderId));
        assertEquals(7, stockOf(skuA));
        assertEquals(0, lockOf(skuA));
        assertEquals(6, stockOf(skuB));
        assertEquals(0, lockOf(skuB));

        PayResult duplicate = orderStateTransitionService.paySuccess(PayCommand.internal(orderId, 1));
        assertEquals(PayResult.Status.DUPLICATE, duplicate.getStatus());
        assertEquals(7, stockOf(skuA), "重复通知不能再次扣库存");
        assertEquals(6, stockOf(skuB));
    }

    @Test
    void paySuccess_aggregatesDuplicateSkuOrderItems() {
        long sku = createSku(10, 5);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, sku, 2);
        addOrderItem(orderId, sku, 3);

        PayResult result = orderStateTransitionService.paySuccess(PayCommand.internal(orderId, 1));
        assertEquals(PayResult.Status.SUCCESS, result.getStatus());
        assertEquals(1, result.getStockRows(), "同 SKU 必须先汇总，只执行一次库存更新");
        assertEquals(5, stockOf(sku));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void paySuccess_rollsBackOrderAndEarlierSkuWhenAnySkuFails() {
        long skuA = createSku(10, 3);
        long skuB = createSku(10, 0); // 锁定库存不足
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, skuA, 3);
        addOrderItem(orderId, skuB, 2);

        assertThrows(OrderConsistencyException.class,
                () -> orderStateTransitionService.paySuccess(PayCommand.internal(orderId, 1)));

        assertEquals(0, statusOf(orderId), "订单状态必须回滚到待付款");
        assertEquals(10, stockOf(skuA), "先成功的 SKU 也必须回滚");
        assertEquals(3, lockOf(skuA));
        assertEquals(10, stockOf(skuB));
        assertEquals(0, lockOf(skuB));
    }

    @Test
    void paySuccess_rejectsMismatchedAmount() {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, sku, 3);

        PayResult result = orderStateTransitionService.paySuccess(
                PayCommand.fromGateway(orderSnOf(orderId), 1, new BigDecimal("1.00"), "TRADE-X", "{}"));
        assertEquals(PayResult.Status.REJECTED, result.getStatus());
        assertEquals(0, statusOf(orderId));
        assertEquals(10, stockOf(sku));
        assertEquals(3, lockOf(sku));
    }

    @Test
    void latePayment_isRecordedAsTrackableAnomaly() throws SQLException {
        long sku = createSku(10, 3);
        long orderId = createOrder(4, 0, AMOUNT); // 订单已取消
        addOrderItem(orderId, sku, 3);

        PayCommand command = PayCommand.fromGateway(
                orderSnOf(orderId), 1, AMOUNT, "TRADE-LATE", "{\"trade_status\":\"TRADE_SUCCESS\"}");
        PayResult first = orderStateTransitionService.paySuccess(command);
        assertEquals(PayResult.Status.LATE_PAYMENT_RECORDED, first.getStatus());
        assertEquals(4, statusOf(orderId), "数据库条件更新不能撤销第三方已发生的支付");
        assertEquals(10, stockOf(sku), "不能因为晚到支付而扣库存");
        assertEquals(3, lockOf(sku));
        assertEquals(1, countPayAnomalies(orderId), "异常必须留下可追踪记录");

        PayResult repeated = orderStateTransitionService.paySuccess(command);
        assertEquals(PayResult.Status.LATE_PAYMENT_RECORDED, repeated.getStatus());
        assertEquals(1, countPayAnomalies(orderId), "重复通知不能重复落库");
    }

    @Test
    void cancelUserOrder_checksOwnershipAndReleasesOnlyOnce() {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, sku, 3);

        CancelResult wrongUser = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, TEST_MEMBER_ID + 1000));
        assertEquals(CancelResult.Status.REJECTED, wrongUser.getStatus());
        assertEquals(0, statusOf(orderId));
        assertEquals(3, lockOf(sku));

        CancelResult first = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, TEST_MEMBER_ID));
        assertEquals(CancelResult.Status.SUCCESS, first.getStatus());
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
        assertEquals(10, stockOf(sku));

        CancelResult again = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, TEST_MEMBER_ID));
        assertEquals(CancelResult.Status.ALREADY_CLOSED, again.getStatus());
        assertEquals(0, lockOf(sku), "重复取消不能再次释放库存");
    }

    @Test
    void cancelSystemOrder_requiresGenuineTimeout() throws SQLException {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, sku, 3);

        CancelResult early = orderStateTransitionService.cancelOrder(
                CancelCommand.bySystem(orderId, 60));
        assertEquals(CancelResult.Status.NOT_TIMED_OUT, early.getStatus(),
                "重复或提前投递的消息不能释放库存");
        assertEquals(0, statusOf(orderId));
        assertEquals(3, lockOf(sku));

        executeUpdate("UPDATE oms_order SET create_time = DATE_SUB(NOW(), INTERVAL 2 HOUR) WHERE id = ?",
                orderId);
        CancelResult late = orderStateTransitionService.cancelOrder(
                CancelCommand.bySystem(orderId, 60));
        assertEquals(CancelResult.Status.SUCCESS, late.getStatus());
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void cancelPaidOrder_doesNotReleaseItsStock() {
        long sku = createSku(10, 0);
        long orderId = createOrder(1, 0, AMOUNT); // 已支付，stock 已扣完
        addOrderItem(orderId, sku, 3);

        CancelResult result = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, TEST_MEMBER_ID));
        assertEquals(CancelResult.Status.REJECTED, result.getStatus());
        assertEquals(1, statusOf(orderId));
        assertEquals(10, stockOf(sku));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void cancelRestoresCouponAndIntegrationAtomically() throws SQLException {
        long sku = createSku(10, 3);
        long couponId = 99990002L;
        execute("DELETE FROM sms_coupon_history WHERE coupon_id = " + couponId);
        executeUpdate("INSERT INTO sms_coupon_history (coupon_id, member_id, coupon_code, use_status) "
                + "VALUES (?, ?, 'IT-RECOVER', 1)", couponId, TEST_MEMBER_ID);
        int integrationBefore = queryInt("SELECT IFNULL(integration, 0) FROM ums_member WHERE id = "
                + TEST_MEMBER_ID);
        long orderId = createOrder(0, 0, AMOUNT, 50, couponId, TEST_MEMBER_ID);
        addOrderItem(orderId, sku, 3);

        CancelResult result = orderStateTransitionService.cancelOrder(
                CancelCommand.byUser(orderId, TEST_MEMBER_ID));
        assertEquals(CancelResult.Status.SUCCESS, result.getStatus());
        assertEquals(integrationBefore + 50,
                queryInt("SELECT IFNULL(integration, 0) FROM ums_member WHERE id = " + TEST_MEMBER_ID));
        assertEquals(0, queryInt("SELECT use_status FROM sms_coupon_history WHERE coupon_id = " + couponId));
        executeUpdate("UPDATE ums_member SET integration = ? WHERE id = ?",
                integrationBefore, TEST_MEMBER_ID);
        execute("DELETE FROM sms_coupon_history WHERE coupon_id = " + couponId);
    }

    /**
     * 支付与取消并发竞争：两条条件更新争抢同一订单行，有且只有一个能成功变更状态。
     * 反复执行多轮，检查最终状态与库存始终一致。
     */
    @Test
    void paymentAndCancelRace_hasExactlyOneWinnerAndConsistentStock() throws Exception {
        int rounds = 20;
        int paidWins = 0;
        int cancelWins = 0;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < rounds; round++) {
                long sku = createSku(10, 3);
                long orderId = createOrder(0, 0, AMOUNT);
                addOrderItem(orderId, sku, 3);
                long currentOrder = orderId;
                CountDownLatch start = new CountDownLatch(1);

                Future<PayResult> pay = pool.submit(() -> {
                    start.await();
                    return orderStateTransitionService.paySuccess(PayCommand.internal(currentOrder, 1));
                });
                Future<CancelResult> cancel = pool.submit(() -> {
                    start.await();
                    return orderStateTransitionService.cancelOrder(
                            CancelCommand.byUser(currentOrder, TEST_MEMBER_ID));
                });
                start.countDown();
                PayResult payResult = pay.get(30, TimeUnit.SECONDS);
                CancelResult cancelResult = cancel.get(30, TimeUnit.SECONDS);

                boolean paid = payResult.getStatus() == PayResult.Status.SUCCESS;
                boolean cancelled = cancelResult.getStatus() == CancelResult.Status.SUCCESS;
                assertTrue(paid ^ cancelled,
                        "第 " + round + " 轮必须恰好一个成功，pay=" + payResult.getStatus()
                                + ", cancel=" + cancelResult.getStatus());

                int status = statusOf(currentOrder);
                if (paid) {
                    paidWins++;
                    assertEquals(1, status);
                    assertEquals(7, stockOf(sku), "支付胜出：stock 应扣减 3");
                    assertEquals(0, lockOf(sku));
                    assertEquals(CancelResult.Status.REJECTED, cancelResult.getStatus());
                } else {
                    cancelWins++;
                    assertEquals(4, status);
                    assertEquals(10, stockOf(sku), "取消胜出：stock 不应变化");
                    assertEquals(0, lockOf(sku), "取消胜出：锁定库存应释放");
                    // 支付晚到属于必须识别的边界，不能简单当成功忽略
                    assertEquals(PayResult.Status.LATE_PAYMENT_RECORDED, payResult.getStatus());
                    assertEquals(1, countPayAnomalies(currentOrder));
                }
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(rounds, paidWins + cancelWins);
        System.out.printf("[RACE] rounds=%d, paidWins=%d, cancelWins=%d%n", rounds, paidWins, cancelWins);
    }

    /** 并发下单：条件更新保证成功锁定总量不超过可售库存。 */
    @Test
    void concurrentLockingNeverOversells() throws Exception {
        int stock = 10;
        int threads = 30;
        int quantity = 1;
        long sku = createSku(stock, 0);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        long begin = System.nanoTime();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        if (portalOrderDao.lockSkuStock(sku, quantity) == 1) {
                            success.incrementAndGet();
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);
        assertEquals(stock, success.get(), "成功锁定次数不能超过可售库存");
        assertEquals(0, errors.get());
        assertEquals(stock, lockOf(sku));
        assertEquals(stock, stockOf(sku));
        System.out.printf("[CONCURRENT] stock=%d, threads=%d, qty=%d, success=%d, errors=%d, "
                        + "finalLock=%d, elapsedMs=%d%n",
                stock, threads, quantity, success.get(), errors.get(), lockOf(sku), elapsedMs);
    }
}
