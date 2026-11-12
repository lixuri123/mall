package com.macro.mall.portal.it;

import com.macro.mall.portal.MallPortalApplication;
import com.macro.mall.portal.domain.CancelCommand;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.OmsOrderMessage;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B 阶段：本地消息表 DAO 与固定到期时间的真实 MySQL 集成测试。
 * 本类不启动 MQ 消费者、不发布消息，只验证数据库条件更新与租约语义。
 */
@SpringBootTest(classes = MallPortalApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://127.0.0.1:3307/mall_it_b?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true",
        "spring.datasource.username=root",
        "spring.datasource.password=root",
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.rabbitmq.listener.direct.auto-startup=false",
        "spring.rabbitmq.listener.simple.acknowledge-mode=manual",
        "spring.rabbitmq.publisher-confirm-type=correlated",
        "spring.rabbitmq.connection-timeout=2000",
        "spring.rabbitmq.publisher-returns=true",
        "mall.order.outbox.dispatch.enabled=true",
        "mall.order.outbox.dispatch.initial-delay-ms=3600000",
        "mall.order.outbox.dispatch.interval-ms=3600000",
        "mall.order.outbox.dispatch.lease-seconds=1",
        "mall.order.outbox.dispatch.max-retry=3",
        "mall.order.outbox.consume.enabled=false",
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
        "logging.level.root=warn",
        "logging.level.com.macro.mall=warn"
})
class OutboxDaoTest extends AbstractOrderMessageIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("100.00");

    @Test
    void claimIsCompareAndSet_acrossTwoInstances() throws Exception {
        long orderId = createOrder(0, -1, AMOUNT);
        String eventId = createMessage(orderId, -1);
        OmsOrderMessage before = messageOf(eventId);
        assertEquals(OmsOrderMessage.STATUS_PENDING, before.getStatus());
        assertEquals(0L, before.getAttemptVersion());

        Date now = new Date();
        Date lease = new Date(now.getTime() + 60_000L);
        OmsOrderMessage claimedA = messageService.claim(before.getId(), 0L, "instance-A", lease, now);
        assertNotNull(claimedA, "第一个实例应领取成功");
        assertEquals(1L, claimedA.getAttemptVersion());
        assertEquals(OmsOrderMessage.STATUS_SENDING, claimedA.getStatus());
        assertEquals("instance-A", claimedA.getLeaseOwner());
        System.out.printf("[B-LEASE] eventId=%s, orderId=%d, winner=instance-A, loser=instance-B, attemptVersion=%d%n",
                eventId, orderId, claimedA.getAttemptVersion());

        OmsOrderMessage claimedB = messageService.claim(before.getId(), 0L, "instance-B", lease, now);
        assertNull(claimedB, "同一 version 只能有一个领取成功");
        assertEquals(1L, messageOf(eventId).getAttemptVersion(), "重复领取不能再次递增版本");
    }

    @Test
    void expiredLeaseCanBeReclaimedAndOldCallbackIsIgnored() throws Exception {
        long orderId = createOrder(0, -1, AMOUNT);
        String eventId = createMessage(orderId, -1);
        OmsOrderMessage before = messageOf(eventId);
        Date now = new Date();
        Date shortLease = new Date(now.getTime() - 1000L); // 立即过期，模拟领取进程崩溃
        OmsOrderMessage first = messageService.claim(before.getId(), 0L, "crashed", shortLease, now);
        assertNotNull(first);

        OmsOrderMessage second = messageService.claim(before.getId(), 1L, "recovered",
                new Date(now.getTime() + 60_000L), now);
        assertNotNull(second, "租约过期后必须能被重新领取");
        assertEquals(2L, second.getAttemptVersion());

        assertFalse(messageService.markSent(eventId, 1L), "旧尝试的迟到 Confirm 不能标记成功");
        assertTrue(messageService.markSent(eventId, 2L), "当前尝试可以标记成功");
        assertEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());
    }

    @Test
    void retryBackoffEscalatesToFailedAndManualRetryRestores() throws Exception {
        long orderId = createOrder(0, -1, AMOUNT);
        String eventId = createMessage(orderId, -1);
        OmsOrderMessage before = messageOf(eventId);
        Date now = new Date();
        OmsOrderMessage claimed = messageService.claim(before.getId(), 0L, "i1",
                new Date(now.getTime() + 60_000L), now);
        assertNotNull(claimed);

        assertTrue(messageService.markRetry(eventId, 1L, "NACK: broker refused",
                new Date(now.getTime() + 1000L), 1));
        OmsOrderMessage after = messageOf(eventId);
        assertEquals(OmsOrderMessage.STATUS_FAILED, after.getStatus(), "达到上限应转 FAILED");
        assertEquals(1, after.getRetryCount());
        assertTrue(after.getLastError().contains("NACK"));

        assertTrue(messageService.manualRetry(eventId));
        OmsOrderMessage reset = messageOf(eventId);
        assertEquals(OmsOrderMessage.STATUS_PENDING, reset.getStatus());
        assertEquals(0, reset.getRetryCount());
        assertFalse(messageService.manualRetry(eventId), "非 FAILED 状态不能重复人工重试");
    }

    @Test
    void earlyDeliveryReschedulesToFixedExpireTime() throws Exception {
        long orderId = createOrder(0, 30, AMOUNT); // 30 分钟后到期
        String eventId = createMessage(orderId, -1); // 消息已提前投递
        execute("UPDATE oms_order_message SET status = 'SENT' WHERE event_id = '" + eventId + "'");
        assertEquals(1, messageService.rescheduleToOrderExpireTime(eventId));

        OmsOrderMessage rescheduled = messageOf(eventId);
        assertEquals(OmsOrderMessage.STATUS_PENDING, rescheduled.getStatus());
        Date expected = queryDate("SELECT expire_time FROM oms_order WHERE id = " + orderId);
        assertEquals(expected.getTime() / 1000, rescheduled.getDueTime().getTime() / 1000);
        assertEquals(expected.getTime() / 1000, rescheduled.getNextRetryTime().getTime() / 1000);
        assertEquals("EARLY_DELIVERY_RESCHEDULED", rescheduled.getLastError());
    }

    @Test
    void expireTimeIsTheOnlyAuthorityForSystemCancel() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 30, AMOUNT); // 未到期
        addOrderItem(orderId, sku, 3);
        // 把 create_time 改得很早，模拟「当前配置 + create_time」会误判为超时
        executeUpdate("UPDATE oms_order SET create_time = DATE_SUB(NOW(), INTERVAL 10 HOUR) WHERE id = ?", orderId);

        CancelResult early = orderStateTransitionService.cancelOrder(CancelCommand.bySystem(orderId, null));
        assertEquals(CancelResult.Status.NOT_TIMED_OUT, early.getStatus(),
                "固定 expire_time 未到，不能因为 create_time 很早就取消");
        assertEquals(0, statusOf(orderId));
        assertEquals(3, lockOf(sku));

        executeUpdate("UPDATE oms_order SET expire_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE) WHERE id = ?", orderId);
        CancelResult late = orderStateTransitionService.cancelOrder(CancelCommand.bySystem(orderId, null));
        assertEquals(CancelResult.Status.SUCCESS, late.getStatus());
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void missingExpireTimeIsRejectedInsteadOfGuessed() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        execute("UPDATE oms_order SET expire_time = NULL WHERE id = " + orderId);

        CancelResult result = orderStateTransitionService.cancelOrder(CancelCommand.bySystem(orderId, null));
        assertEquals(CancelResult.Status.REJECTED, result.getStatus());
        assertEquals(0, statusOf(orderId), "历史数据未回填时不能擅自取消");
        assertEquals(3, lockOf(sku));
    }

    @Test
    void timeoutScanUsesFixedExpireTimeWithIdCursor() throws Exception {
        long sku = createSku(10, 9); // 三笔订单各锁定 3
        long expired1 = createOrder(0, -5, AMOUNT);
        long expired2 = createOrder(0, -5, AMOUNT);
        long fresh = createOrder(0, 60, AMOUNT);
        addOrderItem(expired1, sku, 3);
        addOrderItem(expired2, sku, 3);
        addOrderItem(fresh, sku, 3);

        List<Long> firstPage = portalOrderDao.listExpiredPendingOrderIds(0L, 1);
        assertEquals(1, firstPage.size());
        long cursor = firstPage.get(0);
        List<Long> secondPage = portalOrderDao.listExpiredPendingOrderIds(cursor, 10);
        assertTrue(secondPage.stream().allMatch(id -> id > cursor));
        assertFalse(secondPage.contains(fresh), "未到期的订单不能进入扫描结果");
        assertTrue(secondPage.contains(expired1 == cursor ? expired2 : expired1));

        int handled = portalOrderService.cancelTimeOutOrder();
        assertEquals(2, handled);
        assertEquals(4, statusOf(expired1));
        assertEquals(4, statusOf(expired2));
        assertEquals(0, statusOf(fresh));
        assertEquals(3, lockOf(sku), "只释放已到期两单的锁定库存");
    }

    @Test
    void metricsExposePendingAgeFailuresAndUncancelledOrders() throws Exception {
        long expired = createOrder(0, -5, AMOUNT);
        createMessage(expired, -5);
        Map<String, Object> m = messageService.metrics();
        assertEquals(1L, ((Number) m.get("pending_messages")).longValue());
        assertTrue(((Number) m.get("expired_uncancelled")).longValue() >= 1L);
        assertNotNull(m.get("oldest_pending_seconds"));
        assertNotNull(m.get("consume_failures"));
        assertNotNull(m.get("pending_without_expire_time"));
    }
}
