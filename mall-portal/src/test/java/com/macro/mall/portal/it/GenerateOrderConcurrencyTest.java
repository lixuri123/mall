package com.macro.mall.portal.it;

import com.macro.mall.common.service.RedisService;
import com.macro.mall.model.UmsMember;
import com.macro.mall.model.UmsMemberReceiveAddress;
import com.macro.mall.portal.component.CancelOrderSender;
import com.macro.mall.portal.domain.CartPromotionItem;
import com.macro.mall.portal.domain.MemberDetails;
import com.macro.mall.portal.domain.OrderParam;
import com.macro.mall.portal.service.OmsCartItemService;
import com.macro.mall.portal.service.UmsMemberReceiveAddressService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * A 阶段「并发购买」端到端测试：真实调用 {@code OmsPortalOrderServiceImpl.generateOrder()}，
 * 走真实 Spring 事务与真实 MySQL 条件更新，只是把 SecurityContext 之外的
 * 购物车 / 地址 / 订单号 / MQ 发送这些外围依赖用 Mock 固定住。
 *
 * <p>验收口径来自方案 §9：初始可售 10，多个请求并发下单，成功订单数不超过 10，
 * 且数据库 lock_stock 与成功订单一致（不能只看「没有负库存」）。</p>
 */
class GenerateOrderConcurrencyTest extends AbstractOrderIntegrationTest {

    @MockBean
    private OmsCartItemService cartItemService;
    @MockBean
    private UmsMemberReceiveAddressService memberReceiveAddressService;
    @MockBean
    private RedisService redisService;
    @MockBean
    private CancelOrderSender cancelOrderSender;

    private long skuId;

    @BeforeEach
    void stubPeripherals() throws SQLException {
        skuId = createSku(10, 0);
        when(redisService.incr(anyString(), anyLong()))
                .thenAnswer(invocation -> 1L);
        when(memberReceiveAddressService.getItem(anyLong())).thenAnswer(invocation -> {
            UmsMemberReceiveAddress address = new UmsMemberReceiveAddress();
            address.setName("it-test");
            address.setPhoneNumber("13800000000");
            return address;
        });
        AtomicLong cartId = new AtomicLong(1L);
        when(cartItemService.listPromotion(eq(TEST_MEMBER_ID), any()))
                .thenAnswer(invocation -> List.of(cartItem(cartId.incrementAndGet(), skuId, 1)));
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-test'");
    }

    @AfterEach
    void cleanupGeneratedOrders() throws SQLException {
        SecurityContextHolder.clearContext();
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-test'");
    }

    @Test
    void concurrentGenerateOrder_neverCreatesMoreOrdersThanAvailableStock() throws Exception {
        int stock = 10;
        int threads = 20;
        int quantity = 1;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        long begin = System.nanoTime();
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    setSecurityContext();
                    try {
                        start.await();
                        portalOrderService.generateOrder(orderParam());
                        success.incrementAndGet();
                    } catch (Exception e) {
                        if (e.getMessage() != null && e.getMessage().contains("库存不足")) {
                            rejected.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                            System.err.println("unexpected error: " + e);
                        }
                    } finally {
                        SecurityContextHolder.clearContext();
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - begin);

        int createdOrders = queryInt("SELECT COUNT(*) FROM oms_order WHERE member_username = 'it-test'");
        assertEquals(stock, success.get(), "成功订单数不能超过初始可售库存");
        assertEquals(0, errors.get(), "除库存不足外不应出现其他异常");
        assertEquals(success.get(), createdOrders, "数据库订单数必须与成功返回的次数一致");
        assertEquals(stock, lockOf(skuId), "lock_stock 必须等于成功订单锁定的总量");
        assertEquals(stock, stockOf(skuId), "下单阶段不应扣减实际库存");
        assertEquals(threads - stock, rejected.get());
        System.out.printf("[GENERATE_ORDER] stock=%d, threads=%d, qty=%d, successOrders=%d, "
                        + "rejected=%d, errors=%d, dbOrders=%d, finalStock=%d, finalLock=%d, elapsedMs=%d%n",
                stock, threads, quantity, success.get(), rejected.get(), errors.get(), createdOrders,
                stockOf(skuId), lockOf(skuId), elapsedMs);
    }

    /** 单笔下单：多 SKU 中一个失败，整个事务回滚，不留下订单也不留下锁定库存。 */
    @Test
    void generateOrder_rollsBackAllSkusWhenOneIsInsufficient() throws SQLException {
        long secondSku = createSku(0, 0);
        when(cartItemService.listPromotion(eq(TEST_MEMBER_ID), any())).thenAnswer(invocation ->
                List.of(cartItem(10L, skuId, 2), cartItem(11L, secondSku, 1)));
        setSecurityContext();
        assertThrows(RuntimeException.class, () -> portalOrderService.generateOrder(orderParam()));

        assertEquals(0, lockOf(skuId), "第一个 SKU 的锁定必须回滚");
        assertEquals(0, lockOf(secondSku));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order WHERE member_username = 'it-test'"),
                "失败的下单不能留下订单记录");
    }

    /** 同一 SKU 多条购物车记录：必须先汇总数量再锁库存。 */
    @Test
    void generateOrder_aggregatesDuplicateSkuCartItems() throws SQLException {
        when(cartItemService.listPromotion(eq(TEST_MEMBER_ID), any())).thenAnswer(invocation ->
                List.of(cartItem(20L, skuId, 3), cartItem(21L, skuId, 4)));
        setSecurityContext();
        portalOrderService.generateOrder(orderParam());
        assertEquals(7, lockOf(skuId), "同一 SKU 的 3 + 4 必须汇总成 7");
        assertEquals(10, stockOf(skuId));
        assertEquals(1, queryInt("SELECT COUNT(*) FROM oms_order WHERE member_username = 'it-test'"));
    }

    /** 修复验证：use_integration 必须在 insert 之前设置，取消时才能正确返还。 */
    @Test
    void generateOrder_persistsUsedIntegrationAndCancelReturnsIt() throws SQLException {
        when(cartItemService.listPromotion(eq(TEST_MEMBER_ID), any())).thenAnswer(invocation ->
                List.of(cartItem(30L, skuId, 3)));
        int integrationBefore = queryInt("SELECT IFNULL(integration, 0) FROM ums_member WHERE id = "
                + TEST_MEMBER_ID);
        setSecurityContext();
        OrderParam param = orderParam();
        param.setUseIntegration(100);
        portalOrderService.generateOrder(param);

        long generatedOrderId = queryInt("SELECT id FROM oms_order WHERE member_username = 'it-test' LIMIT 1");
        assertEquals(100, queryInt("SELECT IFNULL(use_integration, 0) FROM oms_order WHERE id = "
                + generatedOrderId), "使用的积分必须写入订单表");
        assertEquals(integrationBefore - 100,
                queryInt("SELECT IFNULL(integration, 0) FROM ums_member WHERE id = " + TEST_MEMBER_ID),
                "下单应原子扣减积分");

        portalOrderService.cancelUserOrder(generatedOrderId);
        assertEquals(integrationBefore,
                queryInt("SELECT IFNULL(integration, 0) FROM ums_member WHERE id = " + TEST_MEMBER_ID),
                "取消订单应返还已使用积分");
        assertEquals(0, lockOf(skuId));
    }

    private CartPromotionItem cartItem(long id, long sku, int quantity) {
        CartPromotionItem item = new CartPromotionItem();
        item.setId(id);
        item.setMemberId(TEST_MEMBER_ID);
        item.setProductId(999999L);
        item.setProductSkuId(sku);
        item.setQuantity(quantity);
        item.setPrice(BigDecimal.ONE);
        item.setReduceAmount(BigDecimal.ZERO);
        item.setPromotionMessage("无优惠");
        item.setIntegration(0);
        item.setGrowth(0);
        item.setDeleteStatus(0);
        return item;
    }

    private OrderParam orderParam() {
        OrderParam param = new OrderParam();
        param.setMemberReceiveAddressId(1L);
        param.setPayType(1);
        param.setCartIds(List.of(1L));
        return param;
    }

    private void setSecurityContext() {
        UmsMember member = new UmsMember();
        member.setId(TEST_MEMBER_ID);
        member.setUsername("it-test");
        member.setIntegration(10000);
        member.setStatus(1);
        MemberDetails details = new MemberDetails(member);
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(details, null, details.getAuthorities()));
    }
}
