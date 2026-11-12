package com.macro.mall.portal.it;

import com.macro.mall.common.service.RedisService;
import com.macro.mall.model.UmsMember;
import com.macro.mall.model.UmsMemberReceiveAddress;
import com.macro.mall.portal.MallPortalApplication;
import com.macro.mall.portal.component.OrderCancelPublisher;
import com.macro.mall.portal.domain.CartPromotionItem;
import com.macro.mall.portal.domain.MemberDetails;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.OrderParam;
import com.macro.mall.portal.domain.PublishOutcome;
import com.macro.mall.portal.exception.OrderConsistencyException;
import com.macro.mall.portal.service.OmsCartItemService;
import com.macro.mall.portal.service.OmsOrderMessageService;
import com.macro.mall.portal.service.UmsMemberReceiveAddressService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * B 阶段：下单事务原子性 + 发送任务故障注入。
 *
 * <p>用可切换的 {@code @Primary} 发布器模拟 MQ 不可用、NACK、不可路由、Confirm 超时，
 * 验证状态流转与「至少一次投递」语义；真实 RabbitMQ 链路在
 * {@link OrderMessageEndToEndRabbitTest} 中验证。</p>
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
        "mall.order.outbox.dispatch.confirm-timeout-ms=2000",
        "mall.order.outbox.dispatch.max-retry=2",
        "mall.order.outbox.dispatch.retry-base-seconds=1",
        "mall.order.outbox.consume.enabled=false",
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
        "logging.level.root=warn",
        "logging.level.com.macro.mall=warn"
})
@Import(OrderMessageDispatchTest.FaultPublisherConfig.class)
class OrderMessageDispatchTest extends AbstractOrderMessageIntegrationTest {

    /** 可切换的发布器：不改变真实 CancelOrderSender 的实现，只替换派发任务的注入对象。 */
    @TestConfiguration
    static class FaultPublisherConfig {
        @Bean
        @Primary
        OrderCancelPublisher faultPublisher() {
            return new FaultPublisher();
        }
    }

    static class FaultPublisher implements OrderCancelPublisher {
        volatile PublishOutcome outcome = PublishOutcome.confirmed();
        volatile RuntimeException networkFailure = null;
        volatile CountDownLatch blockLatch = null;
        final AtomicInteger calls = new AtomicInteger();
        final List<OrderCancelMessage> messages = new CopyOnWriteArrayList<>();

        @Override
        public PublishOutcome publish(OrderCancelMessage message, long confirmTimeoutMs) {
            calls.incrementAndGet();
            messages.add(message);
            CountDownLatch latch = blockLatch;
            if (latch != null) {
                try {
                    latch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (networkFailure != null) {
                throw networkFailure;
            }
            return outcome;
        }

        void reset() {
            outcome = PublishOutcome.confirmed();
            networkFailure = null;
            blockLatch = null;
            calls.set(0);
            messages.clear();
        }
    }

    @MockBean private OmsCartItemService cartItemService;
    @MockBean private UmsMemberReceiveAddressService memberReceiveAddressService;
    @MockBean private RedisService redisService;
    @SpyBean private OmsOrderMessageService spyMessageService;

    private FaultPublisher fault;
    private long skuId;

    @BeforeEach
    void stubPeripherals() throws SQLException {
        fault = (FaultPublisher) publisher;
        fault.reset();
        skuId = createSku(10, 0);
        when(redisService.incr(anyString(), anyLong())).thenAnswer(invocation -> 1L);
        when(memberReceiveAddressService.getItem(anyLong())).thenAnswer(invocation -> {
            UmsMemberReceiveAddress address = new UmsMemberReceiveAddress();
            address.setName("it-b");
            address.setPhoneNumber("13800000000");
            return address;
        });
        when(cartItemService.listPromotion(eq(TEST_MEMBER_ID), any()))
                .thenAnswer(invocation -> List.of(cartItem(1L, skuId, 1)));
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order_message WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-test'");
    }

    @AfterEach
    void clearContext() throws SQLException {
        SecurityContextHolder.clearContext();
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order_message WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-test') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-test'");
    }

    @Test
    void generateOrder_persistsFixedExpireTimeAndPendingMessage() throws Exception {
        setSecurityContext();
        portalOrderService.generateOrder(orderParam());
        long orderId = queryInt("SELECT id FROM oms_order WHERE member_username = 'it-test' LIMIT 1");

        int diffMinutes = queryInt("SELECT TIMESTAMPDIFF(MINUTE, create_time, expire_time) FROM oms_order WHERE id = "
                + orderId);
        assertEquals(120, diffMinutes, "固定到期时间必须按下单时的订单设置计算");

        OmsOrderMessage m = messageOf(OmsOrderMessage.eventIdOf(orderId));
        assertNotNull(m, "订单与消息必须在同一事务里一起提交");
        assertEquals(OmsOrderMessage.STATUS_PENDING, m.getStatus());
        assertEquals(OmsOrderMessage.EVENT_TYPE_TIMEOUT_CANCEL, m.getEventType());
        assertEquals(m.getDueTime().getTime() / 1000,
                queryDate("SELECT expire_time FROM oms_order WHERE id = " + orderId).getTime() / 1000);
        OrderCancelMessage parsed = objectMapper.readValue(m.getPayload(), OrderCancelMessage.class);
        assertTrue(parsed.isValid());
        assertEquals(OmsOrderMessage.eventIdOf(orderId), parsed.getEventId(), "重试中 eventId 必须不变");
        assertEquals(1, lockOf(skuId));
    }

    @Test
    void generateOrder_rollsBackOrderStockAndMessageTogether() throws Exception {
        AtomicBoolean failOnce = new AtomicBoolean(true);
        doAnswer(invocation -> {
            if (failOnce.compareAndSet(true, false)) {
                throw new OrderConsistencyException("模拟本地消息表写入失败");
            }
            return invocation.callRealMethod();
        }).when(spyMessageService).insertPendingForNewOrder(anyLong(), any(Date.class), anyString());

        setSecurityContext();
        assertThrows(OrderConsistencyException.class, () -> portalOrderService.generateOrder(orderParam()));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order WHERE member_username = 'it-test'"));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_message"));
        assertEquals(0, lockOf(skuId), "消息表写入失败时锁定的库存必须一起回滚");
    }

    @Test
    void mqUnavailable_orderStillCreatedAndMessageRecoversAfterBrokerReturns() throws Exception {
        setSecurityContext();
        portalOrderService.generateOrder(orderParam());
        long orderId = queryInt("SELECT id FROM oms_order WHERE member_username = 'it-test' LIMIT 1");
        // 把到期时间提前，模拟订单已到期但发布前 MQ 不可用
        executeUpdate("UPDATE oms_order SET expire_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE) WHERE id = ?", orderId);
        execute("UPDATE oms_order_message SET due_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE), "
                + "next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");

        fault.networkFailure = new IllegalStateException("simulated RabbitMQ connection refused");
        assertEquals(0, dispatcher.dispatchOnce());
        assertEquals(0, statusOf(orderId), "MQ 不可用不能影响已创建的订单（仍为待付款）");
        assertEquals(1, queryInt("SELECT COUNT(*) FROM oms_order WHERE member_username = 'it-test'"));
        OmsOrderMessage afterFailure = messageOf(OmsOrderMessage.eventIdOf(orderId));
        assertEquals(OmsOrderMessage.STATUS_PENDING, afterFailure.getStatus());
        assertEquals(1, afterFailure.getRetryCount());
        assertTrue(afterFailure.getLastError().contains("PUBLISH_EXCEPTION"));

        fault.networkFailure = null;
        execute("UPDATE oms_order_message SET next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");
        assertEquals(1, dispatcher.dispatchOnce("recover-instance"));
        OmsOrderMessage recovered = messageOf(OmsOrderMessage.eventIdOf(orderId));
        assertEquals(OmsOrderMessage.STATUS_SENT, recovered.getStatus(), "SENT 只代表发布阶段成功");
        assertEquals(0, statusOf(orderId), "消息发布不等于订单已取消");
        assertEquals(2, fault.calls.get());
        System.out.printf("[B-MQ-OUTAGE] orderId=%d, eventId=%s, duringStatus=%s, retryCount=%d, "
                        + "afterStatus=%s, publishCalls=%d%n",
                orderId, OmsOrderMessage.eventIdOf(orderId), afterFailure.getStatus(),
                afterFailure.getRetryCount(), recovered.getStatus(), fault.calls.get());
    }

    @Test
    void nackUnroutableAndUnknownConfirmAreNeverMarkedSent() throws Exception {
        long orderId = createOrder(0, -1, BigDecimal.ONE);
        String eventId = createMessage(orderId, -1);

        fault.outcome = PublishOutcome.of(PublishOutcome.Status.NACK, "broker nack");
        assertEquals(0, dispatcher.dispatchOnce());
        assertEquals(OmsOrderMessage.STATUS_PENDING, messageOf(eventId).getStatus());
        assertNotEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());

        execute("UPDATE oms_order_message SET next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");
        // Confirm ACK 但被 Return：不能仅凭 ACK 判成功。
        fault.outcome = PublishOutcome.of(PublishOutcome.Status.UNROUTABLE, "NO_ROUTE");
        assertEquals(0, dispatcher.dispatchOnce());
        OmsOrderMessage failed = messageOf(eventId);
        assertEquals(OmsOrderMessage.STATUS_FAILED, failed.getStatus(), "达到重试上限应置 FAILED");
        assertEquals(2, failed.getRetryCount());
        assertTrue(failed.getLastError().contains("UNROUTABLE"));

        assertTrue(messageService.manualRetry(eventId));
        execute("UPDATE oms_order_message SET next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");
        fault.outcome = PublishOutcome.of(PublishOutcome.Status.UNKNOWN, "confirm timeout");
        assertEquals(0, dispatcher.dispatchOnce());
        assertEquals(OmsOrderMessage.STATUS_PENDING, messageOf(eventId).getStatus());
        assertTrue(messageOf(eventId).getLastError().contains("UNKNOWN"));
    }

    @Test
    void notYetDueMessageIsNotDispatched() throws Exception {
        long orderId = createOrder(0, 60, BigDecimal.ONE);
        String eventId = createMessage(orderId, 60);
        assertEquals(0, dispatcher.dispatchOnce());
        assertEquals(OmsOrderMessage.STATUS_PENDING, messageOf(eventId).getStatus());
        assertEquals(0, fault.calls.get());
    }

    @Test
    void multiInstanceDispatchClaimsEachMessageOnlyOnce() throws Exception {
        long orderId = createOrder(0, -1, BigDecimal.ONE);
        String eventId = createMessage(orderId, -1);
        CountDownLatch release = new CountDownLatch(1);
        fault.blockLatch = release;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Future<Integer> a = pool.submit(() -> {
                start.await();
                return dispatcher.dispatchOnce("instance-A");
            });
            Future<Integer> b = pool.submit(() -> {
                start.await();
                return dispatcher.dispatchOnce("instance-B");
            });
            start.countDown();
            Thread.sleep(150L); // 让两个实例都进入候选读取阶段
            release.countDown();
            int total = a.get(10, TimeUnit.SECONDS) + b.get(10, TimeUnit.SECONDS);
            assertEquals(1, total, "两个实例合计只能成功发布一次");
            assertEquals(1, fault.calls.get(), "CAS 领取应只允许一次实际发布");
            assertEquals(1L, messageOf(eventId).getAttemptVersion());
            assertEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void confirmSucceededButMarkSentCrashed_isResentAfterLeaseExpiry() throws Exception {
        long orderId = createOrder(0, -1, BigDecimal.ONE);
        String eventId = createMessage(orderId, -1);
        assertEquals(1, dispatcher.dispatchOnce());
        assertEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());

        // 模拟「Broker 已确认、进程在更新 SENT 前崩溃」：记录仍是 SENDING 且租约过期。
        execute("UPDATE oms_order_message SET status = 'SENDING', "
                + "lease_until = DATE_SUB(NOW(), INTERVAL 1 MINUTE), attempt_version = 1");
        assertEquals(1, dispatcher.dispatchOnce("after-crash"));
        OmsOrderMessage resent = messageOf(eventId);
        assertEquals(2L, resent.getAttemptVersion());
        assertEquals(OmsOrderMessage.STATUS_SENT, resent.getStatus());
        assertEquals(2, fault.calls.get(), "允许重复投递，由消费端幂等处理");
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
