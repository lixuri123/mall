package com.macro.mall.portal.it;

import com.macro.mall.portal.MallPortalApplication;
import com.macro.mall.portal.component.CancelOrderSender;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PublishOutcome;
import com.macro.mall.portal.domain.QueueEnum;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B 阶段：真实 RabbitMQ 4.2（mall-local 容器）端到端故障注入测试。
 *
 * <p>本类启动真实监听器与手动 ACK，验证：Publisher Confirm / Return、事务提交后 ACK、
 * 重复投递、提前投递、毒消息、受控重试与死信、消息遗漏兜底、真实 Broker 停机恢复。</p>
 */
@SpringBootTest(classes = MallPortalApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://127.0.0.1:3307/mall_it_b?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true",
        "spring.datasource.username=root",
        "spring.datasource.password=root",
        "spring.rabbitmq.listener.simple.auto-startup=true",
        "spring.rabbitmq.listener.direct.auto-startup=true",
        "spring.rabbitmq.listener.simple.acknowledge-mode=manual",
        "spring.rabbitmq.listener.simple.default-requeue-rejected=false",
        "spring.rabbitmq.publisher-confirm-type=correlated",
        "spring.rabbitmq.connection-timeout=2000",
        "spring.rabbitmq.publisher-returns=true",
        "spring.rabbitmq.template.mandatory=true",
        "mall.order.outbox.dispatch.enabled=true",
        "mall.order.outbox.dispatch.initial-delay-ms=3600000",
        "mall.order.outbox.dispatch.interval-ms=3600000",
        "mall.order.outbox.dispatch.lease-seconds=1",
        "mall.order.outbox.dispatch.confirm-timeout-ms=3000",
        "mall.order.outbox.dispatch.max-retry=5",
        "mall.order.outbox.consume.enabled=true",
        "mall.order.outbox.consume.max-retry=2",
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
        "logging.level.root=warn",
        "logging.level.com.macro.mall=warn"
})
class OrderMessageEndToEndRabbitTest extends AbstractOrderMessageIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("100.00");

    @Autowired
    private CancelOrderSender realSender;
    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    @Test
    void publisherConfirmAndReturnAreDistinguished() throws Exception {
        long sku = createSku(10, 1);
        long orderId = createOrder(0, -1, AMOUNT);
        addOrderItem(orderId, sku, 1);
        OrderCancelMessage message = objectMapper.readValue(payload(orderId), OrderCancelMessage.class);

        PublishOutcome ok = realSender.publish(message, 5000L);
        assertEquals(PublishOutcome.Status.CONFIRMED, ok.getStatus(),
                "正确路由键应收到 Confirm ACK 且没有 Return");

        // 临时改成不存在的 routing key：Broker 仍会 ACK，但 mandatory 会触发 basic.return。
        String original = (String) ReflectionTestUtils.getField(realSender, "routingKey");
        try {
            ReflectionTestUtils.setField(realSender, "routingKey", "no.such.route.for.test");
            PublishOutcome returned = realSender.publish(message, 5000L);
            assertEquals(PublishOutcome.Status.UNROUTABLE, returned.getStatus(),
                    "不可路由消息不能仅凭 Confirm ACK 标记为成功");
        } finally {
            ReflectionTestUtils.setField(realSender, "routingKey", original);
        }
        purgeQueues();
    }

    @Test
    void consumerCancelsExpiredOrderAfterTransactionCommit() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        createMessage(orderId, -5);
        publishToMainQueue(payload(orderId));

        assertTrue(awaitUntil(() -> statusOf(orderId) == 4, 10_000L), "消费者应在超时前完成取消");
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 5_000L), "事务提交后必须 ACK");
        assertEquals(0, lockOf(sku));
        assertEquals(10, stockOf(sku));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
    }

    @Test
    void duplicateDeliveryReleasesStockOnlyOnce() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        createMessage(orderId, -5);
        String json = payload(orderId);
        publishToMainQueue(json);
        publishToMainQueue(json);
        publishToMainQueue(json);

        assertTrue(awaitUntil(() -> statusOf(orderId) == 4, 10_000L));
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 5_000L));
        System.out.printf("[B-DUPLICATE] orderId=%d, deliveries=3, finalStatus=%d, "
                        + "stock=%d, lock=%d, consumeFailures=%d%n",
                orderId, statusOf(orderId), stockOf(sku), lockOf(sku),
                queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
        assertEquals(0, lockOf(sku), "重复投递不能重复释放库存");
        assertEquals(10, stockOf(sku));
        assertEquals(4, statusOf(orderId));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
    }

    @Test
    void commitBeforeAckCrashIsIdempotentOnRedelivery() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        createMessage(orderId, -5);
        // 模拟数据库事务已提交、ACK 之前进程退出：业务先完成，消息随后到达。
        portalOrderService.cancelBySystem(orderId);
        assertEquals(4, statusOf(orderId));
        publishToMainQueue(payload(orderId));

        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L));
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
        assertEquals(10, stockOf(sku));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
    }

    @Test
    void earlyDeliveryReschedulesToExpireTimeAndScanCoversOmission() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 30, AMOUNT); // 未到期
        addOrderItem(orderId, sku, 3);
        String eventId = createMessage(orderId, 30);
        execute("UPDATE oms_order_message SET status = 'SENT' WHERE event_id = '" + eventId + "'");
        publishToMainQueue(payload(orderId));

        assertTrue(awaitUntil(() -> OmsOrderMessage.STATUS_PENDING.equals(messageOf(eventId).getStatus()), 10_000L),
                "提前到达的消息不能 ACK 后丢弃，必须按订单到期时间重新排队");
        assertEquals(0, statusOf(orderId), "未到期不能取消");
        assertEquals(3, lockOf(sku));
        Date expected = queryDate("SELECT expire_time FROM oms_order WHERE id = " + orderId);
        assertEquals(expected.getTime() / 1000, messageOf(eventId).getDueTime().getTime() / 1000);

        // 把到期时间推进到过去，模拟时间流逝；不发布任何 MQ 消息，仅靠超时扫描兜底。
        executeUpdate("UPDATE oms_order SET expire_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE) WHERE id = ?", orderId);
        assertTrue(portalOrderService.cancelTimeOutOrder() >= 1);
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void paidOrderIsNotCancelledByLateCancelMessage() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        createMessage(orderId, -5);
        orderStateTransitionService.paySuccess(PayCommand.internal(orderId, 1));
        assertEquals(1, statusOf(orderId));
        assertEquals(7, stockOf(sku));

        publishToMainQueue(payload(orderId));
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L));
        assertEquals(1, statusOf(orderId), "支付胜出后取消消息不能改变订单状态");
        assertEquals(7, stockOf(sku));
        assertEquals(0, lockOf(sku));
    }

    @Test
    void poisonMessageGoesToDeadLetterWithFailureRecord() throws Exception {
        publishToMainQueue("{ this is not json");
        assertTrue(awaitUntil(() -> queueDepth(DLQ) == 1, 10_000L), "毒消息必须进入死信队列");
        assertTrue(awaitUntil(() -> queryIntSilently("SELECT COUNT(*) FROM oms_order_consume_failure") == 1,
                10_000L), "毒消息必须有失败记录");
        System.out.printf("[B-POISON] dlqDepth=%d, mainDepth=%d, retryDepth=%d, failureRows=%d%n",
                queueDepth(DLQ), queueDepth(MAIN_QUEUE), queueDepth(RETRY_QUEUE),
                queryIntSilently("SELECT COUNT(*) FROM oms_order_consume_failure"));
        assertEquals(0, queueDepth(MAIN_QUEUE));
        assertEquals(0, queueDepth(RETRY_QUEUE), "毒消息不能进入无限重试");
    }

    @Test
    void consumerBusinessFailureUsesBoundedRetryThenDeadLetter() throws Exception {
        long badSku = createSku(10, 0); // 订单声明锁定 3，实际 lock_stock=0 → 释放失败
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, badSku, 3);
        createMessage(orderId, -5);
        publishToMainQueue(payload(orderId));

        assertTrue(awaitUntil(() -> queryIntSilently("SELECT COUNT(*) FROM oms_order_consume_failure") == 1,
                20_000L), "超过消费重试上限后必须留下失败记录");
        assertEquals(0, statusOf(orderId), "消费事务失败时订单状态必须回滚");
        assertEquals(0, lockOf(badSku));
        assertEquals(10, stockOf(badSku));
        assertTrue(awaitUntil(() -> queueDepth(DLQ) == 1, 10_000L), "超限消息应进入死信队列");
    }

    @Test
    void realBrokerOutage_messageStaysPendingAndIsSentAfterRecovery() throws Exception {
        long sku = createSku(10, 1);
        long orderId = createOrder(0, -1, AMOUNT);
        addOrderItem(orderId, sku, 1);
        String eventId = createMessage(orderId, -1);
        purgeQueues();
        try {
            assertEquals(0, dockerCompose("stop", "rabbitmq"));
            // 发布阶段失败，但订单与消息记录仍在数据库中，状态回落为可重试。
            dispatcher.dispatchOnce("outage-instance");
            OmsOrderMessage during = messageOf(eventId);
            assertEquals(OmsOrderMessage.STATUS_PENDING, during.getStatus());
            assertEquals(1, during.getRetryCount());
            assertEquals(0, statusOf(orderId));
        } finally {
            dockerCompose("start", "rabbitmq");
            assertTrue(awaitUntil(this::rabbitHealthy, 60_000L), "RabbitMQ 必须恢复");
        }
        // 监听容器的连接恢复有退避，等待消费者真正重新可用后再派发。
        assertTrue(awaitUntil(this::consumerContainersRunning, 30_000L), "监听容器必须恢复");
        Thread.sleep(1000L);
        execute("UPDATE oms_order_message SET next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");
        assertTrue(awaitUntil(() -> dispatcher.dispatchOnce("recovery-instance") == 1, 30_000L),
                "恢复后应能成功发布");
        assertEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 15_000L), "恢复后消费者应处理完消息");
        assertTrue(awaitUntil(() -> statusOf(orderId) == 4, 30_000L),
                "未支付订单最终应被取消（main=" + queueDepth(MAIN_QUEUE)
                        + ", retry=" + queueDepth(RETRY_QUEUE) + ", dlq=" + queueDepth(DLQ) + "）");
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku), "停机恢复后库存只释放一次");
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
        System.out.printf("[B-REAL-OUTAGE] orderId=%d, eventId=%s, finalStatus=%d, stock=%d, lock=%d, "
                        + "messageStatus=%s, retryCount=%d, dlqDepth=%d%n",
                orderId, eventId, statusOf(orderId), stockOf(sku), lockOf(sku),
                messageOf(eventId).getStatus(), messageOf(eventId).getRetryCount(), queueDepth(DLQ));
        purgeQueues();
    }

    private boolean consumerContainersRunning() {
        return listenerRegistry.getListenerContainers().stream()
                .allMatch(org.springframework.amqp.rabbit.listener.MessageListenerContainer::isRunning);
    }

    private int queryIntSilently(String sql) {
        try {
            return queryInt(sql);
        } catch (Exception e) {
            return -1;
        }
    }

    private int dockerCompose(String... args) throws Exception {
        String[] command = new String[args.length + 4];
        command[0] = "docker";
        command[1] = "compose";
        command[2] = "-f";
        command[3] = "document/docker/docker-compose-local.yml";
        System.arraycopy(args, 0, command, 4, args.length);
        ProcessBuilder pb = new ProcessBuilder(command)
                .directory(new java.io.File("D:/agent/mall"))
                .redirectErrorStream(true);
        Process process = pb.start();
        try (var in = process.getInputStream()) {
            in.readAllBytes();
        }
        return process.waitFor();
    }

    private boolean rabbitHealthy() {
        try {
            HttpURLConnection conn = (HttpURLConnection)
                    new URL("http://127.0.0.1:15672/api/overview").openConnection();
            conn.setConnectTimeout(1500);
            conn.setReadTimeout(1500);
            String auth = Base64.getEncoder().encodeToString("mall:mall".getBytes(StandardCharsets.UTF_8));
            conn.setRequestProperty("Authorization", "Basic " + auth);
            return conn.getResponseCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}
