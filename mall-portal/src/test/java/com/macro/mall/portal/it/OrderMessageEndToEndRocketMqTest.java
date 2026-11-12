package com.macro.mall.portal.it;

import com.macro.mall.portal.MallPortalApplication;
import com.macro.mall.portal.component.CancelOrderSender;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PublishOutcome;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * B 阶段：真实 RocketMQ 5.3.2（mall-local 容器）端到端故障注入测试。
 *
 * <p>本类启动真实 PushConsumer，验证：同步发送 SEND_OK / No route info、
 * 事务提交后才确认、重复投递、提前投递、毒消息、受控重试与死信、消息遗漏兜底、
 * 真实 Broker 停机恢复。</p>
 */
/**
 * 显式排序：真实 Broker 停机恢复用例必须最后执行。它会 stop/start Broker，
 * 期间订阅组与消费位点会重建，若排在其它用例之前，会让后续用例的消息被
 * 按 CONSUME_FROM_LAST_OFFSET 跳过。未标注 @Order 的方法默认是
 * {@code Integer.MAX_VALUE / 2}，因此 {@code Integer.MAX_VALUE} 一定排最后。
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(classes = MallPortalApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://127.0.0.1:3307/mall_it_b?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true",
        "spring.datasource.username=root",
        "spring.datasource.password=root",
        "mall.order.outbox.dispatch.enabled=true",
        "mall.order.outbox.dispatch.initial-delay-ms=3600000",
        "mall.order.outbox.dispatch.interval-ms=3600000",
        "mall.order.outbox.dispatch.lease-seconds=1",
        "mall.order.outbox.dispatch.confirm-timeout-ms=3000",
        "mall.order.outbox.dispatch.max-retry=5",
        "mall.order.outbox.consume.enabled=true",
        "mall.order.outbox.consume.max-retry=2",
        // 测试用 1s 固定延迟级别（level 1）：仍验证「固定短延迟重试」机制，
        // 但让重试链在用例内收敛，不把未落地的定时消息留给下一个用例。
        "mall.order.outbox.consume.retry-delay-level=1",
        // 固定测试消费组：RocketMQ 对「新建 Topic + 全新组」会从 0 位点重放历史消息，
        // 每次运行换组名反而会把历史消息重新投递一遍。固定组让位点持久，
        // 由基类 purgeQueues() 把位点推到最新并记录积压基线。
        "mall.order.outbox.consumer-group=" + AbstractOrderMessageIntegrationTest.TEST_CONSUMER_GROUP,
        "mall.order.timeout-scan.enabled=false",
        "mall.order.outbox.metrics.enabled=false",
        "logging.level.root=warn",
        "logging.level.com.macro.mall=warn"
})
class OrderMessageEndToEndRocketMqTest extends AbstractOrderMessageIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("100.00");

    @Autowired
    private CancelOrderSender realSender;

    @Test
    void publisherConfirmAndUnroutableAreDistinguished() throws Exception {
        long sku = createSku(10, 1);
        long orderId = createOrder(0, -1, AMOUNT);
        addOrderItem(orderId, sku, 1);
        OrderCancelMessage message = objectMapper.readValue(payload(orderId), OrderCancelMessage.class);

        PublishOutcome ok = realSender.publish(message, 5000L);
        assertEquals(PublishOutcome.Status.CONFIRMED, ok.getStatus(),
                "同步发送到已预建 Topic 应返回 SEND_OK（Broker 同步刷盘）");

        // 临时改成未预建的 Topic：Broker 关闭了 autoCreateTopicEnable，
        // 发送会抛 No route info of this topic，等价于此前的 mandatory Return 不可路由语义。
        String original = (String) ReflectionTestUtils.getField(realSender, "topic");
        try {
            ReflectionTestUtils.setField(realSender, "topic", "mall_no_such_topic_it");
            PublishOutcome unroutable = realSender.publish(message, 5000L);
            assertEquals(PublishOutcome.Status.UNROUTABLE, unroutable.getStatus(),
                    "不可路由消息不能标记为发布成功");
        } finally {
            ReflectionTestUtils.setField(realSender, "topic", original);
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
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L), "事务提交后必须确认");
        assertEquals(0, lockOf(sku));
        assertEquals(10, stockOf(sku));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure WHERE order_id = "
                + orderId), "本订单不应产生消费失败记录");
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
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L));
        System.out.printf("[B-DUPLICATE] orderId=%d, deliveries=3, finalStatus=%d, "
                        + "stock=%d, lock=%d, consumeFailures=%d%n",
                orderId, statusOf(orderId), stockOf(sku), lockOf(sku),
                queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"));
        assertEquals(0, lockOf(sku), "重复投递不能重复释放库存");
        assertEquals(10, stockOf(sku));
        assertEquals(4, statusOf(orderId));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure WHERE order_id = "
                + orderId), "本订单不应产生消费失败记录");
    }

    @Test
    void commitBeforeAckCrashIsIdempotentOnRedelivery() throws Exception {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, -5, AMOUNT);
        addOrderItem(orderId, sku, 3);
        createMessage(orderId, -5);
        // 模拟数据库事务已提交、确认之前进程退出：业务先完成，消息随后到达。
        portalOrderService.cancelBySystem(orderId);
        assertEquals(4, statusOf(orderId));
        publishToMainQueue(payload(orderId));

        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L));
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku));
        assertEquals(10, stockOf(sku));
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure WHERE order_id = "
                + orderId), "本订单不应产生消费失败记录");
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
                "提前到达的消息不能确认后丢弃，必须按订单到期时间重新排队");
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
        assertTrue(awaitUntil(() -> queueDepth(DLQ) == 1, 10_000L), "毒消息必须进入死信 Topic");
        assertTrue(awaitUntil(() -> queryIntSilently("SELECT COUNT(*) FROM oms_order_consume_failure "
                + "WHERE reason LIKE 'POISON_MESSAGE%'") >= 1, 10_000L), "毒消息必须有失败记录");
        // RocketMQ 的消费位点默认每 5s 批量提交，处理完成与位点可见之间天然有延迟。
        assertTrue(awaitUntil(() -> queueDepth(MAIN_QUEUE) == 0, 10_000L),
                "毒消息处理后必须确认，不能留在主 Topic");
        System.out.printf("[B-POISON] dlqDepth=%d, mainDiff=%d, retryDiff=%d, failureRows=%d%n",
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

        assertTrue(awaitUntil(() -> queryIntSilently("SELECT COUNT(*) FROM oms_order_consume_failure "
                + "WHERE reason LIKE 'MAX_CONSUME_RETRY_EXCEEDED%'") >= 1, 30_000L),
                "超过消费重试上限后必须留下失败记录");
        assertEquals(0, statusOf(orderId), "消费事务失败时订单状态必须回滚");
        assertEquals(0, lockOf(badSku));
        assertEquals(10, stockOf(badSku));
        assertTrue(awaitUntil(() -> queueDepth(DLQ) == 1, 10_000L), "超限消息应进入死信 Topic");
    }

    @Test
    @Order(Integer.MAX_VALUE)
    void realBrokerOutage_messageStaysPendingAndIsSentAfterRecovery() throws Exception {
        long sku = createSku(10, 1);
        long orderId = createOrder(0, -1, AMOUNT);
        addOrderItem(orderId, sku, 1);
        String eventId = createMessage(orderId, -1);
        purgeQueues();
        try {
            assertEquals(0, dockerCompose("stop", "rocketmq-broker"));
            // 发布阶段失败，但订单与消息记录仍在数据库中，状态回落为可重试。
            dispatcher.dispatchOnce("outage-instance");
            OmsOrderMessage during = messageOf(eventId);
            assertEquals(OmsOrderMessage.STATUS_PENDING, during.getStatus());
            assertEquals(1, during.getRetryCount());
            assertEquals(0, statusOf(orderId));
        } finally {
            dockerCompose("start", "rocketmq-broker");
            assertTrue(awaitUntil(this::rocketmqHealthy, 60_000L), "RocketMQ Broker 必须恢复");
            // 必须先等消费者重新注册：Broker 重启后订阅组与位点会重建，
            // 若在重连完成前就发布，客户端按 CONSUME_FROM_LAST_OFFSET 计算起点会跳过消息。
            assertTrue(awaitUntil(this::consumerOnline, 60_000L), "消费者必须重新连上 Broker");
        }
        Thread.sleep(1000L);
        execute("UPDATE oms_order_message SET next_retry_time = DATE_SUB(NOW(), INTERVAL 1 MINUTE)");
        assertTrue(awaitUntil(() -> dispatcher.dispatchOnce("recovery-instance") == 1, 30_000L),
                "恢复后应能成功发布");
        assertEquals(OmsOrderMessage.STATUS_SENT, messageOf(eventId).getStatus());

        // Broker 重启后订阅组与消费位点会重建，客户端按 CONSUME_FROM_LAST_OFFSET
        // 计算起点时可能跳过这条刚发布的单条消息。这正是 B 阶段保留「定时扫描兜底」的原因：
        // 先给 MQ 消费链路一个优先窗口，未覆盖时由扫描补上，两条路径都要求业务幂等。
        boolean viaMessage = awaitUntil(() -> statusOf(orderId) == 4, 12_000L);
        if (!viaMessage) {
            assertTrue(portalOrderService.cancelTimeOutOrder() >= 1,
                    "MQ 消息被跳过时，超时扫描必须兜底取消订单");
        }
        assertTrue(awaitUntil(() -> statusOf(orderId) == 4, 10_000L), "未支付订单最终应被取消");
        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku), "停机恢复后库存只释放一次");
        // Broker 重启后位点回退会重放历史消息，这是 RocketMQ「至少一次投递」的正常表现。
        // 正确性由业务幂等保证，因此只断言本订单没有消费失败；全局计数作为可观测数据打印。
        assertEquals(0, queryInt("SELECT COUNT(*) FROM oms_order_consume_failure WHERE order_id = "
                + orderId), "本订单不应产生消费失败记录");
        System.out.printf("[B-REAL-OUTAGE] orderId=%d, eventId=%s, finalStatus=%d, stock=%d, lock=%d, "
                        + "messageStatus=%s, retryCount=%d, dlqDepth=%d, consumeFailures=%d, "
                        + "viaMessage=%s, scanFallback=%s%n",
                orderId, eventId, statusOf(orderId), stockOf(sku), lockOf(sku),
                messageOf(eventId).getStatus(), messageOf(eventId).getRetryCount(), queueDepth(DLQ),
                queryInt("SELECT COUNT(*) FROM oms_order_consume_failure"),
                viaMessage, !viaMessage);
        purgeQueues();
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

    private boolean rocketmqHealthy() {
        // Broker 停机时 10911 不可连；NameServer 仍在，所以不能用 9876 判断 Broker 存活。
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", 10911), 1500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
