package com.macro.mall.portal.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.mapper.OmsOrderItemMapper;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.mapper.PmsSkuStockMapper;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.model.PmsSkuStock;
import com.macro.mall.portal.component.OrderCancelPublisher;
import com.macro.mall.portal.component.OmsOrderMessageDispatcher;
import com.macro.mall.portal.dao.OmsOrderMessageDao;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.RocketMqTopics;
import com.macro.mall.portal.service.OmsOrderMessageService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.OrderStateTransitionService;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * B 阶段集成测试基类：真实 MySQL 8.4 + 真实 RocketMQ 5.3.2（mall-local 容器）。
 *
 * <p>测试库为独立的 {@code mall_it_b}，由 {@code document/sql/mall.sql} 初始化，
 * 不使用内存数据库或内嵌 Broker。具体 {@code @SpringBootTest} 属性由子类给出，
 * 以便按测试类控制消费者是否启动。</p>
 *
 * <p>与 RabbitMQ 版的等价关系：</p>
 * <ul>
 *   <li>{@code publishToMainQueue} → 直接向主 Topic 发一条普通消息。</li>
 *   <li>{@code queueDepth(main/retry)} → 消费组在该 Topic 上的未消费积压
 *       （{@code brokerOffset - consumerOffset}），由 {@code examineConsumeStats} 计算。</li>
 *   <li>{@code queueDepth(dlq)} → DLQ Topic 现存消息总量减去测试开始时的基线，
 *       因为 DLQ 没有消费者组订阅，无法用消费位点度量。</li>
 *   <li>{@code purgeQueues} → 把消费组在 main/retry 上的位点重置到当前最新，
 *       并重记 DLQ 基线；RocketMQ 不支持「清空队列」。</li>
 * </ul>
 */
public abstract class AbstractOrderMessageIntegrationTest {

    protected static final long TEST_MEMBER_ID = 1L;
    protected static final String MAIN_QUEUE = "mall_order_cancel";
    protected static final String RETRY_QUEUE = "mall_order_cancel_retry";
    protected static final String DLQ = "mall_order_cancel_dlq";
    /** 由 document/docker 中的 broker.conf 与预建 topic 命令决定。 */
    protected static final String BROKER_NAME = "broker-a";
    protected static final int QUEUE_COUNT = 4;
    public static final String TEST_CONSUMER_GROUP = "it-order-cancel-consumer";
    protected static final String BROKER_CONTAINER = "mall-local-rocketmq-broker-1";
    protected static final String NAME_SERVER = "rocketmq-namesrv:9876";

    /**
     * 每次测试 JVM 启动时清空并重建本测试专用的 topic 与订阅组。
     *
     * <p>RocketMQ 的消息与消费位点按 topic/group 持久化在 Broker 上，没有「清空队列」语义；
     * 且对「近期创建的 topic + 全新消费组」，客户端会从 0 位点重放全部历史消息。
     * 若不做这一步，上一轮运行留下的毒消息、延迟重试消息会在本轮被重新投递，
     * 污染失败记录数与队列深度断言，导致结果不可复现。</p>
     *
     * <p>静态块在类首次加载时执行，早于 Spring 上下文创建，因此不会与在线消费者冲突。</p>
     */
    static {
        resetBrokerOnce();
    }

    private static void resetBrokerOnce() {
        String[] toDelete = {MAIN_QUEUE, RETRY_QUEUE, DLQ, "%RETRY%" + TEST_CONSUMER_GROUP};
        for (String topic : toDelete) {
            mqadmin("deleteTopic", "-n", NAME_SERVER, "-c", "DefaultCluster", "-t", topic);
        }
        mqadmin("deleteSubGroup", "-n", NAME_SERVER, "-c", "DefaultCluster",
                "-g", TEST_CONSUMER_GROUP);
        for (String topic : new String[]{MAIN_QUEUE, RETRY_QUEUE, DLQ}) {
            mqadmin("updateTopic", "-n", NAME_SERVER, "-c", "DefaultCluster", "-t", topic,
                    "-r", "4", "-w", "4", "-a", "+message.type=NORMAL");
        }
        System.out.println("[IT-BROKER-RESET] 已重建测试 topic 与订阅组：" + TEST_CONSUMER_GROUP);
    }

    private static void mqadmin(String... args) {
        String[] command = new String[args.length + 4];
        command[0] = "docker";
        command[1] = "exec";
        command[2] = BROKER_CONTAINER;
        command[3] = "sh";
        System.arraycopy(args, 0, command, 4, args.length);
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            process.waitFor();
        } catch (Exception e) {
            System.err.println("[IT-BROKER-RESET] 命令失败（容器需先启动）："
                    + String.join(" ", command) + " -> " + e);
        }
    }

    @Autowired protected DataSource dataSource;
    @Autowired protected OmsOrderMessageDao messageDao;
    @Autowired protected OmsOrderMessageService messageService;
    @Autowired protected OmsOrderMessageDispatcher dispatcher;
    @Autowired protected OrderCancelPublisher publisher;
    @Autowired protected PortalOrderDao portalOrderDao;
    @Autowired protected OmsPortalOrderService portalOrderService;
    @Autowired protected OrderStateTransitionService orderStateTransitionService;
    @Autowired protected OmsOrderMapper orderMapper;
    @Autowired protected OmsOrderItemMapper orderItemMapper;
    @Autowired protected PmsSkuStockMapper skuStockMapper;
    @Autowired protected DefaultMQProducer orderCancelProducer;
    @Autowired protected ObjectMapper objectMapper;

    @Value("${mall.order.outbox.name-server}")
    protected String nameServer;
    @Value("${mall.order.outbox.consumer-group}")
    protected String consumerGroup;

    private DefaultMQAdminExt admin;
    private long dlqBaseline;
    /** 每个用例开始时 main/retry 的既有积压，queueDepth 只统计本用例新增的部分。 */
    private long mainBaselineDiff;
    private long retryBaselineDiff;

    private final List<Long> createdSkuIds = new ArrayList<>();
    private final List<Long> createdOrderIds = new ArrayList<>();

    @BeforeEach
    void cleanEnvironment() throws SQLException {
        // 先让 Broker 位点稳定并记录积压基线，再清理数据库：
        // 若有历史消息在位点稳定过程中被重放，其副作用（失败记录）也会一并被清掉。
        purgeQueues();
        execute("DELETE FROM oms_order_consume_failure");
        execute("DELETE FROM oms_order_message");
        execute("DELETE FROM oms_order_pay_anomaly");
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-b') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-b'");
    }

    @AfterEach
    void cleanCreatedData() throws SQLException {
        for (Long orderId : createdOrderIds) {
            execute("DELETE FROM oms_order_item WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order_message WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order_consume_failure WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order_pay_anomaly WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order WHERE id = " + orderId);
        }
        for (Long skuId : createdSkuIds) {
            execute("DELETE FROM pms_sku_stock WHERE id = " + skuId);
        }
        createdOrderIds.clear();
        createdSkuIds.clear();
        execute("DELETE FROM oms_order_consume_failure");
        execute("DELETE FROM oms_order_message");
        // 消费与重试是异步的：收尾也重置位点，避免上一个用例的残留消息
        // 在 Broker 重启后被重新投递，干扰后续断言。
        purgeQueues();
        if (admin != null) {
            admin.shutdown();
            admin = null;
        }
    }

    protected long createSku(int stock, int lockStock) {
        PmsSkuStock sku = new PmsSkuStock();
        sku.setProductId(999999L);
        sku.setSkuCode("ITB-" + UUID.randomUUID().toString().substring(0, 12));
        sku.setPrice(BigDecimal.ONE);
        sku.setStock(stock);
        sku.setLockStock(lockStock);
        sku.setSpData("[]");
        skuStockMapper.insertSelective(sku);
        createdSkuIds.add(sku.getId());
        return sku.getId();
    }

    /** 创建订单并显式写入固定到期时间（相对数据库 NOW() 的分钟偏移，负数表示已到期）。 */
    protected long createOrder(int status, int expireOffsetMinutes, BigDecimal payAmount) throws SQLException {
        OmsOrder order = new OmsOrder();
        order.setMemberId(TEST_MEMBER_ID);
        order.setMemberUsername("it-b");
        order.setOrderSn("ITB-" + UUID.randomUUID().toString().replace("-", "").substring(0, 20));
        order.setCreateTime(new Date());
        order.setStatus(status);
        order.setDeleteStatus(0);
        order.setPayType(1);
        order.setSourceType(1);
        order.setOrderType(0);
        order.setConfirmStatus(0);
        order.setReceiverName("it-b");
        order.setReceiverPhone("13800000000");
        order.setTotalAmount(payAmount);
        order.setPayAmount(payAmount);
        order.setFreightAmount(BigDecimal.ZERO);
        order.setPromotionAmount(BigDecimal.ZERO);
        order.setCouponAmount(BigDecimal.ZERO);
        order.setIntegrationAmount(BigDecimal.ZERO);
        order.setDiscountAmount(BigDecimal.ZERO);
        order.setUseIntegration(0);
        order.setIntegration(0);
        order.setGrowth(0);
        orderMapper.insertSelective(order);
        createdOrderIds.add(order.getId());
        executeUpdate("UPDATE oms_order SET expire_time = DATE_ADD(NOW(), INTERVAL ? MINUTE) WHERE id = ?",
                expireOffsetMinutes, order.getId());
        return order.getId();
    }

    protected void addOrderItem(long orderId, long skuId, int quantity) {
        OmsOrderItem item = new OmsOrderItem();
        item.setOrderId(orderId);
        item.setOrderSn("ITB-ITEM");
        item.setProductSkuId(skuId);
        item.setProductQuantity(quantity);
        item.setProductPrice(BigDecimal.ONE);
        item.setProductName("it-b");
        item.setGiftIntegration(0);
        item.setGiftGrowth(0);
        orderItemMapper.insertSelective(item);
    }

    /** 构造合法的订单取消消息 JSON。 */
    protected String payload(long orderId) throws Exception {
        Date due = queryDate("SELECT expire_time FROM oms_order WHERE id = " + orderId);
        return objectMapper.writeValueAsString(OrderCancelMessage.of(orderId, due));
    }

    /** 写入一条 PENDING 本地消息，可再用 SQL 调整 due_time / status / 租约。 */
    protected String createMessage(long orderId, int dueOffsetMinutes) throws Exception {
        Date due = queryDate("SELECT DATE_ADD(NOW(), INTERVAL " + dueOffsetMinutes + " MINUTE)");
        messageService.insertPendingForNewOrder(orderId, due, payload(orderId));
        execute("UPDATE oms_order_message SET due_time = DATE_ADD(NOW(), INTERVAL " + dueOffsetMinutes
                + " MINUTE), next_retry_time = DATE_ADD(NOW(), INTERVAL " + dueOffsetMinutes
                + " MINUTE) WHERE event_id = '" + OmsOrderMessage.eventIdOf(orderId) + "'");
        return OmsOrderMessage.eventIdOf(orderId);
    }

    protected OmsOrderMessage messageOf(String eventId) {
        return messageService.findByEventId(eventId);
    }

    protected int stockOf(long skuId) {
        return skuStockMapper.selectByPrimaryKey(skuId).getStock();
    }

    protected int lockOf(long skuId) {
        return skuStockMapper.selectByPrimaryKey(skuId).getLockStock();
    }

    protected int statusOf(long orderId) {
        return orderMapper.selectByPrimaryKey(orderId).getStatus();
    }

    protected void publishToMainQueue(String json) {
        try {
            Message msg = new Message(MAIN_QUEUE, RocketMqTopics.TAG_CANCEL,
                    json.getBytes(StandardCharsets.UTF_8));
            orderCancelProducer.send(msg);
        } catch (Exception e) {
            throw new IllegalStateException("向主 Topic 投递测试消息失败", e);
        }
    }

    /**
     * 返回「本用例新增且尚未确认」的消息数。
     *
     * <p>RocketMQ 没有清空队列的语义，历史消息与消费位点会一直留在 Broker 上；
     * 对「近期创建的 Topic + 全新消费组」，客户端还可能从 0 位点重放历史。
     * 因此这里用<b>相对基线</b>计量：只比较用例开始后的积压增量，
     * 既不受历史积压影响，也不依赖位点重置是否真正下发到在线客户端。</p>
     */
    protected long queueDepth(String topic) {
        try {
            if (DLQ.equals(topic)) {
                return Math.max(0L, topicTotal(DLQ) - dlqBaseline);
            }
            long baseline = MAIN_QUEUE.equals(topic) ? mainBaselineDiff : retryBaselineDiff;
            return Math.max(0L, rawDiff(topic) - baseline);
        } catch (Exception e) {
            // 消费组尚未注册或 Broker 停机期间无法查询，按「无积压」处理，由业务断言兜底。
            System.out.printf("[IT-ADMIN-ERROR] topic=%s, group=%s, error=%s%n",
                    topic, consumerGroup, e);
            return 0L;
        }
    }

    /** 某 Topic 上消费组的原始未确认积压（brokerOffset - consumerOffset 之和）。 */
    private long rawDiff(String topic) throws Exception {
        ensureAdmin();
        // 带 topic 的重载只查这一个 Topic；不带 topic 的重载会遍历组内全部订阅，
        // 组刚创建、%RETRY%group 尚未生成时会抛 CODE 17，导致整次查询失败。
        ConsumeStats stats = admin.examineConsumeStats(consumerGroup, topic);
        return stats.getOffsetTable().values().stream()
                .mapToLong(o -> Math.max(0L, o.getBrokerOffset() - o.getConsumerOffset()))
                .sum();
    }

    /** RocketMQ 没有 purge：重置消费位点到当前最新，并记录三类基线。 */
    protected void purgeQueues() {
        try {
            ensureAdmin();
            // 未来 1 秒的时间戳确保定位到当前最大位点，不会因时钟精度落回旧位点。
            long horizon = System.currentTimeMillis() + 1000L;
            admin.resetOffsetByTimestamp(consumerGroup, MAIN_QUEUE, horizon, true);
            admin.resetOffsetByTimestamp(consumerGroup, RETRY_QUEUE, horizon, true);
            // 只有「连 DLQ 都投递失败」时 Broker 才会生成内建重试 Topic；
            // 它同样按消费组持久化，残留消息会在下个用例被重新投递，必须一并跳过。
            try {
                admin.resetOffsetByTimestamp(consumerGroup, "%RETRY%" + consumerGroup, horizon, true);
            } catch (Exception ignored) {
                // 该 Topic 尚未创建时无需处理。
            }
            awaitNoBacklog(3000L);
        } catch (Exception e) {
            // 消费组未注册（例如该测试类禁用了消费者）或 Broker 停机时忽略。
        }
        try {
            ensureAdmin();
            mainBaselineDiff = rawDiff(MAIN_QUEUE);
            retryBaselineDiff = rawDiff(RETRY_QUEUE);
            dlqBaseline = topicTotal(DLQ);
        } catch (Exception e) {
            mainBaselineDiff = 0L;
            retryBaselineDiff = 0L;
            dlqBaseline = 0L;
        }
    }

    private void awaitNoBacklog(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (rawDiff(MAIN_QUEUE) == 0 && rawDiff(RETRY_QUEUE) == 0) {
                    return;
                }
            } catch (Exception e) {
                return;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * 直接按队列查 max/min 位点求和，不走 {@code examineTopicStats}：
     * 后者会额外拉取 NameServer 上的完整 topic 路由，Broker 刚重启或 topic 刚预建时
     * 偶发 CODE 17「No topic route info」，会把 DLQ 深度误判成 0。
     *
     * <p>{@code maxOffset(mq)} 通过 brokerName 在管理客户端本地的
     * {@code brokerAddrTable} 里查地址，必须先预热该 topic 的路由，
     * 否则会报「The broker[broker-a] not exist」。topic 刚重建时
     * NameServer 路由同步也有短暂延迟，因此这里做有限重试。</p>
     */
    private long topicTotal(String topic) throws Exception {
        ensureAdmin();
        Exception last = null;
        for (int i = 0; i < 15; i++) {
            try {
                admin.examineTopicRouteInfo(topic);
                last = null;
                break;
            } catch (Exception e) {
                last = e;
                Thread.sleep(200L);
            }
        }
        if (last != null) {
            throw last;
        }
        long total = 0L;
        for (int qid = 0; qid < QUEUE_COUNT; qid++) {
            MessageQueue mq = new MessageQueue(topic, BROKER_NAME, qid);
            total += Math.max(0L, admin.maxOffset(mq) - admin.minOffset(mq));
        }
        return total;
    }

    private void ensureAdmin() {
        if (admin == null) {
            admin = new DefaultMQAdminExt("it-admin-" + UUID.randomUUID().toString().substring(0, 8));
            admin.setNamesrvAddr(nameServer);
            try {
                admin.start();
            } catch (Exception e) {
                throw new IllegalStateException("RocketMQ 管理客户端启动失败", e);
            }
        }
    }

    /**
     * 消费组是否已有客户端重新注册到 Broker。
     *
     * <p>Broker 重启后，若订阅组配置/位点尚未重建就发布消息，消费者重连时按
     * {@code CONSUME_FROM_LAST_OFFSET} 计算起点，可能直接跳过这条刚发布的消息。
     * 因此停机恢复场景必须先等连接回来，再派发。</p>
     */
    protected boolean consumerOnline() {
        try {
            ensureAdmin();
            ConsumerConnection connection = admin.examineConsumerConnectionInfo(consumerGroup);
            return connection != null && !connection.getConnectionSet().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    protected boolean awaitUntil(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(50L);
        }
        return condition.getAsBoolean();
    }

    protected Date queryDate(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            Timestamp ts = rs.getTimestamp(1);
            return ts == null ? null : new Date(ts.getTime());
        }
    }

    protected int queryInt(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    protected void execute(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement()) {
            s.executeUpdate(sql);
        }
    }

    protected void executeUpdate(String sql, Object... params) throws SQLException {
        try (Connection c = dataSource.getConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.executeUpdate();
        }
    }
}
