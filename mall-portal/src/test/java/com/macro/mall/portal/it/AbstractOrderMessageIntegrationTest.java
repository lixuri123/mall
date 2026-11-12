package com.macro.mall.portal.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.mapper.OmsOrderItemMapper;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.mapper.PmsSkuStockMapper;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.model.PmsSkuStock;
import com.macro.mall.portal.dao.OmsOrderMessageDao;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.component.OrderCancelPublisher;
import com.macro.mall.portal.component.OmsOrderMessageDispatcher;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.QueueEnum;
import com.macro.mall.portal.service.OmsOrderMessageService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.OrderStateTransitionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * B 阶段集成测试基类：真实 MySQL 8.4 + 真实 RabbitMQ 4.2（mall-local 容器）。
 *
 * <p>测试库为独立的 {@code mall_it_b}，由 {@code document/sql/mall.sql} 初始化，
 * 不使用内存数据库或内嵌 Broker。具体 {@code @SpringBootTest} 属性由子类给出，
 * 以便按测试类控制消费者是否启动。</p>
 */
public abstract class AbstractOrderMessageIntegrationTest {

    protected static final long TEST_MEMBER_ID = 1L;
    protected static final String MAIN_QUEUE = QueueEnum.QUEUE_ORDER_CANCEL.getName();
    protected static final String RETRY_QUEUE = QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getName();
    protected static final String DLQ = QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getName();

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
    @Autowired protected RabbitTemplate rabbitTemplate;
    @Autowired protected ObjectMapper objectMapper;

    private final List<Long> createdSkuIds = new ArrayList<>();
    private final List<Long> createdOrderIds = new ArrayList<>();

    @BeforeEach
    void cleanEnvironment() throws SQLException {
        execute("DELETE FROM oms_order_consume_failure");
        execute("DELETE FROM oms_order_message");
        execute("DELETE FROM oms_order_pay_anomaly");
        execute("DELETE FROM oms_order_item WHERE order_id IN "
                + "(SELECT id FROM (SELECT id FROM oms_order WHERE member_username = 'it-b') t)");
        execute("DELETE FROM oms_order WHERE member_username = 'it-b'");
        purgeQueues();
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
        // 消费与重试是异步的：收尾也清空队列，避免上一条测试的残留消息在下一次
        // Broker 重启后被死信转发回主队列，干扰后续断言。
        purgeQueues();
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
        rabbitTemplate.send(QueueEnum.QUEUE_ORDER_CANCEL.getExchange(),
                QueueEnum.QUEUE_ORDER_CANCEL.getRouteKey(),
                MessageBuilder.withBody(json.getBytes(StandardCharsets.UTF_8))
                        .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                        .setContentEncoding("UTF-8")
                        .build());
    }

    protected long queueDepth(String queue) {
        Long result = rabbitTemplate.execute(channel -> channel.messageCount(queue));
        return result == null ? 0L : result;
    }

    protected void purgeQueues() {
        rabbitTemplate.execute(channel -> {
            channel.queuePurge(MAIN_QUEUE);
            channel.queuePurge(RETRY_QUEUE);
            channel.queuePurge(DLQ);
            return null;
        });
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
