package com.macro.mall.portal.it;

import com.macro.mall.mapper.OmsOrderItemMapper;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.mapper.PmsSkuStockMapper;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.model.PmsSkuStock;
import com.macro.mall.portal.MallPortalApplication;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.macro.mall.portal.service.OrderStateTransitionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * A 阶段集成测试基类：真实 Spring 上下文 + 真实 MySQL。
 *
 * <p>不用 H2 / 内存库，因为行锁、隔离级别、条件更新的影响行数与 MySQL 的行为
 * 直接相关。测试库为独立的 mall_it（由 document/sql/mall.sql 导入并执行
 * document/sql/mall_a_migration.sql），不触碰本机业务数据。</p>
 *
 * <p>RabbitMQ 监听容器与超时扫描定时任务在测试中关闭，避免后台任务干扰断言。</p>
 */
@SpringBootTest(classes = MallPortalApplication.class, properties = {
        "spring.datasource.url=jdbc:mysql://127.0.0.1:3307/mall_it?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false&allowPublicKeyRetrieval=true",
        "spring.datasource.username=root",
        "spring.datasource.password=root",
        "spring.rabbitmq.listener.simple.auto-startup=false",
        "spring.rabbitmq.listener.direct.auto-startup=false",
        "mall.order.timeout-scan.enabled=false",
        "logging.level.root=warn",
        "logging.level.com.macro.mall=warn"
})
public abstract class AbstractOrderIntegrationTest {

    protected static final long TEST_MEMBER_ID = 1L;

    @Autowired
    protected DataSource dataSource;
    @Autowired
    protected PortalOrderDao portalOrderDao;
    @Autowired
    protected OrderStateTransitionService orderStateTransitionService;
    @Autowired
    protected OmsPortalOrderService portalOrderService;
    @Autowired
    protected OmsOrderMapper orderMapper;
    @Autowired
    protected OmsOrderItemMapper orderItemMapper;
    @Autowired
    protected PmsSkuStockMapper skuStockMapper;

    private final List<Long> createdSkuIds = new ArrayList<>();
    private final List<Long> createdOrderIds = new ArrayList<>();

    @BeforeEach
    void cleanPendingDemoOrders() throws SQLException {
        // 种子数据里存在历史 status=0 的演示订单，会干扰超时扫描断言，统一清理。
        execute("DELETE FROM oms_order_item WHERE order_id IN (SELECT id FROM (SELECT id FROM oms_order WHERE status = 0) t)");
        execute("DELETE FROM oms_order WHERE status = 0");
        execute("DELETE FROM oms_order_pay_anomaly");
    }

    @AfterEach
    void cleanCreatedData() throws SQLException {
        for (Long orderId : createdOrderIds) {
            execute("DELETE FROM oms_order_item WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order_pay_anomaly WHERE order_id = " + orderId);
            execute("DELETE FROM oms_order WHERE id = " + orderId);
        }
        for (Long skuId : createdSkuIds) {
            execute("DELETE FROM pms_sku_stock WHERE id = " + skuId);
        }
        createdOrderIds.clear();
        createdSkuIds.clear();
    }

    /** 创建一条独立测试 SKU。 */
    protected long createSku(int stock, int lockStock) {
        PmsSkuStock sku = new PmsSkuStock();
        sku.setProductId(999999L);
        sku.setSkuCode("IT-" + UUID.randomUUID().toString().substring(0, 12));
        sku.setPrice(BigDecimal.ONE);
        sku.setStock(stock);
        sku.setLockStock(lockStock);
        sku.setSpData("[]");
        skuStockMapper.insertSelective(sku);
        createdSkuIds.add(sku.getId());
        return sku.getId();
    }

    /** 创建一条待付款订单。 */
    protected long createOrder(int status, int createMinutesAgo, BigDecimal payAmount) {
        return createOrder(status, createMinutesAgo, payAmount, null, null, TEST_MEMBER_ID);
    }

    protected long createOrder(int status, int createMinutesAgo, BigDecimal payAmount,
                               Integer useIntegration, Long couponId, Long memberId) {
        OmsOrder order = new OmsOrder();
        order.setMemberId(memberId);
        order.setMemberUsername("it-test");
        order.setOrderSn("IT-" + UUID.randomUUID().toString().replace("-", "").substring(0, 20));
        order.setCreateTime(new Date(System.currentTimeMillis() - createMinutesAgo * 60_000L));
        order.setStatus(status);
        order.setDeleteStatus(0);
        order.setPayType(1);
        order.setSourceType(1);
        order.setOrderType(0);
        order.setConfirmStatus(0);
        order.setReceiverName("it-test");
        order.setReceiverPhone("13800000000");
        order.setTotalAmount(payAmount);
        order.setPayAmount(payAmount);
        order.setFreightAmount(BigDecimal.ZERO);
        order.setPromotionAmount(BigDecimal.ZERO);
        order.setCouponAmount(BigDecimal.ZERO);
        order.setIntegrationAmount(BigDecimal.ZERO);
        order.setDiscountAmount(BigDecimal.ZERO);
        order.setUseIntegration(useIntegration);
        order.setCouponId(couponId);
        order.setIntegration(0);
        order.setGrowth(0);
        orderMapper.insertSelective(order);
        createdOrderIds.add(order.getId());
        return order.getId();
    }

    protected void addOrderItem(long orderId, long skuId, int quantity) {
        OmsOrderItem item = new OmsOrderItem();
        item.setOrderId(orderId);
        item.setOrderSn("IT-ITEM");
        item.setProductSkuId(skuId);
        item.setProductQuantity(quantity);
        item.setProductPrice(BigDecimal.ONE);
        item.setProductName("it-test");
        item.setGiftIntegration(0);
        item.setGiftGrowth(0);
        orderItemMapper.insertSelective(item);
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

    protected String orderSnOf(long orderId) {
        return orderMapper.selectByPrimaryKey(orderId).getOrderSn();
    }

    protected void setSku(long skuId, int stock, int lockStock) {
        PmsSkuStock sku = new PmsSkuStock();
        sku.setId(skuId);
        sku.setStock(stock);
        sku.setLockStock(lockStock);
        skuStockMapper.updateByPrimaryKeySelective(sku);
    }

    protected void setStatus(long orderId, int status) {
        OmsOrder order = new OmsOrder();
        order.setId(orderId);
        order.setStatus(status);
        orderMapper.updateByPrimaryKeySelective(order);
    }

    protected int countPayAnomalies(long orderId) throws SQLException {
        return queryInt("SELECT COUNT(*) FROM oms_order_pay_anomaly WHERE order_id = " + orderId);
    }

    protected void execute(String sql) throws SQLException {
        try (Connection c = dataSource.getConnection();
             Statement s = c.createStatement()) {
            s.executeUpdate(sql);
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
