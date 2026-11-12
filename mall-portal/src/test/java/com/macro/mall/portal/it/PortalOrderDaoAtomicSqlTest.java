package com.macro.mall.portal.it;

import com.macro.mall.portal.domain.OmsOrderPayAnomaly;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A 阶段：DAO 层原子 SQL 的真实 MySQL 集成测试。
 * 每个条件更新都必须用影响行数明确表达「成功 / 拒绝」，且不允许静默写入脏数据。
 */
class PortalOrderDaoAtomicSqlTest extends AbstractOrderIntegrationTest {

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void lockStock_respectsAvailableStock() {
        long sku = createSku(10, 0);

        assertEquals(1, portalOrderDao.lockSkuStock(sku, 3));
        assertEquals(10, stockOf(sku));
        assertEquals(3, lockOf(sku));

        assertEquals(1, portalOrderDao.lockSkuStock(sku, 7));
        assertEquals(10, lockOf(sku));

        // 可售为 0，仍不能锁 1 件；stock 与 lock_stock 都保持合法
        assertEquals(0, portalOrderDao.lockSkuStock(sku, 1));
        assertEquals(10, stockOf(sku));
        assertEquals(10, lockOf(sku));

        // 数量非法 / SKU 不存在一律 0 行
        assertEquals(0, portalOrderDao.lockSkuStock(sku, 0));
        assertEquals(0, portalOrderDao.lockSkuStock(sku, -1));
        assertEquals(0, portalOrderDao.lockSkuStock(99999999L, 1));
    }

    @Test
    void lockStock_rejectsAnomalousHistoricalRows() throws SQLException {
        long missing = 99999999L;
        assertEquals(0, portalOrderDao.lockSkuStock(missing, 1));

        long negative = createSku(10, -1);
        assertEquals(0, portalOrderDao.lockSkuStock(negative, 1));
        assertEquals(10, stockOf(negative));
        assertEquals(-1, lockOf(negative));

        long overLocked = createSku(10, 20);
        assertEquals(0, portalOrderDao.lockSkuStock(overLocked, 1));

        // 列定义历史上允许 NULL：临时放开后验证条件更新同样拒绝，且预检能识别。
        long nullStock = createSku(10, 0);
        execute("ALTER TABLE pms_sku_stock MODIFY COLUMN stock INT NULL");
        try {
            executeUpdate("UPDATE pms_sku_stock SET stock = NULL WHERE id = ?", nullStock);
            assertEquals(0, portalOrderDao.lockSkuStock(nullStock, 1));

            List<Map<String, Object>> anomalies = portalOrderDao.listStockAnomalies();
            assertTrue(anomalies.stream()
                            .anyMatch(row -> String.valueOf(row.get("sku_id")).equals(String.valueOf(nullStock))),
                    "预检必须能识别 NULL 库存，且不能静默当成 0 计算");
            assertTrue(anomalies.stream()
                            .anyMatch(row -> String.valueOf(row.get("sku_id")).equals(String.valueOf(negative))),
                    "预检必须能识别负数锁定库存");
        } finally {
            executeUpdate("UPDATE pms_sku_stock SET stock = 10 WHERE id = ?", nullStock);
            execute("ALTER TABLE pms_sku_stock MODIFY COLUMN stock INT NOT NULL DEFAULT 0");
        }
    }

    @Test
    void deductAndReleaseSkuStock_checkTheirOwnPreconditions() {
        long sku = createSku(10, 5);

        // 扣库存必须同时满足 stock >= qty 且 lock_stock >= qty
        assertEquals(0, portalOrderDao.deductSkuStock(sku, 6), "lock_stock 不足必须拒绝");
        assertEquals(1, portalOrderDao.deductSkuStock(sku, 5));
        assertEquals(5, stockOf(sku));
        assertEquals(0, lockOf(sku));

        assertEquals(0, portalOrderDao.releaseSkuStock(sku, 1), "锁定库存不足时不能释放");
        assertEquals(0, portalOrderDao.deductSkuStock(sku, 0));
        assertEquals(0, portalOrderDao.releaseSkuStock(sku, -1));
    }

    @Test
    void orderStatusConditionalUpdate_isTheReferee() {
        long paying = createOrder(0, 0, new BigDecimal("100.00"));
        assertEquals(1, portalOrderDao.markOrderPaid(paying, 1));
        assertEquals(0, portalOrderDao.closePendingOrder(paying), "已支付订单不能被取消");
        assertEquals(1, statusOf(paying));
        assertEquals(Integer.valueOf(1), portalOrderDao.selectOrderStatusForUpdate(paying));

        long closing = createOrder(0, 0, new BigDecimal("100.00"));
        assertEquals(1, portalOrderDao.closePendingOrder(closing));
        assertEquals(0, portalOrderDao.markOrderPaid(closing, 1), "已关闭订单不能被支付");
        assertEquals(4, statusOf(closing));
    }

    @Test
    void multiSkuUpdatesRollBackTogether() {
        long first = createSku(10, 0);
        long second = createSku(1, 0);
        TransactionTemplate template = new TransactionTemplate(transactionManager);

        assertThrows(IllegalStateException.class, () -> template.execute(status -> {
            assertEquals(1, portalOrderDao.lockSkuStock(first, 5));
            assertEquals(0, portalOrderDao.lockSkuStock(second, 5), "第二个 SKU 可售不足");
            throw new IllegalStateException("模拟服务层判定失败");
        }));

        assertEquals(0, lockOf(first), "第一个 SKU 的锁定必须随事务一起回滚");
        assertEquals(0, lockOf(second));
        assertEquals(10, stockOf(first));
    }

    @Test
    void timeoutPagingUsesIncreasingIdCursor() {
        long old1 = createOrder(0, 120, new BigDecimal("10.00"));
        long old2 = createOrder(0, 120, new BigDecimal("10.00"));
        long fresh = createOrder(0, 0, new BigDecimal("10.00"));

        List<Long> firstPage = portalOrderDao.listTimeOutOrderIds(60, 0L, 1);
        assertEquals(1, firstPage.size());
        long cursor = firstPage.get(0);
        List<Long> secondPage = portalOrderDao.listTimeOutOrderIds(60, cursor, 10);
        assertTrue(secondPage.stream().allMatch(id -> id > cursor));
        assertTrue(secondPage.contains(Math.max(old1, old2)));
        assertFalse(secondPage.contains(fresh), "未超时的订单不能进入候选集");
    }

    @Test
    void payAnomalyInsertIsIdempotent() throws SQLException {
        long orderId = createOrder(4, 0, new BigDecimal("100.00"));
        OmsOrderPayAnomaly record = new OmsOrderPayAnomaly();
        record.setOrderId(orderId);
        record.setOrderSn("IT-ANOMALY");
        record.setOrderStatus(4);
        record.setPayAmount(new BigDecimal("100.00"));
        record.setNotifyAmount(new BigDecimal("100.00"));
        record.setTradeNo("TRADE-1");
        record.setPayType(1);
        record.setReason("订单已关闭但支付成功");
        record.setRawParams("{}");
        record.setHandleStatus(0);

        portalOrderDao.insertPayAnomaly(record);
        portalOrderDao.insertPayAnomaly(record);
        assertEquals(1, countPayAnomalies(orderId), "重复通知不得重复落库");
    }

    @Test
    void integrationAndCouponUseConditionalUpdates() throws SQLException {
        // 积分：原子增减，不覆盖写
        assertEquals(1, portalOrderDao.addMemberIntegration(TEST_MEMBER_ID, 50));
        int before = queryInt("SELECT integration FROM ums_member WHERE id = " + TEST_MEMBER_ID);
        assertEquals(1, portalOrderDao.deductMemberIntegration(TEST_MEMBER_ID, 30));
        assertEquals(0, portalOrderDao.deductMemberIntegration(TEST_MEMBER_ID, before + 1000),
                "积分不足必须拒绝");
        assertEquals(before - 30,
                queryInt("SELECT integration FROM ums_member WHERE id = " + TEST_MEMBER_ID));
        portalOrderDao.deductMemberIntegration(TEST_MEMBER_ID, 20); // 还原

        // 优惠券：带原状态的条件更新，重复取消不会二次改动
        long couponId = 99990001L;
        execute("DELETE FROM sms_coupon_history WHERE coupon_id = " + couponId);
        executeUpdate("INSERT INTO sms_coupon_history (coupon_id, member_id, coupon_code, use_status) "
                + "VALUES (?, ?, 'IT-COUPON', 0)", couponId, TEST_MEMBER_ID);
        assertEquals(1, portalOrderDao.markCouponUsed(couponId, TEST_MEMBER_ID));
        assertEquals(0, portalOrderDao.markCouponUsed(couponId, TEST_MEMBER_ID));
        assertEquals(1, portalOrderDao.releaseCoupon(couponId, TEST_MEMBER_ID));
        assertEquals(0, portalOrderDao.releaseCoupon(couponId, TEST_MEMBER_ID));
        execute("DELETE FROM sms_coupon_history WHERE coupon_id = " + couponId);
    }
}
