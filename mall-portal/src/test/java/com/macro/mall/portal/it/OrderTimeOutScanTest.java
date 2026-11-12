package com.macro.mall.portal.it;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A 阶段：超时扫描兜底 / 消息故障场景。
 * 每个订单独立事务，异常订单不能拖垮整批，也不能被错误标记为已关闭。
 */
class OrderTimeOutScanTest extends AbstractOrderIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("100.00");

    @Test
    void scanCancelsEachOrderInItsOwnTransaction() throws SQLException {
        long sku1 = createSku(10, 3);
        long sku2 = createSku(10, 3);
        long badSku = createSku(10, 0); // 库存与订单数据不一致
        long order1 = createOrder(0, 180, AMOUNT);
        long order2 = createOrder(0, 180, AMOUNT);
        long badOrder = createOrder(0, 180, AMOUNT);
        addOrderItem(order1, sku1, 3);
        addOrderItem(order2, sku2, 3);
        addOrderItem(badOrder, badSku, 3);

        int handled = portalOrderService.cancelTimeOutOrder();
        assertEquals(2, handled, "两个正常订单应被取消，异常订单单独回滚");
        assertEquals(4, statusOf(order1));
        assertEquals(4, statusOf(order2));
        assertEquals(0, lockOf(sku1));
        assertEquals(0, lockOf(sku2));
        assertEquals(10, stockOf(sku1));
        assertEquals(10, stockOf(sku2));

        assertEquals(0, statusOf(badOrder), "释放库存失败时订单状态必须一起回滚");
        assertEquals(0, lockOf(badSku));
    }

    @Test
    void scanSkipsOrdersThatAreNotTimedOut() {
        long sku = createSku(10, 3);
        long freshOrder = createOrder(0, 0, AMOUNT);
        addOrderItem(freshOrder, sku, 3);

        assertEquals(0, portalOrderService.cancelTimeOutOrder());
        assertEquals(0, statusOf(freshOrder));
        assertEquals(3, lockOf(sku));
    }

    @Test
    void duplicateCancelMessageCannotReleaseStockTwice() {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 180, AMOUNT);
        addOrderItem(orderId, sku, 3);

        portalOrderService.cancelBySystem(orderId);
        portalOrderService.cancelBySystem(orderId);

        assertEquals(4, statusOf(orderId));
        assertEquals(0, lockOf(sku), "重复消息不能重复释放库存");
        assertEquals(10, stockOf(sku));
    }

    @Test
    void earlyCancelMessageDoesNotReleaseStock() {
        long sku = createSku(10, 3);
        long orderId = createOrder(0, 0, AMOUNT);
        addOrderItem(orderId, sku, 3);

        portalOrderService.cancelBySystem(orderId);

        assertEquals(0, statusOf(orderId));
        assertEquals(3, lockOf(sku), "提前投递的消息必须被超时校验挡住");
    }
}
