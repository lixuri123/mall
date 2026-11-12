package com.macro.mall.portal.it;

import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.portal.domain.CartPromotionItem;
import com.macro.mall.portal.domain.SkuQuantity;
import com.macro.mall.portal.exception.OrderConsistencyException;
import com.macro.mall.portal.util.OrderSkuAggregator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 纯单元测试：多 SKU 汇总、升序与非法输入校验（不依赖数据库）。 */
class OrderSkuAggregatorTest {

    @Test
    void aggregatesDuplicateSkusInAscendingOrder() {
        List<CartPromotionItem> cart = List.of(
                item(30L, 2), item(10L, 3), item(30L, 4), item(20L, 1));
        List<SkuQuantity> result = OrderSkuAggregator.fromCartItems(cart);

        assertEquals(3, result.size());
        assertEquals(10L, result.get(0).skuId());
        assertEquals(3, result.get(0).quantity());
        assertEquals(20L, result.get(1).skuId());
        assertEquals(1, result.get(1).quantity());
        assertEquals(30L, result.get(2).skuId());
        assertEquals(6, result.get(2).quantity(), "同一 SKU 的 2 + 4 必须汇总为 6");
    }

    @Test
    void aggregatesOrderItemsToo() {
        List<OmsOrderItem> items = List.of(orderItem(5L, 2), orderItem(5L, 3), orderItem(1L, 1));
        List<SkuQuantity> result = OrderSkuAggregator.fromOrderItems(items);
        assertEquals(2, result.size());
        assertEquals(1L, result.get(0).skuId());
        assertEquals(5L, result.get(1).skuId());
        assertEquals(5, result.get(1).quantity());
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(OrderConsistencyException.class,
                () -> OrderSkuAggregator.fromCartItems(new ArrayList<>()));
        assertThrows(OrderConsistencyException.class,
                () -> OrderSkuAggregator.fromCartItems(List.of(item(1L, 0))));
        assertThrows(OrderConsistencyException.class,
                () -> OrderSkuAggregator.fromCartItems(List.of(item(null, 1))));
        assertThrows(OrderConsistencyException.class,
                () -> OrderSkuAggregator.fromOrderItems(List.of(orderItem(1L, null))));
    }

    private CartPromotionItem item(Long skuId, Integer quantity) {
        CartPromotionItem item = new CartPromotionItem();
        item.setProductSkuId(skuId);
        item.setQuantity(quantity);
        return item;
    }

    private OmsOrderItem orderItem(Long skuId, Integer quantity) {
        OmsOrderItem item = new OmsOrderItem();
        item.setProductSkuId(skuId);
        item.setProductQuantity(quantity);
        return item;
    }
}
