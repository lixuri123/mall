package com.macro.mall.portal.util;

import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.portal.domain.CartPromotionItem;
import com.macro.mall.portal.domain.SkuQuantity;
import com.macro.mall.portal.exception.OrderConsistencyException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * 把购物车明细 / 订单明细按 SKU 汇总成一份有序的库存操作清单。
 *
 * <p>同一 SKU 出现多条明细时必须先求和再操作库存，否则「按主键逐条更新」
 * 或批量 CASE WHEN 都会漏扣、漏释放。TreeMap 保证 skuId 升序。</p>
 */
public final class OrderSkuAggregator {

    private OrderSkuAggregator() {
    }

    /** 下单：购物车促销项 → 锁定库存清单。 */
    public static List<SkuQuantity> fromCartItems(List<CartPromotionItem> items) {
        return aggregate(items, CartPromotionItem::getProductSkuId,
                CartPromotionItem::getQuantity, "购物车明细");
    }

    /** 支付 / 取消：订单明细 → 扣减或释放清单。 */
    public static List<SkuQuantity> fromOrderItems(List<OmsOrderItem> items) {
        return aggregate(items, OmsOrderItem::getProductSkuId,
                OmsOrderItem::getProductQuantity, "订单明细");
    }

    private static <T> List<SkuQuantity> aggregate(List<T> source,
                                                   Function<T, Long> skuGetter,
                                                   Function<T, Integer> quantityGetter,
                                                   String label) {
        if (source == null || source.isEmpty()) {
            throw new OrderConsistencyException(label + "为空，无法执行库存操作");
        }
        Map<Long, Integer> merged = new TreeMap<>();
        for (T item : source) {
            if (item == null) {
                throw new OrderConsistencyException(label + "包含空记录");
            }
            Long skuId = skuGetter.apply(item);
            Integer quantity = quantityGetter.apply(item);
            if (skuId == null || skuId <= 0) {
                throw new OrderConsistencyException(label + "存在非法的 SKU ID：" + skuId);
            }
            if (quantity == null || quantity <= 0) {
                throw new OrderConsistencyException(label + "存在非法的数量：" + quantity + "（skuId=" + skuId + "）");
            }
            merged.merge(skuId, quantity, (oldValue, newValue) -> {
                long sum = (long) oldValue + newValue;
                if (sum > Integer.MAX_VALUE) {
                    throw new OrderConsistencyException("SKU " + skuId + " 汇总数量溢出");
                }
                return (int) sum;
            });
        }
        List<SkuQuantity> result = new ArrayList<>(merged.size());
        merged.forEach((skuId, quantity) -> result.add(new SkuQuantity(skuId, quantity)));
        return result;
    }
}
