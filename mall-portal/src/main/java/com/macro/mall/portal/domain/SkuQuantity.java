package com.macro.mall.portal.domain;

/**
 * 按 SKU 汇总后的库存操作量。
 * 所有库存 SQL 都以此为单位逐条执行，列表恒按 skuId 升序排列，
 * 用于降低不同订单以相反顺序取行锁造成死锁的概率。
 */
public record SkuQuantity(Long skuId, Integer quantity) {
    public SkuQuantity {
        if (skuId == null || skuId <= 0) {
            throw new IllegalArgumentException("skuId 非法");
        }
        if (quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("quantity 必须大于 0");
        }
    }
}
