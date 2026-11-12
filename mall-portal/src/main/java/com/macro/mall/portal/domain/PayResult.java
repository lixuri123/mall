package com.macro.mall.portal.domain;

import lombok.Getter;

/**
 * 支付成功处理结果。影响 0 行时必须分类，不能一律当作成功或失败。
 */
@Getter
public class PayResult {

    public enum Status {
        /** 本次抢到状态流转，并完成库存扣减。 */
        SUCCESS,
        /** 订单已支付，属于重复通知，库存不应再扣。 */
        DUPLICATE,
        /** 订单已取消但第三方确实支付成功，已落待对账记录，需退款或人工处理。 */
        LATE_PAYMENT_RECORDED,
        /** 订单不存在 / 金额不符 / 状态不允许，拒绝处理。 */
        REJECTED
    }

    private final Status status;
    private final Long orderId;
    private final String message;
    private final int stockRows;

    private PayResult(Status status, Long orderId, String message, int stockRows) {
        this.status = status;
        this.orderId = orderId;
        this.message = message;
        this.stockRows = stockRows;
    }

    public static PayResult success(Long orderId, int stockRows) {
        return new PayResult(Status.SUCCESS, orderId, "支付成功", stockRows);
    }

    public static PayResult duplicate(Long orderId, String message) {
        return new PayResult(Status.DUPLICATE, orderId, message, 0);
    }

    public static PayResult latePayment(Long orderId, String message) {
        return new PayResult(Status.LATE_PAYMENT_RECORDED, orderId, message, 0);
    }

    public static PayResult rejected(String message) {
        return new PayResult(Status.REJECTED, null, message, 0);
    }

    /** 是否可给第三方返回 success，避免无意义重投（异常已落库）。 */
    public boolean isAccepted() {
        return status != Status.REJECTED;
    }
}
