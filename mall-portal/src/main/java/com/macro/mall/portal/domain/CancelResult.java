package com.macro.mall.portal.domain;

import lombok.Getter;

/** 取消订单处理结果。 */
@Getter
public class CancelResult {

    public enum Status {
        /** 本次抢到 0 → 4 并完成库存、券、积分的恢复。 */
        SUCCESS,
        /** 订单已经是已关闭，重复取消，不再做任何库存/券/积分副作用。 */
        ALREADY_CLOSED,
        /** 系统取消：订单尚未达到超时时间（重复消息或提前投递）。 */
        NOT_TIMED_OUT,
        /** 不属于当前用户 / 订单已支付 / 订单不存在。 */
        REJECTED
    }

    private final Status status;
    private final String message;

    private CancelResult(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public static CancelResult success() {
        return new CancelResult(Status.SUCCESS, "取消成功");
    }

    public static CancelResult alreadyClosed() {
        return new CancelResult(Status.ALREADY_CLOSED, "订单已关闭，重复取消不做任何变更");
    }

    public static CancelResult notTimedOut() {
        return new CancelResult(Status.NOT_TIMED_OUT, "订单尚未超时，跳过取消");
    }

    public static CancelResult rejected(String message) {
        return new CancelResult(Status.REJECTED, message);
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
}
