package com.macro.mall.portal.domain;

import lombok.Getter;
import lombok.Setter;

/** 一次取消请求。用户取消与系统超时取消走同一套事务内逻辑，但前置校验不同。 */
@Getter
@Setter
public class CancelCommand {

    public enum Source {
        /** 用户主动取消：必须校验订单归属当前登录会员。 */
        USER,
        /** 消息 / 定时任务取消：必须校验订单确实已经超时。 */
        SYSTEM_TIMEOUT
    }

    private Long orderId;
    private Source source;
    private Long memberId;
    /**
     * @deprecated B 阶段起系统取消一律以数据库中的 {@code oms_order.expire_time} 为准，
     * 本参数仅为兼容旧调用签名保留，不再参与超时判定。
     */
    @Deprecated
    private Integer overtimeMinutes;

    public static CancelCommand byUser(Long orderId, Long memberId) {
        CancelCommand command = new CancelCommand();
        command.setOrderId(orderId);
        command.setSource(Source.USER);
        command.setMemberId(memberId);
        return command;
    }

    public static CancelCommand bySystem(Long orderId, Integer overtimeMinutes) {
        CancelCommand command = new CancelCommand();
        command.setOrderId(orderId);
        command.setSource(Source.SYSTEM_TIMEOUT);
        command.setOvertimeMinutes(overtimeMinutes);
        return command;
    }
}
