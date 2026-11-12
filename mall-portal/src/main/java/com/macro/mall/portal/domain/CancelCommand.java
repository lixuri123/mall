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
