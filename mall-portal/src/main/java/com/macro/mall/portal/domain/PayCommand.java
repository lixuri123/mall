package com.macro.mall.portal.domain;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

/**
 * 一次「支付成功」处理请求。
 * 第三方回调必须带 paidAmount 且 verifyAmount=true；内部调试入口可以不校验金额。
 */
@Getter
@Setter
public class PayCommand {
    /** 订单 ID 与商户订单号二选一，优先 orderId。 */
    private Long orderId;
    private String orderSn;
    private Integer payType;
    /** 第三方通知的实付金额，必须与 oms_order.pay_amount 一致。 */
    private BigDecimal paidAmount;
    private String tradeNo;
    private String rawParams;
    private boolean verifyAmount;

    /** 内部入口（无第三方金额可核对）。 */
    public static PayCommand internal(Long orderId, Integer payType) {
        PayCommand command = new PayCommand();
        command.setOrderId(orderId);
        command.setPayType(payType);
        command.setVerifyAmount(false);
        return command;
    }

    /** 可信第三方回调 / 主动查询结果。 */
    public static PayCommand fromGateway(String orderSn, Integer payType, BigDecimal paidAmount,
                                         String tradeNo, String rawParams) {
        PayCommand command = new PayCommand();
        command.setOrderSn(orderSn);
        command.setPayType(payType);
        command.setPaidAmount(paidAmount);
        command.setTradeNo(tradeNo == null ? "" : tradeNo);
        command.setRawParams(rawParams);
        command.setVerifyAmount(true);
        return command;
    }
}
