package com.macro.mall.portal.domain;

import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 支付成功但本地订单状态不匹配时的待对账 / 待退款记录。
 * 对应表 oms_order_pay_anomaly（见 document/sql/mall_a_migration.sql）。
 */
@Getter
@Setter
public class OmsOrderPayAnomaly {
    private Long id;
    private Long orderId;
    private String orderSn;
    private Long memberId;
    private Integer orderStatus;
    private BigDecimal payAmount;
    private BigDecimal notifyAmount;
    private String tradeNo;
    private Integer payType;
    private String reason;
    private String rawParams;
    private Integer handleStatus;
    private Date createTime;
    private Date modifyTime;
}
