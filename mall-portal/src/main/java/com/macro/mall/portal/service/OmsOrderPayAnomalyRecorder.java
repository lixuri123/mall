package com.macro.mall.portal.service;

import com.macro.mall.model.OmsOrder;
import com.macro.mall.portal.domain.PayCommand;

/**
 * 记录「第三方支付成功、本地订单状态却不允许支付」的异常。
 * 见 {@link com.macro.mall.portal.service.impl.OmsOrderPayAnomalyRecorder}。
 */
public interface OmsOrderPayAnomalyRecorder {

    /**
     * 独立事务（REQUIRES_NEW）落库：即使外层支付处理后续回滚或抛异常，
     * 这条待对账记录也必须保留下来。
     */
    void record(OmsOrder order, PayCommand command, Integer currentStatus, String reason);
}
