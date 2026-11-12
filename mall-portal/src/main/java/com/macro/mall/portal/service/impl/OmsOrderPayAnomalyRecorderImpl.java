package com.macro.mall.portal.service.impl;

import com.macro.mall.model.OmsOrder;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.domain.OmsOrderPayAnomaly;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.service.OmsOrderPayAnomalyRecorder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * 支付晚到异常记录器。
 *
 * <p>必须用独立 Bean + REQUIRES_NEW：支付处理的事务可能随后抛异常回滚，
 * 若异常记录与它共用事务，记录也会一起消失，导致「钱付了但查不到」。</p>
 */
@Slf4j
@Service
public class OmsOrderPayAnomalyRecorderImpl implements OmsOrderPayAnomalyRecorder {

    @Autowired
    private PortalOrderDao portalOrderDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void record(OmsOrder order, PayCommand command, Integer currentStatus, String reason) {
        OmsOrderPayAnomaly record = new OmsOrderPayAnomaly();
        record.setOrderId(order.getId());
        record.setOrderSn(order.getOrderSn() == null ? "" : order.getOrderSn());
        record.setMemberId(order.getMemberId());
        record.setOrderStatus(currentStatus);
        record.setPayAmount(order.getPayAmount());
        record.setNotifyAmount(command.getPaidAmount());
        record.setTradeNo(command.getTradeNo() == null ? "" : command.getTradeNo());
        record.setPayType(command.getPayType());
        record.setReason(reason);
        record.setRawParams(command.getRawParams());
        record.setHandleStatus(0);
        // 唯一键 (order_id, trade_no)：同一笔通知反复重投不会重复落库。
        portalOrderDao.insertPayAnomaly(record);
        log.error("已记录支付晚到异常：orderId={}, orderSn={}, tradeNo={}, handleStatus=0（待对账/退款）",
                order.getId(), order.getOrderSn(), record.getTradeNo());
    }
}
