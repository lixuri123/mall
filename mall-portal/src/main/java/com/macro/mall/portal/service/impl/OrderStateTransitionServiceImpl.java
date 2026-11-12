package com.macro.mall.portal.service.impl;

import com.macro.mall.mapper.OmsOrderItemMapper;
import com.macro.mall.mapper.OmsOrderMapper;
import com.macro.mall.model.OmsOrder;
import com.macro.mall.model.OmsOrderExample;
import com.macro.mall.model.OmsOrderItem;
import com.macro.mall.model.OmsOrderItemExample;
import com.macro.mall.portal.dao.PortalOrderDao;
import com.macro.mall.portal.domain.CancelCommand;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PayResult;
import com.macro.mall.portal.domain.SkuQuantity;
import com.macro.mall.portal.exception.OrderConsistencyException;
import com.macro.mall.portal.service.OmsOrderPayAnomalyRecorder;
import com.macro.mall.portal.service.OrderStateTransitionService;
import com.macro.mall.portal.service.UmsMemberCacheService;
import com.macro.mall.portal.util.OrderSkuAggregator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * 订单状态与库存一致性的事务执行器。
 *
 * <p>核心不变量：</p>
 * <ol>
 *   <li>同一个待付款订单只能有一次有效的状态变更（0 → 1 支付 / 0 → 4 取消，
 *       两条 UPDATE 竞争同一行，由数据库行锁裁决）。</li>
 *   <li>只有抢到状态变更的请求才能改动库存，因此支付只扣一次、取消只释放一次。</li>
 *   <li>库存 SQL 全部为单 SKU 条件更新并检查影响行数，任一失败即抛异常，
 *       订单状态与库存一起回滚。</li>
 *   <li>多 SKU 按 skuId 升序处理，降低死锁概率；死锁由调用方按可重试失败处理。</li>
 * </ol>
 */
@Slf4j
@Service
public class OrderStateTransitionServiceImpl implements OrderStateTransitionService {

    private static final int STATUS_PENDING_PAYMENT = 0;
    private static final int STATUS_TO_BE_DELIVERED = 1;
    private static final int STATUS_CLOSED = 4;
    private static final int STATUS_INVALID = 5;

    @Autowired
    private OmsOrderMapper orderMapper;
    @Autowired
    private OmsOrderItemMapper orderItemMapper;
    @Autowired
    private PortalOrderDao portalOrderDao;
    @Autowired
    private UmsMemberCacheService memberCacheService;
    @Autowired
    private OmsOrderPayAnomalyRecorder payAnomalyRecorder;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public PayResult paySuccess(PayCommand command) {
        OmsOrder order = loadOrder(command);
        if (order == null || !isNotDeleted(order)) {
            return PayResult.rejected("订单不存在或已删除");
        }
        if (command.isVerifyAmount()) {
            BigDecimal paid = command.getPaidAmount();
            BigDecimal expected = order.getPayAmount();
            if (paid == null || expected == null || paid.compareTo(expected) != 0) {
                log.warn("支付金额与订单应付金额不一致，拒绝处理：orderSn={}, paid={}, expected={}",
                        order.getOrderSn(), paid, expected);
                return PayResult.rejected("支付金额与订单应付金额不一致");
            }
        }

        // 1) 抢占订单状态：0 → 1。与取消的 0 → 4 互斥。
        int rows = portalOrderDao.markOrderPaid(order.getId(), command.getPayType());
        if (rows == 1) {
            int stockRows = deductStock(order.getId());
            log.info("支付成功：orderId={}, orderSn={}, 扣库存行数={}",
                    order.getId(), order.getOrderSn(), stockRows);
            return PayResult.success(order.getId(), stockRows);
        }
        if (rows > 1) {
            throw new OrderConsistencyException("支付状态更新影响了多行，orderId=" + order.getId());
        }

        // 2) 影响 0 行：必须用当前读（FOR UPDATE）拿最新状态分类，不能沿用快照。
        Integer currentStatus = portalOrderDao.selectOrderStatusForUpdate(order.getId());
        if (currentStatus == null) {
            return PayResult.rejected("订单不存在");
        }
        if (isPaidStatus(currentStatus)) {
            // 已支付：重复通知，幂等成功，不再扣库存。
            return PayResult.duplicate(order.getId(), "订单已支付，重复通知不做库存变更");
        }
        if (currentStatus == STATUS_CLOSED || currentStatus == STATUS_INVALID) {
            // 数据库只能决定本地状态，无法撤销第三方已经发生的支付。
            String reason = "订单已关闭/无效，但收到可信的支付成功结果，需对账或退款";
            payAnomalyRecorder.record(order, command, currentStatus, reason);
            log.error("支付晚到异常：orderId={}, orderSn={}, status={}, tradeNo={}, amount={}；已落待对账记录",
                    order.getId(), order.getOrderSn(), currentStatus, command.getTradeNo(), command.getPaidAmount());
            return PayResult.latePayment(order.getId(), reason);
        }
        log.warn("订单当前状态不允许支付：orderId={}, status={}", order.getId(), currentStatus);
        return PayResult.rejected("订单当前状态不允许支付：" + currentStatus);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public CancelResult cancelOrder(CancelCommand command) {
        if (command == null || command.getOrderId() == null || command.getSource() == null) {
            return CancelResult.rejected("取消参数不完整");
        }
        OmsOrder order = orderMapper.selectByPrimaryKey(command.getOrderId());
        if (order == null || !isNotDeleted(order)) {
            return CancelResult.rejected("订单不存在或已删除");
        }

        // 1) 不同来源的前置校验不同，但后续共用同一套事务内逻辑。
        if (command.getSource() == CancelCommand.Source.USER) {
            if (command.getMemberId() == null || !command.getMemberId().equals(order.getMemberId())) {
                log.warn("用户尝试取消他人订单：orderId={}, requestMember={}, owner={}",
                        order.getId(), command.getMemberId(), order.getMemberId());
                return CancelResult.rejected("不能取消其他用户的订单");
            }
        } else {
            // B 阶段：系统取消一律以数据库中固定的 expire_time 为准。
            // 消息里的 dueTime 只用于追踪；不用「当前配置 + create_time」重算，
            // 否则后台改了超时配置就会悄悄改变已有订单的到期时间。
            Date expireTime = portalOrderDao.selectOrderExpireTime(order.getId());
            if (expireTime == null) {
                // 历史订单未完成回填：列入待处理，不猜测到期时间，也不自动关闭。
                log.error("系统取消被拒绝：订单缺少固定到期时间，需先完成历史数据回填。orderId={}", order.getId());
                return CancelResult.rejected("订单缺少固定到期时间，需先完成历史数据回填");
            }
            if (expireTime.getTime() > System.currentTimeMillis()) {
                return CancelResult.notTimedOut();
            }
        }

        // 2) 抢占订单状态：0 → 4。已支付的订单不会被这条 UPDATE 命中，也就不会释放库存。
        int rows = portalOrderDao.closePendingOrder(order.getId());
        if (rows == 1) {
            releaseStock(order.getId());
            restoreCouponAndIntegration(order);
            log.info("取消成功：orderId={}, orderSn={}, source={}",
                    order.getId(), order.getOrderSn(), command.getSource());
            return CancelResult.success();
        }
        if (rows > 1) {
            throw new OrderConsistencyException("取消状态更新影响了多行，orderId=" + order.getId());
        }

        Integer currentStatus = portalOrderDao.selectOrderStatusForUpdate(order.getId());
        if (currentStatus == null) {
            return CancelResult.rejected("订单不存在");
        }
        if (currentStatus == STATUS_CLOSED) {
            return CancelResult.alreadyClosed();
        }
        return CancelResult.rejected("订单当前状态不允许取消：" + currentStatus);
    }

    // ---------------------------------------------------------------------
    // 内部实现
    // ---------------------------------------------------------------------

    private int deductStock(Long orderId) {
        List<SkuQuantity> skuList = OrderSkuAggregator.fromOrderItems(loadOrderItems(orderId));
        int total = 0;
        for (SkuQuantity sku : skuList) {
            int affected = portalOrderDao.deductSkuStock(sku.skuId(), sku.quantity());
            if (affected != 1) {
                log.error("支付扣库存失败，整笔回滚：orderId={}, skuId={}, quantity={}, affected={}",
                        orderId, sku.skuId(), sku.quantity(), affected);
                throw new OrderConsistencyException(
                        "支付扣库存失败（orderId=" + orderId + ", skuId=" + sku.skuId() + "），订单状态已回滚");
            }
            total += affected;
        }
        return total;
    }

    private void releaseStock(Long orderId) {
        List<SkuQuantity> skuList = OrderSkuAggregator.fromOrderItems(loadOrderItems(orderId));
        for (SkuQuantity sku : skuList) {
            int affected = portalOrderDao.releaseSkuStock(sku.skuId(), sku.quantity());
            if (affected != 1) {
                log.error("取消释放库存失败，整笔回滚：orderId={}, skuId={}, quantity={}, affected={}",
                        orderId, sku.skuId(), sku.quantity(), affected);
                throw new OrderConsistencyException(
                        "释放锁定库存失败（orderId=" + orderId + ", skuId=" + sku.skuId() + "），订单状态已回滚");
            }
        }
    }

    private void restoreCouponAndIntegration(OmsOrder order) {
        if (order.getCouponId() != null) {
            int affected = portalOrderDao.releaseCoupon(order.getCouponId(), order.getMemberId());
            if (affected == 0) {
                // 历史订单可能没有对应的使用记录；不阻断库存与状态的正确性，但必须留下告警。
                log.warn("取消订单未找到已使用的优惠券记录：orderId={}, couponId={}, memberId={}",
                        order.getId(), order.getCouponId(), order.getMemberId());
            }
        }
        Integer usedIntegration = order.getUseIntegration();
        if (usedIntegration != null && usedIntegration > 0) {
            int affected = portalOrderDao.addMemberIntegration(order.getMemberId(), usedIntegration);
            if (affected != 1) {
                throw new OrderConsistencyException(
                        "返还积分失败（orderId=" + order.getId() + "），订单状态已回滚");
            }
            try {
                memberCacheService.delMember(order.getMemberId());
            } catch (Exception e) {
                // 缓存失效失败不能让已经正确的数据库事务回滚；仅告警，后续读库会自愈。
                log.warn("积分已返还但会员缓存失效失败：memberId={}", order.getMemberId(), e);
            }
        }
    }

    private List<OmsOrderItem> loadOrderItems(Long orderId) {
        OmsOrderItemExample example = new OmsOrderItemExample();
        example.createCriteria().andOrderIdEqualTo(orderId);
        return orderItemMapper.selectByExample(example);
    }

    private OmsOrder loadOrder(PayCommand command) {
        if (command.getOrderId() != null) {
            return orderMapper.selectByPrimaryKey(command.getOrderId());
        }
        if (command.getOrderSn() != null && !command.getOrderSn().isBlank()) {
            OmsOrderExample example = new OmsOrderExample();
            example.createCriteria()
                    .andOrderSnEqualTo(command.getOrderSn())
                    .andDeleteStatusEqualTo(0);
            List<OmsOrder> orders = orderMapper.selectByExample(example);
            return orders.isEmpty() ? null : orders.get(0);
        }
        return null;
    }

    private boolean isNotDeleted(OmsOrder order) {
        return order.getDeleteStatus() == null || order.getDeleteStatus() == 0;
    }

    private boolean isPaidStatus(Integer status) {
        return status != null && status >= STATUS_TO_BE_DELIVERED && status <= 3;
    }

}
