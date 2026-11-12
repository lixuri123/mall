package com.macro.mall.portal.service;

import com.macro.mall.portal.domain.CancelCommand;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.PayCommand;
import com.macro.mall.portal.domain.PayResult;

/**
 * 订单状态机：支付成功与取消订单的唯一事务入口。
 *
 * <p>这是一个<strong>独立的 Spring Bean</strong>。下单服务、MQ 消费者、定时任务
 * 都通过它调用，因而每次调用都会经过 Spring 事务代理，逐单拥有独立事务；
 * 不依赖同类内部的 this.xxx() 自调用。</p>
 */
public interface OrderStateTransitionService {

    /**
     * 支付成功：先用条件更新抢占订单状态 0 → 1，再逐 SKU 扣库存。
     * 重复通知幂等；已取消订单收到可信支付结果时落待对账异常记录。
     */
    PayResult paySuccess(PayCommand command);

    /**
     * 统一取消：用户取消校验归属，系统取消校验超时；
     * 条件更新 0 → 4 成功后才释放库存、恢复券与积分。
     */
    CancelResult cancelOrder(CancelCommand command);
}
