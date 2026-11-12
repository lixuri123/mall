package com.macro.mall.portal.exception;

/**
 * 订单状态与库存数据不一致，或原子更新返回了非预期的影响行数。
 *
 * <p>这是<strong>内部一致性错误</strong>，不是用户可重试的业务校验失败。
 * 所有抛出点都位于数据库事务内，抛出后整笔订单与库存变更一起回滚。</p>
 */
public class OrderConsistencyException extends RuntimeException {

    public OrderConsistencyException(String message) {
        super(message);
    }

    public OrderConsistencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
