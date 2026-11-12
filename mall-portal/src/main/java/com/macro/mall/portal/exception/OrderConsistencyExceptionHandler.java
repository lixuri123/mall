package com.macro.mall.portal.exception;

import com.macro.mall.common.api.CommonResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 一致性异常不暴露内部细节给前端，但必须在服务端留完整堆栈供排查。
 * 抛出点位于事务内，事务已整体回滚，前端可以安全重试或联系客服。
 */
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice
public class OrderConsistencyExceptionHandler {

    @ExceptionHandler(OrderConsistencyException.class)
    public CommonResult handle(OrderConsistencyException e) {
        log.error("订单状态或库存一致性异常，本次事务已回滚：{}", e.getMessage(), e);
        return CommonResult.failed("订单状态或库存数据异常，本次操作已撤销，请联系客服");
    }

    /**
     * 死锁 / 锁等待超时属于可重试失败：事务已经整体回滚，重试不会重复创建订单
     * 或重复改动库存。这里明确返回「请重试」，不把它伪装成业务失败或成功。
     */
    @ExceptionHandler(PessimisticLockingFailureException.class)
    public CommonResult handleRetryableLockFailure(PessimisticLockingFailureException e) {
        log.warn("库存行锁竞争失败（事务已整体回滚），提示调用方重试：{}", e.getMessage());
        return CommonResult.failed("系统繁忙，本次操作未生效，请重试");
    }
}
