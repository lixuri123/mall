package com.macro.mall.portal.component;

import com.macro.mall.portal.service.OmsPortalOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 超时订单扫描兜底。
 *
 * <p>A 阶段消息可靠性边界：DB 事务与 RabbitMQ 发布不是原子操作，
 * 「订单提交成功但取消消息丢失」会让锁定库存长期占用。本任务作为补偿，
 * 按固定间隔扫描确实超时的待付款订单，并逐单调用条件取消逻辑。
 * 每个订单独立事务，某个异常订单不会导致整批回滚。</p>
 *
 * <p>扫描间隔与开关由 {@code mall.order.timeout-scan.*} 配置；失败订单会在
 * 服务层输出 ERROR 级失败告警，并在下一轮继续重试。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mall.order.timeout-scan", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class OrderTimeOutCancelTask {

    @Autowired
    private OmsPortalOrderService portalOrderService;

    @Scheduled(initialDelayString = "${mall.order.timeout-scan.initial-delay-ms:30000}",
            fixedDelayString = "${mall.order.timeout-scan.interval-ms:60000}")
    public void cancelTimeOutOrder() {
        try {
            Integer count = portalOrderService.cancelTimeOutOrder();
            log.info("超时订单扫描完成，本轮取消并释放库存的订单数：{}", count);
        } catch (Exception e) {
            log.error("超时订单扫描执行异常，下一轮继续", e);
        }
    }
}
