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
 * <p>A 阶段启用后，B 阶段把扫描条件改为数据库中固定的 {@code oms_order.expire_time}：
 * 订单创建时就固定到期时间，之后修改后台超时配置不会影响已有订单。
 * 消息表扫描解决「待发送任务未发布」，本任务解决「消息发布后仍未完成取消」，
 * 两者检查的阶段不同，互为补充。</p>
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
