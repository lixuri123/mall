package com.macro.mall.portal.component;

import com.macro.mall.portal.service.OmsPortalOrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 取消订单消息的接收者。
 *
 * <p>A 阶段：不论消息重复、提前投递还是订单状态已变化，都统一走系统取消入口，
 * 由订单状态机做「状态条件更新 + 超时时间校验」，因此无效消息不会释放库存。
 * 单条消息处理失败只记录告警，由定时超时扫描兜底重试（可靠投递属于 B）。</p>
 */
@Component
@RabbitListener(queues = "mall.order.cancel")
public class CancelOrderReceiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(CancelOrderReceiver.class);

    @Autowired
    private OmsPortalOrderService portalOrderService;

    @RabbitHandler
    public void handle(Long orderId) {
        try {
            portalOrderService.cancelBySystem(orderId);
            LOGGER.info("process orderId:{}", orderId);
        } catch (Exception e) {
            // 不向外抛出以避免无界重投；本单会在下一轮定时扫描中重试。
            LOGGER.error("取消订单消息处理失败，等待定时扫描兜底：orderId={}", orderId, e);
        }
    }
}
