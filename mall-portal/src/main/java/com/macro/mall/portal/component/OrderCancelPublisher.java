package com.macro.mall.portal.component;

import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PublishOutcome;

/**
 * B 阶段：订单取消消息的发布抽象。
 *
 * <p>把传输层与发送任务解耦，便于在集成测试中注入故障（NACK / Return / 超时 / 连接中断），
 * 而真实环境与故障注入测试都使用同一个状态机与幂等消费逻辑。</p>
 */
public interface OrderCancelPublisher {

    /**
     * 发布一条持久化消息并等待关联式 Publisher Confirm。
     *
     * @param message         消息体（eventId 在所有重试中保持不变）
     * @param confirmTimeoutMs 等待 Confirm 的上限；超时视为「结果未知」
     */
    PublishOutcome publish(OrderCancelMessage message, long confirmTimeoutMs);
}
