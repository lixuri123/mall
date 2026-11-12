package com.macro.mall.portal.domain;

import lombok.Getter;

/**
 * B 阶段：订单取消消息拓扑常量。
 *
 * <p>不再使用「每条消息独立 TTL + 死信转发」的延迟队列。订单到期时间保存在数据库，
 * 到期后才发布普通消息，避免不同 TTL 在同一队列产生队头阻塞。</p>
 *
 * <p>重试队列使用<strong>固定</strong> TTL（所有重试消息延迟相同），因此同样没有
 * 队头延迟问题。它的作用只是把消费失败的消息延后固定时间再投回主队列。</p>
 */
@Getter
public enum QueueEnum {

    /** 主链路：普通持久化直连交换机 / 队列 / 路由键。 */
    QUEUE_ORDER_CANCEL("mall.order.direct", "mall.order.cancel", "mall.order.cancel"),
    /** 消费失败的死信交换机 / 队列 / 路由键，保留毒消息与超限消息。 */
    QUEUE_ORDER_CANCEL_DLQ("mall.order.cancel.dlx", "mall.order.cancel.dead", "mall.order.cancel.dead"),
    /** 固定 TTL 的受控重试队列，过期后转发回主交换机。 */
    QUEUE_ORDER_CANCEL_RETRY("mall.order.cancel.retry.exchange", "mall.order.cancel.retry", "mall.order.cancel.retry");

    private final String exchange;
    private final String name;
    private final String routeKey;

    QueueEnum(String exchange, String name, String routeKey) {
        this.exchange = exchange;
        this.name = name;
        this.routeKey = routeKey;
    }
}
