package com.macro.mall.portal.config;

import com.macro.mall.portal.domain.QueueEnum;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * B 阶段：订单超时取消的消息拓扑。
 *
 * <pre>
 *   [本地消息表] --到期发布--> mall.order.direct / mall.order.cancel
 *                                  |
 *                         消费失败受控重试（固定 TTL）
 *                                  |
 *                    mall.order.cancel.retry --TTL--> 回到主交换机
 *                                  |
 *                         超过重试次数 / 毒消息
 *                                  v
 *                    mall.order.cancel.dlx / mall.order.cancel.dead
 * </pre>
 *
 * <p>交换机、主队列、死信队列、重试队列全部持久化。主队列与重试队列都声明了
 * 死信转发参数，确保无法处理的消息不会被静默丢弃。</p>
 *
 * <p>A 阶段的「每消息 TTL 延迟队列 + 死信转发」已移除：不同到期时间的消息
 * 进入同一队列时，过期消息会被队头消息阻塞（RabbitMQ 只在队头检查 TTL）。</p>
 */
@Configuration
public class RabbitMqConfig {

    /** 消费失败重试的固定延迟（毫秒），所有重试消息相同，不产生队头延迟差异。 */
    @Value("${mall.order.outbox.consume.retry-ttl-ms:5000}")
    private int consumeRetryTtlMs;

    @Bean
    DirectExchange orderDirect() {
        return ExchangeBuilder.directExchange(QueueEnum.QUEUE_ORDER_CANCEL.getExchange())
                .durable(true).build();
    }

    @Bean
    DirectExchange orderDeadLetterExchange() {
        return ExchangeBuilder.directExchange(QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getExchange())
                .durable(true).build();
    }

    @Bean
    DirectExchange orderRetryExchange() {
        return ExchangeBuilder.directExchange(QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getExchange())
                .durable(true).build();
    }

    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(QueueEnum.QUEUE_ORDER_CANCEL.getName())
                .deadLetterExchange(QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getExchange())
                .deadLetterRoutingKey(QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getRouteKey())
                .build();
    }

    @Bean
    public Queue orderDeadLetterQueue() {
        return QueueBuilder.durable(QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getName()).build();
    }

    @Bean
    public Queue orderRetryQueue() {
        return QueueBuilder.durable(QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getName())
                .ttl(consumeRetryTtlMs)
                .deadLetterExchange(QueueEnum.QUEUE_ORDER_CANCEL.getExchange())
                .deadLetterRoutingKey(QueueEnum.QUEUE_ORDER_CANCEL.getRouteKey())
                .build();
    }

    @Bean
    Binding orderBinding(DirectExchange orderDirect, Queue orderQueue) {
        return BindingBuilder.bind(orderQueue).to(orderDirect)
                .with(QueueEnum.QUEUE_ORDER_CANCEL.getRouteKey());
    }

    @Bean
    Binding orderDeadLetterBinding(DirectExchange orderDeadLetterExchange, Queue orderDeadLetterQueue) {
        return BindingBuilder.bind(orderDeadLetterQueue).to(orderDeadLetterExchange)
                .with(QueueEnum.QUEUE_ORDER_CANCEL_DLQ.getRouteKey());
    }

    @Bean
    Binding orderRetryBinding(DirectExchange orderRetryExchange, Queue orderRetryQueue) {
        return BindingBuilder.bind(orderRetryQueue).to(orderRetryExchange)
                .with(QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getRouteKey());
    }
}
