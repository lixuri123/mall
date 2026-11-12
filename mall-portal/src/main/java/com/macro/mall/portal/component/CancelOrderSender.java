package com.macro.mall.portal.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PublishOutcome;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * B 阶段：普通取消消息发送者（关联式 Publisher Confirm + mandatory Return）。
 *
 * <p>与 A 阶段的区别：</p>
 * <ul>
 *   <li>不再给消息设置独立 TTL，也不再发布到死信延迟队列；到期判定在数据库中完成。</li>
 *   <li>消息持久化、交换机与队列持久化；{@code mandatory=true} 处理不可路由。</li>
 *   <li>用 {@link CorrelationData#getFuture()} 同步等待本次发布的 Confirm，
 *       并同时检查 {@code CorrelationData.getReturned()} 与 ReturnsCallback 记录。</li>
 * </ul>
 *
 * <p><strong>Confirm ACK 不等于消费者处理成功</strong>，也不等于消息可路由：
 * 不可路由消息同样会收到 ACK，因此 Return 必须单独判定。Confirm 超时/连接中断时
 * 结果未知，按「可能已发送」处理并重试，由消费端幂等兜住重复投递。</p>
 */
@Slf4j
@Component
public class CancelOrderSender implements OrderCancelPublisher {

    /** ReturnsCallback 的补充记录，按 correlationId / messageId 索引。 */
    private final ConcurrentHashMap<String, ReturnedMessage> returnedMessages = new ConcurrentHashMap<>();

    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mall.order.outbox.exchange:mall.order.direct}")
    private String exchange;
    @Value("${mall.order.outbox.routing-key:mall.order.cancel}")
    private String routingKey;

    @PostConstruct
    void init() {
        // 显式设置，不依赖 spring.rabbitmq.template.mandatory 的默认值。
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnsCallback(returned -> {
            String key = correlationKey(returned.getMessage());
            if (key != null) {
                returnedMessages.put(key, returned);
            }
            log.error("订单取消消息不可路由（mandatory Return）：exchange={}, routingKey={}, reply={}, eventId={}",
                    returned.getExchange(), returned.getRoutingKey(), returned.getReplyText(), key);
        });
    }

    @Override
    public PublishOutcome publish(OrderCancelMessage message, long confirmTimeoutMs) {
        if (message == null || message.getEventId() == null) {
            return PublishOutcome.of(PublishOutcome.Status.ERROR, "消息或 eventId 为空");
        }
        String eventId = message.getEventId();
        returnedMessages.remove(eventId);
        final String payload;
        try {
            payload = objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            return PublishOutcome.of(PublishOutcome.Status.ERROR, "消息序列化失败：" + e.getMessage());
        }

        CorrelationData correlation = new CorrelationData(eventId);
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, payload,
                    m -> markPersistent(m, eventId), correlation);
            CorrelationData.Confirm confirm = correlation.getFuture()
                    .get(Math.max(1L, confirmTimeoutMs), TimeUnit.MILLISECONDS);

            // Broker 对 mandatory 消息的处理顺序是 basic.return → basic.ack，
            // 因此 Confirm 到达时 Return 已经可见；两处都检查以防实现差异。
            ReturnedMessage returned = returnedMessages.remove(eventId);
            if (correlation.getReturned() != null || returned != null) {
                ReturnedMessage detail = correlation.getReturned() != null ? correlation.getReturned() : returned;
                return PublishOutcome.of(PublishOutcome.Status.UNROUTABLE,
                        "replyCode=" + detail.getReplyCode() + ", replyText=" + detail.getReplyText()
                                + ", routingKey=" + detail.getRoutingKey());
            }
            if (!confirm.isAck()) {
                return PublishOutcome.of(PublishOutcome.Status.NACK,
                        "publisher confirm nack: " + confirm.getReason());
            }
            log.info("订单取消消息发布确认：eventId={}, orderId={}, exchange={}, routingKey={}",
                    eventId, message.getOrderId(), exchange, routingKey);
            return PublishOutcome.confirmed();
        } catch (TimeoutException e) {
            // 结果未知：消息可能已经进入 Broker，必须允许重发，由消费端幂等处理。
            log.warn("Publisher Confirm 超时，结果未知，安排重试：eventId={}, timeoutMs={}", eventId, confirmTimeoutMs);
            return PublishOutcome.of(PublishOutcome.Status.UNKNOWN, "confirm timeout");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return PublishOutcome.of(PublishOutcome.Status.UNKNOWN, "confirm wait interrupted");
        } catch (Exception e) {
            log.error("订单取消消息发布异常：eventId={}, orderId={}", eventId, message.getOrderId(), e);
            return PublishOutcome.of(PublishOutcome.Status.ERROR, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private Message markPersistent(Message message, String eventId) {
        MessageProperties properties = message.getMessageProperties();
        properties.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        properties.setMessageId(eventId);
        properties.setCorrelationId(eventId);
        properties.setHeader("x-event-id", eventId);
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setContentEncoding("UTF-8");
        return message;
    }

    private String correlationKey(Message message) {
        if (message == null || message.getMessageProperties() == null) {
            return null;
        }
        MessageProperties p = message.getMessageProperties();
        if (p.getCorrelationId() != null) {
            return p.getCorrelationId();
        }
        if (p.getMessageId() != null) {
            return p.getMessageId();
        }
        Object header = p.getHeaders().get("x-event-id");
        return header == null ? null : header.toString();
    }

    /** 供测试直接观察 ReturnsCallback 记录，避免仅凭 ACK 判定成功。 */
    public ConcurrentHashMap<String, ReturnedMessage> returnedMessages() {
        return returnedMessages;
    }
}
