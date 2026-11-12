package com.macro.mall.portal.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PublishOutcome;
import com.macro.mall.portal.domain.RocketMqTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * B 阶段（RocketMQ 版）：取消消息发布器。
 *
 * <p>与 RabbitMQ 实现的语义对应关系：</p>
 * <ul>
 *   <li>Publisher Confirm ACK → 同步 {@code send()} 返回 {@code SendStatus.SEND_OK}。
 *       Broker 配置为 {@code flushDiskType=SYNC_FLUSH}，因此 SEND_OK 时消息已落盘。</li>
 *   <li>mandatory Return / UNROUTABLE → {@code No route info of this topic}。
 *       前提是 Broker 关闭了 {@code autoCreateTopicEnable} 并预先建好 Topic，
 *       否则 RocketMQ 会自动创建 Topic，错误路由将被静默吞掉。</li>
 *   <li>Confirm NACK → {@code FLUSH_DISK_TIMEOUT / FLUSH_SLAVE_TIMEOUT / SLAVE_NOT_AVAILABLE}
 *       等明确非 SEND_OK 的发送状态。</li>
 *   <li>Confirm 超时 → 发送超时 / 网络中断，结果未知，按「可能已发送」处理并重试，
 *       由消费端幂等兜住重复投递。</li>
 * </ul>
 *
 * <p><strong>SEND_OK 只说明 Broker 已接收并落盘</strong>，不说明消费者已处理，
 * 也不等于订单已取消。本地消息表的 {@code SENT} 状态同样只包含这一层含义。</p>
 */
@Slf4j
@Component
public class CancelOrderSender implements OrderCancelPublisher {

    @Autowired
    private DefaultMQProducer orderCancelProducer;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mall.order.outbox.topic}")
    private String topic;

    @Override
    public PublishOutcome publish(OrderCancelMessage message, long confirmTimeoutMs) {
        if (message == null || message.getEventId() == null) {
            return PublishOutcome.of(PublishOutcome.Status.ERROR, "消息或 eventId 为空");
        }
        final String payload;
        try {
            payload = objectMapper.writeValueAsString(message);
        } catch (Exception e) {
            return PublishOutcome.of(PublishOutcome.Status.ERROR, "消息序列化失败：" + e.getMessage());
        }

        Message msg = new Message(topic, RocketMqTopics.TAG_CANCEL,
                message.getEventId(), payload.getBytes(StandardCharsets.UTF_8));
        msg.putUserProperty(RocketMqTopics.EVENT_ID_PROPERTY, message.getEventId());

        try {
            SendResult result = orderCancelProducer.send(msg, Math.max(1000L, confirmTimeoutMs));
            SendStatus status = result.getSendStatus();
            if (status == SendStatus.SEND_OK) {
                log.info("订单取消消息发布确认：eventId={}, orderId={}, topic={}, msgId={}",
                        message.getEventId(), message.getOrderId(), topic, result.getMsgId());
                return PublishOutcome.confirmed();
            }
            log.warn("Broker 未确认本次发布：eventId={}, sendStatus={}", message.getEventId(), status);
            return PublishOutcome.of(PublishOutcome.Status.NACK, "sendStatus=" + status);
        } catch (MQClientException e) {
            String text = String.valueOf(e.getMessage());
            if (text.contains("No route info of this topic") || text.contains("topic not exist")) {
                log.error("订单取消消息不可路由（Topic 未预建或名称错误）：eventId={}, topic={}, error={}",
                        message.getEventId(), topic, text);
                return PublishOutcome.of(PublishOutcome.Status.UNROUTABLE, text.trim());
            }
            log.error("订单取消消息被 Broker 拒绝：eventId={}, topic={}", message.getEventId(), topic, e);
            return PublishOutcome.of(PublishOutcome.Status.NACK, e.getClass().getSimpleName() + ": " + text.trim());
        } catch (MQBrokerException e) {
            return PublishOutcome.of(PublishOutcome.Status.NACK,
                    "broker rejected: " + e.getResponseCode() + " " + e.getErrorMessage());
        } catch (RemotingException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // 结果未知：消息可能已进 Broker，必须允许重发，由消费端幂等处理。
            log.warn("发布结果未知，安排重试：eventId={}, error={}", message.getEventId(), e.toString());
            return PublishOutcome.of(PublishOutcome.Status.UNKNOWN, e.getClass().getSimpleName() + ": " + e.getMessage());
        } catch (Exception e) {
            log.error("订单取消消息发布异常：eventId={}, orderId={}", message.getEventId(), message.getOrderId(), e);
            return PublishOutcome.of(PublishOutcome.Status.ERROR, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }
}
