package com.macro.mall.portal.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.RocketMqTopics;
import com.macro.mall.portal.service.OmsOrderMessageService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * B 阶段（RocketMQ 版）：订单超时取消消息消费者。
 *
 * <p><strong>「事务提交后才确认」在 RocketMQ 下的实现</strong>：PushConsumer 没有
 * {@code basicAck(deliveryTag)}，监听器返回值就是确认。业务全部在
 * {@code OrderStateTransitionService} 的 Spring 事务里完成，
 * {@code cancelBySystem()} 返回时事务已经提交；此后才返回 {@code CONSUME_SUCCESS}。
 * 因此「提交 → 确认」的因果顺序与 RabbitMQ 版一致，只是确认动作从显式 ACK
 * 变成了返回值。若提交成功但返回前进程退出，Broker 会重投，
 * 再次处理时 {@code 0 → 4} 条件更新影响 0 行，不会重复释放库存/券/积分。</p>
 *
 * <p>失败治理刻意不使用 Broker 内建的 {@code %RETRY%group} 自动重试：
 * 那个通道的重试次数、延迟和失败明细都不可控。这里沿用业务层受控重试——
 * 把消息带上 {@code x-retry-count} 和一个<strong>固定延迟级别</strong>发到独立重试 Topic，
 * 到期后由同一个消费者组消费；超限或毒消息则由我们主动投递到 DLQ Topic 并落
 * {@code oms_order_consume_failure}，保留原始消息体与失败原因。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mall.order.outbox.consume", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class CancelOrderReceiver {

    @Autowired
    private OmsPortalOrderService portalOrderService;
    @Autowired
    private OmsOrderMessageService messageService;
    @Autowired
    private DefaultMQProducer orderCancelProducer;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mall.order.outbox.name-server}")
    private String nameServer;
    @Value("${mall.order.outbox.consumer-group}")
    private String consumerGroup;
    @Value("${mall.order.outbox.topic}")
    private String mainTopic;
    @Value("${mall.order.outbox.retry-topic}")
    private String retryTopic;
    @Value("${mall.order.outbox.dlq-topic}")
    private String dlqTopic;
    @Value("${mall.order.outbox.consume.max-retry:3}")
    private int maxRetry;
    @Value("${mall.order.outbox.consume.retry-delay-level:2}")
    private int retryDelayLevel;

    private DefaultMQPushConsumer consumer;

    @PostConstruct
    public void start() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        // 同一个测试 JVM 里可能同时存在多个 Spring 上下文，默认 instanceName 会让
        // 它们共用同一个 MQClientInstance，造成同组客户端互相顶掉。
        consumer.setInstanceName("order-cancel-consumer-" + java.util.UUID.randomUUID());
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(4);
        // 一个消费者组同时订阅主 Topic 与固定延迟重试 Topic。
        consumer.subscribe(mainTopic, "*");
        consumer.subscribe(retryTopic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> consume(msgs));
        consumer.start();
        log.info("订单取消消费者已启动：group={}, mainTopic={}, retryTopic={}, dlqTopic={}",
                consumerGroup, mainTopic, retryTopic, dlqTopic);
    }

    @PreDestroy
    public void stop() {
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    private ConsumeConcurrentlyStatus consume(List<MessageExt> msgs) {
        boolean needBrokerRetry = false;
        for (MessageExt msg : msgs) {
            if (handleOne(msg) == ConsumeConcurrentlyStatus.RECONSUME_LATER) {
                needBrokerRetry = true;
            }
        }
        // 绝大多数失败都已被业务层安置到 retry/DLQ Topic，可以确认掉。
        // 只有「连死信/重试都发不出去」才回退给 Broker 内建重试，避免静默丢消息。
        return needBrokerRetry
                ? ConsumeConcurrentlyStatus.RECONSUME_LATER
                : ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    private ConsumeConcurrentlyStatus handleOne(MessageExt msg) {
        String body = new String(msg.getBody(), StandardCharsets.UTF_8);
        int attempt = attemptOf(msg);

        // 1) 解析与结构校验：毒消息直接落库 + 死信，不进入业务与重试循环。
        final OrderCancelMessage command;
        try {
            command = objectMapper.readValue(body, OrderCancelMessage.class);
            if (command == null || !command.isValid()) {
                throw new IllegalArgumentException("消息字段校验不通过");
            }
        } catch (Exception e) {
            log.error("毒消息，写入失败记录并转入死信 Topic：body={}, error={}", body, e.toString());
            messageService.recordConsumeFailure(null, null, body, "POISON_MESSAGE: " + e, attempt);
            return sendToDlq(body, null, "POISON_MESSAGE") ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS
                    : ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        // 2) 业务处理：cancelBySystem 返回即代表数据库事务已提交，之后才确认消息。
        try {
            CancelResult result = portalOrderService.cancelBySystem(command.getOrderId());
            switch (result.getStatus()) {
                case NOT_TIMED_OUT -> {
                    int rows = messageService.rescheduleToOrderExpireTime(command.getEventId());
                    log.warn("取消消息提前到达，已按订单到期时间重新排队：eventId={}, orderId={}, rows={}",
                            command.getEventId(), command.getOrderId(), rows);
                }
                case REJECTED -> log.warn("系统取消被拒绝（终态或历史数据未回填），不重试：eventId={}, orderId={}, reason={}",
                        command.getEventId(), command.getOrderId(), result.getMessage());
                default -> log.info("订单取消消息处理完成：eventId={}, orderId={}, result={}",
                        command.getEventId(), command.getOrderId(), result.getStatus());
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        } catch (Exception e) {
            log.error("订单取消消息业务处理失败：eventId={}, orderId={}, attempt={}",
                    command.getEventId(), command.getOrderId(), attempt, e);
            if (attempt < maxRetry) {
                return republishToRetryTopic(body, command.getEventId(), attempt + 1)
                        ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS
                        : ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            messageService.recordConsumeFailure(command.getEventId(), command.getOrderId(),
                    body, "MAX_CONSUME_RETRY_EXCEEDED: " + e, attempt);
            return sendToDlq(body, command.getEventId(), "MAX_CONSUME_RETRY_EXCEEDED")
                    ? ConsumeConcurrentlyStatus.CONSUME_SUCCESS
                    : ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    /** 固定延迟级别：所有重试消息延迟一致，不存在按消息 TTL 造成的队头阻塞。 */
    private boolean republishToRetryTopic(String body, String eventId, int nextAttempt) {
        try {
            Message retry = new Message(retryTopic, RocketMqTopics.TAG_CANCEL, eventId,
                    body.getBytes(StandardCharsets.UTF_8));
            retry.setDelayTimeLevel(retryDelayLevel);
            retry.putUserProperty(RocketMqTopics.RETRY_COUNT_PROPERTY, String.valueOf(nextAttempt));
            retry.putUserProperty(RocketMqTopics.EVENT_ID_PROPERTY, eventId);
            orderCancelProducer.send(retry);
            log.info("取消消息转入受控重试：eventId={}, nextAttempt={}, delayLevel={}",
                    eventId, nextAttempt, retryDelayLevel);
            return true;
        } catch (Exception e) {
            log.error("重试消息投递失败，回退给 Broker 内建重试：eventId={}", eventId, e);
            return false;
        }
    }

    private boolean sendToDlq(String body, String eventId, String reason) {
        try {
            String safeEventId = eventId == null ? "POISON" : eventId;
            Message dlq = new Message(dlqTopic, RocketMqTopics.TAG_DLQ, safeEventId,
                    body.getBytes(StandardCharsets.UTF_8));
            dlq.putUserProperty(RocketMqTopics.EVENT_ID_PROPERTY, safeEventId);
            dlq.putUserProperty("x-failure-reason", reason);
            orderCancelProducer.send(dlq);
            return true;
        } catch (Exception e) {
            log.error("死信消息投递失败：eventId={}, reason={}", eventId, reason, e);
            return false;
        }
    }

    private int attemptOf(MessageExt msg) {
        String raw = msg.getUserProperty(RocketMqTopics.RETRY_COUNT_PROPERTY);
        if (raw == null || raw.isBlank()) {
            return 0;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
