package com.macro.mall.portal.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.portal.domain.CancelResult;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.QueueEnum;
import com.macro.mall.portal.service.OmsOrderMessageService;
import com.macro.mall.portal.service.OmsPortalOrderService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * B 阶段：订单超时取消消息消费者（手动 ACK）。
 *
 * <p>消费者只负责接收与确认，业务在 {@code OrderStateTransitionService} 的事务里完成：</p>
 * <ul>
 *   <li>数据库事务提交成功后才 basicAck；提交成功但 ACK 前进程退出会重投，
 *       再次处理时订单已关闭，条件更新保证不会重复释放库存/券/积分。</li>
 *   <li>业务异常不 ACK 为成功：通过<strong>固定 TTL 重试队列</strong>做有限次受控重试，
 *       避免立即 requeue 形成高频循环；超限后记录失败并 nack 到死信队列。</li>
 *   <li>消息提前到达：不直接 ACK 丢弃，而是按订单数据库中的固定到期时间重新排队，
 *       并由超时扫描兜底。</li>
 *   <li>无法解析 / 字段非法的毒消息：保留原消息体与原因落库，nack 到死信队列，不无限重试。</li>
 * </ul>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mall.order.outbox.consume", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class CancelOrderReceiver {

    private static final String RETRY_HEADER = "x-retry-count";

    @Autowired
    private OmsPortalOrderService portalOrderService;
    @Autowired
    private OmsOrderMessageService messageService;
    @Autowired
    private RabbitTemplate rabbitTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mall.order.outbox.consume.max-retry:3}")
    private int maxRetry;

    @RabbitListener(queues = "mall.order.cancel", ackMode = "MANUAL")
    public void handle(Message message, Channel channel,
                       @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        int attempt = attemptOf(message);

        // 1) 解析与结构校验：毒消息直接落库 + 死信，不进入业务与重试循环。
        final OrderCancelMessage command;
        try {
            command = objectMapper.readValue(body, OrderCancelMessage.class);
            if (command == null || !command.isValid()) {
                throw new IllegalArgumentException("消息字段校验不通过");
            }
        } catch (Exception e) {
            log.error("毒消息，写入失败记录并转入死信队列：body={}, error={}", body, e.toString());
            messageService.recordConsumeFailure(null, null, body, "POISON_MESSAGE: " + e, attempt);
            channel.basicNack(deliveryTag, false, false);
            return;
        }

        // 2) 业务处理：事务提交后才 ACK。
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
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            log.error("订单取消消息业务处理失败：eventId={}, orderId={}, attempt={}",
                    command.getEventId(), command.getOrderId(), attempt, e);
            if (attempt < maxRetry) {
                republishToRetryQueue(message.getBody(), command.getEventId(), attempt + 1);
                channel.basicAck(deliveryTag, false);
            } else {
                messageService.recordConsumeFailure(command.getEventId(), command.getOrderId(),
                        body, "MAX_CONSUME_RETRY_EXCEEDED: " + e, attempt);
                // 进入队列声明的死信交换机，保留原消息，不 requeue 造成热循环。
                channel.basicNack(deliveryTag, false, false);
            }
        }
    }

    /** 固定 TTL 重试队列里所有消息延迟相同，不存在按消息 TTL 造成的队头阻塞。 */
    private void republishToRetryQueue(byte[] body, String eventId, int nextAttempt) {
        Message retry = MessageBuilder.withBody(body)
                .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                .setContentEncoding("UTF-8")
                .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                .setHeader(RETRY_HEADER, nextAttempt)
                .setHeader("x-event-id", eventId)
                .build();
        rabbitTemplate.send(QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getExchange(),
                QueueEnum.QUEUE_ORDER_CANCEL_RETRY.getRouteKey(), retry);
        log.info("取消消息转入受控重试：eventId={}, nextAttempt={}", eventId, nextAttempt);
    }

    private int attemptOf(Message message) {
        Object raw = message.getMessageProperties().getHeaders().get(RETRY_HEADER);
        if (raw instanceof Number number) {
            return number.intValue();
        }
        return raw == null ? 0 : Integer.parseInt(raw.toString());
    }
}
