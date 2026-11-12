package com.macro.mall.portal.config;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ 生产者配置（B 阶段 MQ 适配层）。
 *
 * <p>只声明生产者；消费者在 {@code CancelOrderReceiver} 里创建，因为它需要注入业务服务，
 * 放在配置类会产生不必要的循环依赖。</p>
 *
 * <p>刻意关闭客户端内建的发送重试：本地消息表已经负责「失败 → 退避 → 重发」，
 * 若客户端再自动重试，一次 {@code publish()} 可能实际发出多条消息，
 * 也会让「发送失败」与「可能已发送」的边界变得模糊。</p>
 *
 * <p>{@code start()} 由 Spring 的 initMethod 调用。RocketMQ 客户端启动时不会强制要求
 * NameServer 可达，因此 Broker 停机期间应用上下文仍能正常启动，下单不受影响。</p>
 */
@Configuration
public class RocketMqConfig {

    @Value("${mall.order.outbox.name-server}")
    private String nameServer;

    @Value("${mall.order.outbox.producer-group}")
    private String producerGroup;

    @Value("${mall.order.outbox.dispatch.confirm-timeout-ms:5000}")
    private long sendTimeoutMs;

    @Bean(initMethod = "start", destroyMethod = "shutdown")
    public DefaultMQProducer orderCancelProducer() {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        // 同一 JVM 内可能并存多个 Spring 上下文（测试尤为明显）；默认 instanceName
        // 会让它们共用同一个 MQClientInstance，一个上下文 shutdown 会影响其它上下文。
        producer.setInstanceName("order-cancel-producer-" + java.util.UUID.randomUUID());
        producer.setNamesrvAddr(nameServer);
        producer.setSendMsgTimeout((int) Math.max(1000L, Math.min(sendTimeoutMs, Integer.MAX_VALUE)));
        producer.setRetryTimesWhenSendFailed(0);
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        return producer;
    }
}
