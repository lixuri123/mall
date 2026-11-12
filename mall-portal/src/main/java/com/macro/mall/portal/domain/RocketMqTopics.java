package com.macro.mall.portal.domain;

/**
 * RocketMQ 消息属性名常量（B 阶段 MQ 适配层）。
 *
 * <p>Topic / Group 名称一律从 {@code mall.order.outbox.*} 配置读取，
 * 这里只放跨发送端与消费端共享的属性名，避免出现两份不一致的字面量。</p>
 */
public final class RocketMqTopics {

    /** 消费端已重试次数，随每次受控重发递增。 */
    public static final String RETRY_COUNT_PROPERTY = "x-retry-count";
    /** 全局唯一事件 ID，所有重试保持不变。 */
    public static final String EVENT_ID_PROPERTY = "x-event-id";
    /** 消息 Tag，用于区分正常投递与死信留痕。 */
    public static final String TAG_CANCEL = "CANCEL";
    public static final String TAG_DLQ = "DLQ";

    private RocketMqTopics() {
    }
}
