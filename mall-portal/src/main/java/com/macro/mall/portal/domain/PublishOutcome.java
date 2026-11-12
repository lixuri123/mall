package com.macro.mall.portal.domain;

import lombok.Getter;

/**
 * B 阶段：一次 RabbitMQ 发布的判定结果。
 *
 * <p>Confirm ACK 只说明 Broker 收到了消息，<strong>不说明消息可路由</strong>，
 * 也完全不说明消费者处理成功。因此 UNROUTABLE 必须单独识别。</p>
 */
@Getter
public class PublishOutcome {

    public enum Status {
        /** Confirm ACK 且未被 Return：发布阶段成功。 */
        CONFIRMED,
        /** Confirm NACK：Broker 明确未接受本次发布。 */
        NACK,
        /** mandatory 消息被 Return：不可路由，不能仅凭 ACK 判成功。 */
        UNROUTABLE,
        /** Confirm 超时 / 连接中断：结果未知，按「可能已发送」处理并重试。 */
        UNKNOWN,
        /** 发送本身抛出异常。 */
        ERROR
    }

    private final Status status;
    private final String detail;

    private PublishOutcome(Status status, String detail) {
        this.status = status;
        this.detail = detail;
    }

    public static PublishOutcome confirmed() {
        return new PublishOutcome(Status.CONFIRMED, "publisher confirm ack and no return");
    }

    public static PublishOutcome of(Status status, String detail) {
        return new PublishOutcome(status, detail == null ? "" : detail);
    }

    public boolean isSuccess() {
        return status == Status.CONFIRMED;
    }
}
