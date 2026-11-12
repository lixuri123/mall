package com.macro.mall.portal.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.time.ZoneId;

/**
 * B 阶段：订单超时取消消息体。
 *
 * <p>只携带标识，不携带订单、金额或库存明细。消费者必须读取数据库中的当前订单、
 * 明细与固定到期时间作为唯一事实来源，因此消息重放不会改变业务判定。</p>
 *
 * <p>{@code dueTime} 仅用于追踪；<strong>是否超时以 oms_order.expire_time 为准</strong>。</p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderCancelMessage {

    public static final int SCHEMA_VERSION = 1;
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Integer schemaVersion;
    private String eventId;
    private String eventType;
    private Long orderId;
    /** 追踪用的到期时间字符串，格式 yyyy-MM-dd HH:mm:ss。 */
    private String dueTime;

    public static OrderCancelMessage of(Long orderId, Date dueTime) {
        String due = dueTime == null ? null
                : FORMATTER.format(LocalDateTime.ofInstant(dueTime.toInstant(), ZoneId.systemDefault()));
        return OrderCancelMessage.builder()
                .schemaVersion(SCHEMA_VERSION)
                .eventId(OmsOrderMessage.eventIdOf(orderId))
                .eventType(OmsOrderMessage.EVENT_TYPE_TIMEOUT_CANCEL)
                .orderId(orderId)
                .dueTime(due)
                .build();
    }

    /** 消息结构校验；不合法的消息属于毒消息，不能进入业务处理。 */
    public boolean isValid() {
        return schemaVersion != null && schemaVersion == SCHEMA_VERSION
                && eventId != null && !eventId.isBlank()
                && OmsOrderMessage.EVENT_TYPE_TIMEOUT_CANCEL.equals(eventType)
                && orderId != null && orderId > 0;
    }
}
