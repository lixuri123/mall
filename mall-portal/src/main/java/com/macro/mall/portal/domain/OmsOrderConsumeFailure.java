package com.macro.mall.portal.domain;

import lombok.Data;

import java.util.Date;

/** B 阶段：消费失败（毒消息 / 超过消费重试上限）的可追踪记录。 */
@Data
public class OmsOrderConsumeFailure {
    private Long id;
    private String eventId;
    private Long orderId;
    private String rawPayload;
    private String reason;
    private Integer retryCount;
    private Integer handleStatus;
    private Date createdAt;
}
