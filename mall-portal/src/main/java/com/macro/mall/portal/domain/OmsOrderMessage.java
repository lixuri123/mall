package com.macro.mall.portal.domain;

import lombok.Data;

import java.util.Date;

/**
 * B 阶段：本地消息表（Outbox）记录。
 *
 * <p>一条记录对应「一笔订单唯一的超时取消任务」。{@code status} 含义：</p>
 * <ul>
 *   <li>PENDING：等待到期发送（或等待下次重试时间）。</li>
 *   <li>SENDING：已被某个发送实例领取，持有租约。</li>
 *   <li>SENT：发布阶段被 Confirm ACK 且未被 Return。<strong>不代表订单已取消</strong>。</li>
 *   <li>FAILED：连续失败达到上限，等待人工排查/重试。</li>
 * </ul>
 */
@Data
public class OmsOrderMessage {

    private Long id;
    private String eventId;
    private Long orderId;
    private String eventType;
    private String payload;
    private Date dueTime;
    private String status;
    private Integer retryCount;
    private Date nextRetryTime;
    private String leaseOwner;
    private Date leaseUntil;
    private Long attemptVersion;
    private String lastError;
    private Date createdAt;
    private Date updatedAt;

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENDING = "SENDING";
    public static final String STATUS_SENT = "SENT";
    public static final String STATUS_FAILED = "FAILED";

    public static final String EVENT_TYPE_TIMEOUT_CANCEL = "ORDER_TIMEOUT_CANCEL";

    public static String eventIdOf(Long orderId) {
        return "OTC-" + orderId;
    }
}
