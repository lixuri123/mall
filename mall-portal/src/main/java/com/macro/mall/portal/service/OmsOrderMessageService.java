package com.macro.mall.portal.service;

import com.macro.mall.portal.domain.OmsOrderMessage;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * B 阶段：本地消息表（Outbox）服务。
 *
 * <p>每个方法都是一个短事务。发送任务在<strong>领取事务提交之后</strong>才进行网络发布，
 * 不会持有数据库事务等待 RabbitMQ Confirm；状态回写再用独立事务完成。</p>
 */
public interface OmsOrderMessageService {

    /** 在下单事务内插入 PENDING 消息；调用方事务回滚时消息一起回滚。 */
    void insertPendingForNewOrder(Long orderId, Date expireTime, String payload);

    OmsOrderMessage findByEventId(String eventId);

    /** 条件领取（CAS）。成功返回带新 attempt_version 的记录，失败返回 null。 */
    OmsOrderMessage claim(Long id, Long version, String owner, Date leaseUntil, Date now);

    /** Confirm ACK 且未 Return。返回 false 表示本次尝试已被更新的领取取代。 */
    boolean markSent(String eventId, Long version);

    /** 发布失败退避重试，达到上限自动转 FAILED。返回 false 表示旧尝试被忽略。 */
    boolean markRetry(String eventId, Long version, String error, Date nextRetryTime, Integer maxRetry);

    /** 提前投递：按订单固定到期时间重新排队。 */
    int rescheduleToOrderExpireTime(String eventId);

    /** 人工重试 FAILED 记录。 */
    boolean manualRetry(String eventId);

    List<OmsOrderMessage> listFailed(int limit);

    void recordConsumeFailure(String eventId, Long orderId, String rawPayload, String reason, int retryCount);

    int countConsumeFailures(String eventId);

    /** 指标与告警数据。 */
    Map<String, Object> metrics();
}
