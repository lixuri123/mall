package com.macro.mall.portal.dao;

import com.macro.mall.portal.domain.OmsOrderConsumeFailure;
import com.macro.mall.portal.domain.OmsOrderMessage;
import org.apache.ibatis.annotations.Param;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * B 阶段：本地消息表（Outbox）自定义 DAO。
 *
 * <p>不使用 MyBatis Generator 生成的代码作为扩展点，避免重新生成时被覆盖。
 * 所有状态流转都是<strong>带条件的单条 UPDATE</strong>，服务层检查影响行数：</p>
 * <ul>
 *   <li>{@code claim} 用 {@code attempt_version} 做 CAS，多实例只有一个能领取成功；</li>
 *   <li>{@code markSent} / {@code markRetry} 必须匹配本次领取的版本，
 *       迟到的旧回调无法覆盖已经重新领取的新尝试；</li>
 *   <li>租约过期的 SENDING 记录会被重新领取，用于处理「发布后、标记前进程崩溃」。</li>
 * </ul>
 */
public interface OmsOrderMessageDao {

    int insert(OmsOrderMessage record);

    OmsOrderMessage selectByEventId(@Param("eventId") String eventId);

    OmsOrderMessage selectById(@Param("id") Long id);

    List<OmsOrderMessage> listFailed(@Param("limit") Integer limit);

    /** 到期且可领取的候选：PENDING 到点，或 SENDING 租约已过期。 */
    List<OmsOrderMessage> listDispatchable(@Param("now") Date now, @Param("limit") Integer limit);

    /** 条件领取（CAS）。返回 1 表示本次领取成功并已递增 attempt_version。 */
    int claim(@Param("id") Long id,
              @Param("version") Long version,
              @Param("owner") String owner,
              @Param("leaseUntil") Date leaseUntil,
              @Param("now") Date now);

    /** Confirm ACK 且未 Return：标记发布阶段成功，必须匹配领取版本。 */
    int markSent(@Param("eventId") String eventId, @Param("version") Long version);

    /** 发布失败：退避重试；达到上限自动转 FAILED。 */
    int markRetry(@Param("eventId") String eventId,
                  @Param("version") Long version,
                  @Param("error") String error,
                  @Param("nextRetryTime") Date nextRetryTime,
                  @Param("maxRetry") Integer maxRetry);

    /** 消息提前到达：按订单数据库中的固定到期时间重新排队，不直接丢弃。 */
    int rescheduleToOrderExpireTime(@Param("eventId") String eventId);

    /** 人工重试 FAILED 记录。 */
    int resetFailedForManualRetry(@Param("eventId") String eventId,
                                  @Param("nextRetryTime") Date nextRetryTime);

    int insertConsumeFailure(OmsOrderConsumeFailure record);

    int countConsumeFailures(@Param("eventId") String eventId);

    /** 指标：待发送数、最老消息年龄、失败数、超时未取消订单数等。 */
    Map<String, Object> selectMetrics();
}
