package com.macro.mall.portal.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.macro.mall.portal.dao.OmsOrderMessageDao;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.domain.OrderCancelMessage;
import com.macro.mall.portal.domain.PublishOutcome;
import com.macro.mall.portal.service.OmsOrderMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * B 阶段：本地消息表的到期发送任务。
 *
 * <p>流程：扫描到期候选 → <strong>短事务条件领取（提交）</strong> → 网络发布并等待
 * Confirm/Return → 独立事务回写 SENT 或退避重试。领取后不持有数据库事务等待网络，
 * 避免 MQ 抖动拖住数据库连接与行锁。</p>
 *
 * <p>多实例安全：领取用 {@code attempt_version} 做 CAS；若某实例领取后崩溃，
 * 租约到期后记录会重新进入候选并被再次领取。结果回写也带版本号，
 * 因此旧尝试的迟到回调无法覆盖新尝试。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mall.order.outbox.dispatch", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class OmsOrderMessageDispatcher {

    /** 按实例标识隔离「同一实例不重入」，不同实例可以并发领取，由 CAS 裁决。 */
    private final Set<String> activeOwners = ConcurrentHashMap.newKeySet();

    @Autowired
    private OmsOrderMessageDao messageDao;
    @Autowired
    private OmsOrderMessageService messageService;
    @Autowired
    private OrderCancelPublisher publisher;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${mall.order.outbox.dispatch.batch-size:100}")
    private int batchSize;
    @Value("${mall.order.outbox.dispatch.lease-seconds:30}")
    private int leaseSeconds;
    @Value("${mall.order.outbox.dispatch.confirm-timeout-ms:5000}")
    private long confirmTimeoutMs;
    @Value("${mall.order.outbox.dispatch.max-retry:5}")
    private int maxRetry;
    @Value("${mall.order.outbox.dispatch.retry-base-seconds:5}")
    private int retryBaseSeconds;
    @Value("${mall.order.outbox.dispatch.retry-max-seconds:300}")
    private int retryMaxSeconds;

    private final String instanceOwner = buildOwner();

    @Scheduled(initialDelayString = "${mall.order.outbox.dispatch.initial-delay-ms:3000}",
            fixedDelayString = "${mall.order.outbox.dispatch.interval-ms:5000}")
    public void dispatch() {
        dispatchOnce();
    }

    /** 执行一轮投递，返回成功标记为 SENT 的消息数。供定时任务与集成测试调用。 */
    public int dispatchOnce() {
        return dispatchOnce(instanceOwner);
    }

    /**
     * 允许显式指定实例标识，用于多实例并发领取测试。
     * 同一时刻本实例只允许一轮派发，避免定时任务与手动调用叠加。
     */
    public int dispatchOnce(String owner) {
        if (!activeOwners.add(owner)) {
            return 0;
        }
        try {
            return doDispatch(owner);
        } catch (Exception e) {
            log.error("本地消息派发轮次异常，下一轮继续：owner={}", owner, e);
            return 0;
        } finally {
            activeOwners.remove(owner);
        }
    }

    private int doDispatch(String owner) {
        Date now = new Date();
        List<OmsOrderMessage> candidates = messageDao.listDispatchable(now, batchSize);
        if (candidates.isEmpty()) {
            return 0;
        }
        Date leaseUntil = new Date(now.getTime() + Math.max(1, leaseSeconds) * 1000L);
        int sent = 0;
        for (OmsOrderMessage candidate : candidates) {
            OmsOrderMessage claimed = messageService.claim(
                    candidate.getId(), candidate.getAttemptVersion(), owner, leaseUntil, now);
            if (claimed == null) {
                // 另一个实例已抢先领取，或记录状态已变化。
                continue;
            }
            OrderCancelMessage message;
            try {
                message = objectMapper.readValue(claimed.getPayload(), OrderCancelMessage.class);
                if (!message.isValid()) {
                    throw new IllegalArgumentException("payload 字段校验不通过");
                }
            } catch (Exception e) {
                log.error("本地消息 payload 无法解析，安排重试：eventId={}", claimed.getEventId(), e);
                applyRetry(claimed, "PAYLOAD_INVALID: " + e.getMessage(), now);
                continue;
            }

            PublishOutcome outcome;
            try {
                outcome = publisher.publish(message, confirmTimeoutMs);
            } catch (Exception e) {
                // 发送端实现异常（连接池耗尽、序列化失败等）也必须落到重试记录，
                // 不能让整批派发直接中断、把已领取记录留在 SENDING。
                log.error("发布器抛出异常，本次尝试计入失败：eventId={}", claimed.getEventId(), e);
                applyRetry(claimed, "PUBLISH_EXCEPTION: " + e, now);
                continue;
            }
            if (outcome.isSuccess()) {
                boolean applied = messageService.markSent(claimed.getEventId(), claimed.getAttemptVersion());
                if (applied) {
                    sent++;
                } else {
                    // 租约过期后已被重新领取，本次是迟到的旧回调，不能覆盖新尝试状态。
                    log.warn("旧发送尝试的 Confirm 被忽略（版本已更新）：eventId={}, version={}",
                            claimed.getEventId(), claimed.getAttemptVersion());
                }
            } else {
                applyRetry(claimed, outcome.getStatus() + ": " + outcome.getDetail(), now);
            }
        }
        return sent;
    }

    private void applyRetry(OmsOrderMessage claimed, String reason, Date now) {
        int nextCount = (claimed.getRetryCount() == null ? 0 : claimed.getRetryCount()) + 1;
        long backoffSeconds = Math.min(retryMaxSeconds,
                (long) Math.max(1, retryBaseSeconds) << Math.min(nextCount - 1, 10));
        Date nextRetryTime = new Date(now.getTime() + backoffSeconds * 1000L);
        boolean applied = messageService.markRetry(
                claimed.getEventId(), claimed.getAttemptVersion(), reason, nextRetryTime, maxRetry);
        if (!applied) {
            log.warn("旧发送尝试的失败回写被忽略（版本已更新）：eventId={}", claimed.getEventId());
            return;
        }
        if (nextCount >= maxRetry) {
            log.error("本地消息发布失败告警：达到重试上限已置 FAILED，需人工排查/重试。"
                            + " eventId={}, orderId={}, retryCount={}, reason={}",
                    claimed.getEventId(), claimed.getOrderId(), nextCount, reason);
        } else {
            log.warn("本地消息发布失败，安排退避重试：eventId={}, retryCount={}, nextRetryTime={}, reason={}",
                    claimed.getEventId(), nextCount, nextRetryTime, reason);
        }
    }

    private static String buildOwner() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + ":" + ProcessHandle.current().pid();
    }
}
