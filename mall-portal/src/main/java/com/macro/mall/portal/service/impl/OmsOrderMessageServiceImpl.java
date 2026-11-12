package com.macro.mall.portal.service.impl;

import com.macro.mall.portal.dao.OmsOrderMessageDao;
import com.macro.mall.portal.domain.OmsOrderConsumeFailure;
import com.macro.mall.portal.domain.OmsOrderMessage;
import com.macro.mall.portal.exception.OrderConsistencyException;
import com.macro.mall.portal.service.OmsOrderMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * B 阶段：Outbox 状态流转实现。
 *
 * <p>关键不变量：</p>
 * <ol>
 *   <li>领取与结果回写都以 {@code attempt_version} 做条件，旧尝试的迟到回调不能覆盖新尝试。</li>
 *   <li>「Confirm 成功 → 更新 SENT」之间进程崩溃时，消息仍是 SENDING，
 *       租约过期后会被重新领取并重发；重复投递由消费端状态条件更新兜住。</li>
 *   <li>失败达到上限置 FAILED 并输出 ERROR 告警，可由人工重试恢复。</li>
 * </ol>
 */
@Slf4j
@Service
public class OmsOrderMessageServiceImpl implements OmsOrderMessageService {

    @Autowired
    private OmsOrderMessageDao messageDao;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void insertPendingForNewOrder(Long orderId, Date expireTime, String payload) {
        if (orderId == null || expireTime == null || payload == null) {
            throw new OrderConsistencyException("超时取消消息参数不完整，下单已回滚");
        }
        Date now = new Date();
        OmsOrderMessage message = new OmsOrderMessage();
        message.setEventId(OmsOrderMessage.eventIdOf(orderId));
        message.setOrderId(orderId);
        message.setEventType(OmsOrderMessage.EVENT_TYPE_TIMEOUT_CANCEL);
        message.setPayload(payload);
        message.setDueTime(expireTime);
        message.setStatus(OmsOrderMessage.STATUS_PENDING);
        message.setRetryCount(0);
        // 未到期的消息不会被发送任务领取。
        message.setNextRetryTime(expireTime.after(now) ? expireTime : now);
        message.setAttemptVersion(0L);
        message.setCreatedAt(now);
        message.setUpdatedAt(now);
        int rows = messageDao.insert(message);
        if (rows != 1) {
            throw new OrderConsistencyException("超时取消消息写入失败（orderId=" + orderId + "），下单已回滚");
        }
        log.info("订单与超时取消消息同事务写入：orderId={}, eventId={}, expireTime={}",
                orderId, message.getEventId(), expireTime);
    }

    @Override
    public OmsOrderMessage findByEventId(String eventId) {
        return messageDao.selectByEventId(eventId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public OmsOrderMessage claim(Long id, Long version, String owner, Date leaseUntil, Date now) {
        int rows = messageDao.claim(id, version, owner, leaseUntil, now);
        if (rows != 1) {
            return null;
        }
        // 重新读取拿到 attempt_version + 1 后的状态，作为后续回写的版本凭据。
        return messageDao.selectById(id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean markSent(String eventId, Long version) {
        return messageDao.markSent(eventId, version) == 1;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean markRetry(String eventId, Long version, String error, Date nextRetryTime, Integer maxRetry) {
        String truncated = error == null ? "unknown" : error.length() > 900 ? error.substring(0, 900) : error;
        return messageDao.markRetry(eventId, version, truncated, nextRetryTime, maxRetry) == 1;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public int rescheduleToOrderExpireTime(String eventId) {
        return messageDao.rescheduleToOrderExpireTime(eventId);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public boolean manualRetry(String eventId) {
        return messageDao.resetFailedForManualRetry(eventId, new Date()) == 1;
    }

    @Override
    public List<OmsOrderMessage> listFailed(int limit) {
        return messageDao.listFailed(limit);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void recordConsumeFailure(String eventId, Long orderId, String rawPayload,
                                     String reason, int retryCount) {
        OmsOrderConsumeFailure failure = new OmsOrderConsumeFailure();
        failure.setEventId(eventId);
        failure.setOrderId(orderId);
        failure.setRawPayload(rawPayload == null ? null
                : (rawPayload.length() > 60000 ? rawPayload.substring(0, 60000) : rawPayload));
        failure.setReason(reason == null ? "unknown" : (reason.length() > 900 ? reason.substring(0, 900) : reason));
        failure.setRetryCount(retryCount);
        failure.setCreatedAt(new Date());
        messageDao.insertConsumeFailure(failure);
    }

    @Override
    public int countConsumeFailures(String eventId) {
        if (eventId == null) {
            return 0;
        }
        return messageDao.countConsumeFailures(eventId);
    }

    @Override
    public Map<String, Object> metrics() {
        return messageDao.selectMetrics();
    }
}
