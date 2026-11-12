package com.macro.mall.portal.component;

import com.macro.mall.portal.service.OmsOrderMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * B 阶段：消息链路指标与告警日志。
 *
 * <p>至少覆盖：待发送消息数、最老消息年龄、发布失败数、消费失败数、
 * 超时但仍未取消的订单数、缺少固定到期时间的待支付订单数。</p>
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "mall.order.outbox.metrics", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class OmsOrderMessageMetricsLogger {

    @Autowired
    private OmsOrderMessageService messageService;

    @Value("${mall.order.outbox.metrics.pending-warn-threshold:100}")
    private long pendingWarnThreshold;
    @Value("${mall.order.outbox.metrics.oldest-pending-warn-seconds:300}")
    private long oldestPendingWarnSeconds;

    @Scheduled(initialDelayString = "${mall.order.outbox.metrics.initial-delay-ms:10000}",
            fixedDelayString = "${mall.order.outbox.metrics.interval-ms:60000}")
    public void report() {
        try {
            Map<String, Object> m = messageService.metrics();
            log.info("订单消息链路指标：PENDING={}, SENDING={}, SENT={}, FAILED={}, 最老待处理={}s, "
                            + "消费失败={}, 超时未取消={}, 待支付缺到期时间={}, 回填待处理={}",
                    num(m, "pending_messages"), num(m, "sending_messages"), num(m, "sent_messages"),
                    num(m, "failed_messages"), num(m, "oldest_pending_seconds"),
                    num(m, "consume_failures"), num(m, "expired_uncancelled"),
                    num(m, "pending_without_expire_time"), num(m, "backfill_exceptions"));

            if (num(m, "failed_messages") > 0 || num(m, "consume_failures") > 0
                    || num(m, "backfill_exceptions") > 0 || num(m, "pending_without_expire_time") > 0) {
                log.error("订单消息链路告警：存在 FAILED / 消费失败 / 历史数据待处理记录，请查询 "
                        + "oms_order_message、oms_order_consume_failure、oms_order_expire_backfill_exception");
            }
            if (num(m, "pending_messages") > pendingWarnThreshold
                    || num(m, "oldest_pending_seconds") > oldestPendingWarnSeconds) {
                log.error("订单消息链路告警：待发送积压或最老消息年龄超阈值（pending={}, oldest={}s）",
                        num(m, "pending_messages"), num(m, "oldest_pending_seconds"));
            }
            if (num(m, "expired_uncancelled") > 0) {
                log.error("订单消息链路告警：{} 笔订单已到期但仍待支付，超时扫描/消费链路需要排查",
                        num(m, "expired_uncancelled"));
            }
        } catch (Exception e) {
            log.error("订单消息链路指标采集失败", e);
        }
    }

    private long num(Map<String, Object> m, String key) {
        Object v = m.get(key);
        return v instanceof Number n ? n.longValue() : 0L;
    }
}
