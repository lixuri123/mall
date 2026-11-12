# B：超时订单取消的消息可靠性（RabbitMQ 版 · 历史快照）

> **⚠️ 本文记录的是 B 阶段最初的 RabbitMQ 4.2.9 实现，不是当前代码。**
> 当前代码已按「换 MQ 不换架构」切换到 **RocketMQ 5.3.2**：
> 交换机 / 队列 / Publisher Confirm / mandatory Return / 手动 `basicAck` 等描述均已失效。
> **请以 [B_ROCKETMQ.md](B_ROCKETMQ.md) 与
> [B_ROCKETMQ_TEST_REPORT.md](B_ROCKETMQ_TEST_REPORT.md) 为准。**
> 本文保留是为了完整记录架构演进与 RabbitMQ 版的验收证据。
>
> 本目录是方案 B 的落地说明与实测证据。A 阶段（订单状态与库存一致性）见  
> [README.md](README.md) 与 [TEST\_REPORT.md](TEST_REPORT.md)。
>
> - B 改造说明：本文件
> - B 测试与验收报告：[B\_TEST\_REPORT.md](B_TEST_REPORT.md)
> - 原始输出：[results/b-test-run-2026-11-12.txt](results/b-test-run-2026-11-12.txt)
> - 已有库升级：[B 预检](../sql/mall_b_precheck.sql) → [B 迁移](../sql/mall_b_migration.sql)
>
> **状态（历史）：RabbitMQ 版已在 `mall-local` 真实 MySQL 8.4.7 + RabbitMQ 4.2.9 上跑通，
> 并已提交（`1f55505 message reliability`）；随后 MQ 被替换为 RocketMQ 5.3.2。
> 所有结论以当时实际完成的范围为准。**

---

## 1. 要解决的问题与设计边界

A 阶段把库存和订单状态放在同一个数据库事务里，但超时取消消息走的是  
「事务提交后直接发一条带 TTL 的延迟消息」。它有四个缺口：

1. 数据库事务与 MQ 发布不是原子操作：事务提交后进程崩溃，消息永久丢失，  
   只能等定时扫描兜底；扫描任务也被停掉时，锁定库存会长期不释放。
2. 每条消息 TTL 不同的延迟队列存在**队头阻塞**：RabbitMQ 只在队头检查 TTL，  
   队头那条 120 分钟的消息会压住后面 1 分钟就该过期的消息。
3. 没有 Publisher Confirm / mandatory Return，发送失败无法区分「Broker 拒收」与  
   「不可路由」，也就无法判断能不能把消息标记为已发送。
4. 消费者自动 ACK、异常吞掉，业务事务是否提交与 ACK 之间没有因果关系。

B 采用用户指定的链路：

```text
下单事务写 订单 + 固定到期时间 + PENDING 本地消息
        │
        ├─ 到期投递任务：租约 CAS 领取 → 发普通 MQ → Confirm + Return 判定 → 回写 SENT / 退避重试 / FAILED
        │
        ├─ 消费者：手动 ACK → 调统一取消服务（数据库事务）→ 提交成功后才 basicAck
        │                    └ 业务失败 → 固定 TTL 重试队列（有限次）→ 超限进 DLQ + 失败记录
        │
        └─ 双层兜底：消息表扫描（解决「没发出去」） + 订单表按 expire_time 扫描（解决「发了却没取消」）
```

**目标**：MQ 暂时不可用、服务重启、消息重复投递时，超时订单最终被处理，且库存最多释放一次。  
**不承诺**：恰好一次投递（本方案是至少一次 + 业务幂等）、毫秒级准时取消、  
自动关闭第三方支付、自动退款。

## 2. 数据库改动

| 对象                                    | 改动                                         | 作用                                                                                        |
| ------------------------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `oms_order.expire_time`               | **新增** `datetime NULL`                     | 下单时按当时配置一次性算好并固化；后台改超时配置不影响已有订单                                                           |
| `idx_order_expire_scan`               | `(status, delete_status, id, expire_time)` | keyset 分页按 `id` 顺序走覆盖索引；列序刻意不用 `(…, expire_time, id)`，否则 `ORDER BY id` 会 `Using filesort` |
| `oms_order_message`                   | **新增**                                     | 本地消息表（Outbox），`uk_event_id` + `uk_order_event` 防重复                                        |
| `oms_order_consume_failure`           | **新增**                                     | 毒消息 / 超过消费重试上限的原始消息与原因，`handle_status=0` 待人工处理                                            |
| `oms_order_expire_backfill_exception` | **新增**                                     | 历史订单 `create_time IS NULL` 等无法推断到期时间的待处理记录                                                |

`oms_order_message` 的关键列：

```text
event_id         全局唯一（OTC-<orderId>），所有重试保持不变
payload          JSON，只放标识（schemaVersion / eventId / eventType / orderId / dueTime），不放库存明细
due_time         与 oms_order.expire_time 一致；dueTime 字段仅用于追踪，最终以数据库为准
status           PENDING → SENDING → SENT；失败回 PENDING 或置 FAILED
retry_count      连续发布失败次数，达到 max-retry 置 FAILED 并告警
next_retry_time  指数退避后的下次可领取时间
lease_owner      当前领取实例（host:pid）
lease_until      租约截止时间；进程崩溃后过期即可被重新领取
attempt_version  领取时 CAS 递增；旧尝试的迟到 Confirm/失败回写无法覆盖新尝试
```

新环境直接用 `document/sql/mall.sql`（已内置全部 B 结构）。  
已有库按顺序执行：`mall_a_precheck.sql` → `mall_a_migration.sql` → `mall_b_precheck.sql` → `mall_b_migration.sql`。  
B 迁移脚本第 0 节有守卫：A 阶段结构缺失时以 `SQLSTATE 45000` 中止。

历史数据回填规则：`expire_time = create_time + @legacy_overtime`，  
`@legacy_overtime` 必须由业务按**历史实际配置**确认（脚本默认取当前 `oms_order_setting` 作为起点）。  
`create_time IS NULL` 的订单**不猜测**，写入 `oms_order_expire_backfill_exception` 等待人工处理；  
已存在消息（含 `FAILED`）用 `NOT EXISTS` 跳过而不是 `INSERT IGNORE`，避免吞掉字段异常。

## 3. 改了哪些文件

| 文件                                                                                       | 改动                                                                                                                                                                      |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `document/sql/mall.sql`                                                                  | 新增 `expire_time`、扫描索引、3 张 B 表；为 65 条无列名位置插入的订单种子补 `NULL`                                                                                                                |
| `document/sql/mall_b_precheck.sql`                                                       | **新增**只读预检，兼容「列/表尚不存在」的旧库（基于 `information_schema` 动态拼 SQL）                                                                                                              |
| `document/sql/mall_b_migration.sql`                                                      | **新增**幂等迁移 + A 阶段守卫 + 历史回填 + 补 `PENDING` 消息 + 迁移后校验                                                                                                                     |
| `document/docker/docker-compose-local.yml`、`LOCAL.md`                                    | MySQL 增加 `--default-time-zone=+08:00` / `TZ: Asia/Shanghai`，与 JDBC `serverTimezone=Asia/Shanghai` 对齐                                                                    |
| `dao/PortalOrderDao.java` + `dao/PortalOrderDao.xml`                                     | 新增「下单同事务写到期时间」「读到固定到期时间」「按 `expire_time <= NOW()` + `id > afterId` 分页扫描」                                                                                                |
| `service/impl/OmsPortalOrderServiceImpl.java`                                            | 下单计算并写入 `expire_time`、在**同一事务末尾**插入 `PENDING` 消息；移除 `CancelOrderSender` 注入、`afterCommit` 发 MQ 路径；`cancelTimeOutOrder()` 改按固定到期时间分页；`cancelBySystem()` 返回 `CancelResult` |
| `service/impl/OrderStateTransitionServiceImpl.java`                                      | 系统取消不再用 `create_time + 当前配置` 重算超时，改为读 `oms_order.expire_time`；为 `NULL` 时**拒绝取消并告警**，不猜测                                                                                 |
| `service/OmsPortalOrderService.java`、`domain/CancelCommand.java`                         | 接口返回 `CancelResult`；`overtimeMinutes` 标 `@Deprecated`                                                                                                                   |
| `domain/QueueEnum.java`                                                                  | 移除 A 的 TTL 延迟队列；新增主队列、DLQ、固定 TTL 重试队列                                                                                                                                   |
| `config/RabbitMqConfig.java`                                                             | 主队列配 DLX；重试队列统一固定 TTL 后转发回主交换机；全部持久化                                                                                                                                    |
| `component/OrderCancelPublisher.java`                                                    | **新增**发布抽象 `PublishOutcome publish(OrderCancelMessage, long)`，便于测试注入故障发布器                                                                                               |
| `component/CancelOrderSender.java`                                                       | 重写：持久化消息 + `CorrelationData(eventId)` 关联 Confirm + `mandatory` Return + 超时等待，返回 `CONFIRMED / NACK / UNROUTABLE / UNKNOWN / ERROR`                                       |
| `component/OmsOrderMessageDispatcher.java`                                               | **新增**到期投递：短事务领取（提交）→ 网络发布 → 独立事务回写；版本 CAS、租约、退避、FAILED 告警                                                                                                              |
| `component/CancelOrderReceiver.java`                                                     | 重写：`ackMode = MANUAL`；事务提交后才 `basicAck`；毒消息落库 + DLQ；业务失败走固定 TTL 有限重试；提前投递按数据库到期时间重新排队                                                                                   |
| `component/OrderTimeOutCancelTask.java`                                                  | 保留 A 的定时扫描兜底，判定条件改为固定 `expire_time`                                                                                                                                     |
| `component/OmsOrderMessageMetricsLogger.java`                                            | **新增**指标与 ERROR 级告警                                                                                                                                                     |
| `controller/OmsOrderMessageAdminController.java`                                         | **新增**运维接口，默认不注册（`admin-enabled=true` 才生效）                                                                                                                              |
| `controller/OmsPortalOrderDebugController.java`                                          | `/order/cancelOrder` 语义改为「给历史订单补本地消息」，不再直接发 MQ                                                                                                                          |
| `dao/OmsOrderMessageDao.java` + `resources/dao/OmsOrderMessageDao.xml`                   | **新增** Outbox 全部 SQL（显式 `resultMap`，项目未开启驼峰映射）                                                                                                                          |
| `service/OmsOrderMessageService.java` + `impl/…`                                         | **新增**短事务封装：插入、领取、回写、重排队、人工重试、失败记录、指标                                                                                                                                   |
| `domain/{OmsOrderMessage,OrderCancelMessage,OmsOrderConsumeFailure,PublishOutcome}.java` | **新增**状态模型、消息体、失败记录、发布结果                                                                                                                                                |
| `application.yml` / `application-dev.yml`                                                | Outbox 拓扑、派发/消费/指标参数；RabbitMQ `correlated` Confirm、`publisher-returns`、`mandatory`、`acknowledge-mode: manual`                                                           |
| `src/test/java/.../it/*`                                                                 | **新增** 3 个 B 测试类 + 1 个基类；A 的测试基类追加 `expire_time` 与消息表清理、关闭后台派发/消费/指标任务                                                                                                  |

## 4. 关键设计

### 4.1 固定到期时间是唯一权威

```java
Date expireTime = new Date(createTime.getTime()
        + orderSettings.get(0).getNormalOrderOvertime() * 60_000L);
orderMapper.insert(order);
if (portalOrderDao.updateOrderExpireTime(order.getId(), expireTime) != 1) {
    throw new OrderConsistencyException("订单固定到期时间写入失败，下单已回滚");
}
```

系统取消入口只读这个值：`expireTime == null` → `REJECTED` 并告警（等人工回填）；  
`expireTime > now` → `NOT_TIMED_OUT`。消息体里的 `dueTime` 只用于追踪，  
**不参与取消判定**，因此消息被篡改/重放/提前到达都不会提前释放库存。

### 4.2 下单事务内写消息，移除所有直发 MQ 路径

订单、明细、锁库存、券/积分、`expire_time`、`PENDING` 消息在**同一个数据库事务**里提交：

```java
String payload = objectMapper.writeValueAsString(OrderCancelMessage.of(order.getId(), expireTime));
messageService.insertPendingForNewOrder(order.getId(), expireTime, payload);
```

`TransactionSynchronization` / `afterCommit` 发送代码已删除。  
因此「订单存在但消息不存在」在本地事务层面不可能发生；  
MQ 不可用不会阻塞下单，消息留在 `PENDING` 由派发任务重试。  
反过来「消息存在但订单不存在」也不会发生（事务一起回滚）。

### 4.3 领取：租约 + 版本 CAS，网络调用不持有数据库事务

```sql
UPDATE oms_order_message
SET status='SENDING', lease_owner=#{owner}, lease_until=#{leaseUntil},
    attempt_version = attempt_version + 1, ...
WHERE id = #{id} AND attempt_version = #{version} AND due_time <= #{now}
  AND ( (status='PENDING' AND next_retry_time <= #{now})
     OR (status='SENDING' AND lease_until <= #{now}) );
```

- 领取是**短事务并立即提交**，随后才做网络发布，避免 MQ 抖动占住数据库连接与行锁。
- 多实例读到同一 `version` 时只有一个能更新成功；`markSent` / `markRetry` 都带  
  `event_id + status='SENDING' + attempt_version`，旧尝试的迟到回调写不进去。
- 领取后进程崩溃：租约到期后该行重新进入候选集（`idx_lease`），被再次领取。  
  这会产生重复投递，由消费端幂等兜住。

### 4.4 Confirm 与 Return 必须同时判

```java
CorrelationData correlation = new CorrelationData(eventId);
rabbitTemplate.convertAndSend(exchange, routingKey, payload,
        m -> markPersistent(m, eventId), correlation);
CorrelationData.Confirm confirm = correlation.getFuture().get(timeout, MILLISECONDS);
if (correlation.getReturned() != null || returned != null)  -> UNROUTABLE
else if (!confirm.isAck())                                  -> NACK
else                                                      -> CONFIRMED
```

`SENT` **只代表「发布阶段成功」**，不代表订单已取消。  
Confirm ACK 但 mandatory Return 命中未路由时仍判失败；  
Confirm 超时/无结果为 `UNKNOWN`，同样不能标 `SENT`，只能走退避重试。  
因此「不可路由的交换机配置错误」不会把消息永久误标为成功。

### 4.5 消费：提交后 ACK、幂等、受控重试、死信出口

```java
@RabbitListener(queues = "mall.order.cancel", ackMode = "MANUAL")
```

| 情况                    | 行为                                                                                                                                |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 取消业务事务提交成功            | `basicAck`；订单 `0 → 4` 条件更新 + 库存/券/积分在同一事务释放                                                                                       |
| 提交成功但 ACK 前进程退出       | Broker 重投；再次处理时 `0 → 4` 影响 0 行，返回 `ALREADY_CLOSED`，不会重复释放                                                                         |
| 重复投递 N 次              | 同样是条件更新幂等；实测 3 次投递 `stock=10, lock=0`                                                                                             |
| 订单已支付                 | 状态已是 1，条件更新 0 行，不释放库存                                                                                                             |
| 提前投递（`NOT_TIMED_OUT`） | **不直接 ACK 丢弃**：`rescheduleToOrderExpireTime()` JOIN `oms_order` 把消息重排到固定 `expire_time`，再由扫描兜底                                     |
| 业务异常                  | 转发到**固定 TTL** 重试队列并带 `x-retry-count`；`attempt < max-retry` 才重试，超限写 `oms_order_consume_failure` 并 `basicNack(requeue=false)` 进 DLQ |
| 毒消息（不可解析/字段非法）        | 保留原消息体与原因落库，直接 nack 进 DLQ，不进入业务和重试循环                                                                                              |

重试队列用**统一固定 TTL**（`consume.retry-ttl-ms`），所有重试消息延迟一致，  
不存在按消息 TTL 造成的队头阻塞；`default-requeue-rejected=false` + Spring retry 关闭，  
避免立即无限 requeue 形成热循环。

### 4.6 双层兜底

- **消息表扫描**（派发任务 + `FAILED` 人工重试）：解决「订单有了但消息没发出去 / 发出去失败」。
- **订单表扫描**（`OrderTimeOutCancelTask` → `cancelTimeOutOrder()`）：  
  按 `expire_time <= NOW()` 和 `id > afterId` keyset 分页逐单独立事务取消，  
  解决「消息已发布但消费链路整体失效」。两条路径最终都汇到同一个  
  `OrderStateTransitionService.cancelBySystem()`，因此不会有两套取消语义。

### 4.7 指标、告警与运维接口

`OmsOrderMessageMetricsLogger` 定时输出（超阈值转 ERROR）：  
`PENDING / SENDING / SENT / FAILED`、最老待处理消息年龄、消费失败数、  
超时未取消订单数、待支付缺到期时间数、历史回填待处理数。

`/order/message/*` 运维接口（`/metrics`、`/failed`、`/{eventId}`、  
`POST /{eventId}/retry`、`/sample`）默认**不注册**，  
只有 `mall.order.outbox.admin-enabled=true` 时才生效，避免把人工重试暴露给普通用户。

## 5. 配置

```yaml
# application.yml
mall:
  order:
    timeout-scan: { enabled: true, initial-delay-ms: 30000, interval-ms: 60000 }
    outbox:
      exchange: mall.order.direct
      routing-key: mall.order.cancel
      admin-enabled: false
      dispatch: { enabled: true, initial-delay-ms: 3000, interval-ms: 5000,
                  batch-size: 100, lease-seconds: 30, confirm-timeout-ms: 5000,
                  max-retry: 5, retry-base-seconds: 5, retry-max-seconds: 300 }
      consume:   { max-retry: 3, retry-ttl-ms: 1000 }
      metrics:   { enabled: true, interval-ms: 60000,
                   pending-warn-threshold: 100, oldest-pending-warn-seconds: 300 }

# application-dev.yml
spring:
  rabbitmq:
    publisher-confirm-type: correlated
    publisher-returns: true
    template: { mandatory: true }
    listener:
      simple: { acknowledge-mode: manual, default-requeue-rejected: false,
                prefetch: 10, retry: { enabled: false } }
```

> **时区**：`document/docker/docker-compose-local.yml` 给 MySQL 加了  
> `--default-time-zone=+08:00` 和 `TZ: Asia/Shanghai`。若用 UTC 跑容器，  
> 数据库 `NOW()` 与应用 JDBC `Asia/Shanghai` 写入的 `create_time` / `expire_time`  
> 会差 8 小时，所有超时判定失效（A 阶段回归测试曾因此失败并据此修正）。


## 6. 实施顺序对照（方案 §11）

| 步骤 | 状态 | 产物 |
| --- | --- | --- |
| 1 固定 `expire_time` + 本地消息表 + 迁移脚本 | 完成 | `mall.sql`、`mall_b_precheck.sql`、`mall_b_migration.sql`，真实旧库迁移验证 |
| 2 下单事务内写 `PENDING` 消息 | 完成 | `OmsPortalOrderServiceImpl`，移除事务内/afterCommit 直发 MQ |
| 3 到期扫描 + 租约 + 版本 CAS | 完成 | `OmsOrderMessageDispatcher`、`OmsOrderMessageService`、Outbox DAO/XML |
| 4 普通消息 + Confirm + mandatory Return | 完成 | `CancelOrderSender`、`OrderCancelPublisher`、`PublishOutcome`、`RabbitMqConfig` |
| 5 消费者手动 ACK + 幂等 + 重试 + DLQ | 完成 | `CancelOrderReceiver`、`oms_order_consume_failure`、`CancelResult` |
| 6 提前投递重排队 + 超时扫描兜底 | 完成 | `rescheduleToOrderExpireTime`、`listExpiredPendingOrderIds` |
| 7 指标、告警、运维入口 | 完成 | `OmsOrderMessageMetricsLogger`、`OmsOrderMessageAdminController` |
| 8 真实中间件故障注入测试与报告 | 完成 | 24 个 B 测试 + 32 个 A 回归，全量 56 通过；[B_TEST_REPORT.md](B_TEST_REPORT.md) |
| 9 文档与可复现结果 | 完成 | 本文件、[B_TEST_REPORT.md](B_TEST_REPORT.md)、[results/](results/) |

## 7. 明确没做的事（不要写进简历）

- **不保证「只投递一次」**：租约过期重发、提交后 ACK 前崩溃、人工重放都会产生重复消息；
  正确性依赖 `0 → 4` 条件更新的业务幂等，而不是 MQ 去重。
- **不保证准时取消**：取消时刻受派发间隔（5s）、Confirm 超时、重试退避、扫描间隔（60s）影响。
- **不关闭第三方支付、不做自动退款**：本地 `status=4` 不等于支付渠道关单；
  支付晚到仍只走 A 阶段的 `oms_order_pay_anomaly` 留痕 + 告警。
- **下单幂等仍未实现**：`uk_order_event` 只保证每单一条超时消息，不防用户重复提交订单。
- **没有引入 Redis 分布式锁、没有事务消息中间件、没有 CDC/binlog 订阅**：消息可靠性靠本地消息表 + 定时扫描。
- **没有多节点真集群压测**：多实例用同一 JVM 内不同 `owner` 做 CAS 并发验证，
  以及真实 Broker 停机恢复；未做长时间压测、网络分区、磁盘满等混沌工程。
- **`SENT` 不等于订单已取消**：它只表示发布阶段成功，最终一致性由订单表扫描与指标告警保证。
- **告警只落日志**：没有接入 Prometheus/企业微信/钉钉等外部告警通道。
- **没有压测/QPS 结论**：报告中的耗时均为单机单次执行值。

## 8. 可以写进简历的结论（仅覆盖实测范围）

> 基于本地消息表和 RabbitMQ 实现订单超时取消。将订单与消息记录写入同一数据库事务，
> 通过定时投递、Publisher Confirm、不可路由处理及重试恢复发送故障；
> 结合订单条件状态更新和消费幂等，验证重复投递和服务中断后库存不会重复释放。
>
> 验证：在 Docker 内 MySQL 8.4.7（REPEATABLE-READ）与 RabbitMQ 4.2.9 上，
> 新增 24 个集成测试并回归 A 阶段 32 个用例，全量 56 个测试 0 失败。
> 覆盖真实 Broker 停机恢复（消息留 `PENDING`，恢复后 `SENT`，订单 `status=4`、`lock_stock=0`、
> DLQ 深度 0）、Confirm NACK / mandatory Return / Confirm 超时不得标 `SENT`、
> 双实例租约 CAS 只有一个赢家、Confirm 成功但回写前崩溃后租约过期重发、
> 3 次重复投递库存只释放一次、毒消息与业务失败进 DLQ 并落失败记录、
> 提前投递按数据库固定到期时间重排 + 扫描兜底。

## 9. 复跑与运维

```powershell
# 1) 起中间件（用户指定 mall-local 项目名）
docker compose -p mall-local -f document/docker/docker-compose-local.yml up -d mysql rabbitmq redis mongo

# 2) 新环境直接导入 mall.sql；已有库按顺序：预检（只读）→ A 迁移 → B 预检 → B 迁移
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall < document/sql/mall_a_precheck.sql
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall < document/sql/mall_a_migration.sql
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall < document/sql/mall_b_precheck.sql
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall < document/sql/mall_b_migration.sql

# 3) 全量测试（根 pom 默认 skipTests=true，必须显式覆盖）
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B -pl mall-portal -am `
    '-DskipTests=false' '-Dsurefire.failIfNoSpecifiedTests=false' '-Ddocker.skip=true' test

# 4) 全项目编译
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B -DskipTests -Ddocker.skip=true compile
```

> 若旧队列 `mall.order.cancel.ttl` / `mall.order.direct.ttl` 仍在，
> 需先从 RabbitMQ management 删除再启动应用，否则新拓扑重声明会参数冲突。

运维排障顺序：看指标 → `GET /order/message/failed` → 查 `oms_order_consume_failure`
与 `oms_order_expire_backfill_exception` → 确认 MQ 拓扑与路由 →
`POST /order/message/{eventId}/retry` 人工重放 `FAILED` 消息。
具体环境参数、逐项验收数据见 [B_TEST_REPORT.md](B_TEST_REPORT.md)。
