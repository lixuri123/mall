# B（RocketMQ 版）测试与验收报告

> 全部数据来自 2026-11-12 在 `mall-local` 容器内**真实 MySQL 8.4.7 + 真实 RocketMQ 5.3.2**
> 上的实际执行，不含估算值。交付前最后一次全量日志 `/tmp/b-rocketmq-delivery.log`
> （16:52:40 +08:00）晚于最后一次源码/配置修改（16:36:16 测试类、
> `application-prod.yml` 的 RocketMQ 覆盖），此后只更新交付文档。
> 原始输出：[results/b-rocketmq-test-run-2026-11-12.txt](results/b-rocketmq-test-run-2026-11-12.txt)；
> 改造说明：[B_ROCKETMQ.md](B_ROCKETMQ.md)；
> A 阶段报告：[TEST_REPORT.md](TEST_REPORT.md)；
> RabbitMQ 版历史报告：[B_TEST_REPORT.md](B_TEST_REPORT.md)。

## 1. 测试环境

| 项 | 值 |
| --- | --- |
| 构建 | Apache Maven 3.9.9 / Spring Boot 3.5.14 / JDK 17 |
| 编排 | `docker compose -p mall-local -f document/docker/docker-compose-local.yml`（文件内 `name: mall-local`） |
| MySQL | **8.4.7**，`@@transaction_isolation = REPEATABLE-READ`，`@@global.time_zone = @@session.time_zone = +08:00`，`NOW() = 2026-11-12 16:43:44` |
| RocketMQ | **apache/rocketmq 5.3.2**，NameServer 9876 / Broker 10911，`broker-a` 上报 `127.0.0.1`，V5_3_2 |
| Broker 关键配置 | `brokerIP1=127.0.0.1`、`flushDiskType=SYNC_FLUSH`、`autoCreateTopicEnable=false` |
| 预建 topic | `mall_order_cancel` / `mall_order_cancel_retry` / `mall_order_cancel_dlq`（各 4 读 4 写，`message.type=NORMAL`） |
| 保留容器 | `rabbitmq:4.2.9-management`（当前代码不再使用）、`redis`、`mongo` |
| JDBC | `jdbc:mysql://127.0.0.1:3307/...&serverTimezone=Asia/Shanghai`，与库时区一致 |
| 数据库 | `mall_it`（A 回归）、`mall_it_b`（B 阶段隔离库）、`mall`（真实旧结构开发库，A+B 迁移已验证） |
| 表结构 | `document/sql/mall.sql`；已有库链路 `mall_a_migration.sql` → `mall_b_migration.sql`（本次未改 SQL） |

> MySQL 时区必须与 JDBC 一致：容器默认 UTC 曾导致 A 回归失败，`docker-compose-local.yml`
> 已加 `--default-time-zone=+08:00` 与 `TZ: Asia/Shanghai`。

## 2. 全量结果

```
[INFO] Tests run: 56, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

**连续三次全量复跑均全绿**（`/tmp/b-rocketmq-final5.log`、`final6.log`、`b-rocketmq-delivery.log`），
端到端类单独跑 9/9 通过（`/tmp/it-e2e5.log`），管理 API 异常计数 0。
全项目 8 模块 `mvn -B -DskipTests -Ddocker.skip=true compile` 全部 SUCCESS。

| 测试类 | 阶段 | 用例数 | 结果 |
| --- | --- | --- | --- |
| `GenerateOrderConcurrencyTest` | A | 4 | 全通过 |
| `OrderSkuAggregatorTest` | A | 3 | 全通过 |
| `OrderStateTransitionServiceTest` | A | 11 | 全通过 |
| `OrderTimeOutScanTest` | A | 4 | 全通过 |
| `PortalOrderDaoAtomicSqlTest` | A | 8 | 全通过 |
| `MallPortalApplicationTests` / `PortalProductDaoTests` | A | 1 + 1 | 全通过 |
| **A 小计** | | **32** | **全通过** |
| `OutboxDaoTest` | B | 8 | 全通过 |
| `OrderMessageDispatchTest` | B | 7 | 全通过 |
| `OrderMessageEndToEndRocketMqTest` | B | 9 | 全通过 |
| **B 小计** | | **24** | **全通过** |

### B 阶段 24 个用例

`OutboxDaoTest`（8）：租约 CAS 双实例、过期租约重领 + 迟到回调隔离、退避升级 FAILED 与人工重试、
提前投递按固定到期时间重排、`expire_time` 为系统取消唯一权威、缺 `expire_time` 拒绝取消、
按 `expire_time` + id 游标分页扫描、监控指标（PENDING 年龄 / 失败数 / 超时未取消 / 回填）。

`OrderMessageDispatchTest`（7）：下单写固定 `expire_time` + `PENDING` 消息、消息表写入失败时
订单/库存/消息一起回滚、MQ 不可用不影响下单且恢复后 `SENT`、NACK/UNROUTABLE/UNKNOWN 均不标 `SENT`、
未到期消息不派发、多实例 CAS 仅一个赢家、Confirm 成功但 `markSent` 前崩溃后租约过期重发。

`OrderMessageEndToEndRocketMqTest`（9）：同步发送 `SEND_OK` 与 `No route info` 区分、
事务提交后确认、3 次重复投递、提交后确认前崩溃重投、提前投递重排 + 扫描兜底、
支付胜出后迟到取消消息无效、毒消息进 DLQ + 失败记录、业务失败受控重试后进 DLQ、
真实 Broker 停机恢复。

## 3. 方案 §10 失败场景逐项验收

| 场景 | 验证用例 | 实测结果 |
| --- | --- | --- |
| 下单事务与消息原子 | `generateOrder_persistsFixedExpireTimeAndPendingMessage` / `...rollsBackOrderStockAndMessageTogether` | 通过。订单、`expire_time`、库存锁定、`PENDING` 消息同事务；注入消息表写入失败时整笔回滚 |
| MQ 不可用 / 发布 NACK | `mqUnavailable_orderStillCreatedAndMessageRecoversAfterBrokerReturns` | `[B-MQ-OUTAGE] duringStatus=PENDING, retryCount=1, afterStatus=SENT, publishCalls=2`，订单仍为待付款且存在 |
| 不可路由必须能被发现 | `publisherConfirmAndUnroutableAreDistinguished` / `nackUnroutableAndUnknownConfirmAreNeverMarkedSent` | 通过。已预建 topic 返回 `CONFIRMED`；临时改成未预建 topic 抛 `No route info of this topic` → `UNROUTABLE`。NACK / UNROUTABLE / UNKNOWN 均不标 `SENT`，达上限置 `FAILED` + ERROR 告警 |
| 多实例并发领取 | `multiInstanceDispatchClaimsEachMessageOnlyOnce` | `[B-LEASE] winner=instance-A, loser=instance-B, attemptVersion=1`（同一 JVM 两个 owner 的 CAS 模拟） |
| 领取后崩溃 / 迟到回调 | `expiredLeaseCanBeReclaimedAndOldCallbackIsIgnored` / `confirmSucceededButMarkSentCrashed_isResentAfterLeaseExpiry` | 通过。租约过期可被重领；旧 attempt 的 `markSent` 因版本不匹配不影响新尝试 |
| 重复投递 | `duplicateDeliveryReleasesStockOnlyOnce` | `[B-DUPLICATE] deliveries=3, finalStatus=4, stock=10, lock=0, consumeFailures=0` |
| 提交成功、确认前退出 | `commitBeforeAckCrashIsIdempotentOnRedelivery` | 通过。先完成业务再投递消息，重投后 `0 → 4` 影响 0 行，库存不再释放 |
| 提前投递 | `earlyDeliveryReschedulesToExpireTimeAndScanCoversOmission` | 通过。`NOT_TIMED_OUT` 时按数据库 `expire_time` 重排为 `PENDING`，订单未取消、`lock_stock` 保持；推进到期后不发 MQ，仅靠扫描兜底取消成功 |
| 支付与取消竞争 | `paidOrderIsNotCancelledByLateCancelMessage` | 通过。支付胜出后取消消息不能改状态、不能动库存 |
| 毒消息 | `poisonMessageGoesToDeadLetterWithFailureRecord` | `[B-POISON] dlqDepth=1, mainDiff=0, retryDiff=0, failureRows=1`；不进入无限重试 |
| 消费业务失败 | `consumerBusinessFailureUsesBoundedRetryThenDeadLetter` | 通过。`lock_stock` 不足 → 事务回滚 → 固定延迟级别受控重试 → 超上限写 `oms_order_consume_failure` 并投 DLQ；订单状态未改变 |
| 消息遗漏兜底 | `earlyDeliveryReschedulesToExpireTimeAndScanCoversOmission` / `timeoutScanUsesFixedExpireTimeWithIdCursor` | 通过。消息表扫描解决「未发布」，订单表扫描解决「发布后仍未完成取消」 |
| 真实 Broker 停机恢复 | `realBrokerOutage_messageStaysPendingAndIsSentAfterRecovery` | `[B-REAL-OUTAGE] finalStatus=4, stock=10, lock=0, messageStatus=SENT, retryCount=1, dlqDepth=0, consumeFailures=0, viaMessage=true, scanFallback=false` |

## 4. 真实故障注入记录

- **`docker compose stop/start rocketmq-broker`**：由测试内部调用，操作真实容器。
  停机期间 `dispatchOnce()` 发送失败，消息保持 `PENDING`、`retryCount=1`、订单未取消；
  恢复后等待 10911 可连 + `examineConsumerConnectionInfo` 返回在线连接，再重派成功置 `SENT`。
- **未预建 topic**：Broker 关闭 `autoCreateTopicEnable`，发送抛
  `No route info of this topic`，等价于此前的 mandatory Return 不可路由语义。
- **FaultPublisher（`@Primary`）**：注入 NACK / UNROUTABLE / UNKNOWN / 连接失败，
  不改变真实 `CancelOrderSender` 实现，只替换派发任务的注入对象。
- **消息体污染**：向主 topic 投非 JSON 字符串，验证毒消息落失败表 + DLQ。
- **业务事务失败**：构造 `lock_stock=0` 但订单声明锁定 3 的 SKU，使取消事务回滚。

> **停机恢复的诚实说明**：Broker 重启会重建订阅组与消费位点，客户端按
> `CONSUME_FROM_LAST_OFFSET` 计算起点时**可能跳过刚发布的单条消息**。
> 这正是 B 阶段保留定时扫描兜底的原因。测试先给 MQ 链路 12 秒窗口，
> 未覆盖则显式触发 `cancelTimeOutOrder()` 并断言订单仍被取消、库存只释放一次；
> 输出明确打印 `viaMessage` / `scanFallback`，不掩盖实际路径。
> 最后三次全量复跑均为 `viaMessage=true`；调试过程中曾出现
> 「消息留在主 topic、扫描兜底生效」的情形，因此该分支确实存在于实现中。

## 5. 调试过程中定位并修复的问题

| # | 现象 | 根因 / 修复 |
| --- | --- | --- |
| 1 | 首轮 51/56，端到端 5 失败 | 人工探针残留 3 条非 JSON 消息被反复消费；`eventId=null` 时 `putUserProperty(key,null)` 导致 DLQ 投递失败并回退 Broker 内建 `%RETRY%`。修 DLQ key 占位，重建 topic |
| 2 | 全新消费组产生 17 条积压 | RocketMQ 对「近期创建的 topic + 全新组」从 0 位点重放历史。改为固定测试组 + `queueDepth` 相对基线计量 |
| 3 | `queueDepth(DLQ)` 恒为 0 | `examineTopicStats` 偶发 CODE 17；改用逐队列 `maxOffset/minOffset`，并先 `examineTopicRouteInfo` 预热路由（否则报 `The broker[broker-a] not exist`） |
| 4 | 120 次管理 API 异常 | 同上，预热 + 有限重试后归零 |
| 5 | `examineConsumeStats` CODE 17 | 5.x 会连带查 `%RETRY%group_topic`；改用带 topic 的重载 |
| 6 | 毒消息用例 `mainDiff=1` | RocketMQ 消费位点默认每 5s 批量提交；断言前统一等待确认 |
| 7 | 全量跑停机用例失败 `main=1` | Broker 重启后订阅组/位点重建跳过消息；加消费者在线等待、扫描兜底分支、并把真实停机用例固定为最后一个执行 |
| 8 | 跨用例失败记录污染 | 停机重放属至少一次投递；失败记录断言从「全局 0」改为「本订单 0」，全局计数仅打印 |
| 9 | 同 JVM 多上下文客户端互相影响 | Producer / Consumer 均设置 UUID `instanceName` |
| 10 | Broker 启动即 NPE 退出 | 镜像以 uid 3000 运行，named volume 由 root 创建不可写；去掉 store/logs 挂载 |
| 11 | 宿主机连不上 Broker | Broker 默认上报容器内网 IP；`broker.conf` 设 `brokerIP1=127.0.0.1` |
| 12 | 多轮结果不可复现 | 测试基类静态块在 Spring 上下文启动前自动重建测试 topic + 订阅组；连续三次全量全绿 |

## 6. 迁移复核（真实旧结构开发库 `mall`）

本次未改任何 SQL，以下为上一阶段已完成并在本次复核的迁移结论：

- `mall_a_precheck.sql` / `mall_b_precheck.sql` 通过；
- 4 条负数 `lock_stock`（id 98 / 102 / 103 / 106）经人工确认无待付款订单占用后清零；
- `mall_a_migration.sql` → `EXIT=0`；`mall_b_migration.sql` → `EXIT=0`；
- `oms_order_message`：`PENDING = 1`；`oms_order_expire_backfill_exception = 0`；
  待支付订单缺 `expire_time = 0`。

## 7. 复跑

完整命令（启动容器、预建 topic、初始化测试库、跑全量）见
[B_ROCKETMQ.md 第 8 节](B_ROCKETMQ.md)。核心命令：

```bash
mvn.cmd -B -pl mall-portal -am -DskipTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false -Ddocker.skip=true test
```

真实停机用例会在测试内执行 `docker compose ... stop/start rocketmq-broker`，
需要本机 Docker CLI 可用且 `mall-local` 容器保持运行。

## 8. 未覆盖 / 边界

- 至少一次投递，不承诺恰好一次；正确性靠 `0 → 4` 条件更新与库存条件更新。
- 不保证准时取消；本地关单不等于关闭第三方支付 / 自动退款。
- 多实例是同一 JVM 不同 `owner` 的 CAS 模拟，非真集群压测。
- 未做性能压测、多 Broker / 主从故障、NameServer 宕机演练。
- `SENT` 只代表发布成功；告警只落日志，运维接口默认关闭。
- 下单幂等键仍未实现。
