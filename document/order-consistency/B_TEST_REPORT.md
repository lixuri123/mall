# B 阶段测试与验收报告

> 全部数据来自 2026-11-12 在用户指定的 `mall-local` Docker 容器内**真实 MySQL 8.4.7 +
> 真实 RabbitMQ 4.2.9** 上的实际执行，不含估算值。
> 原始输出见 [results/b-test-run-2026-11-12.txt](results/b-test-run-2026-11-12.txt)，
> 改造说明见 [B_README.md](B_README.md)，A 阶段报告见 [TEST_REPORT.md](TEST_REPORT.md)。

## 1. 测试环境

| 项 | 值 |
| --- | --- |
| 机器 / JDK | Windows 11 家庭版中文版 / i5-12500H / 16 逻辑核 / 15.73 GB RAM / JDK 17.0.11 |
| 构建 | Apache Maven 3.9.9，Spring Boot 3.5.14 |
| 中间件编排 | `docker compose -p mall-local -f document/docker/docker-compose-local.yml up -d` |
| MySQL | **8.4.7**（容器 `mall-local-mysql-1`，宿主 3307）；`@@transaction_isolation = REPEATABLE-READ`；`@@global.time_zone = +08:00`；`NOW() = 2026-11-12 12:51:38` |
| RabbitMQ | **4.2.9-management**（容器 `mall-local-rabbitmq-1`，5672 / 15672，vhost `/mall`，`mall/mall`） |
| 其他容器 | `mall-local-redis-1`、`mall-local-mongo-1` 同项目启动 |
| JDBC | `jdbc:mysql://localhost:3307/...&serverTimezone=Asia/Shanghai`（与容器 `+08:00` 一致） |
| 数据库 | `mall_it`（A 回归）、`mall_it_b`（B 阶段，隔离测试库）、`mall`（真实旧结构开发库，已完成 A+B 迁移验证） |
| 表结构 | `document/sql/mall.sql`；已有库链路 `mall_a_migration.sql` → `mall_b_migration.sql` |
| 代码版本 | 当前工作区（尚未提交 Git）；最后一个源文件修改于 12:40:52，早于本轮全量测试结束 12:42:47 |
| 测试命令 | `mvn.cmd -B -pl mall-portal -am -DskipTests=false -Dsurefire.failIfNoSpecifiedTests=false -Ddocker.skip=true test` |
| 执行时间 | 2026-11-12 12:41:09 → 12:42:47（+08:00），Total time 01:38 min |

> **时区为何重要**：容器 MySQL 默认是 UTC，而应用 JDBC 指定 `Asia/Shanghai`。
> 二者不一致时 DB `NOW()` 与 `create_time` / `expire_time` 相差 8 小时，
> A 阶段回归里 `OrderTimeOutScanTest`、`PortalOrderDaoAtomicSqlTest` 曾因此失败
> （expected 2 but was 0）。修正 `docker-compose-local.yml` 加
> `--default-time-zone=+08:00` + `TZ: Asia/Shanghai` 并重建容器后 32 个 A 用例全绿。

## 2. 测试清单与结果

| 测试类 | 阶段 | 用例数 | 结果 |
| --- | --- | ---: | --- |
| `GenerateOrderConcurrencyTest` | A 回归 | 4 | 全通过 |
| `OrderSkuAggregatorTest` | A 回归 | 3 | 全通过 |
| `OrderStateTransitionServiceTest` | A 回归 | 11 | 全通过 |
| `OrderTimeOutScanTest` | A 回归 | 4 | 全通过 |
| `PortalOrderDaoAtomicSqlTest` | A 回归 | 8 | 全通过 |
| `MallPortalApplicationTests` / `PortalProductDaoTests` | A 回归 | 2 | 全通过 |
| `OutboxDaoTest` | **B 新增** | 8 | 全通过 |
| `OrderMessageDispatchTest` | **B 新增** | 7 | 全通过 |
| `OrderMessageEndToEndRabbitTest` | **B 新增** | 9 | 全通过 |
| **合计** | | **56** | **0 失败 / 0 错误 / 0 跳过** |

```text
Tests run: 56, Failures: 0, Errors: 0, Skipped: 0
Reactor: mall / mall-common / mall-mbg / mall-security / mall-portal 均 SUCCESS
BUILD SUCCESS
```

另执行全项目编译 `mvn.cmd -B -DskipTests -Ddocker.skip=true compile`，
8 个模块（含 `mall-admin` / `mall-search` / `mall-demo`）全部 `SUCCESS`。

B 阶段测试基类 `AbstractOrderMessageIntegrationTest` 提供真实连接下的
建单、造消息、直接投递到主队列、队列深度查询、队列清理、条件等待；
每个用例结束清理 `oms_order_message` / `oms_order_consume_failure` 并 purge 三个队列，
避免用例间串扰（这是停机恢复用例首轮失败后补上的隔离措施）。

## 3. 方案 §10 验收场景逐项结果

| 故障 / 场景 | 测试位置 | 实测结果 |
| --- | --- | --- |
| 下单写入固定 `expire_time` 与 `PENDING` 消息 | `OrderMessageDispatchTest#generateOrder_persistsFixedExpireTimeAndPendingMessage` | 通过。`expire_time = create_time + 当时配置`，消息 `due_time` 与之一致，状态 `PENDING` |
| 消息表写入失败时整笔回滚 | `...#generateOrder_rollsBackOrderStockAndMessageTogether` | 通过。订单 / 锁定库存 / 消息一起回滚，不出现「有订单无消息」 |
| MQ 不可用不影响下单，恢复后补发 | `...#mqUnavailable_orderStillCreatedAndMessageRecoversAfterBrokerReturns` | 通过。`[B-MQ-OUTAGE] duringStatus=PENDING, retryCount=1, afterStatus=SENT, publishCalls=2`，订单仍为待付款且已落库 |
| Confirm NACK / mandatory Return / Confirm 超时不得标 `SENT` | `...#nackUnroutableAndUnknownConfirmAreNeverMarkedSent`、`OrderMessageEndToEndRabbitTest#publisherConfirmAndReturnAreDistinguished` | 通过。NACK → 退避重试；Confirm ACK 但不可路由 → `UNROUTABLE: NO_ROUTE` 并置 `FAILED` + ERROR 告警；`UNKNOWN` 同样不算成功 |
| 未到期消息不派发 | `...#notYetDueMessageIsNotDispatched` | 通过。`due_time <= NOW()` 才进入候选集 |
| 多实例并发领取 | `OutboxDaoTest#claimIsCompareAndSet_acrossTwoInstances`、`...#multiInstanceDispatchClaimsEachMessageOnlyOnce` | 通过。`[B-LEASE] winner=instance-A, loser=instance-B, attemptVersion=1`，只有 1 个实例领取成功 |
| 租约过期重领 + 迟到回调隔离 | `OutboxDaoTest#expiredLeaseCanBeReclaimedAndOldCallbackIsIgnored`、`...#confirmSucceededButMarkSentCrashed_isResentAfterLeaseExpiry` | 通过。旧 `attempt_version` 的 `markSent` / `markRetry` 影响 0 行；租约到期后重新派发 |
| 退避升级到 `FAILED` 与人工重试恢复 | `OutboxDaoTest#retryBackoffEscalatesToFailedAndManualRetryRestores` | 通过。指数退避后达上限置 `FAILED`；`resetFailedForManualRetry` 恢复 `PENDING` 且 `retry_count=0` |
| 事务提交后才 ACK | `OrderMessageEndToEndRabbitTest#consumerCancelsExpiredOrderAfterTransactionCommit` | 通过。订单 `status=4`、`lock_stock=0` 后消息才从主队列消失 |
| 提交成功但 ACK 前「崩溃」重投 | `...#commitBeforeAckCrashIsIdempotentOnRedelivery` | 通过。重投后条件更新 0 行，库存/券/积分不重复变动 |
| 重复投递 | `...#duplicateDeliveryReleasesStockOnlyOnce` | 通过。`[B-DUPLICATE] deliveries=3, finalStatus=4, stock=10, lock=0, consumeFailures=0` |
| 已支付订单收到迟到取消消息 | `...#paidOrderIsNotCancelledByLateCancelMessage` | 通过。状态保持 1，不释放库存 |
| 提前投递 | `OutboxDaoTest#earlyDeliveryReschedulesToFixedExpireTime`、`...#earlyDeliveryReschedulesToExpireTimeAndScanCoversOmission` | 通过。不直接 ACK 丢弃，JOIN `oms_order` 重排到固定 `expire_time`；消息遗漏时订单表扫描兜底完成取消 |
| 毒消息 | `...#poisonMessageGoesToDeadLetterWithFailureRecord` | 通过。`[B-POISON] dlqDepth=1, mainDepth=0, retryDepth=0, failureRows=1`，原消息体与原因落 `oms_order_consume_failure` |
| 消费业务失败有限重试 | `...#consumerBusinessFailureUsesBoundedRetryThenDeadLetter` | 通过。固定 TTL 重试队列按 `x-retry-count` 受控重试，超限写失败记录并进 DLQ，不无限 requeue |
| 真实 Broker 停机 / 恢复 | `...#realBrokerOutage_messageStaysPendingAndIsSentAfterRecovery` | 通过，见第 5 节 |
| 固定到期时间是唯一权威 | `OutboxDaoTest#expireTimeIsTheOnlyAuthorityForSystemCancel`、`...#timeoutScanUsesFixedExpireTimeWithIdCursor` | 通过。修改后台超时配置不影响已有订单；扫描按 `expire_time <= NOW()` + `id` 游标分页，`EXPLAIN` 命中 `idx_order_expire_scan` 且无 `Using filesort` |
| 缺 `expire_time` 的历史订单 | `...#missingExpireTimeIsRejectedInsteadOfGuessed` | 通过。系统取消返回 `REJECTED` 并 ERROR 告警，不猜测到期时间、不释放库存 |
| 指标与告警 | `...#metricsExposePendingAgeFailuresAndUncancelledOrders` | 通过。9 项指标聚合正确，异常项触发 `OmsOrderMessageMetricsLogger` 的 ERROR 分支 |
| A 阶段一致性回归 | 全部 32 个 A 用例 | 通过。同一轮全量重跑，20 线程端到端下单仍恰好 10 单、支付/取消竞争 20 轮每轮恰好一个胜者 |

## 4. 关键结构化输出

```text
[B-MQ-OUTAGE]    orderId=165, eventId=OTC-165, duringStatus=PENDING, retryCount=1,
                 afterStatus=SENT, publishCalls=2
[B-REAL-OUTAGE]  orderId=168, eventId=OTC-168, finalStatus=4, stock=10, lock=0,
                 messageStatus=SENT, retryCount=1, dlqDepth=0
[B-POISON]       dlqDepth=1, mainDepth=0, retryDepth=0, failureRows=1
[B-DUPLICATE]    orderId=171, deliveries=3, finalStatus=4, stock=10, lock=0, consumeFailures=0
[B-LEASE]        eventId=OTC-181, orderId=181, winner=instance-A, loser=instance-B,
                 attemptVersion=1

[GENERATE_ORDER] successOrders=10, rejected=10, errors=0, dbOrders=10,
                 finalStock=10, finalLock=10, elapsedMs=298
[RACE]           rounds=20, paidWins=11, cancelWins=9
[CONCURRENT]     stock=10, threads=30, qty=1, success=10, errors=0, finalLock=10, elapsedMs=100
```

> 日志里的 `ERROR/WARN`（发布失败告警、毒消息、历史库存预检、Broker 连接异常）
> 都是测试用例**主动注入的故障路径**，并被断言覆盖；Surefire 最终 0 失败 / 0 错误。
> Windows 控制台默认编码导致中文日志显示为乱码，属于显示问题，
> 因此测试主动打印的结构化标记刻意使用 ASCII。

## 5. 真实故障注入记录

### 5.1 发布器故障注入（FaultPublisher）

`OrderMessageDispatchTest` 用 `@Primary @TestConfiguration` 注入可控 `FaultPublisher`，
按用例返回 `CONFIRMED / NACK / UNROUTABLE / UNKNOWN / 抛异常`，验证派发任务不会把
失败结果误标成 `SENT`。典型日志：

```text
WARN  ... reason=NACK: broker nack                      → 回 PENDING，退避重试
ERROR ... retryCount=2, reason=UNROUTABLE: NO_ROUTE     → 达到上限，置 FAILED 并告警
```

### 5.2 真实 Broker 停机恢复（`mall-local-rabbitmq-1`）

测试内通过 `docker compose stop rabbitmq` / `start rabbitmq` 操作**真实容器**：

1. 停机期间下单 → 订单正常创建，消息保持 `PENDING`（`retry_count` 递增），不阻塞交易。
2. 恢复后等待 `RabbitListenerEndpointRegistry` 中所有 listener container 恢复 running。
3. 派发任务重发并收到 Confirm → `SENT`；消费者完成取消事务后 ACK。
4. 最终断言：`status=4`、`stock=10`、`lock=0`、`messageStatus=SENT`、
   `consume_failures=0`、`dlqDepth=0`。

```text
[B-REAL-OUTAGE] orderId=168, eventId=OTC-168, finalStatus=4, stock=10, lock=0,
                messageStatus=SENT, retryCount=1, dlqDepth=0
```

首轮执行时曾出现两类真实问题并已修复：
- 残留消息（上一用例的 `订单不存在`）被恢复后的消费者读到，进入重试并最终进 DLQ →
  测试基类收尾增加 `purgeQueues()`，并改为等待所有 listener container running；
- 停机恢复用例缺订单明细，消费时抛「订单明细为空，无法执行库存操作」→
  补齐 `createSku()` + `addOrderItem()`，并追加 `lock=0` 与 `consume_failure=0` 断言。

### 5.3 真实旧库 A+B 迁移

开发库 `mall` 保持 A/B 迁移前的旧结构，用于验证预检与守卫：

```text
mall_a_precheck.sql：4 条负数 lock_stock —— 98(-24)、102(-8)、103(-8)、106(-8)
交叉核对：这些 SKU 无待付款订单占用
人工确认后：UPDATE pms_sku_stock SET lock_stock = 0 WHERE id IN (98,102,103,106) AND lock_stock < 0;
mall_a_migration.sql -> EXIT=0；mall_b_migration.sql -> EXIT=0
```

迁移后复核（2026-11-12）：

| 检查项 | 值 |
| --- | ---: |
| `oms_order_message` 中 `PENDING` | 1 |
| `oms_order_expire_backfill_exception` 待处理 | 0 |
| 待支付订单缺 `expire_time` | 0 |
| 待支付订单总数 | 1 |

另用**构造的旧库样本**特意放入一笔 `create_time IS NULL` 的订单
`LEGACY-NO-CREATE`，验证「不猜测到期时间」的守卫：
迁移输出 `backfill_exceptions=1` / `pending_without_expire_time=1`，属于预期行为。
真实开发库没有这类订单，两个值都是 0。
预检脚本兼容「`expire_time` 列 / 消息表尚不存在」的旧库，通过
`information_schema` 动态拼 SQL，不会报 `Unknown column 'expire_time'`。

## 6. 已修复的测试/环境问题（留痕）

| 问题 | 处理 |
| --- | --- |
| 容器默认 UTC 导致 `NOW()` 与应用差 8 小时 | `docker-compose-local.yml` 加 `--default-time-zone=+08:00` + `TZ: Asia/Shanghai`，`LOCAL.md` 补充说明 |
| 旧 TTL 队列/交换机残留导致新拓扑重声明参数冲突 | 通过 management API 删除 `mall.order.cancel`、`mall.order.cancel.ttl`、`mall.order.direct.ttl` |
| `mall.sql` 新增列后 65 条无列名位置 `INSERT` 失败 | 脚本为每条订单种子追加 `NULL`，3 个测试库重新导入验证 |
| 中文迁移原因写库报 `ERROR 1366` | 迁移脚本加 `SET NAMES utf8mb4`，客户端加 `--default-character-set=utf8mb4` |
| `ON DUPLICATE KEY UPDATE id = id` 报 `Column 'id' ambiguous` | 改为 `NOT EXISTS (...)`，同时避免 `INSERT IGNORE` 吞异常 |
| 扫描索引 `Using filesort` | 索引列序改为 `(status, delete_status, id, expire_time)`，`EXPLAIN` 验证 `Using where; Using index` |
| `claim()` 回读用了 `listDispatchable()`，刚领取的 `SENDING` 不在候选集 | DAO 新增 `selectById(id)`，`claim()` 直接按主键回读 |
| 超时扫描用例一个 SKU 被 3 单各锁 3，但初始 `lock_stock=6` 导致回滚 | 造数改为 `createSku(10, 9)` |
| MQ 不可用用例断言写成 `status=1` | 改为断言 `status=0`（仍待付款）+ 订单确实落库 |

## 7. 未覆盖与已知边界（必须如实说明）

1. **至少一次投递，不是恰好一次**：租约重发、ACK 前崩溃、人工重放都会产生重复消息；
   正确性由 `0 → 4` 条件更新的业务幂等保证，而非 MQ 去重。
2. **多实例验证是同一 JVM 内不同 `owner` 的并发 CAS**，加上真实 Broker 停机恢复；
   没有部署多进程/多主机集群，也没做长时间压测、网络分区、磁盘满等混沌场景。
3. **不保证准时取消**：受派发间隔 5s、Confirm 超时 5s、退避、扫描间隔 60s 影响。
4. **本地关单不等于关闭第三方支付**：支付晚到仍只走 A 阶段的留痕 + 告警，没有自动退款/对账。
5. **下单幂等未实现**（A 阶段遗留）：`uk_order_event` 只保证每单一条超时消息。
6. **`SENT` 语义有限**：只表示发布阶段成功，不代表订单已取消；最终一致性靠订单表扫描 + 指标告警。
7. **告警只落日志**，未接 Prometheus / IM 通道；运维接口默认关闭，未做权限体系设计。
8. **性能数字**：`01:38 min`、各用例耗时均为单机单次执行值，不是 QPS / 压测结论。
9. **代码与文档尚未提交 Git**；测试依赖 `mall-local` 容器处于运行状态、
   RabbitMQ vhost `/mall` 与 `mall/mall` 账号可用、队列无残留。

## 8. 如何复跑

```powershell
# 1) 起容器（用户指定项目名 mall-local）
docker compose -p mall-local -f document/docker/docker-compose-local.yml up -d mysql rabbitmq redis mongo
docker exec mall-local-mysql-1 mysql -uroot -proot -N -e "SELECT VERSION(), @@transaction_isolation, @@global.time_zone, NOW();"
# 期望：8.4.7 | REPEATABLE-READ | +08:00 | <与本机一致的北京时间>

# 2) 建测试库并导入结构（B 测试使用 mall_it_b，A 回归使用 mall_it，两者都要存在）
mysql -h127.0.0.1 -P3307 -uroot -proot -e "CREATE DATABASE IF NOT EXISTS mall_it_b; CREATE DATABASE IF NOT EXISTS mall_it;"
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall_it_b < document/sql/mall.sql
mysql -h127.0.0.1 -P3307 -uroot -proot --default-character-set=utf8mb4 mall_it   < document/sql/mall.sql

# 3) 确认旧队列已清理（否则 RabbitMqConfig 重声明会参数冲突）
#    RabbitMQ management UI / API 删除 mall.order.cancel.ttl、mall.order.direct.ttl

# 4) 全量测试
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B -pl mall-portal -am `
    '-DskipTests=false' '-Dsurefire.failIfNoSpecifiedTests=false' '-Ddocker.skip=true' test

# 5) 全项目编译
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B -DskipTests -Ddocker.skip=true compile
```

`OrderMessageEndToEndRabbitTest#realBrokerOutage_...` 会在测试内调用
`docker compose -p mall-local ... stop/start rabbitmq`，需要本机 Docker CLI 可用且
当前用户有权限操作该 compose 项目；其他用例不需要停机权限。
