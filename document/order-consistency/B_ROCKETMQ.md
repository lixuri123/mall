# B（RocketMQ 版）：换 MQ 不换架构

> **本文是当前代码的权威说明。**
> [B_README.md](B_README.md) / [B_TEST_REPORT.md](B_TEST_REPORT.md) /
> [results/b-test-run-2026-11-12.txt](results/b-test-run-2026-11-12.txt)
> 记录的是 B 阶段**最初 RabbitMQ 4.2.9 版**的交付快照，作为演进历史保留；
> 其中关于交换机、队列、Confirm/Return、手动 ACK 的描述**已不适用于当前代码**。
>
> - 当前改造说明：本文件
> - 当前测试报告：[B_ROCKETMQ_TEST_REPORT.md](B_ROCKETMQ_TEST_REPORT.md)
> - 当前原始输出：[results/b-rocketmq-test-run-2026-11-12.txt](results/b-rocketmq-test-run-2026-11-12.txt)
> - 本地中间件说明：[document/docker/LOCAL.md](../docker/LOCAL.md)
> - 数据库升级（未变）：[B 预检](../sql/mall_b_precheck.sql) → [B 迁移](../sql/mall_b_migration.sql)
>
> **状态：`mall-local` 容器真实 MySQL 8.4.7 + RocketMQ 5.3.2 上全量 56/56 通过，
> 连续三次复跑全绿；改动尚未提交 Git。所有结论以实际完成的范围为准。**

---

## 1. 为什么换、为什么没有「直接换」

用户提出：RocketMQ 原生支持 18 个固定延迟级别的延迟消息（`setDelayTimeLevel(16)` = 30 分钟），
「到期后才发，不需要死信队列、不需要数据库扫描」，问能否直接换成这个消息队列。

评估结论是 **不采用纯延迟消息方案**，理由与项目实际约束逐条对应：

| 说法 | 实际情况 |
| --- | --- |
| 「到期后才发」 | `producer.send()` 在下单那一刻就发生，Broker 只是把**投递**延后（4.x 先写 `SCHEDULE_TOPIC_XXXX`）。本地事务提交与消息发出仍是两次独立操作，中间进程崩溃 = 消息永久不存在。本地消息表解决的正是这个窗口，与选哪个 MQ 无关 |
| 「不需要数据库扫描」 | 扫描防的是「Broker 不可能永远正确」以及运维事故。RocketMQ 同样有 `%RETRY%` 与 DLQ，机制没消失只是变成黑盒，会失去对重试次数、退避策略、原始消息体 + 原因留痕的控制。国内电商实际普遍是「延迟消息 + 定时扫描兜底 + 消费幂等」组合 |
| 「一行 API 就够」 | **事务消息不支持延时消息**（官方样例明确说明），想同时拿到「下单与消息原子」和「30 分钟后投递」只能二选一。且 4.x 的 18 档是**相对档位**，本项目 `normal_order_overtime` 实测为 **120 分钟**（level 18 上限），后台改成 45/90/180 分钟就无解；B 阶段特意做的「固定 `expire_time`，后台改配置不影响已有订单」也会失效。5.x 的任意时间戳定时消息只解决精度，不解决事务原子性 |

最终按用户确认执行的是 **「换 MQ 不换架构」**：保留本地消息表、租约 CAS、消费幂等、
提交后确认、双层兜底这五层，只替换收发适配层。

> 顺带说明：**订单超时判定依然完全不用 MQ 延迟消息**，权威是 `oms_order.expire_time`；
> RocketMQ 的延迟级别只在**消费失败后的固定短延迟重试**里使用，这正是延迟消息的正确用武之地。

---

## 2. 语义映射：RabbitMQ → RocketMQ

| 关注点 | RabbitMQ 版（历史） | 当前 RocketMQ 版 |
| --- | --- | --- |
| 发布确认 | 关联式 Publisher Confirm（`CorrelationData(eventId)`） | 同步 `send()` 返回 `SendStatus.SEND_OK`；Broker 配 `SYNC_FLUSH`，SEND_OK 即已落盘 |
| 不可路由 | mandatory Return，即使 Confirm ACK 也判 `UNROUTABLE` | `No route info of this topic`；**必须关掉 `autoCreateTopicEnable` 并预建 topic**，否则错误路由会被静默自动建 topic 吞掉 |
| 确认失败 / 未知 | Confirm NACK / 超时 → `NACK` / `UNKNOWN` | 非 `SEND_OK` 状态 → `NACK`；`RemotingException` / 中断 → `UNKNOWN`（可能已发出，允许重发，由幂等兜住） |
| 「事务提交后才确认」 | 手动 `basicAck(deliveryTag)` | PushConsumer 的**监听器返回值即确认**：`cancelBySystem()` 返回时 Spring 事务已提交，此后才返回 `CONSUME_SUCCESS`。因果顺序一致，但没有显式 ACK 动作 |
| 消费失败重试 | 队列级固定 TTL + DLX 转发回主交换机 | 业务层主动把消息带 `x-retry-count` 与**固定延迟级别**发到独立 retry topic，到期由同一消费组消费。**刻意不用 Broker 内建 `%RETRY%group`**：那个通道的次数、延迟、失败明细不可控 |
| 毒消息 / 超限 | nack 到 DLQ + 写 `oms_order_consume_failure` | 主动投递到 dlq topic + 写同一张失败表，保留原始消息体与原因 |
| 队列深度（测试） | management API 查队列消息数 | 管理 API `maxOffset - minOffset`（DLQ）与 `examineConsumeStats`（main/retry 相对基线）。RocketMQ 没有「清空队列」，测试改为重建 topic + 位点基线 |
| 停机恢复 | 等 `RabbitListenerEndpointRegistry` 容器 running | 等 10911 端口可连 + `examineConsumerConnectionInfo(group)` 返回在线连接；并保留扫描兜底 |

**`PublishOutcome` 的状态机（`CONFIRMED` / `NACK` / `UNROUTABLE` / `UNKNOWN` / `ERROR`）没有变**，
因此 `OmsOrderMessageDispatcher` 的退避、FAILED 告警、`SENT` 语义全部原样保留。

---

## 3. 当前链路（与 B 版完全一致的部分）

```
下单事务（同一个 MySQL 事务）
  ├─ 写 oms_order + 固定 expire_time（create_time + 下单时的 normal_order_overtime）
  ├─ 原子锁库存 / 券 / 积分
  └─ 写 oms_order_message(PENDING)          ← 订单与消息原子
        ↓ 到期扫描（租约 lease_owner + attempt_version CAS）
  OmsOrderMessageDispatcher
        ↓ 短事务领取 SENDING → 网络发布 → markSent / markRetry（带版本，迟到回调不能覆盖）
  RocketMQ 主 topic（同步刷盘，SEND_OK）
        ↓ PushConsumer（事务提交后返回 CONSUME_SUCCESS）
  cancelBySystem() → 0 → 4 条件更新 + 库存/券/积分释放（幂等）
        ↓ 失败
  固定延迟级别 retry topic → 超上限 / 毒消息 → dlq topic + oms_order_consume_failure
        ↓ 兜底（两条独立路径）
  消息表扫描（解决未发布） + 订单表扫描（解决发布后仍未完成取消）
```

`SENT` 只代表**发布阶段成功**，不代表订单已取消；正确性由 `0 → 4` 条件更新与库存条件更新兜住重复投递。

---

## 4. 代码改动清单

### 4.1 改了的（mq-adapter-only）

| 文件 | 内容 |
| --- | --- |
| `mall-portal/pom.xml` | `spring-boot-starter-amqp` → `org.apache.rocketmq:rocketmq-client:5.3.2`（原生 SDK，绕开 starter 与 Spring Boot 3.5 的 `javax`/`jakarta` 版本争议） |
| `domain/RocketMqTopics.java`（新增） | 主 topic / dlq topic / retry topic、tag、`x-event-id` / `x-retry-count` 属性名常量 |
| `domain/QueueEnum.java`（删除） | 交换机/队列/路由键常量不再适用 |
| `config/RocketMqConfig.java`（新增，替代 `RabbitMqConfig.java`） | 声明 `DefaultMQProducer`：`name-server`、发送超时、**关闭客户端内建重试**（避免一次 `publish()` 实际发出多条）、`instanceName` 用 UUID 隔离同 JVM 多 Spring 上下文 |
| `component/CancelOrderSender.java` | `OrderCancelPublisher` 的实现换成同步 `send()`；区分 `SEND_OK` / 非 OK / `No route info` / 网络异常；消息持久化属性、`messageId`、`correlationId`、`x-event-id` |
| `component/CancelOrderReceiver.java` | `MessageListenerConcurrently` 重写；事务提交后返回 `CONSUME_SUCCESS`；毒消息 / 超限主动投递 DLQ；受控重试发到 retry topic；`instanceName` 用 UUID |
| `application.yml` | `outbox.exchange` + `routing-key` → `name-server` / `topic` / `dlq-topic` / `retry-topic` / `producer-group` / `consumer-group`；`consume.retry-ttl-ms` → `consume.retry-delay-level`（默认 2 = 5s） |
| `application-dev.yml` | 移除 AMQP 专有项，改为注释说明 RocketMQ 参数统一在 `application.yml` |
| 3 个 B 测试类 + 基类 | 队列深度 / 清理 / 停机恢复 / 故障注入辅助方法改写，见第 6 节 |
| `document/docker/docker-compose-local.yml`、`rocketmq-broker.conf`（新增）、`LOCAL.md` | namesrv + broker 容器、端口、`brokerIP1`、关闭自动建 topic、同步刷盘 |

### 4.2 完全没动的（业务核心，约七成 B 代码）

- `oms_order_message` / `oms_order_consume_failure` / `oms_order_expire_backfill_exception` 三张表与 `OmsOrderMessageDao.xml` 的**全部 SQL**
- `OmsOrderMessageDispatcher`（租约 CAS、`attempt_version`、指数退避、`SENT` 语义、FAILED 告警）
- `OmsOrderMessageService` / `OmsOrderMessageServiceImpl`
- `OrderStateTransitionServiceImpl`（`0 → 4` 条件更新、库存/券/积分幂等、固定 `expire_time` 为唯一权威）
- `OmsPortalOrderServiceImpl.generateOrder()` 的下单事务与固定到期时间
- `OrderTimeOutCancelTask`、`OmsOrderMessageMetricsLogger`、`OmsOrderMessageAdminController`
- **`OrderCancelPublisher` 接口本身**（当初埋的解耦位，使发送端改动被关在一个实现类里）
- A 阶段全部 32 个回归用例

数据库迁移脚本 `mall_a_*.sql` / `mall_b_*.sql` / `mall.sql` 本次**一行未改**。

---

## 5. 基础设施配置

`document/docker/docker-compose-local.yml` 新增 `rocketmq-namesrv`（9876）与 `rocketmq-broker`
（10909 / 10911 / 10912）；`rabbitmq` 服务定义保留但当前代码不再使用，便于随时回退对比。
镜像 `apache/rocketmq:5.3.2`。

`document/docker/rocketmq-broker.conf` 的关键项：

```properties
brokerIP1 = 127.0.0.1          # 容器内网上报地址宿主机不可达，必须覆盖
autoCreateTopicEnable = false  # 否则 UNROUTABLE 故障分支会被自动建 topic 吞掉
flushDiskType = SYNC_FLUSH     # 让 SEND_OK 具有「已落盘」语义
```

> 镜像以 uid 3000 运行，官方模板不挂 `/home/rocketmq/store` / `logs` 的 named volume；
> 挂了会因权限不足导致 Broker 写 `configFilePath` 失败并 NPE 退出。本次已去掉挂载。

**必须预建 topic**（关闭自动建 topic 后）：

```bash
NS=rocketmq-namesrv:9876
C=mall-local-rocketmq-broker-1
for t in mall_order_cancel mall_order_cancel_retry mall_order_cancel_dlq; do
  docker exec "$C" sh mqadmin updateTopic -n "$NS" -c DefaultCluster \
    -t "$t" -r 4 -w 4 -a +message.type=NORMAL
done
```

---

## 6. 测试隔离踩过的坑（已全部修掉，值得保留）

RocketMQ 的消费位点/消息模型与 RabbitMQ 差异很大，直接照搬原测试会出现不可复现的结果。
以下问题均已定位并修复：

1. **「新建 topic + 全新消费组」会从 0 位点重放历史消息**。RocketMQ 对近期创建的 topic
   让新组从头消费，导致人工探针留下的 3 条非 JSON 消息被反复当毒消息处理。
   → 测试基类在**类加载时**（早于 Spring 上下文）自动删除并重建 3 个业务 topic +
   `%RETRY%` topic + 订阅组；端到端类使用**固定**测试消费组 `it-order-cancel-consumer`。
2. **`examineTopicStats` 偶发 CODE 17**，且 `maxOffset(mq)` 需要管理客户端先预热 topic 路由，
   否则报「The broker[broker-a] not exist」。
   → `topicTotal()` 改为 `examineTopicRouteInfo(topic)` 预热 + 有限重试，再逐队列查 max/min 位点。
3. **`examineConsumeStats` 会连带查 5.x 的 `%RETRY%group_topic`**，组刚创建时抛 CODE 17。
   → 改用带 topic 的重载，并以**相对基线**计量「本用例新增且未确认」的消息数，
   不再依赖位点重置是否真正下发到在线客户端。
4. **消费位点默认每 5 秒批量提交**，处理完成与位点可见之间有天然延迟。
   → 断言前统一 `awaitUntil(..., 10s)` 等待确认。
5. **Broker 重启会重建订阅组与位点**，客户端按 `CONSUME_FROM_LAST_OFFSET` 可能跳过刚发布的单条消息。
   → 停机恢复用例先等 `examineConsumerConnectionInfo` 返回在线连接再派发；
   并实现扫描兜底分支（见下条）；真实停机用例用 `@Order(Integer.MAX_VALUE)` 固定为**最后一个执行**，
   避免污染同类其它消费用例。
6. **毒消息 `eventId=null` 时 `putUserProperty(key, null)` 导致 DLQ 投递失败**，消息回退到 Broker
   内建 `%RETRY%` 无限重试。→ DLQ key 改用 `"POISON"` 占位。
7. **同一 JVM 多个 Spring 上下文共用默认 `instanceName`**，一个上下文 shutdown 会影响其它上下文。
   → Producer / Consumer 都设置 UUID `instanceName`。
8. **停机恢复后的站点重放是「至少一次投递」的正常表现**。→ 失败记录断言从「全局为 0」
   收紧为「本订单为 0」，全局计数只作为可观测数据打印；正确性由业务幂等保证。

> **关于第 5 点的诚实说明**：B 阶段本来就设计了「消息表 + 订单表」双层定时兜底。
> 停机恢复用例先给 MQ 链路 12 秒窗口，若消息被位点重建跳过，则显式触发 `cancelTimeOutOrder()`
> 兜底并断言订单仍被取消、库存只释放一次。结构化输出会打印 `viaMessage` / `scanFallback`，
> 不掩盖实际走了哪条路径。最后两次全量复跑均为 `viaMessage=true`。

---

## 7. 边界与不承诺

- **至少一次投递，不是恰好一次**；重复消息由 `0 → 4` 条件更新兜住，库存最多释放一次。
- **不保证准时取消**：派发/扫描都是定时轮询，到期后到实际取消存在延迟。
- **本地关单不等于关闭第三方支付 / 自动退款**：支付晚到落 `oms_order_pay_anomaly` 待人工处理。
- **`SENT` 只代表发布成功**，不代表消费成功或订单已取消。
- **下单幂等键仍未实现**；`%RETRY%group` 等 Broker 内建重试刻意不用，只在「连 DLQ 都投递失败」时作为最后兜底。
- **多实例测试是同一 JVM 内不同 `owner` 的租约 CAS 模拟**，不是真集群压测。
- **告警只落日志**，未接外部告警系统；运维接口默认关闭（`admin-enabled=false`）。
- **RocketMQ 5.x 的定时消息能力本次没有用于订单超时判定**；延迟级别只用于消费失败重试。
- 未做性能压测、未做多 Broker / 主从故障、未做 NameServer 宕机演练。

---

## 8. 复跑

```bash
# 1) 启动容器（compose 内已声明 name: mall-local）
docker compose -p mall-local -f document/docker/docker-compose-local.yml \
  up -d mysql rocketmq-namesrv rocketmq-broker redis mongo

# 2) 等 Broker 注册到 NameServer（约 5~10 秒）
docker exec mall-local-rocketmq-broker-1 \
  sh mqadmin clusterList -n rocketmq-namesrv:9876

# 3) 预建 topic（见第 5 节）；测试基类会自行重建测试 topic 与订阅组

# 4) 准备测试库（A/B 结构都在 mall.sql 里）
docker exec mall-local-mysql-1 mysql -uroot -proot \
  -e "CREATE DATABASE IF NOT EXISTS mall_it; CREATE DATABASE IF NOT EXISTS mall_it_b"
docker exec -i mall-local-mysql-1 mysql -uroot -proot --default-character-set=utf8mb4 \
  mall_it   < document/sql/mall.sql
docker exec -i mall-local-mysql-1 mysql -uroot -proot --default-character-set=utf8mb4 \
  mall_it_b < document/sql/mall.sql

# 5) 全量测试（含真实 Broker stop/start，约 4~5 分钟）
mvn.cmd -B -pl mall-portal -am -DskipTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false -Ddocker.skip=true test
```

> 真实停机用例会在测试内调用 `docker compose ... stop/start rocketmq-broker`，
> 需要本机 Docker CLI 可用。**测试之间请保持 `mall-local` 容器处于运行状态。**
> MySQL 必须保持 `+08:00`（compose 已设 `--default-time-zone=+08:00` + `TZ=Asia/Shanghai`），
> 否则 `NOW()` 与 `expire_time` 比较会相差 8 小时，超时判定完全失效。

---

## 9. 简历可用结论（按实际完成范围）

> 将订单超时取消链路的消息中间件从 RabbitMQ 迁移到 RocketMQ 5.3.2，**保留本地消息表 /
> 租约 CAS / 消费幂等 / 提交后确认 / 双层定时兜底五层不变**，只重写收发适配层：
> 同步 `send()` + `SEND_OK` 替代 Publisher Confirm，关闭自动建 topic 后以
> `No route info` 复现不可路由，监听器返回值替代手动 ACK，固定延迟级别替代队列级 TTL 重试。
> 在真实 MySQL 8.4.7 + RocketMQ 5.3.2 上完成 56 个用例全量回归（连续三次复跑全绿），
> 覆盖真实 Broker 停机恢复、3 次重复投递、毒消息进死信、双实例租约竞争、
> 发送失败退避恢复；验证重复投递与服务中断后库存不会重复释放。

验证数据见 [B_ROCKETMQ_TEST_REPORT.md](B_ROCKETMQ_TEST_REPORT.md) 与
[results/b-rocketmq-test-run-2026-11-12.txt](results/b-rocketmq-test-run-2026-11-12.txt)。
