# A 阶段测试与验收报告

> 全部数据来自 2026-11-12 在真实 MySQL 上的实际执行，不含估算值。
> 原始输出见 `results/`，基线见 `BASELINE.md`。
>
> **本文是 A 阶段的验收快照**：第 5 节关于「消息可靠性属于 B、本次未实现」的描述
> 只针对 A 当时范围。B 阶段已实施并验收：最初用 RabbitMQ 4.2.9
> （[B_TEST_REPORT.md](B_TEST_REPORT.md)，历史），当前代码已切换到 RocketMQ 5.3.2
> （[B_ROCKETMQ_TEST_REPORT.md](B_ROCKETMQ_TEST_REPORT.md)）。
> A 的 32 个用例在两次全量回归中仍全部通过。

## 1. 测试环境

| 项 | 值 |
| --- | --- |
| 机器 | Windows 11 家庭版中文版 / 12th Gen Intel Core i5-12500H / 16 逻辑核 / 15.73 GB RAM |
| JDK | 17.0.11 |
| 构建 | Apache Maven 3.9.9，Spring Boot 3.5.14 |
| 数据库 | **MySQL 8.0.37**（本机独立实例 `mysqld`，端口 3307，独立 datadir `target/it-mysql/data`，与本机 MySQL80 服务完全隔离） |
| 隔离级别 | `REPEATABLE-READ`（`@@transaction_isolation`） |
| 数据库 | `mall_it`（独立测试库）；另有 `mall`（原始库，用于预检对照）、`mall_newcheck`（验证新环境初始化 SQL） |
| 表结构 | `document/sql/mall.sql`（77 张表）+ `document/sql/mall_a_migration.sql` |
| 代码版本 | 改造前 `dcaa93b`（HEAD，工作区改动前的原始下单/支付/取消实现）；改造后为当前工作区（尚未提交） |
| 并发脚本 | `mall-portal/src/test/java/com/macro/mall/portal/it/InventoryProbe.java`，`legacy` / `atomic` 两种模式共用同一线程模型与参数 |

> 方案建议 MySQL 8.4；本机 Docker 守护进程未启动且 WSL 被安全策略禁用，因此改用同机安装的
> MySQL 8.0.37 独立实例。行锁、隔离级别、条件更新影响行数均为真实 InnoDB 行为，
> 与 H2/内存库无关。若后续用 `document/docker/docker-compose-local.yml` 起 8.4.7，
> 可用同一套脚本复跑。

## 2. 改造前后同一脚本对比

`InventoryProbe` 唯一差别是执行的 SQL：`legacy` 复现改造前
`SELECT lock_stock → Java 加 → UPDATE 覆盖写`，`atomic` 执行改造后的条件更新。
其余（线程数、屏障、建 SKU、清理、统计口径）完全一致。

| 初始库存 | 线程 | 每单量 | 改造前成功 | 改造前 lock_stock | 改造前超卖 | 改造后成功 | 改造后 lock_stock | 改造后超卖 |
| ---: | ---: | ---: | ---: | ---: | :---: | ---: | ---: | :---: |
| 10 | 20 | 1 | 20 | **1** | 是 | 10 | 10 | 否 |
| 10 | 20 | 2 | 20 | **2** | 是 | 5 | 10 | 否 |
| 10 | 50 | 1 | 50 | **1** | 是 | 10 | 10 | 否 |
| 30 | 30 | 2 | 30 | **2** | 是 | 15 | 30 | 否 |

| 组 | 改造前耗时 | 改造后耗时 |
| --- | ---: | ---: |
| 10 / 20 / 1 | 108 ms | 55 ms |
| 10 / 20 / 2 | 37 ms | 48 ms |
| 10 / 50 / 1 | 68 ms | 82 ms |
| 30 / 30 / 2 | 49 ms | 93 ms |

耗时量级相近，且受本机调度影响较大，**不据此宣称性能提升**；这里只证明
「同一并发压力下，改造前会凭空多出成功订单且 lock_stock 丢失更新，改造后不会」。
库存字段在改造前后都非负，说明「只检查没有负库存」确实无法发现超卖。

端到端 `generateOrder()` 并发（真实 Spring 事务 + 真实 MySQL，20 线程 / 初始可售 10）：

```
successOrders=10, rejected=10, errors=0, dbOrders=10, finalStock=10, finalLock=10, elapsedMs=184
```

成功订单数与数据库订单数、lock_stock 三者一致。

## 3. 验收场景逐项结果

| 方案 §9 场景 | 测试位置 | 结果 |
| --- | --- | --- |
| 并发购买 | `GenerateOrderConcurrencyTest#concurrentGenerateOrder_neverCreatesMoreOrdersThanAvailableStock`、`OrderStateTransitionServiceTest#concurrentLockingNeverOversells` | 通过。10 个成功 / 20 请求；30 线程抢 10 库存也只成功 10 次 |
| 多 SKU 部分失败 | `...#generateOrder_rollsBackAllSkusWhenOneIsInsufficient`、`...#paySuccess_rollsBackOrderAndEarlierSkuWhenAnySkuFails` | 通过。先成功的 SKU 与订单状态一起回滚 |
| 同 SKU 多明细 | `...#generateOrder_aggregatesDuplicateSkuCartItems`、`...#paySuccess_aggregatesDuplicateSkuOrderItems`、`OrderSkuAggregatorTest` | 通过。3+4 汇总为 7，只执行一次库存更新 |
| 重复支付通知 | `...#paySuccess_deductsStockExactlyOnce` | 通过。第二次返回 `DUPLICATE`，库存不变 |
| 重复取消 | `...#cancelUserOrder_checksOwnershipAndReleasesOnlyOnce`、`OrderTimeOutScanTest#duplicateCancelMessageCannotReleaseStockTwice` | 通过。第二次返回 `ALREADY_CLOSED`，库存/券/积分不再变化 |
| 支付与取消竞争 | `...#paymentAndCancelRace_hasExactlyOneWinnerAndConsistentStock`（20 轮） | 通过。每轮恰好一个胜者；支付胜出 stock-3，取消胜出 stock 不变且 lock 归零；取消胜出时支付侧落待对账记录 |
| 库存异常 | `PortalOrderDaoAtomicSqlTest#lockStock_rejectsAnomalousHistoricalRows`、`...#deductAndReleaseSkuStock_checkTheirOwnPreconditions` | 通过。SKU 不存在 / NULL / 负数 / 超锁定 / 可售不足一律 0 行；订单状态回滚 |
| 用户权限 | `...#cancelUserOrder_checksOwnershipAndReleasesOnlyOnce` | 通过。非归属用户取消被拒，状态与库存不变 |
| 支付晚到 | `...#latePayment_isRecordedAsTrackableAnomaly` | 通过。已关闭订单收到可信支付结果 → `LATE_PAYMENT_RECORDED`，`oms_order_pay_anomaly` 落 1 条，重复通知不重复落库；本地不扣库存 |
| 历史数据 | `document/sql/mall_a_precheck.sql` 实测 + `PortalOrderDaoAtomicSqlTest#lockStock_rejectsAnomalousHistoricalRows` | 通过。原始库识别出 4 条负数 `lock_stock`，交叉核对确认这些 SKU 无待付款订单占用；启动预检与 DAO 查询能识别 NULL/负数/超锁定，不静默按 0 计算；迁移脚本守卫在未修复的 `mall` 库上以 `ERROR 1644 (45000)` 中止且未改动任何表结构 |
| 消息故障 | `OrderTimeOutScanTest`（全部 4 个用例） | 通过。取消消息重复/提前投递不会释放库存；MQ 不可用时由定时扫描兜底；逐单独立事务，异常订单不拖垮整批 |
| 事务回滚 | `PortalOrderDaoAtomicSqlTest#multiSkuUpdatesRollBackTogether` 及上述失败用例 | 通过。异常影响行数导致整事务回滚 |
| 积分持久化与返还 | `GenerateOrderConcurrencyTest#generateOrder_persistsUsedIntegrationAndCancelReturnsIt` | 通过。`use_integration` 已写入订单表，取消后积分原额返还 |

全部 32 个测试通过（0 失败 / 0 错误 / 0 跳过），全项目 8 个模块编译通过。

## 4. 并发测试记录

| 项 | 值 |
| --- | --- |
| MySQL 版本 / 隔离级别 | 8.0.37 / REPEATABLE-READ |
| 机器 | i5-12500H / 16 核 / 15.73 GB / Windows 11 |
| 初始库存 | 10（探针另测 30） |
| 请求数 / 并发线程 | 20 / 20（探针另测 50 / 30） |
| 每单数量 | 1（探针另测 2） |
| 成功 / 失败订单 | 10 / 10（端到端）；10 / 20（DAO 30 线程） |
| 最终 stock / lock_stock | 10 / 10 |
| 异常数 | 0（除预期内的「库存不足」业务拒绝） |
| 耗时 | 端到端 184 ms；DAO 并发 52 ms；探针 48–93 ms |
| 执行次数 | 全量套件重复运行 3 次均通过；支付/取消竞争每轮 20 次，累计 ≥60 轮 |
| 改造前后代码版本 | `dcaa93b` → 当前工作区（未提交） |
| 同一测试脚本 | `InventoryProbe.java` |

竞争测试多次运行的胜者分布：11:9 / 10:10 / 7:13 / 6:14（支付胜:取消胜）。
分布随线程调度变化，但每轮都恰好一个胜者，且库存与最终状态一致。

## 5. 未覆盖与已知边界（必须如实说明）

1. **没有引入 Redis 分布式锁**，也不需要做；数据库条件更新是唯一防线。
2. **下单幂等未实现**。库存原子更新只能防止总量超卖，用户重复提交仍会生成两笔合法订单。
   需要额外的请求幂等键 + `oms_order` 唯一约束，属于 A 的补充任务 / B 阶段。
   本次测试中的「重复」只针对支付通知与取消消息，不针对下单提交。
3. **支付晚到只做到「识别 + 留痕 + 告警」**，未实现自动关单、对账与退款。
   数据库条件更新无法撤销第三方已发生的支付。
   `AlipayServiceImpl.notify()` 对 `LATE_PAYMENT_RECORDED` 返回 `success` 是为了避免第三方
   无限重投；异常已用 `REQUIRES_NEW` 独立事务落库并输出 ERROR 日志，`handle_status=0` 待人工处理。
4. **未持久化第三方交易号**：重复通知按商户订单号 + 金额 + 本地状态判幂等，
   无法区分「同一订单出现两个不同 trade_no」。若需要，可加 `trade_no` 唯一约束后收紧。
5. **消息可靠性属于 B**：本次实现了「事务提交后发送」+「定时超时扫描兜底」+
   「消费者按状态与超时时间校验」，但没有发送端 Confirm、本地消息表、消费失败重试。
   因此「订单创建后消息彻底丢失且扫描任务也被停掉」仍会长期占用锁定库存。
6. **优惠券/积分**：积分与券状态已改为原子条件更新并纳入验收；
   但历史订单可能没有对应的券使用记录，此时只告警不回滚（保持库存与状态正确）。
7. **死锁**：多 SKU 按 ID 升序已降低概率，本次测试未出现死锁；仍可能发生。
   死锁/锁等待超时被识别为「事务已回滚、可重试失败」，返回「请重试」，不伪装成成功。
8. **性能数字**：所有耗时来自本机单次运行，不是 QPS 或压测结论，不能写成性能提升。
9. **端口 3307 的独立实例**是本次测试临时启动的，未改动本机 `MySQL80` 服务与任何业务数据。

## 6. 如何复跑

```powershell
# 1) 起独立 MySQL（若已用 docker-compose-local.yml 则可跳过，端口同为 3307）
#    本文档使用本机 MySQL 8.0 的 mysqld，独立 datadir，root/root

# 2) 建库导数据
mysql -h127.0.0.1 -P3307 -uroot -proot -e "CREATE DATABASE mall_it"
mysql -h127.0.0.1 -P3307 -uroot -proot mall_it < document/sql/mall.sql
#    已有库先跑预检，人工修复异常数据后再跑迁移
mysql -h127.0.0.1 -P3307 -uroot -proot mall_it < document/sql/mall_a_precheck.sql
mysql -h127.0.0.1 -P3307 -uroot -proot mall_it < document/sql/mall_a_migration.sql

# 3) 全量测试（根 pom 默认 skipTests=true，必须显式覆盖）
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B -pl mall-portal -am `
    '-DskipTests=false' '-Dsurefire.failIfNoSpecifiedTests=false' '-Ddocker.skip=true' test

# 4) 同一脚本的改造前后对比探针
#    javac 后用 java -cp <classes>;<mysql-connector-j.jar> com.macro.mall.portal.it.InventoryProbe \
#         legacy|atomic <jdbc-url> root root <stock> <threads> <qty>
```
