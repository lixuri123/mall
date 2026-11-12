# A：订单状态与库存一致性改造

> 本目录是方案 A 的落地说明与实测证据。方案覆盖**下单锁库存、支付扣库存、取消释放库存、
> 订单状态条件流转、并发与回滚测试**。消息可靠性属于 B，但 A 明确了消息丢失时的边界与兜底。
>
> - 改造前基线：[BASELINE.md](BASELINE.md)
> - 测试报告与实测数据：[TEST_REPORT.md](TEST_REPORT.md)
> - 原始输出：[results/](results/)
> - **后续阶段 B（消息可靠性）已实施**：[B_README.md](B_README.md) / [B_TEST_REPORT.md](B_TEST_REPORT.md)
>
> **状态：A 阶段代码与测试已完成并实际跑通；尚未提交 Git。本文描述的是 A 阶段当时的范围，
> 其中「属于 B、本次未实现」的表述是 A 的边界快照，不代表当前工作区仍未实现。
> 所有结论以实际完成的范围为准。**

---

## 1. 规则与状态机

`pms_sku_stock`：`stock` = 实际库存，`lock_stock` = 待支付订单占用量，可售 = `stock - lock_stock`。

| 操作 | `stock` | `lock_stock` | 可售 |
| --- | ---: | ---: | ---: |
| 初始（10） | 10 | 0 | 10 |
| 下单 3 件 | 10 | 3 | 7 |
| 支付成功 | 7 | 0 | 7 |
| 订单取消 | 10 | 0 | 10 |

```text
0 待付款 ──支付成功──> 1 待发货
    │
    └────取消 / 超时──> 4 已关闭
```

不变量：

1. 同一个待付款订单只能有一次有效状态变更（两条条件 UPDATE 竞争同一行，数据库行锁裁决）。
2. 支付成功后只扣一次库存；取消后只释放一次锁定库存。
3. 库存变更失败时订单状态一起回滚（同一事务）。
4. 并发下单成功锁定量不超过可售库存（条件更新 + 检查影响行数）。
5. 多 SKU 恒按 `skuId` 升序处理，降低死锁概率。

## 2. 改了哪些文件

| 文件 | 改动 |
| --- | --- |
| `mall-portal/.../dao/PortalOrderDao.java` | 重写为单 SKU 原子方法 + 订单条件状态更新 + 积分/券条件更新 + 超时分页 + 异常记录 + 数据预检，全部返回影响行数 |
| `mall-portal/src/main/resources/dao/PortalOrderDao.xml` | 删除批量 `CASE WHEN` 与无状态批量更新，实现全部带条件的 SQL |
| `mall-portal/.../service/impl/OmsPortalOrderServiceImpl.java` | `generateOrder()` 输入校验、SKU 汇总排序、原子锁库存、事务内回滚、修复 `useIntegration` 持久化、积分原子扣减、消息改到事务提交后发送、`cancelTimeOutOrder()` 逐单分页取消 |
| `mall-portal/.../service/OmsPortalOrderService.java` | 事务注解从接口移到实现类；明确用户取消 / 系统取消 / 可信支付结果三类入口 |
| `mall-portal/.../service/OrderStateTransitionService.java` + `impl/OrderStateTransitionServiceImpl.java` | **新增**独立 Bean，承载支付成功与统一取消的全部事务逻辑（避免自调用失效） |
| `mall-portal/.../service/OmsOrderPayAnomalyRecorder.java` + `impl/...` | **新增** `REQUIRES_NEW` 异常记录器，保证支付晚到记录不随外层事务回滚 |
| `mall-portal/.../domain/{SkuQuantity,PayCommand,PayResult,CancelCommand,CancelResult,OmsOrderPayAnomaly}.java` | **新增**状态机输入输出与异常记录模型 |
| `mall-portal/.../util/OrderSkuAggregator.java` | **新增**按 SKU 汇总（`TreeMap` 保证升序），拒绝非法数量/SKU |
| `mall-portal/.../exception/OrderConsistencyException.java` + `...Handler.java` | **新增**内部一致性异常与统一响应；死锁/锁等待超时按「可重试失败」返回 |
| `mall-portal/.../controller/OmsPortalOrderController.java` | 移除对普通用户开放的 `/order/paySuccess` 与 `/order/cancelOrder`；用户取消与系统超时取消分离 |
| `mall-portal/.../controller/OmsPortalOrderDebugController.java` | **新增**调试入口，默认不注册（`mall.order.debug-endpoints-enabled=true` 才生效） |
| `mall-portal/.../service/impl/AlipayServiceImpl.java` | 验签外核对 `app_id` / 订单号 / 实付金额 / 交易状态 / 交易号；重复通知与支付晚到分流；主动查询同样核对金额 |
| `mall-portal/.../component/CancelOrderReceiver.java` | 统一走系统取消入口，重复/提前消息不会释放库存，失败由扫描兜底 |
| `mall-portal/.../component/OrderTimeOutCancelTask.java` | **启用**定时扫描兜底（间隔可配），逐单调用条件取消逻辑 |
| `mall-portal/.../component/StockDataPrecheckRunner.java` | **新增**启动历史库存预检（只告警，不自动修复） |
| `mall-portal/src/main/resources/application.yml` | 新增调试入口开关与超时扫描开关/间隔 |
| `document/sql/mall.sql` | `stock`/`lock_stock` 改 `NOT NULL DEFAULT 0`、修正 4 条负数种子数据、新增超时扫描索引与 `oms_order_pay_anomaly` 表 |
| `document/sql/mall_a_precheck.sql` | **新增**已有库历史数据预检（只读） |
| `document/sql/mall_a_migration.sql` | **新增**已有库迁移（守卫 + 新表 + 索引 + 列定义收紧） |
| `mall-portal/src/test/java/com/macro/mall/portal/it/*` | **新增** 5 个集成/单元测试类 + 同一脚本并发探针；原有 2 个测试类加测试期后台任务开关 |

## 3. 关键设计

### 3.1 库存 SQL：条件更新 + 影响行数

```sql
-- 下单锁库存
UPDATE pms_sku_stock SET lock_stock = lock_stock + #{quantity}
WHERE id = #{skuId} AND #{quantity} > 0
  AND stock IS NOT NULL AND lock_stock IS NOT NULL
  AND stock >= 0 AND lock_stock >= 0 AND stock >= lock_stock
  AND stock - lock_stock >= #{quantity};

-- 支付成功
UPDATE pms_sku_stock
SET stock = stock - #{quantity}, lock_stock = lock_stock - #{quantity}
WHERE id = #{skuId} AND #{quantity} > 0
  AND stock >= #{quantity} AND lock_stock >= #{quantity};

-- 取消
UPDATE pms_sku_stock SET lock_stock = lock_stock - #{quantity}
WHERE id = #{skuId} AND #{quantity} > 0 AND lock_stock >= #{quantity};
```

服务层对每个 SKU 检查「影响行数必须为 1」，否则抛异常让整笔事务回滚。
`hasStock()` 预检查已移除——检查与更新之间的窗口无法靠它消除。

### 3.2 订单状态是支付与取消的裁判

```sql
UPDATE oms_order SET status = 1, payment_time = NOW(), pay_type = #{payType}
WHERE id = #{orderId} AND status = 0 AND delete_status = 0;

UPDATE oms_order SET status = 4 WHERE id = #{orderId} AND status = 0 AND delete_status = 0;
```

影响 0 行时**用 `SELECT ... FOR UPDATE` 当前读**分类（避免 REPEATABLE-READ 快照读到旧状态）：

- 已支付 → `DUPLICATE`，幂等成功，不再扣库存。
- 已关闭/无效 → 落 `oms_order_pay_anomaly`（`REQUIRES_NEW`）+ ERROR 告警，进入待对账/退款流程。
- 订单不存在 / 金额不符 / 状态不允许 → `REJECTED`，回调返回 `failure` 让第三方重投。

只有抢到状态变更的请求才允许改动库存，因此支付只扣一次、取消只释放一次，
已支付订单永远不会被取消逻辑释放库存。

### 3.3 事务边界

- 事务注解统一在实现类的入口方法上，显式 `rollbackFor = Exception.class`。
- 支付与取消的真实事务入口是独立的 `OrderStateTransitionService` Bean；下单服务、MQ 消费者、
  定时任务都通过它调用，不再出现 `this.paySuccess()` 这类自调用。
- `cancelTimeOutOrder()` **本身不加事务**，分页（keyset 游标）取候选订单 ID 后逐单调用状态机，
  每单独立事务；某单异常只回滚自己并输出失败告警，下一轮扫描继续重试。
- 下单：锁库存、订单与明细写入、券/积分、购物车清理全在一个事务内；消息改为
  `afterCommit` 发送。

### 3.4 消息与超时的边界

```text
订单提交成功但消息未送达  → 定时超时扫描兜底（默认 60s 间隔，可配）
订单回滚但消息已发出      → afterCommit 发送；且消费者按状态与超时时间校验
重复 / 提前投递的消息      → 条件更新 0 行 + 超时校验，不会重复释放库存
MQ 完全不可用             → 消息发送失败只告警，订单照常提交，由扫描兜底
```

扫描间隔 `mall.order.timeout-scan.interval-ms`（默认 60000），
开关 `mall.order.timeout-scan.enabled`（默认 true），首次延迟默认 30s。
失败订单保持 `status=0` 并输出 ERROR 级告警。
**发送端 Confirm / 本地消息表 / 消费失败重试属于 B，A 阶段未实现；
B 阶段已在当前工作区补齐，见 [B_README.md](B_README.md)。**

## 4. 历史数据处理

原始 `mall.sql` 的演示数据里，id 98/102/103/106 的 `lock_stock` 为负数（-24 / -8 / -8 / -8）。
条件更新会直接拒绝这些 SKU，因此：

- **新环境**：`document/sql/mall.sql` 已修正这 4 条数据并把两列改为 `NOT NULL DEFAULT 0`。
- **已有库**：先跑 `mall_a_precheck.sql`（只读）逐条确认来源，再跑 `mall_a_migration.sql`。
  迁移脚本带守卫，仍有异常数据时直接中止，**不会把异常值静默清零**，
  因为那可能抹掉真实待支付订单的锁定库存。
- 运行时 `StockDataPrecheckRunner` 启动即预检并告警。
- 可选的数据库层 CHECK 约束在两个 SQL 文件里都以注释形式提供，需先评估后台改库存流程再启用。

## 5. 实施顺序对照（方案 §10）

| 步骤 | 状态 | 产物 |
| --- | --- | --- |
| 1 建立基线测试 | 完成 | `BASELINE.md`、`InventoryProbe` `legacy` 模式 4 组实测 |
| 2 预检历史库存数据 | 完成 | `mall_a_precheck.sql`，实测识别出 4 条负数记录 |
| 3 实现原子锁库存 | 完成 | DAO/XML + 下单改造 + 并发/回滚/重复 SKU 测试 |
| 4 支付条件流转与扣减 | 完成 | 状态机 + 支付宝回调核对 + 重复通知/回滚测试 |
| 5 统一取消逻辑 | 完成 | 用户/系统双入口 + 归属/超时校验 + 重复取消/竞争测试 |
| 6 积分、券、支付晚到 | 完成（留痕，不含自动退款） | 原子增减、`use_integration` 持久化修复、`oms_order_pay_anomaly` |
| 7 超时补偿并对接 B | 完成 A 的兜底部分 | `afterCommit` 发送、定时扫描、消费者幂等；B 已独立实施，见 [B_README.md](B_README.md) |
| 8 重复执行测试并保存数据 | 完成 | 32 个测试全绿，`results/` 与 `TEST_REPORT.md` |

## 6. 明确没做的事（不要写进简历）

- 没有 Redis 分布式锁、没有 Redis 预扣库存、没有秒杀/限流/熔断。
- **下单幂等未实现**：库存原子更新只防超卖，用户重复提交仍可能产生两笔合法订单。
  需要请求幂等键 + 数据库唯一约束，属于 A 的补充任务。
- 没有实现自动关单、对账、退款；支付晚到只做到「识别 + 独立事务留痕 + 告警」。
- 没有持久化第三方交易号，重复通知按商户订单号 + 金额 + 状态判幂等。
- 没有发送端 Confirm、本地消息表、消费失败重试（属 B 阶段范围，已在当前工作区补齐，见 [B_README.md](B_README.md)）。
- 没有压测/QPS 结论；报告里的耗时只是本机单次执行耗时。

## 7. 可以写进简历的结论（仅覆盖实测范围）

> 在开源电商系统 mall（Spring Boot 3.5 + MyBatis + MySQL，7 模块 / 77 张表）上，
> 针对下单链路做并发一致性重构：定位到原 `lockStock()` 的 `select → 内存计算 → update`
> 丢失更新，改为 MySQL 单 SKU 条件更新并按影响行数判定；用订单 `status` 条件更新作为
> 支付与取消竞争的唯一裁判，库存与状态变更同事务提交或回滚；多 SKU 按 ID 升序处理。
> 同时修复支付/取消重复副作用、`use_integration` 未持久化、事务内发 MQ 等问题，
> 并补齐事务提交后发送 + 定时超时扫描兜底。
>
> 验证：真实 MySQL 8.0（REPEATABLE-READ）上用同一并发脚本对比，初始可售 10 / 20 线程时
> 改造前 20 单全部「成功」且 `lock_stock` 最终只有 1，改造后成功 10 单、`lock_stock=10`；
> 端到端并发下单 20 请求产生 10 笔订单且订单数与 `lock_stock` 一致；
> 32 个集成/单元测试覆盖重复支付、重复取消、支付取消竞争 20 轮、多 SKU 回滚、
> 库存异常、用户越权、支付晚到留痕与消息重复/提前投递，全部通过。

## 8. 配置与复跑

```yaml
mall:
  order:
    debug-endpoints-enabled: false   # 开启后才有 /order/paySuccess、/order/cancelOrder
    timeout-scan:
      enabled: true
      initial-delay-ms: 30000
      interval-ms: 60000
```

复跑命令、环境要求、原始输出见 [TEST_REPORT.md](TEST_REPORT.md) 第 6 节。
