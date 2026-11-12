# A 阶段基线：改造前行为实测

> 方案 §10 第 1 步「建立基线测试」。所有数据来自本次真实 MySQL 执行，未做任何推断。

## 环境与脚本

| 项 | 值 |
| --- | --- |
| 数据库 | MySQL 8.0.37（本机独立实例，端口 3307，独立 datadir，与本机 MySQL80 服务隔离） |
| 隔离级别 | `REPEATABLE-READ`（`@@transaction_isolation`） |
| 表结构 | `document/sql/mall.sql` 原始版本（76 张表，`stock`/`lock_stock` 允许 NULL） |
| 数据库 | `mall_it`，由原始 `mall.sql` 导入，未做任何数据修复 |
| 测试脚本 | `mall-portal/src/test/java/com/macro/mall/portal/it/InventoryProbe.java`，`legacy` 模式 |
| 脚本逻辑 | 复现改造前 `lockStock()`：`SELECT lock_stock` → Java 内存 `old + quantity` → `UPDATE ... SET lock_stock = ?`。用 `CyclicBarrier` 把并发线程统一卡在「读完之后、写之前」，稳定放大竞争窗口（不改变算法本身） |
| 判定口径 | `success` = 返回影响行数 1 的「下单成功」数；`oversold` = 成功锁定总量 > 初始可售库存，或最终 `lock_stock` 与成功锁定总量不一致 |

## 并发下单实测

| 初始 stock | 线程数 | 每单数量 | 成功订单 | 失败 | 异常 | 应锁定 | 最终 stock | 最终 lock_stock | 超卖 | 耗时 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: | ---: |
| 10 | 20 | 1 | 20 | 0 | 0 | 20 | 10 | **1** | 是 | 108 ms |
| 10 | 20 | 2 | 20 | 0 | 0 | 40 | 10 | **2** | 是 | 37 ms |
| 10 | 50 | 1 | 50 | 0 | 0 | 50 | 10 | **1** | 是 | 68 ms |
| 30 | 30 | 2 | 30 | 0 | 0 | 60 | 30 | **2** | 是 | 49 ms |

原始 JSON 输出保存在 `document/order-consistency/results/baseline-legacy.json`。

**结论**：改造前 4 组测试全部超卖。最典型的一组：初始可售 10，20 个请求全部返回下单成功，数据库 `lock_stock` 最终只有 1（并发覆盖丢失更新）。库存字段始终非负，仅检查「没有负库存」完全无法发现该问题——这正是方案 §9 指出的验收陷阱。

## 改造前代码级基线（源码核对，非运行时）

| 环节 | 改造前实现 | 问题 |
| --- | --- | --- |
| 下单锁库存 | `OmsPortalOrderServiceImpl.lockStock()`：`selectByPrimaryKey` → `setLockStock(old + qty)` → `updateByPrimaryKeySelective` | 读改写非原子；`hasStock()` 与 `lockStock()` 之间存在竞争窗口 |
| 支付成功 | `paySuccess()`：`updateByPrimaryKeySelective` 直接置 `status=1`，随后 `PortalOrderDao.updateSkuStock()` 批量 `CASE WHEN` 扣减 | 无状态条件、无影响行数检查；重复通知会重复扣减 |
| 取消订单 | `cancelOrder()`：先 `selectByExample(status=0)`，再 `updateByPrimaryKeySelective` 置 4，再批量释放 | 检查与更新分离；重复取消可能重复释放 |
| 超时取消 | `cancelTimeOutOrder()`：批量查出后 `updateOrderStatus(ids,4)` 无状态条件，再逐单释放 | 一个订单异常导致整批回滚；无逐单状态裁判 |
| 事务 | `@Transactional` 标在 `OmsPortalOrderService` **接口**上，无 `rollbackFor`；`paySuccessByOrderSn()` 自调用 `this.paySuccess()` | 事务入口不清晰；`ApiException` 虽为 RuntimeException 仍需显式声明 |
| 消息 | `generateOrder()` 在事务内 `sendDelayMessageCancelOrder()` | 事务回滚与消息发送非原子；消息丢失导致锁定库存长期占用 |
| 支付回调 | `AlipayServiceImpl.notify()` 仅验签 + 判 `trade_status` | 未核对 `app_id` / 金额 / 商户 / 订单归属；已取消订单的支付成功被静默忽略 |
| 积分 | `memberService.updateIntegration()` 读旧值写新值；`order.setUseIntegration()` 在 `insert` 之后调用 | 并发丢失更新；使用的积分未持久化，取消时无法返还 |
| 库存历史数据 | 原始 `mall.sql` 种子数据含 4 条负数 `lock_stock`（id 98/102/103/106） | 条件更新会拒绝这些 SKU，必须先预检并人工修复 |

预检脚本 `document/sql/mall_a_precheck.sql` 在原始 `mall_it` 库上的输出：

```
total_rows=96  stock_null=0  lock_stock_null=0  stock_negative=0
lock_negative=4  lock_gt_stock=0  anomaly_rows=4
98 / 201808270027001 / stock=86 / lock_stock=-24
102 / 201808270028001 / stock=99 / lock_stock=-8
103 / 201808270028002 / stock=99 / lock_stock=-8
106 / 201808270029001 / stock=99 / lock_stock=-8
```

## 边界说明

- 基线只复现了「锁库存」这一段，因为支付/取消流程涉及 SecurityContext、购物车、优惠券等上下文，改造后由状态机集成测试单独覆盖；两者使用同一套真实 MySQL。
- 探针使用 `CyclicBarrier` 放大窗口以稳定复现；不放大窗口时同样会超卖，只是概率性出现。
- 本基线不代表生产 QPS，只用于改造前后的**同一脚本对比**。
