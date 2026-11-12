# mall 项目学习路线与项目经历指南

> 适用对象：有 Java / Spring Boot 基础，想通过本项目形成一段可写进简历的项目经历。
> 本指南基于**上游原始 mall 源码**撰写，描述的是改造前的实现。
>
> **⚠️ 本工作区已完成 A / B / MQ 替换三阶段改造，下文多处内容已过时**：
> `QueueEnum` / `RabbitMqConfig` / `CancelOrderSender` 的 TTL 延迟队列、`@RabbitListener`、
> `normal_order_ovetime` 驱动超时等均已不存在；当前订单链路与类名以
> [order-consistency/B_ROCKETMQ.md](order-consistency/B_ROCKETMQ.md) 与
> [根 README 的「本地改造记录」](../README.md) 为准。
> 本指南保留作为「读原始项目、对照改造前后差异」的参考。

---

## 0. 先校准：你手上的是哪个版本

| 项 | 当前工作区实际情况 |
| --- | --- |
| 框架 | Spring Boot 3.5.14、JDK 17、`jakarta.*`，7 个 Maven 模块 |
| 定位 | **单体多模块**电商系统（前台 mall-portal + 后台 mall-admin + 搜索 mall-search），不是微服务；微服务版是另一个仓库 mall-swarm |
| 规模 | `document/sql/mall.sql` 共 76 张表：pms 18 / oms 8 / sms 13 / ums 25 / cms 12 |
| 模块 | mall-common（底座）→ mall-mbg（MBG 生成物）→ mall-security → mall-admin / mall-portal；mall-search 只依赖 mbg；mall-demo 是教学示例 |

**最容易浪费时间的一点**：网上《mall 学习教程》大多是 Spring Boot 2.x 版，类名对不上。

| 老教程 | 本仓库实际 |
| --- | --- |
| `DynamicSecurityFilter` / `AccessDecisionManager` | `DynamicAuthorizationManager implements AuthorizationManager` |
| jjwt `Jwts.builder()` | Hutool `JWTUtil` |
| `RestHighLevelClient` / `NativeSearchQueryBuilder` | `co.elastic.clients` + `ElasticsearchTemplate` + `NativeQueryBuilder` |
| `EsProductService.suggest()` | 已删除，全仓库无 `suggest` |
| `javax.*` | `jakarta.*` |

结论：**以本地源码为准**，教程只用来理解思路。这本身也是一个可讲的技术点——同一业务模型在 Security 5→6、ES 7→8 两代 API 下的迁移差异。

---

## 1. 重要的点在哪：四条链路 + 三层地基

不要按模块从头翻到尾。70 多个 Controller 里绝大部分是 CRUD，真正的价值集中在下面这些地方。

### 1.1 认证与授权（优先级最高，面试第一问）

完整链路（按顺序读）：

1. `mall-security/.../config/SecurityConfig.java` → `filterChain()`：白名单放行、OPTIONS 放行、`STATELESS` 无状态会话、自定义 401/403 处理、`addFilterBefore(jwtAuthenticationTokenFilter, ...)`。
2. `.../component/JwtAuthenticationTokenFilter.java` → `doFilterInternal()`：取 `Authorization: Bearer xxx` → 解析用户名 → `loadUserByUsername` → 验签 → 写 `SecurityContextHolder`。
3. `.../util/JwtTokenUtil.java`：生成、验签、过期判断、`refreshHeadToken()`（30 分钟内不重复刷新）。
4. 后台动态权限四件套：
   - `mall-admin/.../config/MallSecurityConfig.java` → `dynamicSecurityService().loadDataSource()`：把 `ums_resource` 表读成 `url → "id:name"` 映射；
   - `.../security/component/DynamicSecurityMetadataSource.java`：`AntPathMatcher` 匹配 URL → 需要的权限集合；
   - `.../security/component/DynamicAuthorizationManager.java` → `check()`：与当前用户权限求交集，返回 `AuthorizationDecision`；
   - `mall-admin/.../bo/AdminUserDetails.java` → `getAuthorities()`：用户拥有的 `id:name` 字符串。**这个字符串是整套机制的"接头暗号"**。
5. 权限变更闭环：`UmsResourceController` 增删改后调 `clearDataSource()` 触发规则重建。

**最值得学的技巧**：`CommonSecurityConfig` 用 `@ConditionalOnBean(name = "dynamicSecurityService")` 条件注册动态权限 Bean。mall-admin 注册了 → 走完整 RBAC；mall-portal **只注册 `UserDetailsService`** → `SecurityConfig` 里 `dynamicAuthorizationManager == null`，退化为"只验登录"。同一份 security 模块，两种安全级别。

要能回答：JWT 存在哪、怎么失效/续期、权限数据存在哪、改了权限多久生效、为什么 portal 不需要 RBAC、两个模块 token 为什么不通（`jwt.secret` 不同）。

### 1.2 下单与库存状态机（项目最值钱的部分）

主入口：`mall-portal/.../service/impl/OmsPortalOrderServiceImpl.java` → `generateOrder(OrderParam)`（约 250 行，逐行读）。

```
校验地址 → 取当前会员(SecurityContext) → 购物车带促销项
  → 校库存 → 优惠券资格与按金额比例分摊 → 积分抵扣与分摊
  → 计算实付金额 → 锁定库存(lock_stock) → 组装订单(status=0)
  → 写 oms_order + oms_order_item → 券置已用 / 扣积分 → 清购物车
  → 发延迟消息取消订单
```

配套：
- 订单号 `generateOrderSn()`：日期 + 来源 + 支付类型 + **Redis `incr` 自增**（`mall:oms:orderId` + yyyyMMdd）。
- 支付成功 `paySuccess()`：`status=1`，`PortalOrderDao.xml` 的 `updateSkuStock` 扣真实库存、释放锁定库存。
- 取消 `cancelOrder()`：`status=4` → 释放锁定库存 → 券回退 → 积分返还，靠 `status=0` 条件做天然幂等。
- 超时取消：`QueueEnum` + `RabbitMqConfig`（TTL 队列配 `x-dead-letter-exchange`）+ `CancelOrderSender`（逐条消息 `setExpiration`）+ `CancelOrderReceiver`（`@RabbitListener` → `cancelOrder`）。时长来自 `oms_order_setting.normal_order_ovetime`。
- 定时任务 `OrderTimeOutCancelTask` 的 `@Component` 被注释，实际只有 MQ 一套在跑。
- 金额体系：`total_amount / promotion_amount / coupon_amount / integration_amount / freight_amount / pay_amount` 六个字段如何流转，务必自己算一遍。
- 促销计算：`OmsPromotionServiceImpl`、`SmsCouponHistoryDao.xml`、`CartPromotionItem`。

**已知缺陷（改掉就是你的项目经历）**：`lockStock()` 是非原子读改写，`hasStock()` 与 `lockStock()` 之间无锁 → 超卖窗口；`@Transactional` 标在接口上且无 `rollbackFor`；事务内发 MQ，DB 回滚消息收不回；无消费失败重试、无发送端 Confirm。

### 1.3 商品搜索：MySQL → ES 的读写分离

- `mall-search/.../domain/EsProduct.java`：`@Document(indexName="pms")`，`name/subTitle/keywords` 用 `ik_max_word`，`attrValueList` 为 `Nested`。
- `.../dao/EsProductDao.xml`：`pms_product` 三联表 + `columnPrefix="attr_"` 装配 Nested 属性。
- `EsProductServiceImpl`：`importAll()` 全量导入；`search(...)` 用 `bool` 过滤 + `function_score`（name 权重 10 / subTitle 5 / keywords 2、`ScoreMode.Sum`、`minScore=2.0`）+ 四种排序；`recommend()` 排除自身后加权；`searchRelatedInfo()` 用 `multiMatch` + 嵌套聚合手工解析品牌/分类/属性。
- 缺口：只有手动 `importAll`，MySQL 商品变更后 ES 不会自动同步 → 适合做增量同步改造。

### 1.4 缓存与降级

- `UmsAdminCacheServiceImpl` + `UmsAdminServiceImpl.getAdminByUsername()/getResourceList()`：手写 **Cache-Aside**（先缓存 → miss 查库 → 回填），更新/改角色/登出时主动失效；全项目**没有** `@Cacheable`（`RedisCacheManager` 定义了但没用）。
- `mall-security/.../aspect/RedisCacheAspect.java`：切面吞掉缓存异常，Redis 挂了不影响主流程；**除非**方法上有 `@CacheException`（验证码 `setAuthCode/getAuthCode` 带此注解，必须强一致）。这是一个很好的"按业务区分降级策略"设计。
- `UmsMemberCacheServiceImpl`：验证码 90 秒过期；`UmsMemberServiceImpl.generateAuthCode()` 是模拟短信，验证码直接返回前端。
- MongoDB：浏览记录 / 商品收藏 / 品牌关注，三个 `MongoRepository` + `PageRequest.of(pageNum-1, pageSize)`。

### 1.5 三层工程地基

| 层 | 看什么 |
| --- | --- |
| 统一底座 | `CommonResult` / `ResultCode` / `CommonPage`（同时适配 PageHelper 的 `PageInfo` 和 Spring Data 的 `Page`）、`GlobalExceptionHandler`（`@ControllerAdvice` 三类异常）、`Asserts.fail()`、`WebLogAspect`（AOP 记录耗时/IP/入参并输出 JSON 给 Logstash）、`BaseRedisConfig` |
| 数据访问 | `mall-mbg/generatorConfig.xml` + `Generator.main()` 生成 model/mapper/Example 三件套（230 个 Java + 74 个 XML，只管单表 CRUD）；复杂联表全靠各业务模块 `resources/dao/*.xml`。精读 `PmsProductDao.xml#getUpdateInfo`（`resultMap extends` MBG 映射 + 5 个 `columnPrefix` collection + 6 个 LEFT JOIN）和 `OmsOrderDao.xml#delivery`（CASE WHEN + foreach 批量发货）。分页一律 `PageHelper.startPage()` + `CommonPage.restPage()` |
| 多模块装配 | 根 `pom.xml` 的 `dependencyManagement`、各模块 `MyBatisConfig` 的 `@MapperScan` + `@EnableTransactionManagement`、`application.yml` 的 `mapper-locations` 同时加载 `dao/*.xml` 与 `com/**/mapper/*.xml` |

### 1.6 锦上添花（写简历时作技术栈，不值得深挖）

OSS 服务端签名直传（`OssServiceImpl.policy()`）、MinIO 上传并自动建桶设策略、支付宝 `AlipayServiceImpl`（验签 `rsaCheckV1`、金额从 DB 取防篡改，但 `notifyUrl` 为空所以真实回调跑不通）、SpringDoc/OpenAPI 3 分组、`document/docker/docker-compose-env.yml` 一键起 9 个中间件、ELK 日志（`logback-spring.xml` 条件加载 4560–4563 端口，dev 默认关闭）。

---

## 2. 需要了解什么：知识清单

**必须已经会**（不会就边补边看）：

- Spring IoC / AOP / 条件装配（`@ConditionalOnBean`）、Spring Boot 自动配置与 `@ConfigurationProperties`
- Spring MVC 参数绑定、`@Validated` + 自定义校验注解、`@ControllerAdvice`
- MyBatis：`ResultMap` / `association` / `collection` / 动态 SQL / `Example` 条件、`@MapperScan`
- 事务：传播行为、rollbackFor、自调用失效、事务边界
- MySQL：索引、事务隔离级别、`LEFT JOIN` / `GROUP BY` / `CASE WHEN`、乐观锁条件更新
- Redis 常用数据结构、`incr`、过期时间
- HTTP / JWT / CORS / RBAC 基本概念

**看项目时顺势补上**：

- Spring Security 6 过滤器链、`SecurityContextHolder`、`AuthorizationManager`
- RabbitMQ：`TTL + 死信队列`实现延迟消息、手动 ACK、发送端 Confirm、幂等
- ES：倒排索引与 IK 分词、`function_score` 权重、bool 查询、嵌套聚合
- MongoDB 文档建模、`MongoRepository` 方法名派生
- Maven 多模块与依赖管理、Docker Compose

**加分（改造阶段用）**：并发与锁（CAS / 悲观锁 / Redisson）、缓存穿透击穿雪崩、消息最终一致性、JUnit + Mockito、JMeter / wrk 压测。

---

## 3. 学会什么：可验收的能力目标

学完应该能做到下面每一条，做不到说明还停留在"看过"：

- [ ] 不看代码画出**登录 → JWT → 过滤器 → 动态鉴权 → 401/403** 的完整时序图，并说清 `id:name` 字符串在哪生成、在哪比对。
- [ ] 讲清"为什么 mall-admin 有 RBAC、mall-portal 没有"，并说明条件 Bean 起了什么作用。
- [ ] 白板画出下单全流程与订单状态机（0 待付款 → 1 待发货 → 2 已发货 → 3 已完成 / 4 已关闭），标注每步改了哪些表、`stock` 与 `lock_stock` 如何变化。
- [ ] 解释券/积分为什么按金额比例分摊到订单项，以及 `pay_amount` 的计算公式。
- [ ] 说清 TTL+DLX 延迟队列的三个组件和消息流转路径，以及它的队头阻塞隐患。
- [ ] 讲清 MBG 生成物和手写 DAO XML 的分工，能独立写一个带 `collection` 的 resultMap。
- [ ] 说清 Cache-Aside 的读/写/失效路径，以及 `@CacheException` 为什么存在。
- [ ] 独立把项目从零跑起来，用 Swagger / Postman 调通登录、商品列表、加购、下单、搜索。
- [ ] 能指出项目至少 5 个不适合生产环境的点，并给出改造方案。

---

## 4. 做出来什么：四阶段产出

### 阶段 0 · 跑通（勿求全）

最小组合：**MySQL（导入 `document/sql/mall.sql`）+ Redis**，启动 `MallAdminApplication`，打开 Swagger `/swagger-ui.html`，调通 `/admin/login` → 带 `Authorization: Bearer xxx` 调 `/brand/list`。

其他模块按需追加：mall-portal 需 MongoDB + RabbitMQ（注意 yml 要求 `virtual-host=/mall` 和 `mall/mall` 账号）、mall-search 需 ES + IK 分词插件。Logstash / MinIO / OSS / 支付宝都可先不管。

> 产出：一份自己的《启动手册》，记录踩过的坑（端口、vhost、IK 插件、驱动版本、yml 里哪些占位配置必须改）。

### 阶段 1 · 读懂

用 `document/postman/` 里现成的两个集合调接口，在 1.1–1.4 的四条链路上打断点，画四张图：认证鉴权时序图、下单状态机、延迟消息流转图、缓存读写路径图。每张图配一页笔记：涉及的类、表、配置、以及"如果让我改，我会改哪里"。

> 产出：4 张图 + 4 页笔记。这是面试时能流畅讲清楚的底气。

### 阶段 2 · 改造（决定简历质量）

**至少选 2 个做深**，留下可对比的 commit、测试和数据。

| 任务 | 现状与问题 | 改造要求 | 验收方式 |
| --- | --- | --- | --- |
| **A. 修复超卖** | `lockStock()` 是 `select → Java 加 → update`，并发下会超卖 | 改成一条原子 SQL：`UPDATE pms_sku_stock SET lock_stock = lock_stock + #{qty} WHERE id = #{id} AND stock - lock_stock >= #{qty}`，按影响行数判定；或加分布式锁 / Redis Lua。同时给支付扣减加条件 | 用 JMeter / 线程池并发下单同一 SKU，改造前能复现负库存，改造后 0 超卖；记录并发数与耗时 |
| **B. 消息可靠性** | 事务内发 MQ 无 Confirm、无重试、无幂等、TTL+DLX 有队头阻塞 | 发送端 Confirm + 本地消息表 / 事务提交后发；消费端手动 ACK + 失败重试 + 幂等（订单 `status` 条件）；或换 RabbitMQ 延迟插件 | 断开 MQ / 故意抛异常，验证订单仍能正确取消且不重复释放库存 |
| **C. 缓存重构** | 手写 Cache-Aside，无穿透/击穿防护 | 用 Spring Cache 注解重构，补空值缓存 / 互斥锁 / 随机 TTL；保留 `@CacheException` 的强一致语义 | 停掉 Redis 主流程仍可用；压测验证无大量穿透到 DB |
| **D. 搜索增量同步** | 只能手动 `importAll` | 商品上下架/改价时发 MQ，search 模块消费并增量更新 ES；补搜索联想（completion suggester） | 改一次商品价格，ES 查询随之变化；贴出前后对比 |
| **E. 测试补齐** | 5 个测试文件多为 `contextLoads()`，无断言，根 pom `skipTests=true` | 给 `generateOrder`、优惠券分摊、`cancelOrder` 写带断言的单元/集成测试；用 Testcontainers 或 H2 隔离外部依赖 | 测试可重复运行、覆盖异常分支，输出覆盖率 |
| **F. 部署与可观测** | 有 compose 文件但版本与代码有出入（ES 7.17 镜像 vs ES 8 Java Client） | 修好 compose，一次拉起全套并跑通主流程；打开 Logstash 日志链路，加一个健康检查看板 | 一份可复现的部署文档 + 截图 |

### 阶段 3 · 沉淀

- 给自己的 fork 写一份 README：架构图、启动方式、**我改了什么、为什么改、效果数据**。
- 按第 5 节写项目经历，并为每条 bullet 准备 3 个追问的答案。

---

## 5. 项目经历怎么写

### 5.1 结构公式

```
项目名称 | 时间 | 角色
一句话背景：什么系统、多大规模、技术栈、你负责哪部分
技术栈：Spring Boot 3 / MyBatis / Spring Security + JWT / MySQL / Redis / RabbitMQ / ES / MongoDB
3–4 条 bullet，每条 = 场景或问题 → 你的方案（具体技术 + 关键细节）→ 可验证结果
```

每条 bullet 必须能回答"**为什么这么做 / 换别的会怎样 / 数据从哪来**"。不要写"熟悉""了解""参与开发"。

### 5.2 版本 A：只跑通 + 读懂（诚实版，不要硬编）

> **电商系统 mall（学习/复现项目）** | 2026.xx–2026.xx
> 基于 Spring Boot 3.5 + MyBatis 的单体多模块电商系统，含前台商城、后台管理与商品搜索，76 张表；独立完成本地环境搭建、源码研读与核心链路复现。
> - 梳理并复现「JWT 无状态认证 + 基于 `ums_resource` 的动态 RBAC」链路，说明后台按 URL 鉴权、前台仅校验登录的条件装配差异，输出时序图与源码笔记。
> - 拆解下单链路：优惠券/积分按订单项金额比例分摊、`lock_stock` 锁定库存、Redis `incr` 生成订单号、RabbitMQ TTL+死信队列超时取消，绘出订单状态机与金额流转表。
> - 使用 MySQL + Redis + Postman 完成本地部署与接口联调，整理启动手册与踩坑记录。

这版能写，但面试官一问"你做了什么改动"就没了。**建议至少推进到版本 B。**

### 5.3 版本 B：做过改造（推荐，方括号处必须换成你的真实数据）

> **电商系统 mall（个人重构项目）** | 2026.xx–2026.xx | 独立开发
> 在开源电商系统（Spring Boot 3.5 + MyBatis，7 模块 / 76 张表）基础上，针对下单链路的并发安全与消息可靠性做重构。
> 技术栈：Spring Boot 3、Spring Security + JWT、MyBatis、MySQL、Redis、RabbitMQ、Elasticsearch、JUnit、JMeter、Docker Compose
> - **修复并发下单超卖**：定位到原实现 `select → 内存计算 → update` 的非原子读改写，改为 `UPDATE ... SET lock_stock = lock_stock + #{qty} WHERE stock - lock_stock >= #{qty}` 的条件原子更新，以影响行数判定成败；[N] 线程并发压测下，超卖订单由 [改造前 X 单] 降为 0，[QPS / 平均耗时]。
> - **保证超时取消的可靠投递**：原方案在事务内直接发 MQ 且无确认与重试，改为 [事务提交后发送 + 发送端 Confirm + 本地消息表]，消费端手动 ACK、失败重试并用订单 `status=0` 条件保证幂等；模拟 [broker 宕机 / 消费异常] 场景下订单状态与库存最终一致。
> - **重构缓存层**：将手写 Cache-Aside 改为 Spring Cache 注解 + [空值缓存 / 互斥锁 / 随机 TTL] 防穿透击穿，保留验证码场景的强一致（Redis 不可用时直接失败）；缓存命中率由 [X%] 提升至 [Y%]。
> - **补齐自动化测试**：为下单、优惠分摊、取消订单补充 [N] 个带断言的集成测试，覆盖 [库存不足 / 券不可用 / 重复取消] 等分支。

### 5.4 雷区

- **不要写项目里没有的东西**：真实秒杀、限流熔断、分布式事务、Redis 预扣库存、单点登录、短信服务——仓库里全都没有（`/sso/**` 只是路径名，两个模块 `jwt.secret` 不同）。写了必被问穿。
- **不要写没测过的数字**，QPS / 命中率 / 性能提升必须来自你自己的压测日志。
- **不要把技术栈罗列当经历**，"熟悉 Spring Boot、Redis、ES"这类话没有信息量。
- **不要只讲用了什么**，要讲"遇到什么问题 → 有哪些方案 → 为什么选这个 → 怎么验证"。
- 明确标注这是**学习/重构项目**并说清你的改动边界，比含糊地写成"公司项目"更稳。

### 5.5 高频追问（每条 bullet 都要能答）

1. JWT 怎么主动失效 / 续期？为什么用无状态会话？token 放哪里、怎么防 XSS？
2. 动态权限的数据从哪来？改了角色/资源后多久生效？`AntPathMatcher` 匹配的顺序问题？
3. `stock` 和 `lock_stock` 的区别？支付成功、取消订单各怎么变？为什么条件更新能防超卖？
4. 优惠券和积分为什么要分摊到订单项？分摊时的舍入误差怎么处理（源码用 `RoundingMode.HALF_EVEN`）？
5. 为什么用 TTL + 死信队列而不是延迟插件？队头阻塞是什么？消息丢了 / 重复消费了怎么办？
6. `@Transactional` 标在接口上有什么问题？事务里发 MQ 会有什么后果？
7. Cache-Aside 的读写顺序？为什么删除而不是更新缓存？缓存与 DB 不一致怎么办？
8. ES 与 MySQL 的数据一致性怎么保证？为什么搜索要用 `function_score` 加权？
9. 76 张表里，SPU/SKU、订单、权限分别是哪几张？为什么这么拆？
10. 如果 QPS 再涨 10 倍，这个系统最先挂在哪里，你会先改哪三处？

### 5.6 交付前自检

- [ ] 每条 bullet 都有"问题 → 方案 → 结果"三要素
- [ ] 所有数字都能拿出截图 / 日志 / commit
- [ ] 提到的每个技术点都能讲清原理和取舍
- [ ] 没有出现项目里不存在的功能
- [ ] 能对着自己的架构图讲 5 分钟不卡壳
- [ ] README 里能直接跳到改动代码与测试

---

## 附：关键文件速查

| 主题 | 路径 |
| --- | --- |
| 安全总装 | `mall-security/src/main/java/com/macro/mall/security/config/SecurityConfig.java` |
| JWT 过滤器 / 工具 | `.../security/component/JwtAuthenticationTokenFilter.java`、`.../security/util/JwtTokenUtil.java` |
| 动态权限 | `.../security/component/DynamicAuthorizationManager.java`、`DynamicSecurityMetadataSource.java`、`mall-admin/.../config/MallSecurityConfig.java` |
| 缓存降级切面 | `mall-security/.../aspect/RedisCacheAspect.java`、`.../annotation/CacheException.java` |
| 统一底座 | `mall-common/src/main/java/com/macro/mall/common/api/CommonResult.java`、`.../exception/GlobalExceptionHandler.java`、`.../log/WebLogAspect.java` |
| 下单主流程 | `mall-portal/.../service/impl/OmsPortalOrderServiceImpl.java`、`mall-portal/src/main/resources/dao/PortalOrderDao.xml` |
| 延迟队列 | `mall-portal/.../config/RabbitMqConfig.java`、`.../domain/QueueEnum.java`、`.../component/CancelOrderSender.java`、`CancelOrderReceiver.java` |
| 复杂联表映射 | `mall-admin/src/main/resources/dao/PmsProductDao.xml`、`OmsOrderDao.xml` |
| MBG 配置 | `mall-mbg/src/main/resources/generatorConfig.xml`、`.../java/com/macro/mall/Generator.java` |
| ES 搜索 | `mall-search/.../service/impl/EsProductServiceImpl.java`、`.../domain/EsProduct.java`、`.../resources/dao/EsProductDao.xml` |
| 缓存实现 | `mall-admin/.../service/impl/UmsAdminCacheServiceImpl.java`、`UmsAdminServiceImpl.java` |
| 建表脚本 / 接口集合 | `document/sql/mall.sql`、`document/postman/*.json` |
| 中间件编排 | `document/docker/docker-compose-env.yml`、`docker-compose-app.yml` |
