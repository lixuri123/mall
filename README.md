# mall

<p>
  <a href="#公众号"><img src="http://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E5%85%AC%E4%BC%97%E5%8F%B7-macrozheng-blue.svg" alt="公众号"></a>
  <a href="#公众号"><img src="http://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E4%BA%A4%E6%B5%81-%E5%BE%AE%E4%BF%A1%E7%BE%A4-2BA245.svg" alt="交流"></a>
  <a href="https://github.com/macrozheng/mall-learning"><img src="http://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E5%AD%A6%E4%B9%A0%E6%95%99%E7%A8%8B-mall--learning-green.svg" alt="学习教程"></a>
  <a href="https://github.com/macrozheng/mall-swarm"><img src="http://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/Cloud%E7%89%88%E6%9C%AC-mall--swarm-brightgreen.svg" alt="SpringCloud版本"></a>
  <a href="https://github.com/macrozheng/mall-admin-web"><img src="https://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E5%90%8E%E5%8F%B0%E7%AE%A1%E7%90%86%E7%B3%BB%E7%BB%9F-mall--admin--web-green.svg" alt="后台管理系统"></a>
  <a href="https://github.com/macrozheng/mall-app-web"><img src="https://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E5%89%8D%E5%8F%B0%E5%95%86%E5%9F%8E%E9%A1%B9%E7%9B%AE-mall--app--web-green.svg" alt="前台商城项目"></a>
  <a href="https://gitee.com/macrozheng/mall"><img src="http://macro-oss.oss-cn-shenzhen.aliyuncs.com/mall/badge/%E7%A0%81%E4%BA%91-%E9%A1%B9%E7%9B%AE%E5%9C%B0%E5%9D%80-orange.svg" alt="码云"></a>
</p>

## 友情提示

> 1. **快速体验项目**：[在线访问地址](https://www.macrozheng.com/admin/index.html) 。
> 2. **全套学习教程**：[《mall学习教程》](https://www.macrozheng.com) 。
> 3. **视频教程**：[《mall视频教程》](https://www.macrozheng.com/mall/catalog/mall_video.html) 。
> 4. **微服务版本**：基于Spring Cloud Alibaba的项目：[mall-swarm](https://github.com/macrozheng/mall-swarm) 。
> 5. **分支说明**：`master`分支基于Spring Boot 3.5+JDK 17，`dev-v2`分支基于Spring Boot 2.7+JDK 8。

## 前言

`mall`项目致力于打造一个完整的电商系统，采用现阶段主流技术实现。

## 项目文档

文档地址：[https://www.macrozheng.com](https://www.macrozheng.com)

## 项目介绍

`mall`项目是一套电商系统，包括前台商城系统及后台管理系统，基于SpringBoot+MyBatis实现，采用Docker容器化部署。前台商城系统包含首页门户、商品推荐、商品搜索、商品展示、购物车、订单流程、会员中心、客户服务、帮助中心等模块。后台管理系统包含商品管理、订单管理、会员管理、促销管理、运营管理、内容管理、统计报表、财务管理、权限管理、设置等模块。

### 项目演示

#### 后台管理系统

前端项目`mall-admin-web`地址：https://github.com/macrozheng/mall-admin-web

项目演示地址： [https://www.macrozheng.com/admin/index.html](https://www.macrozheng.com/admin/index.html)  

![后台管理系统功能演示](./document/resource/mall_admin_show.png)

#### 前台商城系统

前端项目`mall-app-web`地址：https://github.com/macrozheng/mall-app-web

项目演示地址（将浏览器切换为手机模式效果更佳）：[https://www.macrozheng.com/app/](https://www.macrozheng.com/app/)

![前台商城系统功能演示](./document/resource/re_mall_app_show.jpg)

### 组织结构

```
mall
├── mall-common -- 工具类及通用代码
├── mall-mbg -- MyBatisGenerator生成的数据库操作代码
├── mall-security -- SpringSecurity封装公用模块
├── mall-admin -- 后台商城管理系统接口
├── mall-search -- 基于Elasticsearch的商品搜索系统
├── mall-portal -- 前台商城系统接口
└── mall-demo -- 框架搭建时的测试代码
```

### 技术选型

#### 后端技术

| 技术                 | 说明                | 官网                                           |
| -------------------- | ------------------- | ---------------------------------------------- |
| SpringBoot           | Web应用开发框架      | https://spring.io/projects/spring-boot         |
| SpringSecurity       | 认证和授权框架      | https://spring.io/projects/spring-security     |
| MyBatis              | ORM框架             | http://www.mybatis.org/mybatis-3/zh/index.html |
| MyBatisGenerator     | 数据层代码生成器     | http://www.mybatis.org/generator/index.html    |
| Elasticsearch        | 搜索引擎            | https://github.com/elastic/elasticsearch       |
| RabbitMQ             | 消息队列            | https://www.rabbitmq.com/                      |
| Redis                | 内存数据存储         | https://redis.io/                              |
| MongoDB              | NoSql数据库         | https://www.mongodb.com                        |
| LogStash             | 日志收集工具        | https://github.com/elastic/logstash            |
| Kibana               | 日志可视化查看工具  | https://github.com/elastic/kibana              |
| Nginx                | 静态资源服务器      | https://www.nginx.com/                         |
| Docker               | 应用容器引擎        | https://www.docker.com                         |
| Jenkins              | 自动化部署工具      | https://github.com/jenkinsci/jenkins           |
| Druid                | 数据库连接池        | https://github.com/alibaba/druid               |
| OSS                  | 对象存储            | https://github.com/aliyun/aliyun-oss-java-sdk  |
| MinIO                | 对象存储            | https://github.com/minio/minio                 |
| JWT                  | JWT登录支持         | https://github.com/jwtk/jjwt                   |
| Lombok               | Java语言增强库      | https://github.com/rzwitserloot/lombok         |
| Hutool               | Java工具类库        | https://github.com/looly/hutool                |
| PageHelper           | MyBatis物理分页插件 | http://git.oschina.net/free/Mybatis_PageHelper |
| SpringDoc            | API文档生成工具      | https://github.com/springdoc/springdoc-openapi |
| Hibernator-Validator | 验证框架            | http://hibernate.org/validator                 |

#### 前端技术

| 技术       | 说明                  | 官网                                   |
| ---------- | --------------------- | -------------------------------------- |
| Vue        | 前端框架              | https://vuejs.org/                     |
| Vue-router | 路由框架              | https://router.vuejs.org/              |
| Vuex       | 全局状态管理框架      | https://vuex.vuejs.org/                |
| Element    | 前端UI框架            | https://element.eleme.io               |
| Axios      | 前端HTTP框架          | https://github.com/axios/axios         |
| v-charts   | 基于Echarts的图表框架 | https://v-charts.js.org/               |
| Js-cookie  | cookie管理工具        | https://github.com/js-cookie/js-cookie |
| nprogress  | 进度条控件            | https://github.com/rstacruz/nprogress  |

#### 移动端技术

| 技术         | 说明             | 官网                                    |
| ------------ | ---------------- | --------------------------------------- |
| Vue          | 核心前端框架     | https://vuejs.org                       |
| Vuex         | 全局状态管理框架 | https://vuex.vuejs.org                  |
| uni-app      | 移动端前端框架   | https://uniapp.dcloud.io                |
| mix-mall     | 电商项目模板     | https://ext.dcloud.net.cn/plugin?id=200 |
| luch-request | HTTP请求框架     | https://github.com/lei-mu/luch-request  |

#### 架构图

##### 系统架构图

![系统架构图](./document/resource/re_mall_system_arch.jpg)

##### 业务架构图

![业务架构图](./document/resource/re_mall_business_arch.jpg)

#### 模块介绍

##### 后台管理系统 `mall-admin`

- 商品管理：[功能结构图-商品.jpg](document/resource/mind_product.jpg)
- 订单管理：[功能结构图-订单.jpg](document/resource/mind_order.jpg)
- 促销管理：[功能结构图-促销.jpg](document/resource/mind_sale.jpg)
- 内容管理：[功能结构图-内容.jpg](document/resource/mind_content.jpg)
- 用户管理：[功能结构图-用户.jpg](document/resource/mind_member.jpg)

##### 前台商城系统 `mall-portal`

[功能结构图-前台.jpg](document/resource/mind_portal.jpg)

#### 开发进度

![项目开发进度图](./document/resource/re_mall_dev_flow.jpg)

## 环境搭建

### 开发工具

| 工具          | 说明                | 官网                                            |
| ------------- | ------------------- | ----------------------------------------------- |
| IDEA          | 开发IDE             | https://www.jetbrains.com/idea/download         |
| RedisDesktop  | redis客户端连接工具 | https://github.com/qishibo/AnotherRedisDesktopManager  |
| Robomongo     | mongo客户端连接工具 | https://robomongo.org/download                  |
| SwitchHosts   | 本地host管理        | https://oldj.github.io/SwitchHosts/             |
| X-shell       | Linux远程连接工具   | http://www.netsarang.com/download/software.html |
| Navicat       | 数据库连接工具      | http://www.formysql.com/xiazai.html             |
| PowerDesigner | 数据库设计工具      | http://powerdesigner.de/                        |
| Axure         | 原型设计工具        | https://www.axure.com/                          |
| MindMaster    | 思维导图设计工具    | http://www.edrawsoft.cn/mindmaster              |
| ScreenToGif   | gif录制工具         | https://www.screentogif.com/                    |
| ProcessOn     | 流程图绘制工具      | https://www.processon.com/                      |
| PicPick       | 图片处理工具        | https://picpick.app/zh/                         |
| Snipaste      | 屏幕截图工具        | https://www.snipaste.com/                       |
| Postman       | API接口调试工具      | https://www.postman.com/                        |
| Typora        | Markdown编辑器      | https://typora.io/                              |

### 开发环境

| 工具          | 版本号 | 下载                                                         |
| ------------- | ------ | ------------------------------------------------------------ |
| JDK           | 17     | https://www.oracle.com/java/technologies/javase/jdk17-archive-downloads.html |
| MySQL         | 5.7    | https://www.mysql.com/                                       |
| Redis         | 7.0    | https://redis.io/download                                    |
| MongoDB       | 5.0    | https://www.mongodb.com/download-center                      |
| RabbitMQ      | 3.10.5 | http://www.rabbitmq.com/download.html                        |
| Nginx         | 1.22   | http://nginx.org/en/download.html                            |
| Elasticsearch | 7.17.3 | https://www.elastic.co/downloads/elasticsearch               |
| Logstash      | 7.17.3 | https://www.elastic.co/cn/downloads/logstash                 |
| Kibana        | 7.17.3 | https://www.elastic.co/cn/downloads/kibana                   |

### 搭建步骤

> Windows环境部署

- Windows环境搭建请参考：[mall在Windows环境下的部署](https://www.macrozheng.com/mall/deploy/mall_deploy_windows.html);
- 注意：如果只启动`mall-admin`模块，仅需安装MySQL、Redis即可;
- 克隆`mall-admin-web`项目，并导入到IDEA中完成编译：[前端项目地址](https://github.com/macrozheng/mall-admin-web);
- `mall-admin-web`项目的安装及部署请参考：[mall前端项目的安装与部署](https://www.macrozheng.com/mall/deploy/mall_deploy_web.html) 。

> Docker环境部署

- 使用虚拟机安装CentOS7.6请参考：[虚拟机安装及使用Linux，看这一篇就够了](https://www.macrozheng.com/tool/linux_install.html);
- 本项目Docker镜像构建请参考：[使用Maven插件为SpringBoot应用构建Docker镜像](https://www.macrozheng.com/project/maven_docker_fabric8.html);
- 本项目在Docker容器下的部署请参考：[mall在Linux环境下的部署（基于Docker容器）](https://www.macrozheng.com/mall/deploy/mall_deploy_docker.html);
- 本项目使用Docker Compose请参考： [mall在Linux环境下的部署（基于Docker Compose）](https://www.macrozheng.com/mall/deploy/mall_deploy_docker_compose.html);
- 本项目在Linux下的自动化部署请参考：[mall在Linux环境下的自动化部署（基于Jenkins）](https://www.macrozheng.com/mall/deploy/mall_deploy_jenkins.html);

> 相关环境部署

- ELK日志收集系统的搭建请参考：[mall项目ELK日志收集解决方案](https://www.macrozheng.com/project/mall_kibana_start.html);
- 使用MinIO存储文件请参考：[使用MinIO实现文件存储](https://www.macrozheng.com/project/minio_console_start.html);
- 读写分离解决方案请参考：[你还在代码里做读写分离么，试试这个中间件吧](https://www.macrozheng.com/project/gaea.html);
- Redis集群解决方案请参考：[Docker环境下秒建Redis集群](https://www.macrozheng.com/blog/redis_cluster.html) 。

## 公众号

加微信群交流，关注公众号「**macrozheng**」，回复「**加群**」即可。

![公众号图片](./document/resource/qrcode_for_macrozheng_258.jpg)

## 本地改造记录

本工作区在源码基础上完成了两阶段订单链路改造，**代码与测试均已实际跑通，尚未提交 Git**。

- **A：订单状态与库存一致性改造** — MySQL 条件更新防超卖、订单状态条件流转、
  支付/取消幂等、事务边界与超时补偿，并补齐真实 MySQL 集成测试。
  说明 [document/order-consistency/README.md](./document/order-consistency/README.md)、
  报告 [TEST_REPORT.md](./document/order-consistency/TEST_REPORT.md)。
- **B：超时订单取消的消息可靠性** — 固定 `oms_order.expire_time` + 本地消息表（Outbox），
  下单事务内写 `PENDING` 消息、到期租约派发、发布确认 / 不可路由处理、
  提交后确认 / 固定延迟重试 / 死信队列、双层定时兜底与指标告警。
- **B-MQ：消息队列从 RabbitMQ 切换到 RocketMQ 5.3.2** — 采用「换 MQ 不换架构」：
  保留本地消息表、租约 CAS、消费幂等、提交后确认、双层兜底五层，只重写收发适配层。
  在 `mall-local` 容器的真实 MySQL 8.4.7 + RocketMQ 5.3.2 上完成故障注入测试。
  说明 [B_ROCKETMQ.md](./document/order-consistency/B_ROCKETMQ.md)、
  报告 [B_ROCKETMQ_TEST_REPORT.md](./document/order-consistency/B_ROCKETMQ_TEST_REPORT.md)、
  原始输出 [results/b-rocketmq-test-run-2026-11-12.txt](./document/order-consistency/results/b-rocketmq-test-run-2026-11-12.txt)。
  RabbitMQ 版说明与报告作为历史快照保留在 [B_README.md](./document/order-consistency/B_README.md)、
  [B_TEST_REPORT.md](./document/order-consistency/B_TEST_REPORT.md)。

全量结果：**56 个集成/单元测试 0 失败 / 0 错误 / 0 跳过，连续三次复跑全绿**，8 个模块编译通过。
订单超时判定始终以数据库 `expire_time` 为权威，不使用 MQ 延迟消息；RocketMQ 的固定延迟级别
只用于消费失败后的受控重试。

已有库升级顺序（**先备份，预检只读，人工确认后再迁移**）：

1. [A 库存数据预检](./document/sql/mall_a_precheck.sql) → [A 迁移](./document/sql/mall_a_migration.sql)
2. [B 预检](./document/sql/mall_b_precheck.sql) → [B 迁移](./document/sql/mall_b_migration.sql)

新环境直接导入 [document/sql/mall.sql](./document/sql/mall.sql)（已内置 A+B 全部结构）。
本地中间件用 `docker compose -p mall-local -f document/docker/docker-compose-local.yml up -d`。
订单取消消息走 RocketMQ，需先启动 `rocketmq-namesrv` / `rocketmq-broker` 并预建 3 个 topic
（Broker 关闭了自动建 topic，命令见 [B_ROCKETMQ.md](./document/order-consistency/B_ROCKETMQ.md) 第 5、8 节）。
MySQL 必须保持 `+08:00` 时区，说明见 [document/docker/LOCAL.md](./document/docker/LOCAL.md)。

## 许可证

[Apache License 2.0](https://github.com/macrozheng/mall/blob/master/LICENSE)

Copyright (c) 2018-2026 macrozheng
