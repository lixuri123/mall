# 本地中间件

在项目根目录使用 PowerShell：

```powershell
docker compose -f document/docker/docker-compose-local.yml up -d --build
docker compose -f document/docker/docker-compose-local.yml ps
```

该环境使用独立 Docker 数据卷。MySQL 首次创建空卷时导入 `document/sql/mall.sql`，之后重启不会再次导入。它不会自动迁移旧数据库、消息或搜索索引。SQL 文件包含删表语句，不要手动导入有用数据的数据库。

本地 MySQL 端口为 3307，用户名和密码均为 `root`。MySQL 以 `--default-time-zone=+08:00` 启动，与应用 JDBC 的 `serverTimezone=Asia/Shanghai` 一致；不要用 UTC 时区运行，否则 `NOW()` 与 `expire_time` / `create_time` 比较会相差 8 小时。

订单超时取消消息当前走 **RocketMQ 5.3.2**（`rocketmq-namesrv` 9876 / `rocketmq-broker` 10911），不再是 RabbitMQ。`rabbitmq:4.2.9-management` 容器仍保留在 compose 里便于回退对比，账号 `mall/mall`、vhost `/mall`，但当前代码不连接它。Elasticsearch 关闭了安全认证，所有这些设置仅用于本地开发。

启动 RocketMQ 并预建 topic（Broker 关闭了自动建 topic，必须预建，否则发送不可路由；也是 `UNROUTABLE` 故障分支可复现的前提）：

```powershell
docker compose -f document/docker/docker-compose-local.yml up -d rocketmq-namesrv rocketmq-broker
$ns = 'rocketmq-namesrv:9876'
$c  = 'mall-local-rocketmq-broker-1'
foreach ($t in @('mall_order_cancel','mall_order_cancel_retry','mall_order_cancel_dlq')) {
  docker exec $c sh mqadmin updateTopic -n $ns -c DefaultCluster -t $t -r 4 -w 4 -a +message.type=NORMAL
}
docker exec $c sh mqadmin clusterList -n $ns
```

`rocketmq-broker.conf` 里 `brokerIP1=127.0.0.1`（容器内网地址宿主机不可达）、`flushDiskType=SYNC_FLUSH`（让 `SEND_OK` 具备已落盘语义）、`autoCreateTopicEnable=false`。镜像以 uid 3000 运行，**不要**给 Broker 挂 `/home/rocketmq/store` / `logs` 的 named volume，否则会因权限不足在启动时 NPE 退出。完整改造说明见 [../order-consistency/B_ROCKETMQ.md](../order-consistency/B_ROCKETMQ.md)。

停止容器但保留数据：

```powershell
docker compose -f document/docker/docker-compose-local.yml down
```

构建应用：

```powershell
& 'D:\Tools\apache-maven-3.9.9\bin\mvn.cmd' -B '-DskipTests' '-Ddocker.skip=true' package
```

在三个终端分别启动：

```powershell
java -jar .\mall-admin\target\mall-admin-1.0-SNAPSHOT.jar
java -jar .\mall-portal\target\mall-portal-1.0-SNAPSHOT.jar
java -jar .\mall-search\target\mall-search-1.0-SNAPSHOT.jar
```

首次启动搜索后，可调用 `POST http://localhost:8081/esProduct/importAll` 从 MySQL 导入商品。该环境不包含对象存储、Nginx 和日志采集服务。MinIO 官方镜像拉取失败，上传功能需要另行配置可用的对象存储。
