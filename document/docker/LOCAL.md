# 本地中间件

在项目根目录使用 PowerShell：

```powershell
docker compose -f document/docker/docker-compose-local.yml up -d --build
docker compose -f document/docker/docker-compose-local.yml ps
```

该环境使用独立 Docker 数据卷。MySQL 首次创建空卷时导入 `document/sql/mall.sql`，之后重启不会再次导入。它不会自动迁移旧数据库、消息或搜索索引。SQL 文件包含删表语句，不要手动导入有用数据的数据库。

本地 MySQL 端口为 3307，用户名和密码均为 `root`。MySQL 以 `--default-time-zone=+08:00` 启动，与应用 JDBC 的 `serverTimezone=Asia/Shanghai` 一致；不要用 UTC 时区运行，否则 `NOW()` 与 `expire_time` / `create_time` 比较会相差 8 小时。RabbitMQ 用户名和密码均为 `mall`，虚拟主机为 `/mall`。Elasticsearch 关闭了安全认证，所有这些设置仅用于本地开发。

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
