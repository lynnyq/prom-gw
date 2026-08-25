# prom-gw 开发部署文档

> 本目录面向开发者本机调试,采用单节点全本地部署,不依赖 Docker。

## 文档列表

| 序号 | 文档 | 说明 |
|------|------|------|
| 01 | [本地开发与测试指南](01-local-dev-guide.md) | 单节点 Prometheus + Kafka + prom-gw 本地原生部署、编译调试 |
| 02 | [Flink 消费 Kafka 开发指南](02-flink-consumer-guide.md) | Flink 作业开发、Protobuf 解码、5min 聚合、Stream Load |

## 本地开发环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Go | 1.23+ | prom-gw 编译 |
| OpenJDK | 25 | Kafka/StarRocks 本地运行 |
| OpenJDK | 17 | Flink 本地运行 |
| Maven | 3.9+ | Flink 作业编译 |
| Kafka | 3.9+ (KRaft) | 单节点本地模式 |
| Prometheus | 2.51+ | 单节点本地模式 |

## 与生产部署的区别

| 维度 | 本地开发 | 生产部署 |
|------|---------|---------|
| 部署用户 | 当前用户 | bdops (uid 6000) |
| 程序目录 | `~/prom-gw-dev/` | `/appdata/<component>/` |
| 日志目录 | 程序目录下 | `/applog/<component>/` |
| Kafka | 单节点 KRaft | 3 Broker 跨 AZ + JBOD |
| 服务管理 | 前台进程 | systemd |
| 高可用 | 无 | LVS + Keepalived VIP |
| StarRocks | 可选(Docker) | 3 FE + 3 BE |
| Flink | 本地 `LocalEnvironment` | Standalone HA 集群 |
