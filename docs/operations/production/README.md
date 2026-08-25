# prom-gw 生产部署文档

> 三城同城采集 → 同城清洗聚合 → 跨城汇聚到北京 StarRocks

## 技术说明

| 序号 | 文档 | 说明 |
|------|------|------|
| 00 | [技术方案说明](00-technical-overview.md) | 整体技术方案、数据流向、核心实现细节、容错机制、容量规划 |

## 组件部署文档

| 序号 | 文档 | 说明 |
|------|------|------|
| 01 | [Prometheus 部署与配置详解](01-prometheus-deployment.md) | remote_write 配置、Bearer Token 鉴权、LVS VIP 对接 |
| 02 | [prom-gw 部署与配置详解](02-prom-gw-deployment.md) | Go 二进制编译、systemd template 部署、Ruleset/Token 配置 |
| 03 | [Kafka 部署与配置详解](03-kafka-deployment.md) | KRaft 模式、3 Broker 跨 AZ、JBOD 多盘、64 分区 3 副本 |
| 04 | [StarRocks 部署与配置详解](04-starrocks-deployment.md) | 3 FE + 3 BE 存算一体、Nginx 反向代理、JBOD 存储 |
| 05 | [Kafka UI (Kafbat) 部署与配置详解](05-kafka-ui-deployment.md) | JAR + systemd 部署、多集群监控、RBAC 认证 |
| 06 | [Flink 部署与配置详解](06-flink-deployment.md) | Standalone HA、3 节点 ZK、JM HA、Checkpoint 配置 |

## 运维与参考文档

| 序号 | 文档 | 说明 |
|------|------|------|
| 07 | [高可用与负载均衡部署详解](07-ha-lb-deployment.md) | LVS/Keepalived、Nginx 4/7 层负载、VIP 故障切换 |
| 08 | [prom-gw 配置参数参考](08-configuration-reference.md) | 全部启动参数、环境变量、Ruleset 格式速查 |
| 09 | [故障响应与排查手册 (Runbook)](09-runbook.md) | 常见故障排查流程、日志关键字、应急操作 |
| 10 | [SLO 指标定义](10-slo.md) | 服务等级目标、可用性指标、延迟预算 |
| 11 | [安全审计报告](11-security-audit.md) | 安全加固清单、Token 管理、网络隔离 |
| 12 | [压力测试指南](12-stress-test.md) | 压测方案、性能基线、Profile 分析 |
| 13 | [端到端测试验证](13-end-to-end-testing.md) | WAL-only 冒烟、完整链路、故障切换测试 |

## 统一约定

### 部署用户

所有组件统一使用 **bdops** 用户(uid 6000)部署,该用户由基础环境预先创建。

### 目录规范

| 用途 | 路径 | 示例 |
|------|------|------|
| 程序安装目录 | `/appdata/<component>/` | `/appdata/kafka/`、`/appdata/prom-gw/` |
| 日志目录 | `/applog/<component>/` | `/applog/kafka/`、`/applog/flink/` |
| 配置文件 | `/appdata/<component>/conf/` | `/appdata/kafka/config/`、`/appdata/prom-gw/conf/` |
| 数据目录(组件专用盘) | `/data01~11/<component>/` | `/data01/kafka/`(JBOD 多盘) |

### 操作系统与 JDK

- **OS**: Kylin V10 SP2
- **Kafka/StarRocks**: OpenJDK 25
- **Flink**: OpenJDK 17(Flink 1.19 官方仅支持 Java 11/17)
- **prom-gw**: Go 1.23+(编译时),运行时无 JDK 依赖
- **服务管理**: systemd(systemd >= 245)

### 服务启动顺序

```
OpenJDK25 → Kafka → Prometheus → prom-gw → StarRocks → Flink
```
