# prom-gw 生产运维完整指南

> 本文档合并了 prom-gw 项目所有运维文档,涵盖架构设计、Kafka/StarRocks/Kafbat UI/Flink 部署、高可用与负载均衡、压力测试、配置参数、本地开发、故障响应、SLO 指标和安全审计。
>
> 合并自以下文档:production-guide.md、kafka-production-deployment.md、starrocks-deployment.md、kafka-ui-deployment.md、flink-production-deployment.md、flink-consumer-guide.md、ha-lb-deployment.md、stress-test-guide.md、configuration-reference.md、local-dev-guide.md、runbook.md、slo.md、security-audit.md

## 目录

1. [架构概述与环境规划](#1-架构概述与环境规划)
2. [Kafka 生产部署](#2-kafka-生产部署)
3. [StarRocks 生产部署](#3-starrocks-生产部署)
4. [Kafbat UI 部署](#4-kafbat-ui-部署)
5. [Flink 生产部署](#5-flink-生产部署)
6. [Flink 消费 Kafka 开发指南](#6-flink-消费-kafka-开发指南)
7. [高可用与负载均衡](#7-高可用与负载均衡)
8. [压力测试指南](#8-压力测试指南)
9. [配置参数参考](#9-配置参数参考)
10. [本地开发指南](#10-本地开发指南)
11. [故障响应与排查](#11-故障响应与排查)
12. [SLO 指标](#12-slo-指标)
13. [安全审计报告](#13-安全审计报告)


---

## 1. 架构概述与环境规划 {#1-架构概述与环境规划}
> 本文档覆盖 Prometheus + Kafka + prom-gw 全链路生产部署、配置说明、测试验证与运维操作。
> 配套文档:**Kafka 生产部署**(见 §2)、**StarRocks 生产部署**(见 §3)、**Kafbat UI 部署**(见 §4)、**Flink 生产部署**(见 §5)、**高可用与负载均衡**(见 §7)、**故障响应与排查 Runbook**(见 §11)、**SLO 指标**(见 §12)。


---

### 1. 架构概述

#### 1.1 整体拓扑

```
三城同城采集 → 同城清洗聚合 → 跨城汇聚到北京 StarRocks

DC-A Prometheus ─┐                        ┌─> Kafka BJ ─> Flink BJ ─┐
DC-B Prometheus ─┼─> LVS → prom-gw (各机房) ─┤                        ├─> StarRocks (北京)
DC-C Prometheus ─┘                        └─> Kafka SZ ─> Flink SZ ─┘
                                           └─> Kafka HF ─> Flink HF ─┘
```

#### 1.2 组件职责

| 组件 | 职责 | 部署形态 |
|---|---|---|
| **Prometheus** | 采集本地业务指标,通过 `remote_write` 上报 | 每机房 1~2 套(已有) |
| **LVS** | 4 层负载均衡,DR 模式转发到 prom-gw | 每机房 2 台主备(Keepalived) |
| **prom-gw** | RemoteWrite 网关,鉴权/限流/规则清洗/Kafka 投递/WAL 故障切换 | 每机房 2~4 个实例(VM) |
| **Kafka** | 同城消息队列,KRaft 模式,3 副本 | 每机房 3 Broker(物理机) |
| **Flink** | 同城 5min 聚合,跨城 Stream Load 写 StarRocks | 每机房 JM×2 + TM×2~6 |
| **StarRocks** | 统一查询分析层,3 独立物理表 + 级联聚合 | 北京 3 节点(物理机) |
| **Nacos**(可选) | 配置中心,ruleset 热推送 | 北京 3 节点 |

#### 1.3 数据可靠性保证

- Kafka producer:`acks=all` + `enable.idempotence=true` + `delivery.timeout.ms=120000` + `retries=10`
- Kafka 故障时 prom-gw 自动降级到本地 WAL(`/data/wal`),恢复后自动 drain 回灌
- WAL 使用 segment + CRC32 校验,启动时 replay 未 `.done` 的段
- 跨城仅传 5min 聚合数据(1TB/天,占 1G 专线 9.3%),原始 sample 明细严禁跨城

---

### 2. 环境要求与资源规划

#### 2.1 硬件资源清单

| 角色 | 形态 | 单台规格 | 数量(BJ/SZ/HF) | 小计 | 备注 |
|---|---|---|---|---|---|
| **Prometheus** | 已有 | 8C/16G | 2/2/1 | 5 | 已在生产运行 |
| **LVS (Keepalived)** | VM | 8C/16G/200G | 2/2/2 | 6 | 每机房主备 |
| **prom-gw** | VM | 16C/32G/500G SSD | 4/4/2 | 10 | `prom-gw@<city>.service` |
| **Kafka Broker (KRaft)** | 物理机 | 64C/512G/11×16T HDD JBOD | 3/3/3 | 9 | PLAINTEXT,3 副本,3 天留存 |
| **Flink JobManager** | VM | 32C/64G/1T | 2/2/2 | 6 | 1 Active + 1 Standby |
| **Flink Zookeeper** | VM | 8C/16G/200G | 3/3/3 | 9 | HA 选主 |
| **Flink TaskManager** | VM | 16C/32G/500G SSD | 6/4/2 | 12 | 每 TM 4 slot |
| **StarRocks FE** | 物理机 | 8C/16G/100G SSD | 3(全北京) | 3 | 元数据 + 查询调度 |
| **StarRocks BE** | 物理机 | 64C/256G/22×16T HDD JBOD | 3(全北京) | 3 | 存储 + 计算,3 副本 |
| **Kafbat UI** | VM | 4C/8G/50G SSD | 1(北京) | 1 | Kafka Web 监控(v1.5.0) |
| **Nacos** | VM | 16C/32G/1T | 3(北京) | 3 | 配置中心 |

> **JDK 统一**:全栈使用 **OpenJDK 25**(Kafka、Kafbat UI、StarRocks、Flink)。详见各组件部署文档。

#### 2.2 操作系统

- Linux(x86_64),内核 ≥ 4.19
- systemd ≥ 245(支持 `MemoryMax` / `TasksMax`)
- 文件系统:ext4 或 xfs(WAL/Kafka 目录建议 `noatime` 挂载)
- 时间同步:全集群 `chrony` 对齐北京 NTP 源(北斗 + GPS)

#### 2.3 网络规划

| 链路 | 带宽 | 延迟 | 说明 |
|---|---|---|---|
| Prom → LVS | 10G 同城 LAN | < 1ms | `remote_write` 到 LVS VIP |
| LVS → prom-gw | 10G 内网 | < 1ms | DR 模式直接转发 |
| prom-gw → Kafka | 10G 内网 | < 1ms | Kafka `advertised.listeners` 绑内网 |
| Flink → StarRocks | 走 HTTP 8030 Stream Load(FE `http_port`) | — | FE VIP 负载均衡 |
| 深圳 ⇄ 北京专线 | 1G×2(主备) | P95 ≤ 30ms | 跨城仅传 5min 聚合 |
| 合肥 ⇄ 北京专线 | 1G×1 | P95 ≤ 25ms | 故障时降级本地 ClickHouse |

#### 2.4 端口规划

| 端口 | 组件 | 用途 | 暴露范围 |
|---|---|---|---|
| `9090` | Prometheus | Web UI / API | 运维网段 |
| `9092` | Kafka | 客户端通信(PLAINTEXT) | prom-gw + Flink |
| `9093` | Kafka | Controller(KRaft) | Kafka 内部 |
| `9404` | Kafka | JMX Exporter(Prometheus 指标) | Prometheus 抓取 |
| `19201` | prom-gw | RemoteWrite 接入 | Prometheus / LVS |
| `8080` | prom-gw | `/metrics` + pprof | Prometheus 抓取 |
| `8081` | prom-gw | healthz / readyz | LB health check |
| `8082` | prom-gw | Admin API | 运维网段(白名单) |
| `8030` | StarRocks FE | Web UI / REST API | 运维网段 / Nginx |
| `9030` | StarRocks FE | MySQL 协议(查询) | 运维网段 / Nginx |
| `9020` | StarRocks FE | Thrift RPC(FE 间通信) | FE 内部 |
| `9010` | StarRocks FE | Edit Log(Follower 同步) | FE 内部 |
| `8040` | StarRocks BE | HTTP REST API | FE → BE 网段 |
| `9060` | StarRocks BE | Thrift(FE → BE 通信) | FE → BE 网段 |
| `9050` | StarRocks BE | 心跳服务 | FE → BE 网段 |
| `8060` | StarRocks BE | bRPC | FE → BE / BE 间 |
| `8080` | Kafbat UI | Web 监控界面 | 本地 / Nginx |

---

### 3. 中间件集群部署

> 本节覆盖 Kafka、StarRocks、Kafbat UI 的部署摘要。Kafka 详细部署见 **Kafka 生产部署**(见 §2),StarRocks 见 **StarRocks 生产部署**(见 §3),Kafbat UI 见 **Kafbat UI 部署**(见 §4)。

#### 3.1 Kafka 部署摘要

| 项目 | 配置 |
|---|---|
| 版本 | Kafka 3.4.0 (Scala 2.13) + KRaft 模式(无 ZooKeeper) |
| JDK | OpenJDK 25 |
| 部署目录 | `/appdata/kafka` |
| 日志目录 | `/applog/kafka` |
| 数据目录 | `/data01/kafka` ~ `/data11/kafka`(11 块 JBOD 挂载点) |
| 客户端协议 | PLAINTEXT(无 SSL/SASL,通过 VPC 网络隔离) |
| 客户端端口 | `9092` |
| Controller 端口 | `9093` |
| JMX Exporter | `9404`(Prometheus 指标抓取) |
| 拓扑 | 每机房 3 Broker,跨 AZ `2+1` 分布 |
| 副本数 | 3(`default.replication.factor=3`,`min.insync.replicas=2`) |
| 留存 | 72 小时(3 天) |

#### 3.2 关键配置

**`/appdata/kafka/config/local.properties`(Broker 1 示例)**:

```properties
broker.id=1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka-1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=/data01/kafka,/data02/kafka,/data03/kafka,/data04/kafka,/data05/kafka,/data06/kafka,/data07/kafka,/data08/kafka,/data09/kafka,/data10/kafka,/data11/kafka
metadata.log.dir=/data01/kafka
num.partitions=64
default.replication.factor=3
min.insync.replicas=2
log.retention.hours=72
broker.rack=az-1
```

#### 3.3 systemd 管理

```bash
# 格式化存储(首次)
CLUSTER_UUID=$(/appdata/kafka/bin/kafka-storage.sh random-uuid)
/appdata/kafka/bin/kafka-storage.sh format \
  --config /appdata/kafka/config/local.properties \
  --cluster-id $CLUSTER_UUID

# 启动
sudo systemctl enable --now kafka
```

#### 3.4 创建 Topic

```bash
# 原始数据 topic
for city in bj sz hf; do
  for tenant in app_business infra; do
    /appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
      --create --topic prom.${city}.raw.${tenant} \
      --partitions 64 --replication-factor 3
  done
done

# 路由后 topic
for city in bj sz hf; do
  for biz in core infra data app_business; do
    /appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 \
      --create --topic prom.${city}.routed.${biz} \
      --partitions 64 --replication-factor 3
  done
done
```

> **完整部署步骤**(系统调优 / JBOD 挂载 / JVM 配置 / 监控 / 扩缩容 / 故障恢复)见 **Kafka 生产部署**(见 §2)。

#### 3.5 StarRocks 部署

> **详细部署见** **StarRocks 生产部署**(见 §3),本节仅给出关键配置摘要。

| 项目 | 配置 |
|---|---|
| 版本 | StarRocks 3.4.10 |
| JDK | OpenJDK 25(JDK 11+ 即可) |
| 部署目录 | `/appdata/starrocks`(安装 + 日志) |
| 数据目录 | `/data01/starrocks` ~ `/data22/starrocks`(22 块 JBOD) |
| 拓扑 | 3 FE(1 Leader + 2 Follower)+ 3 BE(存算一体) |
| FE 端口 | 8030(Web)/ 9030(MySQL)/ 9020(Thrift)/ 9010(EditLog) |
| BE 端口 | 8040(HTTP)/ 9060(Thrift)/ 9050(心跳)/ 8060(bRPC) |
| 副本数 | 3(`default_replication_num=3`) |

#### 3.6 Kafbat UI 部署

> **详细部署见** **Kafbat UI 部署**(见 §4),本节仅给出关键配置摘要。

| 项目 | 配置 |
|---|---|
| 版本 | Kafbat UI v1.5.0(JAR + systemd) |
| JDK | OpenJDK 25 |
| 部署目录 | `/appdata/kafka-ui`(JAR + 配置) |
| 日志目录 | `/applog/kafka-ui` |
| 端口 | `8080`(本地监听,通过 Nginx 对外) |
| Kafka 连接 | PLAINTEXT `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| 指标集成 | Prometheus JMX Exporter `9404` |

---

### 4. Prometheus 部署与配置

#### 4.1 Prometheus 安装(全新部署,已有环境可跳过)

> 若机房已运行 Prometheus,直接跳到 [4.2](#42-remote_write-配置对接-prom-gw) 配置 `remote_write`。
> 北京 2 套 + 深圳 2 套 + 合肥 1 套均为已有环境,本节供新机房扩容或灾备重建使用。

**下载并安装**:

```bash
sudo useradd -r -m -d /var/lib/prometheus -s /sbin/nologin prometheus
cd /opt
sudo wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.linux-amd64.tar.gz
sudo tar -xzf prometheus-2.51.0.linux-amd64.tar.gz
sudo ln -s prometheus-2.51.0.linux-amd64 prometheus
sudo mkdir -p /etc/prometheus /var/lib/prometheus
sudo chown -R prometheus:prometheus /etc/prometheus /var/lib/prometheus /opt/prometheus
```

**基础配置** `/etc/prometheus/prometheus.yml`(最小可用,`remote_write` 在 §4.2 补充):

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source_dc: dc-bj-dongba          # 按机房修改:dc-bj-dongba / dc-sz-wulian / dc-hf

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']
  # 业务 exporter 按需追加 node_exporter / kube-state-metrics / pushgateway 等
```

**systemd 服务** `/etc/systemd/system/prometheus.service`:

```ini
[Unit]
Description=Prometheus
After=network.target

[Service]
Type=simple
User=prometheus
ExecStart=/opt/prometheus/prometheus \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.path=/var/lib/prometheus \
  --storage.tsdb.retention.time=15d \
  --web.enable-lifecycle
Restart=always
RestartSec=5
LimitNOFILE=65535
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/prometheus

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now prometheus
curl http://localhost:9090/-/healthy
# 期望: Prometheus is Healthy.
```

#### 4.2 remote_write 配置对接 prom-gw

修改每套 Prometheus 的 `prometheus.yml`,添加 `remote_write` 段:

**北京东坝 Prometheus**:

```yaml
# /etc/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source_dc: dc-bj-dongba          # 标识机房,会被 prom-gw 读取

remote_write:
  - url: http://lvs-bj-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"    # 替换为实际 token
    write_relabel_configs:
      # 可选:本地过滤一份(如丢弃内部指标)
      - source_labels: [__name__]
        regex: 'go_.*|prometheus_.*'
        action: drop
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
      min_backoff: 500ms
      max_backoff: 10s
    metadata_config:
      send: true
      send_interval: 1m
```

**深圳五联 Prometheus**:

```yaml
global:
  external_labels:
    source_dc: dc-sz-wulian

remote_write:
  - url: http://lvs-sz-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

**合肥 Prometheus**:

```yaml
global:
  external_labels:
    source_dc: dc-hf

remote_write:
  - url: http://lvs-hf-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

#### 4.3 高可用配置(多实例)

```yaml
remote_write:
  # 主:LVS VIP(LB 到 prom-gw-1 ~ prom-gw-4)
  - url: http://lvs-bj-vip:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

> prom-gw 的 Kafka producer 开启幂等写,多实例重复消息在 Kafka 端去重。

#### 4.4 Prometheus 验证

```bash
# 1. reload prometheus
curl -X POST http://prometheus:9090/-/reload

# 2. 查看 remote_write 配置
curl -s http://prometheus:9090/api/v1/status/config | jq '.data.yaml' | grep remote_write -A 15

# 3. 查看 remote_write 状态(发送/失败/排队)
curl -s http://prometheus:9090/api/v1/status/runtimeinfo | jq '.data.remoteWrite'

# 4. 看指标
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_total | jq .
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_pending | jq .
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_dropped_total | jq .
```

---

### 5. prom-gw 编译与部署

#### 5.1 编译

```bash
# 依赖 Go 1.22+
make build           # 产物:bin/prom-gw
make test            # 单元测试 + 覆盖率(-race)
make lint            # golangci-lint
make release         # 产物:dist/prom-gw-<version>.tar.gz
```

版本注入:

```bash
VERSION=v1.2.3 make build
./bin/prom-gw --version  # prom-gw v1.2.3
```

#### 5.2 Token 配置

路径:`/etc/prom-gw/tokens.yaml`

```yaml
tokens:
  "tk_app_business_prod":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.bj.raw.app_business
    rate_limit: 80000          # 该 tenant 的 samples/s 上限

  "tk_infra_prod":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.bj.raw.infra
    rate_limit: 50000
```

修改后通过 `kill -HUP <pid>` 热重载,**不重启进程**。

#### 5.3 Ruleset 配置

路径:`/etc/prom-gw/config-<city>.yaml`(按城市分目录)

```yaml
rulesets:
  - name: app-business
    tenant: app-business
    input_topic: prom.bj.raw.app_business
    default_topic: prom.bj.routed.app_business
    version: 1
    match:
      metric_prefix: ""        # 空 = 全量接收
    stages:
      - type: relabel
        drop_labels: [env, instance, pod]
        keep_labels: []
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.bj.routed.core
          - match: { team: "infra" }
            topic: prom.bj.routed.infra

      - type: sample
        rate: 0.1               # 保留 10%

global:
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

**stage 执行顺序**(固定):`relabel → enrich → route → sample → downsample → deadvalue`

**topic 命名规范**:`prom.<city>.<stage>.<tenant>`

#### 5.4 systemd template 部署

prom-gw 使用 **template unit**(`prom-gw@.service`,`%i` 为城市标识):

**步骤 1:创建用户和目录**

```bash
sudo useradd -r -s /sbin/nologin -d /var/lib/prom-gw prom-gw
sudo mkdir -p /opt/prom-gw/bin /etc/prom-gw /data/wal /var/log/prom-gw
sudo chown -R prom-gw:prom-gw /data/wal /var/log/prom-gw
```

**步骤 2:放置二进制和配置**

```bash
sudo cp bin/prom-gw /opt/prom-gw/bin/
sudo cp configs/tokens/local.yaml /etc/prom-gw/tokens.yaml
sudo cp configs/rules/bj/default.yaml /etc/prom-gw/config-bj.yaml
sudo chmod 600 /etc/prom-gw/tokens.yaml
```

**步骤 3:配置 Kafka broker 地址**

```bash
# /etc/prom-gw/prom-gw.env
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
GOMAXPROCS=8
GOMEMLIMIT=6GiB
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317   # 可选
```

**步骤 4:安装 systemd unit**

`/etc/systemd/system/prom-gw@.service`(由仓库 `deploy/systemd/prom-gw@.service` 拷贝):

```ini
[Unit]
Description=Prometheus RemoteWrite Gateway (city=%i)
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=prom-gw
Group=prom-gw
Environment=INGEST_CITY=%i
Environment=PROM_GW_CONFIG=/etc/prom-gw/config-%i.yaml
Environment=PROM_GW_TOKENS=/etc/prom-gw/tokens.yaml
ExecStart=/opt/prom-gw/bin/prom-gw \
  --config=/etc/prom-gw/config-%i.yaml \
  --tokens=/etc/prom-gw/tokens.yaml \
  --ingest-city=%i
Restart=always
RestartSec=5
LimitNOFILE=65535
MemoryMax=8G
KillSignal=SIGTERM
TimeoutStopSec=30
EnvironmentFile=-/etc/prom-gw/prom-gw.env

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/data/wal /var/log/prom-gw
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload

# 北京机房
sudo systemctl enable --now prom-gw@bj

# 查看状态
sudo systemctl status prom-gw@bj

# 看日志
sudo journalctl -u prom-gw@bj -f
```

#### 5.5 启动参数速查

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--config` | `PROM_GW_CONFIG` | `configs/rules/default.yaml` | ruleset 配置文件 |
| `--tokens` | `PROM_GW_TOKENS` | `configs/tokens/local.yaml` | token 配置文件 |
| `--write-addr` | - | `:19201` | RemoteWrite 接入地址 |
| `--metrics-addr` | - | `:8080` | Prometheus 指标 + pprof |
| `--health-addr` | - | `:8081` | healthz / readyz |
| `--admin-addr` | - | `:8082` | Admin API |
| `--admin-allow-cidr` | - | `127.0.0.1/32,10.0.0.0/8` | Admin API IP 白名单 |
| `--source-dc` | - | `dc-unknown` | 本实例所属机房标识 |
| `--ingest-city` | `INGEST_CITY` | `dc-unknown` | 城市标识(bj/sz/hf) |
| `--wal-dir` | - | `/data/wal` | WAL 数据目录 |
| `--wal-max-bytes` | - | `50GB` | WAL 总字节上限 |
| `--nacos-addr` | - | (空) | Nacos 地址,逗号分隔 |

Kafka broker 列表通过 `KAFKA_BROKERS` 环境变量注入,未设置时进入 **WAL-only 模式**。

---

### 6. LVS 负载均衡部署

#### 6.1 LVS + Keepalived 配置

每机房 2 台 LVS 主备,DR 模式转发到 prom-gw 实例。

**`/etc/keepalived/keepalived.conf`(LVS-Master)**:

```
global_defs {
    router_id LVS_BJ
}

vrrp_instance VI_1 {
    state MASTER
    interface eth0
    virtual_router_id 51
    priority 100
    advert_int 1
    authentication {
        auth_type PASS
        auth_pass promgw_lvs
    }
    virtual_ipaddress {
        10.0.1.100/24          # LVS VIP
    }
}

virtual_server 10.0.1.100 19201 {
    delay_loop 2
    lb_algo rr                  # 轮询
    lb_kind DR                  # 直接路由
    protocol TCP

    real_server 10.0.1.11 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.12 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.13 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
    real_server 10.0.1.14 19201 { weight 1 TCP_CHECK { connect_timeout 3 } }
}
```

#### 6.2 prom-gw 实例配置 VIP

DR 模式要求 real_server( prom-gw)配置 VIP 到 lo 接口:

```bash
# 在每台 prom-gw 机器上
sudo ip addr add 10.0.1.100/32 dev lo
echo 1 | sudo tee /proc/sys/net/ipv4/conf/lo/arp_ignore
echo 2 | sudo tee /proc/sys/net/ipv4/conf/lo/arp_announce
echo 1 | sudo tee /proc/sys/net/ipv4/conf/all/arp_ignore
echo 2 | sudo tee /proc/sys/net/ipv4/conf/all/arp_announce
```

---

### 7. 端到端测试验证

#### 7.1 测试环境准备

```bash
# 1. 确认 Kafka 可达
/appdata/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 | head

# 2. 确认 Topic 已创建
/appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list | grep prom

# 3. 编译 prom-gw
make build

# 4. 确认配置文件
cat configs/tokens/local.yaml
cat configs/rules/app-business.yaml
```

#### 7.2 测试 1:WAL-only 模式冒烟测试(无 Kafka)

> 验证 prom-gw 基本功能:接收、解码、鉴权、WAL 落盘、Admin API。

```bash
# 启动 prom-gw(无 KAFKA_BROKERS → WAL-only 模式)
KAFKA_BROKERS="" \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-test-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --admin-allow-cidr=127.0.0.1/32 \
  --source-dc=dc-test &

GW_PID=$!
echo "prom-gw pid=$GW_PID"
```

**验证步骤**:

```bash
# 1. 健康检查
curl http://127.0.0.1:8081/healthz
# 期望: {"status":"ok"}

# 2. 就绪检查
curl -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/readyz
# 期望: 204

# 3. 构造 RemoteWrite 请求
RUN_ID=test-1 go run ./scripts/e2e_payload > /tmp/payload.bin
ls -la /tmp/payload.bin

# 4. 正常写入
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/payload.bin)
echo "写入返回: $HTTP_CODE"  # 期望: 200

# 5. 鉴权失败(无 token)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  --data-binary @/tmp/payload.bin)
echo "无 token 返回: $HTTP_CODE"  # 期望: 401

# 6. 鉴权失败(非法 token)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_invalid" \
  --data-binary @/tmp/payload.bin)
echo "非法 token 返回: $HTTP_CODE"  # 期望: 401

# 7. 错误请求(非 snappy)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary "not-snappy-bytes")
echo "非法 snappy 返回: $HTTP_CODE"  # 期望: 400

# 8. 指标校验
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total
curl -s http://127.0.0.1:8080/metrics | grep gateway_bytes_in_total

# 9. WAL 落盘校验
sleep 1
ls -la /tmp/prom-gw-test-wal/
find /tmp/prom-gw-test-wal/ -name 'seg-*.log*' | wc -l  # 期望 ≥ 1

# 10. Admin API
curl -s http://127.0.0.1:8082/v1/rulesets | jq .
curl -s http://127.0.0.1:8082/v1/stats | jq .
curl -s http://127.0.0.1:8082/v1/tenants | jq .

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-test-wal /tmp/payload.bin
```

#### 7.3 测试 2:完整端到端测试(Kafka + prom-gw)

> 验证数据从 Prometheus → prom-gw → Kafka 全链路。

**启动 prom-gw(连接 Kafka)**:

```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-e2e-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-e2e-test &

GW_PID=$!

# 等待启动
for i in $(seq 1 50); do
  curl -fsS http://127.0.0.1:8081/healthz >/dev/null 2>&1 && break
  sleep 0.2
done
echo "prom-gw started (pid=$GW_PID)"
```

**模拟 Prometheus 写入**:

```bash
# 构造并写入 10 条 sample
for i in $(seq 1 10); do
  RUN_ID="e2e-$i-$(date +%s)" go run ./scripts/e2e_payload > /tmp/payload-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/payload-$i.bin
done
```

**验证 Kafka 消费**:

```bash
# 消费 Topic 验证数据到达
/appdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.bj.raw.app_business \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 15000 \
  | xxd | head -50
# 期望:能看到二进制数据(prompb.WriteRequest snappy 编码)
```

**验证 prom-gw 指标**:

```bash
# 1. sample 计数(应递增)
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 2. Kafka 写入字节(应 > 0)
curl -s http://127.0.0.1:8080/metrics | grep gateway_bytes_out_total

# 3. 错误计数(应为 0 或极少)
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total

# 4. 请求延迟
curl -s http://127.0.0.1:8080/metrics | grep gateway_request_duration_seconds

# 5. WAL 状态(应无积压)
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
```

**清理**:

```bash
kill $GW_PID
rm -rf /tmp/prom-gw-e2e-wal /tmp/payload-*.bin
```

#### 7.4 测试 3:WAL 故障切换测试

> 验证 Kafka 故障时 prom-gw 自动降级到 WAL,Kafka 恢复后自动 drain。

```bash
# 1. 启动 prom-gw(连接一个不存在的 Kafka 地址模拟故障)
KAFKA_BROKERS=127.0.0.1:9999 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-failover-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-failover-test &

GW_PID=$!

# 2. 等待启动(应进入 WAL degraded mode)
sleep 3
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
# 期望:WAL 指标 > 0

# 3. 写入数据(应全部落 WAL)
for i in $(seq 1 5); do
  RUN_ID="failover-$i" go run ./scripts/e2e_payload > /tmp/failover-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/failover-$i.bin
done

# 4. 验证 WAL 落盘
sleep 1
ls -la /tmp/prom-gw-failover-wal/
WAL_SEGMENTS=$(find /tmp/prom-gw-failover-wal/ -name 'seg-*.log*' | wc -l)
echo "WAL segments: $WAL_SEGMENTS"  # 期望 ≥ 1

# 5. 检查指标:Kafka 写入失败,WAL 接管
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total{stage=\"kafka\"}

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-failover-wal /tmp/failover-*.bin
```

#### 7.5 测试 4:规则引擎验证

> 验证 relabel/route/sample 规则正确执行。

```bash
# 使用 app-business ruleset(包含 relabel + route + sample)
KAFKA_BROKERS=kafka-1:9092 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-rule-wal \
  --write-addr=:19201 \
  --admin-addr=:8082 &

GW_PID=$!
sleep 2

# 构造带 team 标签的 sample
cat > /tmp/rule-test.go << 'EOF'
package main
import (
  "io"
  "os"
  "time"
  "github.com/klauspost/compress/snappy"
  "github.com/prometheus/prometheus/prompb"
)
func main() {
  req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
    {Labels: []prompb.Label{
      {Name: "__name__", Value: "app_cpu_usage"},
      {Name: "team", Value: "core"},
      {Name: "env", Value: "prod"},
      {Name: "instance", Value: "10.0.0.1:9090"},
    }, Samples: []prompb.Sample{{Value: 88.5, Timestamp: time.Now().UnixMilli()}}},
  }}
  raw, _ := req.Marshal()
  encoded := snappy.Encode(nil, raw)
  io.Copy(os.Stdout, &byteReader{b: encoded})
}
type byteReader struct{ b []byte; i int }
func (r *byteReader) Read(p []byte) (int, error) {
  if r.i >= len(r.b) { return 0, io.EOF }
  n := copy(p, r.b[r.i:]); r.i += n; return n, nil
}
EOF
go run /tmp/rule-test.go > /tmp/rule-payload.bin

# 写入
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/rule-payload.bin

# 验证路由到 prom.bj.routed.core(team=core)
/appdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.bj.routed.core \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head

# 验证 ruleset 指标
curl -s http://127.0.0.1:8080/metrics | grep gateway_ruleset_routed_total

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-rule-wal /tmp/rule-test.go /tmp/rule-payload.bin
```

#### 7.6 测试 5:单元测试 + 集成测试

```bash
# 单元测试(快速,不需 Docker)
make test
# 期望:coverage > 60%,全部 PASS

# 集成测试(需要 Docker,启动 Kafka testcontainer)
make test-integration
# 期望:全部 PASS

# 压测(30s 冒烟)
make test-loadgen
# 期望:50000 samples/s 持续 30s 无错误

# 端到端手动脚本
bash test/manual/e2e.sh
# 期望:✅ 全部检查通过
```

#### 7.7 测试 6:全链路验证清单

部署完成后,按以下清单逐项验证:

| 序号 | 验证项 | 命令 | 期望结果 |
|---|---|---|---|
| 1 | Kafka Broker 状态 | `kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092` | 3 个 Broker 在线 |
| 2 | Topic 列表 | `kafka-topics.sh --list --bootstrap-server kafka-1:9092 \| grep prom` | 包含 raw/routed topic |
| 3 | prom-gw healthz | `curl http://prom-gw:8081/healthz` | `{"status":"ok"}` |
| 4 | prom-gw readyz | `curl -o /dev/null -w "%{http_code}" http://prom-gw:8081/readyz` | `204` |
| 5 | prom-gw metrics | `curl http://prom-gw:8080/metrics \| grep gateway_samples_total` | 指标可见 |
| 6 | Prometheus remote_write | `curl http://prom:9090/api/v1/query?query=prometheus_remote_storage_samples_total` | 计数递增 |
| 7 | 写入 200 | `curl -w "%{http_code}" -X POST .../api/v1/write` | `200` |
| 8 | 鉴权 401 | 无 token 写入 | `401` |
| 9 | Kafka 消费 | `kafka-console-consumer.sh --topic prom.bj.raw.app_business` | 收到数据 |
| 10 | Admin API | `curl http://prom-gw:8082/v1/rulesets` | 返回 ruleset 列表 |
| 11 | LVS VIP | `curl http://lvs-vip:19201/api/v1/write` | 可达 |
| 12 | Grafana 大盘 | 打开 Grafana → prom-gw dashboard | 数据有曲线 |
| 13 | 告警规则 | Prometheus → Alerts 页面 | 告警规则已加载 |
| 14 | WAL 状态 | `curl http://prom-gw:8080/metrics \| grep gateway_wal_bytes` | 正常为 0 |
| 15 | 跨机房 | 合肥 Prometheus → 合肥 prom-gw → 合肥 Kafka | 数据流通 |

---

### 8. 监控与告警接入

#### 8.1 Prometheus 抓取配置

```yaml
scrape_configs:
  - job_name: prom-gw
    scrape_interval: 15s
    static_configs:
      - targets:
        - prom-gw-1:8080
        - prom-gw-2:8080
        - prom-gw-3:8080
        - prom-gw-4:8080

  - job_name: kafka
    scrape_interval: 15s
    static_configs:
      - targets:
        - kafka-1:9404
        - kafka-2:9404
        - kafka-3:9404
```

#### 8.2 关键指标速查

| 指标 | 含义 |
|---|---|
| `gateway_samples_total{stage,tenant,status,ingest_city,source_dc}` | 各阶段处理的 sample 数 |
| `gateway_request_duration_seconds{endpoint,status,ingest_city}` | HTTP 请求延迟 |
| `gateway_errors_total{stage,type,ingest_city,source_dc}` | 错误计数 |
| `gateway_backpressure_rejected_total{reason,ingest_city,source_dc}` | 背压拒绝(503) |
| `gateway_wal_bytes` | WAL 当前字节数 |
| `gateway_wal_oldest_age_seconds` | WAL 最老 segment 存活秒数 |
| `gateway_wal_hard_reject_total` | WAL 硬拒绝(磁盘满) |
| `gateway_ruleset_version{ruleset,ingest_city}` | 当前 ruleset 版本 |
| `gateway_produce_errors_total{reason,ingest_city,source_dc}` | Kafka produce 错误 |

#### 8.3 告警规则

仓库提供开箱即用的告警规则:`deploy/grafana/alerts/prom-gw.yaml`

在 `prometheus.yml` 引入:

```yaml
rule_files:
  - /etc/prometheus/rules/prom-gw.yaml
```

告警分级:

| 告警 | 严重度 | 触发条件 |
|---|---|---|
| `PromGwHighErrorRate` | critical | 错误率 > 1% 持续 5m |
| `PromGwWalHardReject` | critical | WAL 硬拒绝 > 0 |
| `PromGwP99LatencyHigh` | warning | p99 > 1s 持续 5m |
| `PromGwBackpressureHigh` | warning | 背压拒绝持续 5m |
| `PromGwWalOldestTooOld` | warning | WAL 最老 segment > 60s |
| `PromGwAuthFailSpike` | warning | 鉴权失败 > 50/s 持续 5m |
| `PromGwGoroutineLeak` | warning | goroutines > 5000 持续 10m |

#### 8.4 Grafana 大盘

导入 `deploy/grafana/dashboards/prom-gw.json`,选 Prometheus 数据源。

---

### 9. Admin API 使用

Admin API 监听 `:8082`,默认 IP 白名单 `127.0.0.1/32,10.0.0.0/8`。

```bash
# 健康检查
curl http://127.0.0.1:8082/v1/healthz

# 查询 ruleset 列表
curl http://127.0.0.1:8082/v1/rulesets | jq .

# 查询单个 ruleset
curl http://127.0.0.1:8082/v1/rulesets/app-business | jq .

# 更新 ruleset
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @new-ruleset.yaml

# 强制 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app-business:reload

# 回滚到历史版本
curl -X POST 'http://127.0.0.1:8082/v1/rulesets/app-business:rollback?to_version=2'

# 查询统计
curl http://127.0.0.1:8082/v1/stats | jq .
```

错误码:

| HTTP | 业务码 | 含义 |
|---|---|---|
| 400 | `4000` | 参数错误 |
| 401 | `4001` | 鉴权失败 |
| 403 | `4003` | IP 白名单拒绝 |
| 404 | `4040` | ruleset 不存在 |
| 409 | `4090` | ruleset 版本冲突 |
| 422 | `4220` | YAML 校验失败 |

---

### 10. 运维操作

#### 10.1 热重载 Token

```bash
sudo vim /etc/prom-gw/tokens.yaml
sudo kill -HUP $(pidof prom-gw)
sudo journalctl -u prom-gw@bj --since "1m ago" | grep "tokens reloaded"
```

#### 10.2 热更新 Ruleset

**文件监听(自动)**:修改 `/etc/prom-gw/config-<city>.yaml` 后 fsnotify 5s 内自动检测。

**Admin API(手动)**:

```bash
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @new-ruleset.yaml
```

#### 10.3 滚动升级

```bash
# 1. LB 摘流
sudo systemctl reload nginx

# 2. 停实例(SIGTERM,prom-gw 30s 内优雅退出)
sudo systemctl stop prom-gw@bj

# 3. 替换二进制
sudo cp /tmp/prom-gw /opt/prom-gw/bin/prom-gw

# 4. 启动
sudo systemctl start prom-gw@bj

# 5. 验证
curl http://127.0.0.1:8081/healthz
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 6. LB 上线
```

灰度策略:10% → 50% → 100%,每阶段观察 30 分钟。

#### 10.4 优雅停机顺序

prom-gw 收到 SIGTERM 后(spec §6.5):

1. 停止接收新请求(receiver Shutdown,超时 30s)
2. drain WAL → Kafka(超时 30s)
3. 关闭 WAL(确保 pending 数据落盘)
4. 关闭 Kafka producer(等待 in-flight 消息 ack,超时 30s)
5. 关闭 Admin API
6. 关闭 tracing exporter

#### 10.5 Ansible 一键部署/回滚

```bash
cd deploy/ansible

# 部署
ansible-playbook -i inventory/production.ini playbooks/deploy.yml \
  -e prom_gw_version=v1.0.0

# 回滚
ansible-playbook -i inventory/production.ini playbooks/rollback.yml \
  -e prom_gw_rollback_version=v0.9.0
```

---

### 11. 故障排查

#### 11.1 常见问题速查

| 现象 | 可能原因 | 排查命令 |
|---|---|---|
| 写入 401 | token 错误/过期 | `journalctl -u prom-gw \| grep "auth failed"` |
| 写入 403 | IP 不在白名单 | `journalctl -u prom-gw \| grep "source ip"` |
| 写入 503 | 背压(Kafka 满/WAL 满) | `curl /metrics \| grep backpressure` |
| p99 延迟高 | Kafka 慢/规则引擎慢 | `go tool pprof http://:8080/debug/pprof/profile` |
| WAL 硬拒绝 | 磁盘满/WAL 超限 | `df -h /data/wal` |
| OOM | 状态型 stage series 过多 | `go tool pprof http://:8080/debug/pprof/heap` |
| Kafka 消费无数据 | topic 未创建/路由错误 | `kafka-topics.sh --list` |

#### 11.2 日志关键字

| 关键字 | 含义 |
|---|---|
| `receiver listening` | receiver 启动成功 |
| `kafkasink started` | Kafka producer 启动成功 |
| `sink adapter: switched to WAL degraded mode` | Kafka 故障,降级 WAL |
| `sink adapter: kafka recovered, switching back` | Kafka 恢复,切回 + drain |
| `sink adapter: draining WAL to Kafka` | 正在 drain WAL |
| `rule engine: rules swapped` | ruleset 热切换成功 |
| `tokens reloaded` | token 热重载成功 |

#### 11.3 全实例巡检

```bash
HOSTS="10.0.1.11,10.0.1.12,10.0.1.13,10.0.1.14"
for h in $(echo $HOSTS | tr ',' ' '); do
  printf '%s\t' "$h"
  curl -fsS -m 3 http://$h:8081/readyz && echo OK || echo FAIL
done

# 全实例 5xx 计数
for h in $(echo $HOSTS | tr ',' ' '); do
  echo "=== $h ==="
  curl -s http://$h:8080/metrics | grep "gateway_errors_total{stage=\"kafka\""
done
```

---

### 12. 安全加固

#### 12.1 systemd 安全选项(已配置)

```ini
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/data/wal /var/log/prom-gw
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
RestrictRealtime=true
RestrictNamespaces=true
```

#### 12.2 Token 管理

- token 文件权限 `0600`,owner `root:prom-gw`
- 生产 token 不入仓,通过 Ansible vault 或 secret manager 分发
- token 定期轮换(SIGHUP 热重载,不中断服务)

#### 12.3 网络隔离

- RemoteWrite 端口(`:19201`)仅对 Prometheus/LVS 开放
- Metrics 端口(`:8080`)仅对 Prometheus 抓取实例开放
- Admin 端口(`:8082`)仅对运维网段开放
- Kafka 端口(`:9092`)仅对 prom-gw + Flink 开放
- 跨机房走专线,不开公网

---

### 附录

#### A. 文件布局

```
/opt/prom-gw/bin/prom-gw                    # prom-gw 二进制
/etc/prom-gw/
  ├── tokens.yaml                           # token 配置
  ├── prom-gw.env                           # 环境变量(KAFKA_BROKERS 等)
  ├── config-bj.yaml                        # 北京 ruleset
  ├── config-sz.yaml                        # 深圳 ruleset
  └── config-hf.yaml                        # 合肥 ruleset
/data/wal/                                  # prom-gw WAL 数据
/appdata/kafka/                             # Kafka 安装目录
/data01/kafka/ ~ /data11/kafka/             # Kafka 数据(JBOD 11 盘)
/applog/kafka/                              # Kafka 日志
/appdata/starrocks/                         # StarRocks 安装目录(含 fe/ be/)
/data01/starrocks/ ~ /data22/starrocks/     # StarRocks BE 数据(JBOD 22 盘)
/appdata/kafka-ui/                          # Kafbat UI JAR + 配置
/applog/kafka-ui/                           # Kafbat UI 日志
/etc/systemd/system/
  ├── prom-gw@.service                      # prom-gw template unit
  ├── kafka.service                         # Kafka service
  ├── starrocks-fe.service                  # StarRocks FE (各节点)
  ├── starrocks-be.service                  # StarRocks BE (各节点)
  └── kafbat-ui.service                     # Kafbat UI service
```



---

## 2. Kafka 生产部署 {#2-kafka-生产部署}
> 本文档覆盖 prom-gw 配套 Kafka 集群的生产环境完整部署,采用 KRaft 模式 + PLAINTEXT 协议(不启用 SSL/SASL 等任何认证),包括集群搭建、监控告警、Topic 管理、性能调优、扩缩容、备份恢复和灾难恢复。
>
> 基础安装步骤( JDK / 系统调优 / KRaft 格式化 / Topic 创建)见 **生产部署指南 §3**(见 §1),本文档聚焦**监控、运维和调优**,与 §3 互补。
>
> 配套文档:**生产部署指南**(见 §1)、**高可用与负载均衡**(见 §7)、**Flink 生产部署**(见 §5)、**压力测试指南**(见 §8)、**故障剧本**(见 §11)


---

### 1. 部署架构

#### 1.1 单机房标准拓扑

每机房部署 3 Broker KRaft 模式(无 ZooKeeper),跨 2 个 AZ 分布,采用 PLAINTEXT 协议(不启用 SSL/SASL):

```
机房 (深圳)
┌──────────────── AZ-1 ────────────────┐  ┌──────── AZ-2 ─────────┐
│                                      │  │                       │
│  Kafka-1 (broker+controller)         │  │  Kafka-3              │
│  broker.id=1, rack=az-1             │  │  broker.id=3           │
│  10.0.1.21:9092(PLAINTEXT)          │  │  rack=az-2            │
│  10.0.1.21:9093(CONTROLLER)         │  │  10.0.1.23            │
│                                      │  │                       │
│  Kafka-2 (broker+controller)         │  │                       │
│  broker.id=2, rack=az-1             │  │                       │
│  10.0.1.22                          │  │                       │
│                                      │  │                       │
└──────────────────────────────────────┘  └───────────────────────┘

                   │
                   │ prom-gw / Flink 消费
                   │ PLAINTEXT → 9092
                   ▼
          ┌─────────────────┐
          │  prom-gw × 4    │
          │  Flink × 2-6 TM │
          └─────────────────┘
```

> **安全说明**:本部署不启用 SSL/SASL 认证,通过**网络隔离**(VPC / 安全组)保证 Kafka 只对 prom-gw / Flink 网段开放 9092 端口。生产环境务必确保 Kafka 不暴露在公网。

#### 1.2 端口规划

| 端口 | 协议 | 用途 | 暴露范围 |
|---|---|---|---|
| 9092 | PLAINTEXT | 客户端访问(Broker 间 + 生产/消费) | prom-gw / Flink 网段 |
| 9093 | CONTROLLER | KRaft 控制器间通信 | Kafka 节点间 |
| 9404 | HTTP | JMX Exporter 指标暴露 | Prometheus 网段 |

#### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 |
|---|---|---|---|
| Kafka Broker | 64C/512G | 3 | 11×16T HDD(JBOD) |
| prom-gw(客户端) | 8C/16G | 2-4 | 100G SSD(WAL) |
| Flink TM(客户端) | 16C/32G | 2-6 | 500G SSD(state) |

#### 1.4 网络隔离建议

不启用 Kafka 认证时,必须通过网络层保证安全:

```bash
# 安全组示例(仅放行 prom-gw / Flink 网段)
# 入方向:
#   9092  源:10.0.2.0/24 (prom-gw 网段)
#                10.0.3.0/24 (Flink 网段)
#   9093  源:10.0.1.21, 10.0.1.22, 10.0.1.23 (Kafka 节点间)
#   9404  源:10.0.10.10 (Prometheus)
# 出方向:全部放行
```

---

### 2. 前置准备

#### 2.1 操作系统

```bash
# CentOS / RHEL 8+
cat /etc/redhat-release

# Ubuntu / Debian 22+
cat /etc/os-release
```

#### 2.2 OpenJDK 25 安装

```bash
# CentOS / RHEL
sudo yum install -y java-25-openjdk java-25-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-25-jdk

java -version   # 期望: openjdk version "25.x.x"
```

#### 2.3 创建 Kafka 用户与目录

```bash
sudo useradd -r -m -d /appdata/kafka -s /sbin/nologin kafka
sudo mkdir -p /appdata/kafka /applog/kafka
# 11 个 JBOD 挂载点下的数据目录
for i in 01 02 03 04 05 06 07 08 09 10 11; do
    sudo mkdir -p /data${i}/kafka
done
sudo chown -R kafka:kafka /appdata/kafka /applog/kafka /data01/kafka /data02/kafka /data03/kafka /data04/kafka /data05/kafka /data06/kafka /data07/kafka /data08/kafka /data09/kafka /data10/kafka /data11/kafka
```

#### 2.4 内核参数调优

**`/etc/sysctl.d/99-kafka.conf`**:

```ini
# 内存
vm.swappiness=1                         # 尽量不用 swap
vm.max_map_count=262144
vm.dirty_ratio=10                       # 脏页占 10% 内存时阻塞写
vm.dirty_background_ratio=2             # 脏页占 2% 时开始异步刷盘

# 网络
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
net.ipv4.tcp_fin_timeout=15
net.ipv4.tcp_tw_reuse=1
net.core.rmem_default=262144
net.core.wmem_default=262144
net.core.rmem_max=16777216
net.core.wmem_max=16777216

# 文件句柄
fs.file-max=1000000
```

```bash
sudo sysctl --system
```

#### 2.5 文件句柄限制

**`/etc/security/limits.d/kafka.conf`**:

```
kafka  soft  nofile  100000
kafka  hard  nofile  100000
kafka  soft  nproc   100000
kafka  hard  nproc   100000
```

#### 2.6 磁盘挂载(JBOD)

每台 Kafka 物理机 11 × 16T HDD,JBOD 模式(不做 RAID,Kafka 自带副本冗余):

```bash
# /etc/fstab
/dev/sdb1 /data01  ext4 noatime,nodiratime 0 2
/dev/sdc1 /data02  ext4 noatime,nodiratime 0 2
/dev/sdd1 /data03  ext4 noatime,nodiratime 0 2
/dev/sde1 /data04  ext4 noatime,nodiratime 0 2
/dev/sdf1 /data05  ext4 noatime,nodiratime 0 2
/dev/sdg1 /data06  ext4 noatime,nodiratime 0 2
/dev/sdh1 /data07  ext4 noatime,nodiratime 0 2
/dev/sdi1 /data08  ext4 noatime,nodiratime 0 2
/dev/sdj1 /data09  ext4 noatime,nodiratime 0 2
/dev/sdk1 /data10  ext4 noatime,nodiratime 0 2
/dev/sdl1 /data11  ext4 noatime,nodiratime 0 2

sudo mount -a
sudo mkdir -p /data{01..11}/kafka
sudo chown -R kafka:kafka /data{01..11}/kafka
```

> **为什么不用 RAID?** Kafka 通过副本机制保证数据可靠性,JBOD 模式下单盘故障只影响该盘上的 partition,其他盘不受影响。RAID 会增加写放大和性能开销。

#### 2.7 下载并安装 Kafka

```bash
cd /appdata
sudo wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
sudo tar -xzf kafka_2.13-3.4.0.tgz
sudo ln -s kafka_2.13-3.4.0 kafka
sudo chown -R kafka:kafka /appdata/kafka
ls /appdata/kafka/bin/kafka-server-start.sh   # 确认解压成功
```

---

### 3. KRaft 集群安装

> 基础安装(JDK / 系统调优 / Kafka 下载)见 **生产部署指南 §3.1-§3.3**(见 §1),本节聚焦 KRaft 配置与格式化。

#### 3.1 集群规划

| Broker | node.id | AZ | 角色 | rack |
|---|---|---|---|---|
| kafka-1 (10.0.1.21) | 1 | AZ-1 | broker+controller | az-1 |
| kafka-2 (10.0.1.22) | 2 | AZ-1 | broker+controller | az-1 |
| kafka-3 (10.0.1.23) | 3 | AZ-2 | broker+controller | az-2 |

> 3 节点 KRaft 可容忍 1 个节点故障(Quorum 需要 2/3 存活)。跨 2 个 AZ 分布,单 AZ 故障不影响集群。

#### 3.2 Broker 配置

**`/appdata/kafka/config/server.properties`(Broker 1 示例,Broker 2/3 修改 broker.id / node.id / advertised.listeners / rack)**:

```properties
# ====== 基础 ======
broker.id=1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093

# ====== 监听器(PLAINTEXT,不启用 SSL/SASL) ======
# PLAINTEXT://9092 用于客户端通信(生产/消费)
# CONTROLLER://9093 用于 KRaft 控制器间通信
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka-1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT

# ====== 监听器安全协议映射 ======
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
control.plane.listener.name=CONTROLLER

# ====== 日志目录(JBOD) ======
log.dirs=/data01/kafka,/data02/kafka,/data03/kafka,/data04/kafka,/data05/kafka,/data06/kafka,/data07/kafka,/data08/kafka,/data09/kafka,/data10/kafka,/data11/kafka
metadata.log.dir=/data01/kafka

# ====== Topic 默认 ======
num.partitions=64
default.replication.factor=3
min.insync.replicas=2
log.retention.hours=72                    # 3 天留存
log.retention.bytes=-1                    # 按时间清理,不限字节
log.segment.bytes=1073741824              # 1GB segment
log.cleanup.policy=delete                 # 原始数据 topic 用 delete
compression.type=producer                 # 由 producer 决定(prom-gw 用 zstd)

# ====== 性能 ======
num.network.threads=16
num.io.threads=64
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600
queued.max.requests=1000
num.replica.fetchers=4
num.replica.alter.log.dirs.threads=4

# ====== Rack awareness ======
broker.rack=az-1                          # Kafka-1/Kafka-2 = az-1, Kafka-3 = az-2
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

# ====== 内部 topic(单 Broker 调试时改为 1,生产必须 3) ======
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
```

> **注意**:本配置不包含任何 `sasl.*` / `ssl.*` / `authorizer.class.name` 参数,即不启用 SASL 认证、SSL 加密和 ACL 授权。安全由网络隔离保障(见 §1.4)。

#### 3.3 JVM 配置

修改 `/appdata/kafka/bin/kafka-server-start.sh` 或通过 systemd `Environment` 传入:

```bash
export KAFKA_HEAP_OPTS="-Xms32g -Xmx32g -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+ExplicitGCInvokesConcurrent -XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

| 参数 | 值 | 说明 |
|---|---|---|
| `-Xms32g -Xmx32g` | 32G 堆 | 512G 物理内存的 1/16,Kafka 主要用堆外内存(PageCache) |
| `-XX:+UseG1GC` | G1 GC | 大堆低延迟 |
| `-XX:MaxGCPauseMillis=20` | 20ms | GC 暂停目标 |
| `-XX:InitiatingHeapOccupancyPercent=35` | 35% | 堆使用率 35% 时触发 GC |

#### 3.4 systemd 服务

**`/etc/systemd/system/kafka.service`**:

```ini
[Unit]
Description=Apache Kafka (KRaft mode, PLAINTEXT)
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms32g -Xmx32g -XX:+UseG1GC -XX:MaxGCPauseMillis=20"
Environment="KAFKA_JVM_PERFORMANCE_OPTS=-XX:+AlwaysPreTouch -Djava.awt.headless=true"
ExecStart=/appdata/kafka/bin/kafka-server-start.sh /appdata/kafka/config/server.properties
ExecStop=/appdata/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=5
LimitNOFILE=100000
LimitNPROC=100000
MemoryMax=48G
TimeoutStopSec=300

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/data01/kafka /data02/kafka /data03/kafka /data04/kafka /data05/kafka /data06/kafka /data07/kafka /data08/kafka /data09/kafka /data10/kafka /data11/kafka /applog/kafka /tmp

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
```

#### 3.5 KRaft 格式化(首次启动)

```bash
# 1. 生成 Cluster UUID(所有 Broker 共用一个)
CLUSTER_UUID=$(/appdata/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"
# 将此 UUID 同步到所有 Broker 节点

# 2. 每台 Broker 格式化(在 3 台机器上分别执行)
/appdata/kafka/bin/kafka-storage.sh format \
  --config /appdata/kafka/config/server.properties \
  --cluster-id $CLUSTER_UUID

# 3. 启动所有 Broker
sudo systemctl start kafka

# 4. 验证集群状态(无需 --command-config,PLAINTEXT 直连)
/appdata/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 | head
```

> **注意**:格式化前确保 `/data01/kafka` ~ `/data11/kafka` 为空,否则会报 Cluster ID mismatch。

---

### 4. Topic 管理与最佳实践

#### 4.1 创建 Topic

```bash
# 原始数据 topic(每城每个 tenant 一个)
for city in bj sz hf; do
  for tenant in app_business infra; do
    /appdata/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-1:9092 \
      --create --topic prom.${city}.raw.${tenant} \
      --partitions 64 \
      --replication-factor 3 \
      --config retention.ms=259200000 \
      --config compression.type=producer \
      --config max.message.bytes=10485760
  done
done

# 路由后 topic
for city in bj sz hf; do
  for biz in core infra data app_business; do
    /appdata/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-1:9092 \
      --create --topic prom.${city}.routed.${biz} \
      --partitions 64 \
      --replication-factor 3 \
      --config retention.ms=259200000
  done
done

# DLQ topic(Flink 创建)
for city in bj sz hf; do
  /appdata/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092 \
    --create --topic prom.${city}.dlq.sr.5m \
    --partitions 12 \
    --replication-factor 3 \
    --config retention.ms=604800000  # 7 天
done
```

#### 4.2 Topic 分区数选择

| 场景 | partition 数 | 说明 |
|---|---|---|
| 本地开发 | 4 | 单 Broker,低流量 |
| 小型生产(< 100K samples/s) | 12 | 3 Broker × 4 partition/Broker |
| 中型生产(100K-1M samples/s) | 24-32 | 推荐 |
| 大型生产(> 1M samples/s) | 64 | 生产默认值 |

> **注意**:partition 数只能增加不能减少。建议初始值保守,后续按需扩。

#### 4.3 Topic 配置最佳实践

| 配置 | 推荐值 | 说明 |
|---|---|---|
| `retention.ms` | 259200000(72h) | 3 天留存,足以让 Flink 消费 + DLQ 重放 |
| `compression.type` | `producer` | 由 producer 决定(prom-gw 用 zstd) |
| `max.message.bytes` | 10485760(10MB) | prom-gw batch 可能较大 |
| `min.insync.replicas` | 2 | 配合 acks=all,2 副本写入成功才算成功 |
| `unclean.leader.election.enable` | false | 禁止未同步副本成为 leader,防数据丢失 |

#### 4.4 Topic 运维命令

```bash
# 列出所有 topic
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list | grep prom

# 查看 topic 详情
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe --topic prom.sz.raw.app_business

# 增加 partition(只能增,不能减)
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --topic prom.sz.raw.app_business \
  --partitions 128

# 修改 topic 配置
/appdata/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --topic prom.sz.raw.app_business \
  --add-config retention.ms=172800000  # 改为 2 天

# 删除 topic(谨慎!)
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --delete --topic prom.sz.dlq.sr.5m
```

---

### 5. 监控部署

#### 5.1 JMX Exporter

部署 `jmx_prometheus_javaagent` 暴露 Kafka JMX 指标到 Prometheus:

```bash
# 下载 JMX exporter
cd /appdata/kafka
sudo wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar

# 创建配置文件
sudo cat > /appdata/kafka/jmx-exporter.yml << 'EOF'
lowercaseOutputName: true
lowercaseOutputLabelNames: true
rules:
  - pattern: kafka.<type=(.+), name=(.+)><>Value
    name: kafka_$1_$2
  - pattern: kafka.<type=(.+), name=(.+), (.+)=(.+)><>Value
    name: kafka_$1_$2
    labels:
      "$3": "$4"
  - pattern: java.lang<type=(.+), name=(.+)><>Value
    name: java_$1_$2
EOF

sudo chown kafka:kafka /appdata/kafka/jmx-exporter.yml
```

修改 systemd 服务,添加 JMX exporter agent:

```ini
# /etc/systemd/system/kafka.service 的 [Service] 段追加
Environment="KAFKA_OPTS=-javaagent:/appdata/kafka/jmx_prometheus_javaagent-0.20.0.jar=9404:/appdata/kafka/jmx-exporter.yml"
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart kafka

# 验证
curl -s http://kafka-1:9404/metrics | head -20
```

#### 5.2 Prometheus 抓取配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: kafka
    static_configs:
      - targets:
        - kafka-1:9404
        - kafka-2:9404
        - kafka-3:9404
    scrape_interval: 15s
```

#### 5.3 关键监控指标

| 指标 | JMX 名称 | 告警阈值 |
|---|---|---|
| Under-replicated partitions | `kafka_server_replicamanager_underreplicatedpartitions` | > 0 |
| Offline partitions | `kafka_controller_kafkacontroller_offlinepartitionscount` | > 0 |
| Active controller count | `kafka_controller_kafkacontroller_activecontrollercount` | != 1(应为 1) |
| ISR shrink rate | `kafka_server_replicamanager_isrshrinkspersec` | 持续 > 0 |
| Leader election rate | `kafka_controller_controllerstats_leaderelectionrateandtimems` | 突增 |
| 消息入速率 | `kafka_server_brokertopicmetrics_messagesinpersec` | 突降 50% |
| 字节入速率 | `kafka_server_brokertopicmetrics_bytesinpersec` | - |
| 请求延迟 | `kafka_network_requestmetrics_totaltimems` | p99 > 100ms |
| 磁盘使用率 | `kafka_log_log_size` | > 80% |
| GC 暂停 | `java_lang_garbagecollector_collectiontime` | > 500ms |

#### 5.4 Consumer Lag 监控

```bash
# 安装 kafka-exporter(独立组件,监控 consumer group lag)
docker run -d --name kafka-exporter \
  -p 9308:9308 \
  danielqsj/kafka-exporter:latest \
  --kafka.server=kafka-1:9092 \
  --kafka.server=kafka-2:9092 \
  --kafka.server=kafka-3:9092
```

```yaml
# prometheus.yml 追加
scrape_configs:
  - job_name: kafka-exporter
    static_configs:
      - targets: ['kafka-exporter:9308']
```

**Consumer Lag 告警规则**:

```yaml
groups:
  - name: kafka-consumer-lag
    rules:
      - alert: KafkaConsumerLagHigh
        expr: sum(kafka_consumergroup_lag) by (consumergroup) > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer group {{ $labels.consumergroup }} lag > 10000"

      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka broker {{ $labels.instance }} has under-replicated partitions"

      - alert: KafkaOfflinePartitions
        expr: kafka_controller_kafkacontroller_offlinepartitionscount > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka has offline partitions"
```

#### 5.5 Grafana Dashboard

导入 Kafka Dashboard:

| Dashboard | ID | 说明 |
|---|---|---|
| Kafka Overview | 721 | Broker 指标总览 |
| Kafka Exporter | 7589 | Consumer lag 监控 |
| JVM (Micrometer) | 4701 | JVM/GC 监控 |

---

### 6. 性能调优

#### 6.1 Producer 调优(prom-gw 侧)

prom-gw 已内置优化(见 [internal/kafkasink/producer.go](../../internal/kafkasink/producer.go)):

| 参数 | 值 | 说明 |
|---|---|---|
| `acks` | `all` | 等待所有 ISR 副本确认,防数据丢失 |
| `compression.type` | `zstd` | 最高压缩比,CPU 开销小 |
| `enable.idempotence` | `true` | 幂等写,防重复 |
| `retries` | `10` | 自动重试 |
| `max.in.flight.requests.per.connection` | `5` | 幂等模式下安全 |
| `batch.size` | `65536` | 64KB batch |
| `linger.ms` | `50` | 50ms 攒批延迟 |
| `buffer.memory` | `134217728` | 128MB producer buffer |

#### 6.2 Consumer 调优(Flink 侧)

| 参数 | 值 | 说明 |
|---|---|---|
| `fetch.min.bytes` | `1024` | 最少 1KB 才返回 |
| `fetch.max.wait.ms` | `500` | 最多等 500ms |
| `max.partition.fetch.bytes` | `10485760` | 单 partition 最多 10MB |
| `session.timeout.ms` | `30000` | 心跳超时 30s |
| `heartbeat.interval.ms` | `10000` | 心跳间隔 10s |
| `max.poll.records` | `500` | 单次 poll 最多 500 条 |
| `auto.offset.reset` | `latest` | 无 offset 时从最新开始 |
| `enable.auto.commit` | `false` | 由 Flink checkpoint 管理 offset |

#### 6.3 Broker 调优

| 参数 | 值 | 说明 |
|---|---|---|
| `num.network.threads` | `16` | 网络线程数(= CPU 核数 / 4) |
| `num.io.threads` | `64` | IO 线程数(= 磁盘数 × 2) |
| `socket.send.buffer.bytes` | `1048576` | 1MB 发送缓冲 |
| `socket.receive.buffer.bytes` | `1048576` | 1MB 接收缓冲 |
| `queued.max.requests` | `1000` | 等待 IO 线程的请求队列 |
| `num.replica.fetchers` | `4` | 副本拉取线程数 |
| `log.flush.interval.messages` | `9223372036854775807` | 不按消息数刷盘(依赖 OS PageCache) |
| `log.flush.interval.ms` | (不设) | 不按时间刷盘(依赖 OS PageCache) |

> **关键**:Kafka 生产环境不建议主动刷盘,依赖操作系统 PageCache 异步刷盘。主动刷盘会导致性能下降 90%+。

#### 6.4 磁盘 IO 调优

```bash
# 查看磁盘 IO
iostat -x 1 5

# 关键指标
# %util    > 80% → 瓶颈
# await    > 20ms → 瓶颈
# svctm    > 10ms → 瓶颈
```

**IO 调优**:

```bash
# 调整磁盘调度器(SSD 用 none,HDD 用 mq-deadline)
echo none > /sys/block/sdX/queue/scheduler    # SSD
echo mq-deadline > /sys/block/sdX/queue/scheduler  # HDD

# 调整 read-ahead
blockdev --setra 4096 /dev/sdX    # 2MB read-ahead
```

#### 6.5 网络调优

```bash
# 网卡 ring buffer
ethtool -G eth0 rx 4096 tx 4096

# TCP 参数
echo "net.ipv4.tcp_window_scaling=1" >> /etc/sysctl.d/99-kafka.conf
echo "net.core.netdev_max_backlog=5000" >> /etc/sysctl.d/99-kafka.conf
```

---

### 7. 运维操作

#### 7.1 集群状态检查

```bash
# 1. Broker 列表
/appdata/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:9092 | grep -oP 'id=\K[0-9]+'

# 2. Controller 选举状态
/appdata/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 \
  describe --status

# 3. Topic 列表
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list

# 4. Consumer Group 列表
/appdata/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --list

# 5. Consumer Group lag
/appdata/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --describe --group flink-agg5m-sz-app-business
```

#### 7.2 消费与生产测试

```bash
# 生产测试
/appdata/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.sz.raw.app_business

# 消费测试
/appdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.sz.raw.app_business \
  --from-beginning --max-messages 10 --timeout-ms 10000

# 生产性能测试
/appdata/kafka/bin/kafka-producer-perf-test.sh \
  --producer-props bootstrap.servers=kafka-1:9092 \
  --topic prom.sz.raw.app_business \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 50000

# 消费性能测试
/appdata/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.sz.raw.app_business \
  --messages 100000 \
  --threads 4
```

#### 7.3 优雅停机

```bash
# 1. 检查该 Broker 是否为 Controller
/appdata/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 \
  describe --status | grep Leader

# 2. 如果是 Controller,先迁移 Controller(可选)
# KRaft 模式下 Controller 会自动重新选举

# 3. 优雅停机(systemd 会发 SIGTERM,Kafka 会完成写入后退出)
sudo systemctl stop kafka

# 4. 验证副本同步
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe | grep -v "UnderReplicated"
```

#### 7.4 日志管理

```bash
# Kafka 日志位置
ls /applog/kafka/
# server.log    → 主日志
# state-change.log → 状态变更日志
# controller.log   → Controller 日志

# 日志轮转配置(/appdata/kafka/config/log4j.properties)
log4j.appender.kafkaAppender.maxFileSize=100MB
log4j.appender.kafkaAppender.maxBackupIndex=10

# 查看错误日志
grep -i "error\|warn\|fatal" /applog/kafka/server.log | tail -50
```

---

### 8. 扩容与缩容

#### 8.1 扩容(增加 Broker)

```bash
# 1. 部署新 Broker(按 §2-§3 步骤)
# 假设新增 kafka-4 (10.0.1.24),node.id=4, rack=az-2

# 2. 更新 controller.quorum.voters(所有 Broker)
#   controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093,4@kafka-4:9093

# 3. 逐台重启 Broker(滚动重启)
for broker in kafka-1 kafka-2 kafka-3 kafka-4; do
    echo "重启 ${broker}..."
    ssh ${broker} "sudo systemctl restart kafka"
    sleep 10
    # 等待 Broker 恢复
    /appdata/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server ${broker}:9092 | head -1
done

# 4. 迁移 partition 副本到新 Broker(使用 kafka-reassign-partitions)
# 生成迁移方案
/appdata/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9092 \
  --topics-to-move-json-file topics-to-move.json \
  --broker-list "1,2,3,4" \
  --generate

# 执行迁移
/appdata/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9092 \
  --reassignment-json-file reassignment.json \
  --execute

# 验证迁移完成
/appdata/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9092 \
  --reassignment-json-file reassignment.json \
  --verify
```

#### 8.2 缩容(移除 Broker)

```bash
# 1. 先将待移除 Broker 上的 partition 迁移到其他 Broker
#   kafka-reassign-partitions --broker-list "1,2,3"(排除待移除的 4)

# 2. 确认待移除 Broker 上无 partition
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe | grep "Broker: 4"
# 期望: 无输出(该 Broker 上无 partition)

# 3. 停机
ssh kafka-4 "sudo systemctl stop kafka"

# 4. 更新 controller.quorum.voters(移除 4@kafka-4:9093)
# 逐台重启剩余 Broker
```

---

### 9. 备份与恢复

#### 9.1 数据备份策略

| 方案 | 工具 | 频率 | 适用场景 |
|---|---|---|---|
| Topic 级别复制 | MirrorMaker2 | 实时 | 跨机房灾备 |
| 快照备份 | kafka-export-snapshot | 按需 | 临时备份 |
| 配置备份 | git + 文件同步 | 每次变更 | 配置版本管理 |

#### 9.2 配置备份

```bash
# 定期备份 Kafka 配置(建议纳入 Git)
tar -czf kafka-config-backup-$(date +%Y%m%d).tar.gz \
    /appdata/kafka/config/server.properties \
    /appdata/kafka/jmx-exporter.yml

# 备份 Topic 列表与配置
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe > topic-backup-$(date +%Y%m%d).txt
```

#### 9.3 MirrorMaker2 跨机房复制

```bash
# mm2.properties(在灾备机房执行)
clusters = primary, dr
primary.bootstrap.servers = kafka-1.sz:9092,kafka-2.sz:9092,kafka-3.sz:9092
dr.bootstrap.servers = kafka-1.dr:9092,kafka-2.dr:9092,kafka-3.dr:9092

# 复制 prom-gw 相关 topic
primary->dr.enabled = true
primary->dr.topics = prom\..*
primary->dr.groups = prom-gw.*|flink-agg5m.*

# 同步设置
sync.topic.configs.enabled = true
sync.topic.acls.enabled = false                    # 未启用 ACL,不同步
replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3

# 启动 MirrorMaker2
/appdata/kafka/bin/connect-mirror-maker.sh mm2.properties
```

#### 9.4 数据恢复

```bash
# 从 MirrorMaker2 备份恢复(灾备机房 → 主机房)
# 1. 切换 prom-gw / Flink 到灾备机房 Kafka
# 2. 或反向复制:灾备 → 主机房

# 从配置备份恢复
tar -xzf kafka-config-backup-20260812.tar.gz -C /
sudo systemctl restart kafka
```

---

### 10. 灾难恢复

#### 10.1 单 Broker 故障

```bash
# 1. 确认 Broker 宕机
/appdata/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:9092 | grep -c "id="   # 期望: 3(如果只有 2,说明有 Broker 宕机)

# 2. 检查 under-replicated partitions
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe | grep "UnderReplicated"

# 3. 自动恢复:Kafka 会自动在其他 Broker 上重建副本
#    等待 ISR 恢复即可,无需人工干预

# 4. 如果 Broker 无法恢复,需要:
#    a. 部署新 Broker(同 node.id)
#    b. 或使用 kafka-reassign-partitions 将副本迁移到其他 Broker
```

#### 10.2 全集群故障

```bash
# 1. 确认所有 Broker 宕机
for broker in kafka-1 kafka-2 kafka-3; do
    echo -n "${broker}: "
    ssh ${broker} "systemctl is-active kafka" || echo "DOWN"
done

# 2. 逐台启动
for broker in kafka-1 kafka-2 kafka-3; do
    echo "启动 ${broker}..."
    ssh ${broker} "sudo systemctl start kafka"
    sleep 15
done

# 3. 等待 Controller 选举完成
/appdata/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 \
  describe --status

# 4. 验证所有 partition 恢复
/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe | grep "UnderReplicated"
# 期望: 无输出

# 5. prom-gw 会自动从 WAL 降级恢复,无需人工干预
```

#### 10.3 数据损坏恢复

```bash
# 1. 定位损坏的 log directory
grep -i "corrupt\|error" /applog/kafka/server.log

# 2. 停止对应 Broker
sudo systemctl stop kafka

# 3. 删除损坏的 log directory(该 Broker 上的数据会从其他副本同步)
sudo rm -rf /data06/kafka/*
# 注意:只删除损坏的目录,不要删除全部

# 4. 重新启动 Broker,Kafka 会自动从其他副本同步数据
sudo systemctl start kafka

# 5. 监控同步进度
watch -n 5 '/appdata/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe | grep "UnderReplicated"'
```

---

### 11. 附录

#### 11.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `server.properties` | `/appdata/kafka/config/server.properties` | Broker 主配置(PLAINTEXT) |
| `jmx-exporter.yml` | `/appdata/kafka/jmx-exporter.yml` | JMX exporter 配置 |
| `kafka.service` | `/etc/systemd/system/kafka.service` | systemd 服务 |
| `99-kafka.conf` | `/etc/sysctl.d/99-kafka.conf` | 内核参数 |
| `kafka.conf` | `/etc/security/limits.d/kafka.conf` | 文件句柄限制 |

> **与启用认证的部署相比,本配置不包含以下文件**:
> - `kafka_server_jaas.conf`(SASL JAAS 配置)
> - `admin-client.properties`(管理客户端认证配置)
> - `kafka.keystore.jks` / `kafka.truststore.jks`(SSL 证书库)

#### 11.2 常用命令速查

```bash
# 集群管理
/appdata/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
/appdata/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092

# Topic 管理
/appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list
/appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic <topic>
/appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --alter --topic <topic> --partitions 128

# Consumer Group
/appdata/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --list
/appdata/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --describe --group <group>

# Topic 配置修改
/appdata/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9092 --alter --topic <topic> --add-config retention.ms=172800000

# 性能测试
/appdata/kafka/bin/kafka-producer-perf-test.sh --topic <topic> --num-records 100000 --record-size 1024 --throughput 50000 --producer-props bootstrap.servers=kafka-1:9092
/appdata/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-1:9092 --topic <topic> --messages 100000

# 副本迁移
/appdata/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 --topics-to-move-json-file topics.json --broker-list "1,2,3" --generate
/appdata/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 --reassignment-json-file plan.json --execute
/appdata/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 --reassignment-json-file plan.json --verify
```

#### 11.3 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| Broker 无法启动 | 检查 `/data01/kafka/meta.properties` 的 cluster.id 是否匹配 | 重新格式化或恢复 cluster.id |
| Client 连接超时 | 检查 `advertised.listeners` 是否可达;检查安全组是否放行 9092 | 修正 `advertised.listeners` / 安全组规则 |
| 连接被拒绝 | 检查 Broker 是否启动;检查 9092 端口是否监听 | 启动 Broker / 检查 listeners 配置 |
| Under-replicated partitions | 检查 Broker 状态和磁盘 IO | 恢复 Broker 或扩容 |
| Consumer lag 持续增大 | 检查 Flink 消费速率 | 扩 partition / 扩 TM |
| 磁盘满 | 检查 retention 配置 | 调整 retention 或扩容 |
| Controller 选举失败 | 检查 Quorum 是否有 2/3 存活 | 恢复 Broker |

#### 11.4 安全替代方案说明

本部署**不启用 Kafka 自身的 SSL/SASL/ACL 认证**,通过以下方式保证安全:

| 安全层 | 措施 | 说明 |
|---|---|---|
| 网络隔离 | VPC + 安全组 | Kafka 9092 端口仅对 prom-gw / Flink 网段开放 |
| 主机访问控制 | SSH key +堡垒机 | 限制可直接访问 Kafka 主机的人员 |
| 监控审计 | JMX + 日志审计 | 监控异常连接和操作 |
| 数据隔离 | Topic 命名规范 | 按 `prom.<city>.<stage>.<tenant>` 隔离不同业务数据 |

> **如需启用认证**,可参考 Kafka 官方文档添加 SASL/SSL 配置,本工程的 prom-gw 和 Flink 客户端已预留环境变量/参数接入点(见 **prom-gw 配置参考**(见 §9) 和 **Flink 生产部署**(见 §5))。



---

## 3. StarRocks 生产部署 {#3-starrocks-生产部署}
> 本文档覆盖 prom-gw 配套 MPP 数据仓库 **StarRocks v3.4.10** 的生产环境完整部署,包括存算一体(Shared-Nothing)集群架构、FE/BE 部署、JBOD 多盘存储配置、Nginx 反向代理、监控集成和运维操作。
>
> StarRocks 是高性能 MPP 数据库,用于 prom-gw 的实时数仓分析,支持亚秒级查询、实时更新、联邦查询等功能。
>
> 配套文档:**Kafka 生产部署**(见 §2)、**Flink 生产部署**(见 §5)、**高可用与负载均衡**(见 §7)、**生产部署指南**(见 §1)


---

### 1. 部署架构

#### 1.1 存算一体拓扑

StarRocks 采用存算一体(Shared-Nothing)架构,3 FE + 3 BE 节点组成高可用集群。FE 管理元数据和查询调度,BE 负责数据存储和计算执行:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  ┌─────────────────┐    ┌──────────────────────────────────┐     │
│  │  Nginx + VIP    │    │  FE Cluster (元数据 + 查询调度)   │     │
│  │  VIP:10.0.10.100│───▶│  sr-fe-1 (Leader)  10.0.10.31   │     │
│  │  443 → 8030      │    │  sr-fe-2 (Follower) 10.0.10.32   │     │
│  └─────────────────┘    │  sr-fe-3 (Follower) 10.0.10.33   │     │
│                          └──────────┬───────────────────────┘     │
│  ┌──────────────────────────────────┐                              │
│  │  MySQL Client (10.0.10.50)      │  9030 (MySQL 协议)            │
│  └──────────────────────────────────┘                              │
│                                     │                              │
│          ┌──────────────────────────┘                              │
│          ▼                                                         │
│  ┌──────────────────────────────────────────────────────┐         │
│  │  BE Cluster (存储 + 计算)                             │         │
│  │  sr-be-1  10.0.1.31   22×16T JBOD (/data01~/data22)  │         │
│  │  sr-be-2  10.0.1.32   22×16T JBOD (/data01~/data22)  │         │
│  │  sr-be-3  10.0.1.33   22×16T JBOD (/data01~/data22)  │         │
│  └──────────────────────────────────────────────────────┘         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

> **存算一体说明**:BE 节点同时承担数据存储和 SQL 计算执行,数据按 Tablet(分片)分布到 3 个 BE 节点,默认 3 副本冗余。22 块盘通过 JBOD 挂载,BE 自动将 Tablet 均匀分布到各盘。

#### 1.2 端口规划

**FE 端口**:

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| 8030 | FE HTTP(Web UI + REST API) | Nginx VIP / 运维网段 |
| 9020 | FE RPC(Thrift,FE 间通信) | FE 网段内部 |
| 9030 | FE MySQL 协议(客户端查询) | 运维网段 / Nginx |
| 9010 | FE Edit Log(Follower 同步) | FE 网段内部 |

**BE 端口**:

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| 8040 | BE HTTP(REST API + 文件传输) | FE → BE 网段 |
| 9060 | BE Thrift(FE → BE 通信) | FE → BE 网段 |
| 9050 | BE Heartbeat(心跳上报) | FE → BE 网段 |
| 8060 | BE bRPC(FE → BE / BE 间通信) | FE → BE / BE 网段 |
| 9070 | BE Starlet(存算分离用,存算一体可忽略) | — |

#### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 | 网络 | 说明 |
|---|---|---|---|---|---|
| FE | 8C/16G | 3 | 100G SSD | 万兆 | 元数据管理,轻量级,可与 BE 共置 |
| BE | 64C/256G | 3 | 22×16T HDD(JBOD) | 万兆 | 数据存储 + 计算,CPU 需支持 AVX2 |
| Nginx + Keepalived | 2C/4G | 2 | 50G SSD | 千兆 | 复用 **HA 部署**(见 §7) |

> **AVX2 检查**:BE 依赖 AVX2 指令集加速向量化执行,部署前需验证:
> ```bash
> cat /proc/cpuinfo | grep avx2
> # 有输出 = 支持
> ```

---

### 2. 前置准备

#### 2.1 操作系统

```bash
# CentOS / RHEL 8+
cat /etc/redhat-release

# Ubuntu / Debian 22+
cat /etc/os-release
```

#### 2.2 OpenJDK 25 安装

StarRocks v3.4 要求 JDK 11+,统一使用 OpenJDK 25(与 Kafka / Kafka-UI 保持一致):

```bash
# CentOS / RHEL
sudo yum install -y java-25-openjdk java-25-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-25-jdk

java -version   # 期望: openjdk version "25.x.x"
```

> **注意**:StarRocks 不支持 JRE,必须安装 JDK(含 `java-devel`)。若实例存在多个 JDK,可在 `fe.conf` / `be.conf` 中指定 `JAVA_HOME`。

#### 2.3 系统调优

所有 FE 和 BE 节点执行以下系统配置:

```bash
# ====== 1. 关闭 THP(Transparent Huge Pages) ======
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag

# 持久化(/etc/rc.local)
cat >> /etc/rc.local << 'EOF'
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
EOF
sudo chmod +x /etc/rc.local

# ====== 2. 关闭 Swap ======
sudo swapoff -a
sudo sed -i '/swap/s/^/#/' /etc/fstab

# ====== 3. 文件描述符限制 ======
sudo tee /etc/security/limits.d/starrocks.conf << 'EOF'
*   soft    nofile    655360
*   hard    nofile    655360
*   soft    nproc     655350
*   hard    nproc     655350
*   soft    memlock   unlimited
*   hard    memlock   unlimited
EOF

# 重新登录 shell 生效,验证:
ulimit -n   # 期望: 655360

# ====== 4. 内核参数(/etc/sysctl.conf) ======
sudo tee -a /etc/sysctl.conf << 'EOF'
# 文件描述符
fs.file-max = 6553500

# 内存 overcommit(BE 需要)
vm.overcommit_memory = 1
vm.overcommit_ratio = 90

# 网络
net.ipv4.tcp_max_tw_buckets = 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 10000 65535
net.core.somaxconn = 65535

# 禁用 THP
kernel.numa_balancing = 0
EOF

sudo sysctl -p
```

#### 2.4 NTP 时钟同步

```bash
# 安装 chrony
sudo yum install -y chrony        # CentOS / RHEL
sudo apt install -y chrony        # Ubuntu / Debian

# 配置 NTP 服务器
sudo vi /etc/chrony.conf
# 添加: server ntp.aliyun.com iburst
# (或内网 NTP 服务器)

sudo systemctl enable --now chronyd
chronyc tracking   # 验证同步状态
```

> **强制**:StarRocks 集群所有节点时钟偏差必须 < 5 秒,否则 BE 心跳异常导致节点状态波动。

#### 2.5 安装 MySQL 客户端

FE 节点(或运维节点)需要 MySQL 客户端连接 StarRocks:

```bash
# CentOS / RHEL
sudo yum install -y mysql
# Ubuntu / Debian
sudo apt install -y mysql-client

mysql --version   # 5.5.0+ 即可
```

#### 2.6 创建用户与目录

**在所有节点(FE + BE)上执行**:

```bash
# 创建专用用户
sudo useradd -r -m -d /appdata/starrocks -s /bin/bash starrocks

# 部署目录(STARROCKS_HOME)
sudo mkdir -p /appdata/starrocks

# ====== BE 节点:创建 22 个 JBOD 数据目录 ======
for i in $(seq -w 1 22); do
    sudo mkdir -p /data${i}/starrocks
done

# ====== 设置属主 ======
sudo chown -R starrocks:starrocks /appdata/starrocks
for i in $(seq -w 1 22); do
    sudo chown -R starrocks:starrocks /data${i}/starrocks
done
```

> **目录说明**:
> - 安装目录 = 日志目录 = `/appdata/starrocks`(FE 日志在 `fe/log/`,BE 日志在 `be/log/`)
> - FE 元数据 = `/appdata/starrocks/fe/meta`(默认,可改至独立磁盘)
> - BE 数据 = `/data01/starrocks` ~ `/data22/starrocks`(22 个 JBOD 挂载点)

#### 2.7 磁盘挂载(BE 节点 JBOD)

每台 BE 物理机 22 × 16T 盘,JBOD 模式(不做 RAID):

```bash
# /etc/fstab(BE 节点)
/dev/sdb1 /data01  ext4 noatime,nodiratime 0 2
/dev/sdc1 /data02  ext4 noatime,nodiratime 0 2
/dev/sdd1 /data03  ext4 noatime,nodiratime 0 2
/dev/sde1 /data04  ext4 noatime,nodiratime 0 2
/dev/sdf1 /data05  ext4 noatime,nodiratime 0 2
/dev/sdg1 /data06  ext4 noatime,nodiratime 0 2
/dev/sdh1 /data07  ext4 noatime,nodiratime 0 2
/dev/sdi1 /data08  ext4 noatime,nodiratime 0 2
/dev/sdj1 /data09  ext4 noatime,nodiratime 0 2
/dev/sdk1 /data10  ext4 noatime,nodiratime 0 2
/dev/sdl1 /data11  ext4 noatime,nodiratime 0 2
/dev/sdm1 /data12  ext4 noatime,nodiratime 0 2
/dev/sdn1 /data13  ext4 noatime,nodiratime 0 2
/dev/sdo1 /data14  ext4 noatime,nodiratime 0 2
/dev/sdp1 /data15  ext4 noatime,nodiratime 0 2
/dev/sdq1 /data16  ext4 noatime,nodiratime 0 2
/dev/sdr1 /data17  ext4 noatime,nodiratime 0 2
/dev/sds1 /data18  ext4 noatime,nodiratime 0 2
/dev/sdt1 /data19  ext4 noatime,nodiratime 0 2
/dev/sdu1 /data20  ext4 noatime,nodiratime 0 2
/dev/sdv1 /data21  ext4 noatime,nodiratime 0 2
/dev/sdw1 /data22  ext4 noatime,nodiratime 0 2

sudo mount -a
```

#### 2.8 /etc/hosts 配置

所有节点配置主机名解析(或使用内部 DNS):

```bash
# /etc/hosts 追加
# FE nodes
10.0.10.31  sr-fe-1
10.0.10.32  sr-fe-2
10.0.10.33  sr-fe-3
# BE nodes
10.0.1.31   sr-be-1
10.0.1.32   sr-be-2
10.0.1.33   sr-be-3
```

---

### 3. 下载与安装

#### 3.1 下载 StarRocks 3.4.10

```bash
# 在运维节点下载(或任一 FE 节点)
cd /tmp
wget https://github.com/StarRocks/starrocks/releases/download/3.4.10/StarRocks-3.4.10.tar.gz

# 备用下载地址(如 GitHub 无法访问)
# wget https://releases.starrocks.com/StarRocks-3.4.10.tar.gz

# 校验文件大小(约 1-2 GB)
ls -lh StarRocks-3.4.10.tar.gz
```

> **版本固定**:生产环境必须使用固定版本 `3.4.10`,禁止使用 main 分支构建。

#### 3.2 解压与分发

```bash
# 解压到 STARROCKS_HOME
sudo mkdir -p /appdata/starrocks
sudo tar -xzf /tmp/StarRocks-3.4.10.tar.gz -C /appdata/starrocks --strip-components=1

# 目录结构
ls /appdata/starrocks/
# 期望: fe/  be/  LICENSE  NOTICE  README.md

sudo chown -R starrocks:starrocks /appdata/starrocks
```

**分发到其他节点**:

```bash
# 分发到 FE-2, FE-3
for host in sr-fe-2 sr-fe-3; do
    sudo -u starrocks scp -r /appdata/starrocks starrocks@$host:/appdata/
done

# 分发到 BE-1, BE-2, BE-3
for host in sr-be-1 sr-be-2 sr-be-3; do
    sudo -u starrocks scp -r /appdata/starrocks starrocks@$host:/appdata/
done

# 设置属主(远程节点)
for host in sr-fe-2 sr-fe-3 sr-be-1 sr-be-2 sr-be-3; do
    ssh starrocks@$host "sudo chown -R starrocks:starrocks /appdata/starrocks"
done
```

#### 3.3 目录结构

```
/appdata/starrocks/          ← STARROCKS_HOME(安装 + 日志)
  ├── fe/
  │   ├── bin/
  │   │   ├── start_fe.sh
  │   │   └── stop_fe.sh
  │   ├── conf/
  │   │   └── fe.conf        ← FE 主配置
  │   ├── lib/
  │   ├── log/               ← FE 日志目录
  │   │   ├── fe.log
  │   │   ├── fe.warn.log
  │   │   └── fe.audit.log
  │   └── meta/              ← FE 元数据(运行时创建)
  └── be/
      ├── bin/
      │   ├── start_be.sh
      │   └── stop_be.sh
      ├── conf/
      │   └── be.conf        ← BE 主配置
      ├── lib/
      ├── log/               ← BE 日志目录
      │   ├── be.INFO
      │   └── be.WARNING
      └── storage/           ← 默认存储(生产用 JBOD,见 §5)
```

---

### 4. FE 部署

#### 4.1 fe.conf 配置

**在所有 FE 节点上配置 `/appdata/starrocks/fe/conf/fe.conf`**:

```bash
# ======================================================
# StarRocks FE 配置 - sr-fe-1 (Leader)
# 其他 FE 节点(sr-fe-2, sr-fe-3)仅 priority_networks 不同
# ======================================================

# ====== 网络端口 ======
http_port = 8030         # FE HTTP(Web UI + REST API)
rpc_port = 9020          # FE Thrift RPC(FE 间通信)
query_port = 9030         # MySQL 协议(客户端查询)
edit_log_port = 9010     # Edit Log(Follower 同步)

# ====== 元数据目录 ======
meta_dir = /appdata/starrocks/fe/meta

# ====== 网络(IP 访问模式) ======
# sr-fe-1:
priority_networks = 10.0.10.31/24
# sr-fe-2:
# priority_networks = 10.0.10.32/24
# sr-fe-3:
# priority_networks = 10.0.10.33/24

# ====== JDK(若多版本 JDK,指定路径) ======
# JAVA_HOME = /usr/lib/jvm/java-25-openjdk

# ====== JVM 参数 ======
# FE 默认堆 4G,生产建议 8G(取决于元数据量)
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
# JDK 8 兼容(JAVA_OPTS_FOR_JDK_8 在 JDK 11+ 可忽略)
JAVA_OPTS_FOR_JDK_11 = "-Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# ====== 集群配置 ======
# 默认副本数(3 BE 节点 = 3 副本)
default_replication_num = 3
# 默认存储介质(HDD,如全 SSD 则改为 ssd)
default_storage_medium = hdd
# Tablet 单副本存储上限(GB),超出自动分裂
storage_max_storages_per_disk = 100

# ====== 审计日志 ======
enable_audit_log = true
audit_log_dir = /appdata/starrocks/fe/log
audit_log_modules = slow_query,external

# ====== 慢查询阈值(秒) ======
qe_max_connection = 2000
```

> **多 FE 节点注意**:所有 FE 的 `http_port` 必须相同。`priority_networks` 按节点 IP 设置。

#### 4.2 启动 Leader FE(sr-fe-1)

```bash
# 在 sr-fe-1 上执行
su - starrocks
cd /appdata/starrocks
./fe/bin/start_fe.sh --daemon

# 检查启动日志
cat fe/log/fe.log | grep thrift
# 期望输出: thrift server started with port 9020

# 检查进程
jps | grep StarRocksFE
# 期望: xxxxx StarRocksFE
```

#### 4.3 添加 Follower FE(sr-fe-2, sr-fe-3)

**通过 MySQL 客户端添加 Follower**:

```bash
# 在运维节点或 sr-fe-1 上执行
mysql -h sr-fe-1 -P9030 -uroot
```

```sql
-- 添加 Follower FE 节点(逐个添加)
ALTER SYSTEM ADD FOLLOWER "sr-fe-2:9010";
ALTER SYSTEM ADD FOLLOWER "sr-fe-3:9010";

-- 查看当前 FE 列表
SHOW PROC '/frontends'\G
```

**在 sr-fe-2 上启动 Follower FE**:

```bash
su - starrocks
cd /appdata/starrocks
# 首次启动需指定 helper(指向 Leader FE)
./fe/bin/start_fe.sh --helper sr-fe-1:9010 --daemon

# 检查日志
cat fe/log/fe.log | grep thrift
```

**在 sr-fe-3 上启动 Follower FE**:

```bash
su - starrocks
cd /appdata/starrocks
./fe/bin/start_fe.sh --helper sr-fe-1:9010 --daemon

cat fe/log/fe.log | grep thrift
```

> **helper 说明**:新 Follower 首次启动时需通过 `--helper` 指定一个已有 Follower FE(通常为 Leader)来同步全量元数据。仅首次启动需指定,后续重启无需 `--helper`。

#### 4.4 验证 FE 集群

```sql
-- 通过 MySQL 客户端连接 Leader FE
mysql -h sr-fe-1 -P9030 -uroot

-- 查看 FE 节点状态
SHOW PROC '/frontends'\G
```

期望输出:
```
*************************** 1. row ***************************
              Name: 10.0.10.31_9010_xxx
                IP: 10.0.10.31
      EditLogPort: 9010
          HttpPort: 8030
         QueryPort: 9030
           RpcPort: 9020
             Role: LEADER
         ClusterId: xxxxxxx
             Join: true
            Alive: true
 ReplayedJournalId: xxx
     LastHeartbeat: 2026-08-20 10:00:00
      IsHelper: true
          ErrMsg:
        StartTime: 2026-08-20 09:50:00
          Version: 3.4.10-xxxxx
*************************** 2. row ***************************
             Role: FOLLOWER
            Alive: true
...
*************************** 3. row ***************************
             Role: FOLLOWER
            Alive: true
...
```

> **验证要点**:
> - `Alive` = `true` → 节点正常
> - `Role` = `LEADER` / `FOLLOWER` → 角色正确
> - `Join` = `true` → 已加入集群
> - 3 个 FE 中 1 个 LEADER + 2 个 FOLLOWER(自动选举)

---

### 5. BE 部署

#### 5.1 be.conf 配置

**在所有 BE 节点上配置 `/appdata/starrocks/be/conf/be.conf`**:

```bash
# ======================================================
# StarRocks BE 配置 - sr-be-1
# 其他 BE 节点(sr-be-2, sr-be-3)仅 priority_networks 不同
# ======================================================

# ====== 网络端口 ======
be_port = 9060               # BE Thrift(FE → BE 通信)
be_http_port = 8040           # BE HTTP(REST API + 文件传输)
heartbeat_service_port = 9050 # BE 心跳(FE → BE 探活)
brpc_port = 8060              # BE bRPC(FE → BE / BE 间通信)
# starlet_port = 9070        # 存算分离用,存算一体可忽略

# ====== 数据存储目录(JBOD 22 块盘,分号分隔) ======
storage_root_path = /data01/starrocks;/data02/starrocks;/data03/starrocks;/data04/starrocks;/data05/starrocks;/data06/starrocks;/data07/starrocks;/data08/starrocks;/data09/starrocks;/data10/starrocks;/data11/starrocks;/data12/starrocks;/data13/starrocks;/data14/starrocks;/data15/starrocks;/data16/starrocks;/data17/starrocks;/data18/starrocks;/data19/starrocks;/data20/starrocks;/data21/starrocks;/data22/starrocks

# ====== 网络(IP 访问模式) ======
# sr-be-1:
priority_networks = 10.0.1.31/24
# sr-be-2:
# priority_networks = 10.0.1.32/24
# sr-be-3:
# priority_networks = 10.0.1.33/24

# ====== JDK(若需 Java UDF) ======
# JAVA_HOME = /usr/lib/jvm/java-25-openjdk
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC"
JAVA_OPTS_FOR_JDK_11 = "-Xmx8192m -XX:+UseG1GC"

# ====== 内存配置 ======
# mem_limit = 0.90            # BE 使用内存上限(物理内存比例)
# tablet_wal_max_size = 1GB   # 单 Tablet WAL 大小上限

# ====== 存储配置 ======
storage_format = default       # 默认行列混存格式
# 默认存储介质(如全 SSD 可改为 ssd)
# default_storage_medium = hdd

# ====== 并发配置 ======
# num_threads = 64             # CPU 核心数(默认自动检测)

# ====== 诊断配置 ======
sys_log_dir = /appdata/starrocks/be/log
sys_log_level = INFO
# audit_log_dir = /appdata/starrocks/be/log
```

> **storage_root_path 说明**:
> - 多盘使用分号(`;`)分隔,不是逗号
> - 可指定介质类型:`/data01/starrocks,medium:ssd;/data02/starrocks,medium:hdd`
> - BE 自动将 Tablet 均匀分布到所有盘,单盘故障不影响其他盘

#### 5.2 启动 BE 节点

**在所有 BE 节点上执行(sr-be-1, sr-be-2, sr-be-3)**:

```bash
# 在 sr-be-1 上执行
su - starrocks
cd /appdata/starrocks
./be/bin/start_be.sh --daemon

# 检查启动日志
cat be/log/be.INFO | grep heartbeat
# 期望输出: heartbeat has started listening port on 9050

# 在 sr-be-2 上执行
cd /appdata/starrocks
./be/bin/start_be.sh --daemon
cat be/log/be.INFO | grep heartbeat

# 在 sr-be-3 上执行
cd /appdata/starrocks
./be/bin/start_be.sh --daemon
cat be/log/be.INFO | grep heartbeat
```

#### 5.3 添加 BE 到集群

通过 MySQL 客户端连接 Leader FE,添加 BE 节点:

```bash
mysql -h sr-fe-1 -P9030 -uroot
```

```sql
-- 添加 3 个 BE 节点(一条 SQL 添加多个)
ALTER SYSTEM ADD BACKEND "sr-be-1:9050", "sr-be-2:9050", "sr-be-3:9050";

-- 查看 BE 节点状态
SHOW PROC '/backends'\G
```

期望输出(每个 BE 一行):
```
*************************** 1. row ***************************
        BackendId: 10001
              IP: 10.0.1.31
    HeartbeatPort: 9050
         BePort: 9060
        HttpPort: 8040
        BrpcPort: 8060
   LastStartTime: 2026-08-20 10:00:00
  LastHeartbeat: 2026-08-20 10:01:00
           Alive: true
SystemDecommissioned: false
ClusterDecommissioned: false
       TabletNum: 0
DataUsedCapacity: 0.000
   AvailCapacity: 22.0 TB        ← 22 块盘总可用
   TotalCapacity: 352.0 TB       ← 22 × 16T
         UsedPct: 0.00 %
  MaxDiskUsedPct: 0.00 %
         ErrMsg:
        Version: 3.4.10-xxxxx
```

> **验证要点**:
> - `Alive` = `true` → BE 正常
> - `TotalCapacity` ≈ 22 × 16T → 确认 22 块盘全部识别
> - `TabletNum` 初始为 0,建表后自动增长

---

### 6. 集群初始化与验证

#### 6.1 连接集群

```bash
# 通过 MySQL 协议连接(可连任意 FE)
mysql -h sr-fe-1 -P9030 -uroot

# 或通过 Nginx VIP(见 §7)
mysql -h 10.0.10.100 -P9030 -uroot
```

> **初始密码**:root 默认无密码,生产必须设置。

#### 6.2 设置 root 密码

```sql
-- 设置 root 密码
SET PASSWORD FOR 'root' = PASSWORD('YourStrongPassword123');

-- 退出后使用密码重连
-- mysql -h sr-fe-1 -P9030 -uroot -p
```

#### 6.3 创建数据库与用户

```sql
-- 创建 prom-gw 分析库
CREATE DATABASE prom_gw_analytics;

-- 创建专用用户
CREATE USER 'prom_gw' IDENTIFIED BY 'PromGwPassword123';

-- 授权
GRANT SELECT, INSERT, UPDATE, DELETE ON prom_gw_analytics.* TO 'prom_gw';

-- 刷新权限
FLUSH PRIVILEGES;

-- 验证
SHOW DATABASES;
-- 期望: information_schema / prom_gw_analytics
```

#### 6.4 建表示例

```sql
USE prom_gw_analytics;

-- 创建分区表示例(Kafka 消费指标分析)
CREATE TABLE IF NOT EXISTS kafka_consumer_metrics (
    dt          DATE         NOT NULL COMMENT '日期',
    cluster     VARCHAR(64)  NOT NULL COMMENT 'Kafka集群',
    topic       VARCHAR(256) NOT NULL COMMENT 'Topic',
    consumer_group VARCHAR(128) NOT NULL COMMENT '消费组',
    partition   INT          NOT NULL COMMENT '分区',
    offset      BIGINT       NOT NULL COMMENT '当前offset',
    lag         BIGINT       NOT NULL COMMENT '积压量',
    create_time DATETIME     NOT NULL COMMENT '采集时间'
)
ENGINE = OLAP
DUPLICATE KEY (dt, cluster, topic)
PARTITION BY RANGE (dt) (
    PARTITION p20260820 VALUES **('2026-08-20'), ('2026-08-21')),
    PARTITION p20260821 VALUES [('2026-08-21'), ('2026-08-22'))
)
DISTRIBUTED BY HASH(cluster, topic) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "storage_medium" = "hdd"
);

-- 验证表创建
SHOW TABLES;
DESCRIBE kafka_consumer_metrics;
```

#### 6.5 集群健康检查

```sql
-- 查看 FE 状态
SHOW PROC '/frontends'\G

-- 查看 BE 状态
SHOW PROC '/backends'\G

-- 查看集群整体状态
SHOW BACKENDS;
SHOW FRONTENDS;

-- 查看数据库
SHOW DATABASES;

-- 查看 Tablet 分布(建表后)
SHOW TABLET FROM prom_gw_analytics.kafka_consumer_metrics LIMIT 10;
```

---

### 7. Nginx 反向代理

#### 7.1 Nginx 配置

复用现有 [HA 与负载均衡部署**(见 §7) 的 Nginx,为 StarRocks FE Web UI 和 MySQL 协议做反向代理:

**`/etc/nginx/conf.d/starrocks.conf`**:

```nginx
# FE Web UI (HTTP 8030)
upstream starrocks_fe_http {
    server 10.0.10.31:8030;
    server 10.0.10.32:8030 backup;
    server 10.0.10.33:8030 backup;
}

# FE MySQL 协议 (TCP 9030) - stream 模块
# 需要 nginx 编译 --with-stream 模块

server {
    listen 443 ssl http2;
    server_name starrocks.prom-gw.internal;

    ssl_certificate     /etc/nginx/ssl/prom-gw.crt;
    ssl_certificate_key /etc/nginx/ssl/prom-gw.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # FE Web UI 反向代理
    location / {
        proxy_pass http://starrocks_fe_http;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 支持(Web UI 实时查询)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # 访问控制
    allow 10.0.10.0/24;
    allow 10.0.1.0/24;
    deny all;
}
```

**MySQL 协议 TCP 代理**(`/etc/nginx/stream.d/starrocks.conf`):

```nginx
# FE MySQL 协议负载均衡(TCP)
stream {
    upstream starrocks_fe_mysql {
        server 10.0.10.31:9030;
        server 10.0.10.32:9030;
        server 10.0.10.33:9030;
    }

    server {
        listen 9030;
        proxy_pass starrocks_fe_mysql;
        proxy_connect_timeout 10s;
        proxy_timeout 300s;
    }
}
```

```bash
sudo nginx -t
sudo nginx -s reload
```

---

### 8. 监控集成

#### 8.1 FE Prometheus 指标

StarRocks FE 通过 HTTP `8030` 端口暴露 Prometheus 指标:

```bash
# 验证 FE 指标端点
curl -s http://sr-fe-1:8030/metrics | head -20
```

**Prometheus 抓取配置**(`prometheus.yml` 追加):

```yaml
scrape_configs:
  # StarRocks FE
  - job_name: starrocks-fe
    static_configs:
      - targets:
          - sr-fe-1:8030
          - sr-fe-2:8030
          - sr-fe-3:8030
    metrics_path: /metrics
    scrape_interval: 15s

  # StarRocks BE
  - job_name: starrocks-be
    static_configs:
      - targets:
          - sr-be-1:8040
          - sr-be-2:8040
          - sr-be-3:8040
    metrics_path: /metrics
    scrape_interval: 15s
```

#### 8.2 关键指标

| 组件 | 指标 | 说明 |
|---|---|---|
| FE | `starrocks_fe_query_total` | 查询总数 |
| FE | `starrocks_fe_query_err` | 查询错误数 |
| FE | `starrocks_fe_query_latency` | 查询延迟 |
| FE | `starrocks_fe_txn_status` | 事务状态 |
| BE | `starrocks_be_process_cpu_usage` | CPU 使用率 |
| BE | `starrocks_be_process_mem_resident` | 内存使用 |
| BE | `starrocks_be_disks_total_capacity` | 磁盘总量 |
| BE | `starrocks_be_disks_avail_capacity` | 磁盘可用 |
| BE | `starrocks_be_tablet_num` | Tablet 数量 |
| BE | `starrocks_be_row_count` | 行数 |

#### 8.3 告警规则

**`prometheus-rules.yml` 追加**:

```yaml
groups:
  - name: starrocks
    rules:
      - alert: StarRocksFEDown
        expr: up{job="starrocks-fe"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks FE down on {{ $labels.instance }}"

      - alert: StarRocksBEDown
        expr: up{job="starrocks-be"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StarRocks BE down on {{ $labels.instance }}"

      - alert: StarRocksDiskUsageHigh
        expr: 1 - starrocks_be_disks_avail_capacity / starrocks_be_disks_total_capacity > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks disk usage > 85% on {{ $labels.instance }}"

      - alert: StarRocksQueryErrorRate
        expr: rate(starrocks_fe_query_err[5m]) / rate(starrocks_fe_query_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "StarRocks query error rate > 5%"
```

---

### 9. 运维操作

#### 9.1 启停服务

```bash
# ====== 启动(必须先 FE 后 BE) ======
# 启动 Leader FE
su - starrocks
cd /appdata/starrocks && ./fe/bin/start_fe.sh --daemon

# 启动 Follower FE(非首次,无需 --helper)
./fe/bin/start_fe.sh --daemon

# 启动 BE
cd /appdata/starrocks && ./be/bin/start_be.sh --daemon

# ====== 停止(必须先 BE 后 FE) ======
# 停止 BE
./be/bin/stop_be.sh

# 停止 FE
./fe/bin/stop_fe.sh
```

> **启停顺序**:
> - **启动**:FE → BE(FE 就绪后 BE 才能注册心跳)
> - **停止**:BE → FE(BE 优雅下线后再停 FE)

#### 9.2 扩容(新增 BE)

```bash
# 1. 新 BE 节点:安装软件、配置 be.conf(§2 + §3 + §5)
# 2. 启动新 BE
su - starrocks
cd /appdata/starrocks && ./be/bin/start_be.sh --daemon

# 3. 通过 MySQL 客户端添加
mysql -h sr-fe-1 -P9030 -uroot -p
```

```sql
ALTER SYSTEM ADD BACKEND "sr-be-4:9050";

-- 查看是否加入
SHOW PROC '/backends'\G

-- 触发负载均衡(可选)
-- ADMIN SHOW REPLICA DISTRIBUTION FROM TABLE prom_gw_analytics.kafka_consumer_metrics;
```

> **扩容后自动均衡**:BE 加入集群后,StarRocks 自动迁移部分 Tablet 到新节点,实现负载均衡。可通过 `SHOW PROC '/backends'` 的 `TabletNum` 观察均衡进度。

#### 9.3 缩容(下线 BE)

```sql
-- 1. 标记 DECOMMISSION(安全下线,自动迁移数据)
ALTER SYSTEM DECOMMISSION BACKEND "sr-be-4:9050";

-- 2. 等待数据迁移完成(TabletNum 降为 0)
SHOW PROC '/backends'\G

-- 3. 确认 SystemDecommissioned = true 且 TabletNum = 0 后移除
ALTER SYSTEM DROP BACKEND "sr-be-4:9050";
```

> **禁止直接 DROP**:未 DECOMMISSION 直接 DROP 会导致数据丢失副本,需等待 DECOMMISSION 完成再 DROP。

#### 9.4 版本升级

```bash
# 1. 备份配置
sudo cp /appdata/starrocks/fe/conf/fe.conf /appdata/starrocks/fe/conf/fe.conf.bak.$(date +%Y%m%d)
sudo cp /appdata/starrocks/be/conf/be.conf /appdata/starrocks/be/conf/be.conf.bak.$(date +%Y%m%d)

# 2. 下载新版本
cd /tmp
wget https://github.com/StarRocks/starrocks/releases/download/<新版本>/StarRocks-<新版本>.tar.gz

# 3. 逐节点滚动升级(先升级 Follower FE,再 Leader FE,最后 BE)
# 3a. 停止节点
su - starrocks
cd /appdata/starrocks
./fe/bin/stop_fe.sh    # 或 ./be/bin/stop_be.sh

# 3b. 解压新版本(覆盖 lib 和 bin)
sudo tar -xzf /tmp/StarRocks-<新版本>.tar.gz -C /appdata/starrocks_new --strip-components=1

# 3c. 保留旧配置
sudo cp /appdata/starrocks/fe/conf/fe.conf /appdata/starrocks_new/fe/conf/
sudo cp /appdata/starrocks/be/conf/be.conf /appdata/starrocks_new/be/conf/

# 3d. 切换目录
sudo mv /appdata/starrocks /appdata/starrocks_old
sudo mv /appdata/starrocks_new /appdata/starrocks
sudo chown -R starrocks:starrocks /appdata/starrocks

# 3e. 启动
./fe/bin/start_fe.sh --daemon    # 或 ./be/bin/start_be.sh --daemon

# 4. 验证
SHOW PROC '/frontends'\G
SHOW PROC '/backends'\G
```

> **升级前必读**:查阅 [3.4.10 Release Notes](https://github.com/StarRocks/starrocks/releases/tag/3.4.10),确认 Breaking Changes。滚动升级时保持集群至少 1 个 FE + 2 个 BE 在线。

#### 9.5 备份与恢复

```bash
# 备份配置
sudo tar -czf starrocks-config-backup-$(date +%Y%m%d).tar.gz \
    -C /appdata/starrocks fe/conf/ be/conf/

# 恢复配置
sudo tar -xzf starrocks-config-backup-20260820.tar.gz -C /appdata/starrocks/
```

#### 9.6 常见排查

```bash
# FE 日志
tail -100f /appdata/starrocks/fe/log/fe.log
tail -100f /appdata/starrocks/fe/log/fe.warn.log    # 警告日志
tail -100f /appdata/starrocks/fe/log/fe.audit.log   # 审计日志

# BE 日志
tail -100f /appdata/starrocks/be/log/be.INFO
tail -100f /appdata/starrocks/be/log/be.WARNING

# 检查端口监听
ss -tlnp | grep -E '8030|9020|9030|9010'   # FE
ss -tlnp | grep -E '8040|9060|9050|8060'  # BE

# 检查磁盘使用
df -h /data01 /data22

# 检查 Tablet 分布
mysql -h sr-fe-1 -P9030 -uroot -p -e "SHOW PROC '/backends'\G"
```

---

### 10. 附录

#### 10.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `fe.conf` | `/appdata/starrocks/fe/conf/fe.conf` | FE 主配置(端口 / 元数据 / JVM) |
| `be.conf` | `/appdata/starrocks/be/conf/be.conf` | BE 主配置(端口 / JBOD 存储 / JVM) |
| `fe.log` | `/appdata/starrocks/fe/log/fe.log` | FE 主日志 |
| `fe.warn.log` | `/appdata/starrocks/fe/log/fe.warn.log` | FE 警告日志 |
| `fe.audit.log` | `/appdata/starrocks/fe/log/fe.audit.log` | FE 审计日志 |
| `be.INFO` | `/appdata/starrocks/be/log/be.INFO` | BE 主日志 |
| `be.WARNING` | `/appdata/starrocks/be/log/be.WARNING` | BE 警告日志 |
| `starrocks.conf` | `/etc/nginx/conf.d/starrocks.conf` | Nginx 反向代理(HTTP) |
| `starrocks.conf` | `/etc/nginx/stream.d/starrocks.conf` | Nginx TCP 代理(MySQL) |
| `starrocks.conf` | `/etc/security/limits.d/starrocks.conf` | 文件描述符限制 |
| `/data01~22/starrocks/` | BE 节点各盘 | BE 数据存储(JBOD 22 块盘) |

#### 10.2 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| FE 启动失败 | `cat fe/log/fe.warn.log`;检查端口占用 | 清理 `meta/` 后重新初始化 / 释放端口 |
| BE 启动失败 | `cat be/log/be.WARNING`;检查 `storage_root_path` 路径是否存在 | 创建目录 / 修正配置 / 清理 `storage/` |
| BE 不加入集群 | `SHOW PROC '/backends'`;检查 `Alive` 和 `ErrMsg` | 检查网络 / NTP 时钟同步 / 端口放行 |
| Tablet 不可用 | `SHOW TABLET FROM <db>.<table>`;检查 `State` | `ADMIN REPAIR TABLE <db>.<table>` 触发修复 |
| 查询超时 | 检查 `fe.audit.log`;确认 BE 负载 / 数据量 | 调优 SQL / 扩容 BE / 分区裁剪 |
| 磁盘满 | `SHOW PROC '/backends'`;检查 `UsedPct` | 扩容 / 清理过期分区 / DECOMMISSION |
| FE 脑裂 | `SHOW PROC '/frontends'`;多个 LEADER | 检查 NTP / 网络,停止异常 FE 重新加入 |
| BE OOM | `dmesg \| grep -i oom`;检查 `mem_limit` | 调小 `mem_limit` / 扩容内存 |
| 副本不足 | `SHOW TABLET FROM <db>.<table>`;`ReplicaCount` < 3 | 检查 BE 存活 / `ADMIN REPAIR TABLE` |

#### 10.3 JVM 调优

**FE JVM**(fe.conf):

```bash
JAVA_OPTS = "-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -Xlog:gc*:file=/appdata/starrocks/fe/log/fe.gc.log:time,uptime:filecount=10,filesize=100m"
```

| 参数 | 值 | 说明 |
|---|---|---|
| `-Xmx8192m` | 8G | FE 元数据堆内存(表多时调大) |
| `-XX:+UseG1GC` | G1 GC | 低延迟 |
| `-XX:MaxGCPauseMillis=200` | 200ms | GC 暂停目标 |

**BE JVM**(be.conf,主要用于 Java UDF):

```bash
JAVA_OPTS = "-Xmx8192m -XX:+UseG1GC"
```

> **说明**:BE 主体为 C++ 进程,JVM 仅用于 Java UDF / 外部 Catalog 等功能。BE 内存管理通过 `mem_limit` 配置,不依赖 JVM 堆。

#### 10.4 BE 存储路径速查

22 块 JBOD 盘的 `storage_root_path` 配置(分号分隔):

```bash
# be.conf 中的 storage_root_path(单行):
storage_root_path = /data01/starrocks;/data02/starrocks;/data03/starrocks;/data04/starrocks;/data05/starrocks;/data06/starrocks;/data07/starrocks;/data08/starrocks;/data09/starrocks;/data10/starrocks;/data11/starrocks;/data12/starrocks;/data13/starrocks;/data14/starrocks;/data15/starrocks;/data16/starrocks;/data17/starrocks;/data18/starrocks;/data19/starrocks;/data20/starrocks;/data21/starrocks;/data22/starrocks
```

如需指定 SSD 介质(前 2 块为 SSD,后 20 块为 HDD):

```bash
storage_root_path = /data01/starrocks,medium:ssd;/data02/starrocks,medium:ssd;/data03/starrocks,medium:hdd;/data04/starrocks,medium:hdd;/data05/starrocks,medium:hdd;/data06/starrocks,medium:hdd;/data07/starrocks,medium:hdd;/data08/starrocks,medium:hdd;/data09/starrocks,medium:hdd;/data10/starrocks,medium:hdd;/data11/starrocks,medium:hdd;/data12/starrocks,medium:hdd;/data13/starrocks,medium:hdd;/data14/starrocks,medium:hdd;/data15/starrocks,medium:hdd;/data16/starrocks,medium:hdd;/data17/starrocks,medium:hdd;/data18/starrocks,medium:hdd;/data19/starrocks,medium:hdd;/data20/starrocks,medium:hdd;/data21/starrocks,medium:hdd;/data22/starrocks,medium:hdd
```

#### 10.5 v3.4.10 主要变更

| 类别 | 说明 |
|---|---|
| Security | 修复 LZ4-java CVE 漏洞 (CVE-2025-12183, CVE-2025-66566) |
| Bug Fix | 多语句提交时 Profile 中 SQL 信息记录错误 |
| Bug Fix | Java UDF/UDAF 在全 NULL 列时 OOM |
| Bug Fix | 排名窗口函数(无 PARTITION BY)执行计划异常致 BE 崩溃 |
| Bug Fix | Object/JSON 列操作后悬垂指针致段错误 |
| Bug Fix | trim() 特殊 Unicode 空格致越界 |
| Bug Fix | BE 心跳在崩溃时短暂返回成功致 FE 误判 |
| Bug Fix | ExecutionGroup + JOIN + 窗口函数数据乱序 |
| Bug Fix | CASE-WHEN 深嵌套致 FE OOM |
| 说明 | v3.4.10 推荐作为升级到 v3.5 的前置版本 |

> **升级 v3.5 注意**:从 v3.4 升级 v3.5 需将 JDK 升级到 17+,并移除 `JAVA_OPTS` 中 CMS/CMS 相关参数。建议先升级到 3.4.10 再升级 v3.5。



---

## 4. Kafbat UI 部署 {#4-kafbat-ui-部署}
> 本文档覆盖 prom-gw 配套 Kafka 集群的 Web 监控管理界面 **Kafbat UI v1.5.0** 的生产环境完整部署,包括 JAR 部署、systemd 服务管理、集群配置、Nginx 反向代理、认证授权、监控和运维操作。
>
> Kafbat UI 是 [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) 的开源 Web UI,用于监控和管理 Apache Kafka 集群,支持 Topic 管理、消息浏览、Consumer Group 监控、Schema Registry、多集群管理等功能。
>
> 配套文档:**Kafka 生产部署**(见 §2)、**生产部署指南**(见 §1)、**高可用与负载均衡**(见 §7)、**故障剧本**(见 §11)


---

### 1. 部署架构

#### 1.1 标准拓扑

Kafbat UI 部署在独立运维节点(或与 Prometheus 共置),通过 PLAINTEXT 协议连接 Kafka 集群,通过 Prometheus 协议拉取 JMX Exporter 指标:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  ┌─────────────────┐         ┌─────────────────────┐    │
│  │  Kafbat UI      │         │  Nginx + Keepalived │    │
│  │  10.0.10.30:8080│ ←────── │  VIP: 10.0.10.100   │    │
│  │  (JAR/systemd) │  反向代理 │  443 → 8080        │    │
│  └────┬──────┬─────┘         └─────────────────────┘    │
│       │      │                                          │
│       │      │ PLAINTEXT                Prometheus       │
│       ▼      ▼                          ▼                │
│  ┌────────────────────┐         ┌──────────────────┐    │
│  │ Kafka Cluster      │         │ JMX Exporter     │    │
│  │ kafka-1:9092       │         │ kafka-1:9404     │    │
│  │ kafka-2:9092       │         │ kafka-2:9404     │    │
│  │ kafka-3:9092       │         │ kafka-3:9404     │    │
│  └────────────────────┘         └──────────────────┘    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

> **安全说明**:Kafbat UI 与 Kafka 集群在同一 VPC 内,通过 PLAINTEXT 协议(无 SSL/SASL)通信。对外暴露的 Web 界面通过 Nginx 反向代理 + Basic Auth / OAuth2 认证保护,详见 [§5](#5-nginx-反向代理) 和 [§6](#6-认证与-rbac)。

#### 1.2 端口规划

| 端口 | 协议 | 用途 | 暴露范围 |
|---|---|---|---|
| 8080 | HTTP | Kafbat UI Web 界面(本地监听) | 仅本机 / Nginx |
| 443 | HTTPS | Nginx 对外暴露(VIP) | 运维网段 |
| 9092 | PLAINTEXT | 连接 Kafka Broker(客户端通信) | Kafbat UI 节点 → Kafka 网段 |
| 9404 | HTTP | 拉取 Kafka JMX Exporter 指标 | Kafbat UI 节点 → Kafka 网段 |

#### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 | 说明 |
|---|---|---|---|---|
| Kafbat UI | 4C/8G | 1-2 | 50G SSD | JAR + systemd 部署,可挂载到 Nginx VIP 后做 HA |
| Nginx + Keepalived | 2C/4G | 2 | 50G SSD | 复用现有 **HA 部署**(见 §7) |

> **说明**:Kafbat UI 为无状态应用(配置通过 YAML 文件持久化),可通过 Nginx VIP 后部署多实例实现 HA。单实例也可满足 prom-gw 集群的监控需求。

---

### 2. 前置准备

#### 2.1 操作系统

```bash
# CentOS / RHEL 8+
cat /etc/redhat-release

# Ubuntu / Debian 22+
cat /etc/os-release
```

#### 2.2 OpenJDK 25 安装

Kafbat UI 基于 Spring Boot 3,需要 JDK 17+,统一使用 OpenJDK 25(与 Kafka 部署保持一致):

```bash
# CentOS / RHEL
sudo yum install -y java-25-openjdk java-25-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-25-jdk

java -version   # 期望: openjdk version "25.x.x"
```

#### 2.3 创建用户与目录

```bash
# 创建专用用户
sudo useradd -r -m -d /appdata/kafka-ui -s /sbin/nologin kafbat-ui

# 部署目录(JAR 包 + 配置文件)
sudo mkdir -p /appdata/kafka-ui/config
# 日志目录
sudo mkdir -p /applog/kafka-ui

sudo chown -R kafbat-ui:kafbat-ui /appdata/kafka-ui /applog/kafka-ui
```

#### 2.4 下载 JAR 包

```bash
# 下载 v1.5.0 版本(固定版本)
cd /appdata/kafka-ui
sudo wget -O api-v1.5.0.jar \
  https://github.com/kafbat/kafka-ui/releases/download/v1.5.0/api-v1.5.0.jar

# 校验 SHA256
echo "8bebff7b21ddb084b5b647e271136f7d97f46da6c7bc70f9cd47775dfbd3c10e  api-v1.5.0.jar" | sha256sum -c -
# 期望: api-v1.5.0.jar: OK

# 创建版本软链(方便升级)
sudo ln -s api-v1.5.0.jar kafka-ui.jar
sudo chown -R kafbat-ui:kafbat-ui /appdata/kafka-ui

# 验证
ls -lh /appdata/kafka-ui/kafka-ui.jar
# 期望: lrwxrwxrwx ... kafka-ui.jar -> api-v1.5.0.jar
```

> **版本固定**:生产环境必须使用固定版本 `1.5.0`,禁止直接使用 main 分支构建,避免引入不兼容变更。升级时替换 JAR 并更新软链即可。

---

### 3. JAR 部署与 systemd 服务

#### 3.1 application.yml 主配置

**`/appdata/kafka-ui/config/application.yml`**:

```yaml
# ======================================================
# Kafbat UI 应用配置
# ======================================================
kafka:
  clusters:
    - name: prom-gw-sz               # 深圳集群
      bootstrapServers: kafka-1:9092,kafka-2:9092,kafka-3:9092
      # PLAINTEXT 协议,不设置 securityProtocol / ssl
      readOnly: false                # 生产建议 false(需要 Topic 管理操作时)
      metrics:
        type: PROMETHEUS             # 使用 Prometheus JMX Exporter
        port: 9404                   # Kafka JMX Exporter 端口(见 Kafka 部署 §5.1)
      # 消息浏览限制
      polling:
        throttleRate: 0              # 0 = 不限速(内网环境)

    # 多集群示例(如需监控北京/合肥集群)
    # - name: prom-gw-bj
    #   bootstrapServers: kafka-bj-1:9092,kafka-bj-2:9092,kafka-bj-3:9092
    #   metrics:
    #     type: PROMETHEUS
    #     port: 9404

# ======================================================
# 通用配置
# ======================================================
server:
  port: 8080

logging:
  level:
    root: info
    io.kafbat.ui: info
  file:
    name: /applog/kafka-ui/kafbat-ui.log

# 动态配置(允许 Web 界面修改运行时配置)
dynamic_config:
  enabled: true

# 内部 Topic 前缀
kafka_internalTopicPrefix: "_"

# 会话超时
server:
  reactive:
    session:
      timeout: 30m

# 关闭 GitHub 版本检查(内网无法访问)
github_release_info:
  enabled: false

# 生产关闭 Swagger UI(调试时可开)
swagger_ui:
  enabled: false

# Actuator 端点暴露
management:
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

> **动态配置文件**:首次部署创建空文件,Web 界面修改的配置会写入此文件:
> ```bash
> sudo touch /appdata/kafka-ui/config/dynamic_config.yaml
> sudo chown kafbat-ui:kafbat-ui /appdata/kafka-ui/config/dynamic_config.yaml
> ```

#### 3.2 systemd 服务

**`/etc/systemd/system/kafbat-ui.service`**:

```ini
[Unit]
Description=Kafbat UI v1.5.0 (JAR)
After=network.target
Wants=network.target

[Service]
Type=simple
User=kafbat-ui
Group=kafbat-ui

# JVM 参数
Environment="JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+AlwaysPreTouch -Djava.awt.headless=true"

# 启动命令:指定配置文件目录 + 动态配置
ExecStart=/usr/bin/java $JAVA_OPTS \
  -Dspring.config.additional-location=file:/appdata/kafka-ui/config/ \
  -Dspring.config.name=application \
  -jar /appdata/kafka-ui/kafka-ui.jar

# 优雅停机:SIGTERM 触发 Spring Boot 优雅关闭
ExecStop=/bin/kill -TERM $MAINPID

# 重启策略
Restart=always
RestartSec=10

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096

# 工作目录
WorkingDirectory=/appdata/kafka-ui

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/appdata/kafka-ui /applog/kafka-ui /tmp

# 超时
TimeoutStartSec=120
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

> **配置加载说明**:`-Dspring.config.additional-location` 指向自定义配置目录,Kafbat UI 会优先读取 `/appdata/kafka-ui/config/application.yml`(覆盖 JAR 包内默认配置)。动态配置 `dynamic_config.yaml` 也在此目录下自动加载。

#### 3.3 启动与验证

```bash
# 1. 加载 systemd 配置
sudo systemctl daemon-reload
sudo systemctl enable kafbat-ui

# 2. 启动
sudo systemctl start kafbat-ui

# 3. 查看状态
sudo systemctl status kafbat-ui
# 期望: active (running)

# 4. 查看启动日志
sudo journalctl -u kafbat-ui -f --no-pager
# 或查看应用日志
tail -f /applog/kafka-ui/kafbat-ui.log

# 5. 验证健康检查
curl -s http://localhost:8080/actuator/health
# 期望: {"status":"UP"}

# 6. 验证 Web 界面(本地访问)
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/
# 期望: 200 或 401(启用 Basic Auth 时)
```

> **首次启动可能较慢**:Kafbat UI 首次启动需初始化 Kafka Admin Client 连接、加载集群元数据,通常 30-60 秒内就绪。systemd 的 `TimeoutStartSec=120` 已预留足够时间。

---

### 4. 配置文件详解

#### 4.1 关键配置项说明

| 配置项(YAML 路径) | 环境变量 | 推荐值 | 说明 |
|---|---|---|---|
| `kafka.clusters[0].name` | `KAFKA_CLUSTERS_0_NAME` | `prom-gw-sz` | 唯一标识,Web 界面显示 |
| `kafka.clusters[0].bootstrapServers` | `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` | `kafka-1:9092,...` | Kafka Broker 地址列表 |
| 安全协议 | `KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL` | (不设) | PLAINTEXT 不设置,SSL 时设为 `SSL` |
| `kafka.clusters[0].metrics.type` | `KAFKA_CLUSTERS_0_METRICS_TYPE` | `PROMETHEUS` | 使用 Prometheus JMX Exporter(已部署) |
| `kafka.clusters[0].metrics.port` | `KAFKA_CLUSTERS_0_METRICS_PORT` | `9404` | 与 Kafka 部署 §5.1 一致 |
| `kafka.clusters[0].readOnly` | `KAFKA_CLUSTERS_0_READONLY` | `false` | `true` 时禁止 Topic 增删改 |
| `dynamic_config.enabled` | `DYNAMIC_CONFIG_ENABLED` | `true` | 允许 Web 界面修改配置 |
| `swagger_ui.enabled` | `SWAGGER_UI_ENABLED` | `false` | 生产关闭,调试时开 |
| `github_release_info.enabled` | `GITHUB_RELEASE_INFO_ENABLED` | `false` | 内网关闭 |
| `server.port` | `SERVER_PORT` | `8080` | Web 界面端口 |
| `server.servlet.context-path` | `SERVER_SERVLET_CONTEXT_PATH` | (不设) | 如需 `/kafka-ui` 前缀路径 |
| 管理超时 | `KAFKA_ADMIN-CLIENT-TIMEOUT` | `30000` | Kafka Admin API 超时(ms) |
| `kafka.clusters[0].polling.throttleRate` | `KAFKA_CLUSTERS_0_POLLING_THROTTLE_RATE` | `0` | 消息浏览限速(bytes/sec),0=不限 |
| `logging.level.root` | `LOGGING_LEVEL_ROOT` | `info` | 日志级别(trace/debug/info/warn/error) |

> **配置优先级**:命令行参数 > 环境变量 > `application.yml`(additional-location)> JAR 包内默认配置。推荐使用 YAML 文件管理,环境变量用于临时覆盖。

#### 4.2 多集群配置

prom-gw 多城部署(深圳/北京/合肥)场景,可在一个 Kafbat UI 实例中管理所有集群:

```yaml
# /appdata/kafka-ui/config/application.yml
kafka:
  clusters:
    - name: prom-gw-sz
      bootstrapServers: kafka-sz-1:9092,kafka-sz-2:9092,kafka-sz-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404

    - name: prom-gw-bj
      bootstrapServers: kafka-bj-1:9092,kafka-bj-2:9092,kafka-bj-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404

    - name: prom-gw-hf
      bootstrapServers: kafka-hf-1:9092,kafka-hf-2:9092,kafka-hf-3:9092
      metrics:
        type: PROMETHEUS
        port: 9404
```

对应环境变量方式(索引从 0 开始):

```bash
# /etc/systemd/system/kafbat-ui.service 的 [Service] 段
Environment="KAFKA_CLUSTERS_0_NAME=prom-gw-sz"
Environment="KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=kafka-sz-1:9092,..."
Environment="KAFKA_CLUSTERS_0_METRICS_TYPE=PROMETHEUS"
Environment="KAFKA_CLUSTERS_0_METRICS_PORT=9404"

Environment="KAFKA_CLUSTERS_1_NAME=prom-gw-bj"
Environment="KAFKA_CLUSTERS_1_BOOTSTRAPSERVERS=kafka-bj-1:9092,..."
...
```

---

### 5. Nginx 反向代理

#### 5.1 Nginx 配置

复用现有 **HA 与负载均衡部署**(见 §7) 的 Nginx,增加 Kafbat UI 的反向代理:

**`/etc/nginx/conf.d/kafka-ui.conf`**:

```nginx
upstream kafbat_ui {
    server 10.0.10.30:8080;
    # 如有多实例:
    # server 10.0.10.31:8080;
}

server {
    listen 443 ssl http2;
    server_name kafka-ui.prom-gw.internal;

    # SSL 证书(复用现有证书)
    ssl_certificate     /etc/nginx/ssl/prom-gw.crt;
    ssl_certificate_key /etc/nginx/ssl/prom-gw.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # 反向代理到 Kafbat UI
    location / {
        proxy_pass http://kafbat_ui;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket 支持(实时消息浏览需要)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";

        # 超时设置(消息浏览可能耗时较长)
        proxy_read_timeout 300s;
        proxy_send_timeout 300s;
    }

    # 健康检查端点(不走认证,供 Prometheus / Keepalived 探测)
    location /actuator/health {
        proxy_pass http://kafbat_ui;
        access_log off;
    }

    # 访问控制(仅运维网段)
    allow 10.0.10.0/24;       # 运维网段
    allow 10.0.1.0/24;        # Kafka 运维网段
    deny all;
}
```

```bash
# 测试配置
sudo nginx -t
# 重载
sudo nginx -s reload
```

#### 5.2 Keepalived VIP

若复用现有 Keepalived VIP(见 **HA 部署**(见 §7)),VIP 已配置好 SSL 和浮动 IP,Kafbat UI 只需作为 Nginx upstream 接入即可。

---

### 6. 认证与 RBAC

#### 6.1 Basic Auth(推荐内网使用)

Kafbat UI 支持内置 Basic Auth,适合内网运维场景。在 `application.yml` 中启用:

**`/appdata/kafka-ui/config/application.yml` 追加**:

```yaml
# ====== Basic Auth 认证 ======
auth:
  type: BASIC

# Spring Security 配置
spring:
  security:
    user:
      name: admin
      password: "{bcrypt}$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68deJU3uOAvXm"  # bcrypt 加密
```

> **多用户 RBAC**:Kafbat UI 的完整 RBAC 需要 OAuth2 / LDAP 后端支持。Basic Auth 模式下使用单一管理员账号,适合小型运维团队。如需多角色管理,见 [§6.3](#63-oauth2-认证可选)。

**密码生成**(使用 Spring Boot 内置 BCrypt):

```bash
# 使用 htpasswd 生成 bcrypt 密码
htpasswd -nbB admin "YourPassword123"
# 输出: admin:$2y$10$xxxxxxx...
# 将 $2y$ 替换为 {bcrypt}$2a$ 后填入配置

# 或使用 Java 生成(利用已安装的 JDK)
java -cp /appdata/kafka-ui/kafka-ui.jar \
  -Dloader.main=org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder \
  org.springframework.boot.loader.PropertiesApplication \
  "YourPassword123"
```

#### 6.2 RBAC 角色说明

| 角色 | 权限 | 适用场景 |
|---|---|---|
| `VIEWER` | 只读:查看 Topic / Consumer Group / 消息浏览 | 运维查看 |
| `DEVELOPER` | Topic 管理:创建 / 删除 / 修改配置 / 生产消息 | 开发调试 |
| `ADMIN` | 全部权限 + 集群配置管理 | 集群管理员 |

#### 6.3 OAuth2 认证(可选)

如需对接企业 GitLab / Google / Keycloak OAuth2 实现多用户 RBAC:

**`/appdata/kafka-ui/config/application.yml` 追加**:

```yaml
auth:
  type: OAUTH2

spring:
  security:
    oauth2:
      client:
        registration:
          keycloak:
            client-id: "kafbat-ui"
            client-secret: "xxx"
            scope: openid,profile,email
        provider:
          keycloak:
            issuer-uri: "https://keycloak.internal/realms/prom-gw"
```

> **详细 OAuth2 / LDAP 配置**见 [Kafbat UI 官方文档](https://ui.docs.kafbat.io/configuration/rbac-role-based-access-control)。

---

### 7. 监控集成

#### 7.1 Kafbat UI 自身监控

Kafbat UI 暴露 Spring Boot Actuator 健康检查和 Prometheus 指标端点(已在 `application.yml` 的 `management` 段开启):

```bash
# 健康检查
curl -s http://localhost:8080/actuator/health
# {"status":"UP"}

# 应用信息
curl -s http://localhost:8080/actuator/info

# Prometheus 指标
curl -s http://localhost:8080/actuator/prometheus | head -20
```

**Prometheus 抓取配置**(`prometheus.yml` 追加):

```yaml
scrape_configs:
  - job_name: kafbat-ui
    static_configs:
      - targets: ['10.0.10.30:8080']
        # 如有多实例:
        # - '10.0.10.31:8080'
    metrics_path: /actuator/prometheus
    scrape_interval: 30s
```

#### 7.2 Kafka 集群监控(已集成)

Kafbat UI 通过 Prometheus JMX Exporter(端口 9404)直接在 Web 界面展示 Kafka 指标,无需额外配置。Web 界面可查看:

| 功能 | 对应 Kafka 指标 |
|---|---|
| Broker Overview | 在线 Broker 列表、controller 状态 |
| Topic 详情 | partition 数、副本分布、ISR 状态 |
| Consumer Group | 各 partition 的 offset、lag |
| 消息浏览 | 实时查看 / 搜索 / 过滤 Topic 消息 |
| 指标图表 | BytesIn/Out、MessagesIn、RequestLatency 等 |

#### 7.3 告警规则补充

Kafbat UI 自身告警规则(`prometheus-rules.yml` 追加):

```yaml
groups:
  - name: kafbat-ui
    rules:
      - alert: KafbatUIDown
        expr: up{job="kafbat-ui"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafbat UI is down on {{ $labels.instance }}"

      - alert: KafbatUIHighMemory
        expr: jvm_memory_used_bytes{job="kafbat-ui", area="heap"} > 2*1024*1024*1024
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafbat UI JVM heap > 2GB on {{ $labels.instance }}"
```

---

### 8. 运维操作

#### 8.1 启停与重启

```bash
# 启动
sudo systemctl start kafbat-ui

# 停止(优雅停机,SIGTERM 触发 Spring Boot 优雅关闭)
sudo systemctl stop kafbat-ui

# 重启
sudo systemctl restart kafbat-ui

# 查看状态
sudo systemctl status kafbat-ui

# 查看实时日志(systemd journal)
sudo journalctl -u kafbat-ui -f --no-pager

# 查看应用日志
tail -f /applog/kafka-ui/kafbat-ui.log

# 查看最近 100 行日志
sudo journalctl -u kafbat-ui --no-pager -n 100
```

#### 8.2 配置变更

```bash
# 1. 修改 application.yml
sudo vi /appdata/kafka-ui/config/application.yml

# 2. 重启生效
sudo systemctl restart kafbat-ui

# 3. 验证
sudo systemctl status kafbat-ui
curl -s http://localhost:8080/actuator/health
```

#### 8.3 版本升级

```bash
cd /appdata/kafka-ui

# 1. 备份配置
sudo cp config/application.yml config/application.yml.bak.$(date +%Y%m%d)

# 2. 下载新版本 JAR(如升级到 1.6.0)
sudo wget -O api-1.6.0.jar \
  https://github.com/kafbat/kafka-ui/releases/download/v1.6.0/api-v1.6.0.jar

# 3. 校验 SHA256(从 Release 页面获取)
echo "<新版本sha256>  api-1.6.0.jar" | sha256sum -c -

# 4. 更新软链
sudo chown kafbat-ui:kafbat-ui api-1.6.0.jar
sudo ln -sfn api-1.6.0.jar kafka-ui.jar

# 5. 重启
sudo systemctl restart kafbat-ui

# 6. 验证
sudo systemctl status kafbat-ui
curl -s http://localhost:8080/actuator/health
curl -s http://localhost:8080/actuator/info
```

> **升级前必读**:升级前查阅 [Release Notes](https://github.com/kafbat/kafka-ui/releases),确认 Breaking Changes。建议在测试环境先验证。旧版本 JAR 可保留以便回滚:更新软链指回旧版本即可。

#### 8.4 备份与恢复

```bash
# 备份配置
sudo tar -czf kafka-ui-config-backup-$(date +%Y%m%d).tar.gz \
    -C /appdata/kafka-ui config/ \
    -C /etc/systemd/system kafbat-ui.service

# 恢复配置
sudo tar -xzf kafka-ui-config-backup-20260820.tar.gz -C /
sudo systemctl daemon-reload
sudo systemctl restart kafbat-ui
```

#### 8.5 常见排查操作

```bash
# 查看进程信息
ps -ef | grep kafka-ui.jar

# 查看 JVM 堆信息(排查 OOM)
# 获取 PID
PID=$(pgrep -f kafka-ui.jar)
sudo -u kafbat-ui jcmd $PID GC.heap_info

# 查看 JVM 线程栈(排查卡死)
sudo -u kafbat-ui jstack $PID | head -100

# 查看端口监听
ss -tlnp | grep 8080

# 查看磁盘使用
du -sh /appdata/kafka-ui /applog/kafka-ui

# 清理旧版本 JAR(保留当前版本)
cd /appdata/kafka-ui
ls -lh api-*.jar
# 确认 kafka-ui.jar 软链指向后,删除旧版本
# sudo rm api-1.4.0.jar
```

---

### 9. 附录

#### 9.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `kafka-ui.jar` → `api-v1.5.0.jar` | `/appdata/kafka-ui/` | Kafbat UI JAR 包(软链) |
| `application.yml` | `/appdata/kafka-ui/config/application.yml` | 主配置(集群 / 通用 / Actuator) |
| `dynamic_config.yaml` | `/appdata/kafka-ui/config/dynamic_config.yaml` | 运行时动态配置(Web 修改) |
| `kafbat-ui.service` | `/etc/systemd/system/kafbat-ui.service` | systemd 服务 |
| `kafka-ui.conf` | `/etc/nginx/conf.d/kafka-ui.conf` | Nginx 反向代理 |
| `kafbat-ui.log` | `/applog/kafka-ui/kafbat-ui.log` | 应用日志 |

#### 9.2 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| 服务无法启动 | `journalctl -u kafbat-ui` 查看启动日志;检查 JDK 是否安装 | 安装 OpenJDK 25 / 修正配置 |
| Web 界面无法访问 | `systemctl status kafbat-ui`;`curl localhost:8080` 测试 | 重启服务 / 检查端口监听 |
| 连接 Kafka 超时 | 检查 `bootstrapServers` 是否可达;检查安全组 9092 | 修正配置 / 开放安全组 |
| Broker 指标不显示 | 检查 `metrics.type=PROMETHEUS` 和 `port=9404`;`curl kafka-1:9404/metrics` | 确认 JMX Exporter 已部署(见 **Kafka 部署 §5.1**(见 §2)) |
| 消息浏览卡住 | 检查 Kafka Broker 状态;检查 `polling.throttleRate` | 调整限速 / 检查 Broker |
| OOM(进程被 kill) | `dmesg \| grep -i oom`;`jcmd PID GC.heap_info` | 调大 `-Xmx` / 检查内存泄漏 |
| Basic Auth 登录失败 | 检查 `application.yml` 密码格式(bcrypt);检查 `auth.type=BASIC` | 重新生成密码 |
| 动态配置丢失 | 检查 `dynamic_config.yaml` 文件权限 | 确认 kafbat-ui 用户有写权限 |
| Nginx 502 | 检查 Kafbat UI 服务是否运行;检查 upstream 地址 | 重启服务 / 修正 Nginx upstream |
| 端口被占用 | `ss -tlnp \| grep 8080` | 修改 `server.port` 或释放端口 |

#### 9.3 JVM 调优

systemd 服务中 `JAVA_OPTS` 环境变量控制 JVM 参数:

```ini
# /etc/systemd/system/kafbat-ui.service
Environment="JAVA_OPTS=-Xms2g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

| 参数 | 值 | 说明 |
|---|---|---|
| `-Xms2g -Xmx2g` | 2G 堆 | 单集群监控足够,多集群建议 4G |
| `-XX:+UseG1GC` | G1 GC | 低延迟,适合中等堆 |
| `-XX:MaxGCPauseMillis=100` | 100ms | GC 暂停目标 |
| `-XX:+AlwaysPreTouch` | - | 启动时预触达堆内存,减少运行时页错误 |
| `-Djava.awt.headless=true` | - | 无头模式,服务器环境必需 |

> **GC 日志**:如需排查 GC 问题,追加 `-Xlog:gc*:file=/applog/kafka-ui/gc.log:time,uptime:filecount=10,filesize=100m`。

#### 9.4 日志轮转

Kafbat UI 日志通过 Logback 配置,已在 `application.yml` 中指定日志文件路径。如需日志轮转,在 `application.yml` 追加 Logback 配置,或使用 logrotate:

**`/etc/logrotate.d/kafbat-ui`**:

```
/applog/kafka-ui/*.log {
    daily
    rotate 10
    size 100M
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
```

```bash
sudo logrotate -d /etc/logrotate.d/kafbat-ui   # 测试(dry-run)
```

#### 9.5 v1.5.0 主要特性

| 特性 | 说明 |
|---|---|
| MessagePack SerDe | 支持 MessagePack 序列化格式 |
| Swagger UI | 内置 API 文档(通过 `swagger_ui.enabled` 开启) |
| Consumer Lag 实时更新 | Consumer Group lag 实时刷新 |
| CSV 导出 | 表格数据导出为 CSV |
| Connector Consumer Group 集成 | Kafka Connect connector 关联 Consumer Group |
| OAuth2 增强 | Schema Registry 和代理的 OAuth2 改进 |
| ACL 增强 | ACL 管理功能增强 |



---

## 5. Flink 生产部署 {#5-flink-生产部署}
> 本文档覆盖 prom-gw 配套 Flink 集群的生产环境完整部署,包括集群搭建、JM HA、TM 配置、作业提交管理、Checkpoint/Savepoint、监控告警和运维操作。
>
> Flink 作业的开发实现见 **Flink 消费 Kafka 写入 StarRocks 开发指南**(见 §6),本文档聚焦**集群部署与运维**。
>
> 配套文档:**Kafka 生产部署**(见 §2)、**生产部署指南**(见 §1)、**压力测试指南**(见 §8)、**故障剧本**(见 §11)


---

### 1. 部署架构

#### 1.1 单机房标准拓扑

每机房部署 JM×2(1 Active + 1 Standby)+ TM×2~6,Flink 作业消费本城 Kafka,跨城写北京 StarRocks:

```
机房 (深圳)
┌──────────────────────────────────────────────────────────┐
│                                                          │
│  ┌─────────────┐         ┌─────────────┐                │
│  │ JM-1 (Active)│ ← HA → │ JM-2 (Standby)│               │
│  │ 10.0.1.31    │  ZK    │ 10.0.1.32    │                │
│  └──────┬───────┘         └──────┬───────┘                │
│         │                        │                        │
│  ┌──────┴────────────────────────┴──────┐                │
│  │            ZK Quorum × 3             │                │
│  │      10.0.1.41 / 42 / 43             │                │
│  └──────────────────────────────────────┘                │
│         │                                                │
│  ┌──────┼──────────────────────────┐                    │
│  │     │                          │                    │
│  ▼     ▼                          ▼                    │
│ TM-1  TM-2        ...          TM-6                    │
│ 16C/32G            16C/32G            16C/32G           │
│ 4 slot             4 slot             4 slot             │
│                                                          │
│  消费 → Kafka (本城 9094)                                 │
│  写入 → StarRocks (北京 FE VIP:8030)    ──跨城专线──→    │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

#### 1.2 资源规划

| 角色 | 规格 | 数量 | 说明 |
|---|---|---|---|
| JobManager | 8C/16G/200G | 2 | 1 Active + 1 Standby |
| TaskManager | 16C/32G/500G SSD | 2-6 | 每 TM 4 slot,按 series 数扩展 |
| ZooKeeper | 4C/8G/100G | 3 | JM HA 依赖,可与 Kafka 共用 |

#### 1.3 端口规划

| 端口 | 组件 | 用途 | 暴露范围 |
|---|---|---|---|
| 8081 | JobManager Web UI | 作业管理界面 | 运维网段 |
| 6123 | JobManager RPC | TM ↔ JM 通信 | Flink 内部 |
| 6124 | JobManager Blob Server | JAR 分发 | Flink 内部 |
| 9999 | Metrics | Prometheus 抓取 | Prometheus 网段 |
| 2181 | ZooKeeper | JM HA 选主 | Flink 内部 |

---

### 2. 前置准备

#### 2.1 操作系统

```bash
# CentOS / RHEL 8+ 或 Ubuntu / Debian 22+
# 所有 JM / TM 节点执行
```

#### 2.2 JDK 17 安装

```bash
# CentOS / RHEL
sudo yum install -y java-17-openjdk java-17-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-17-jdk

java -version   # 期望: openjdk version "17.x.x"
```

#### 2.3 创建 Flink 用户与目录

```bash
sudo useradd -r -m -d /opt/flink -s /sbin/nologin flink
sudo mkdir -p /opt/flink /data/flink/checkpoints /data/flink/savepoints /data/flink/logs
sudo chown -R flink:flink /opt/flink /data/flink
```

#### 2.4 内核参数调优

**`/etc/sysctl.d/99-flink.conf`**:

```ini
# 内存
vm.swappiness=10                        # Flink 用堆外内存,适度使用 swap
vm.max_map_count=262144                 # RocksDB 需要
vm.overcommit_memory=1                  # 允许内存超分

# 网络
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
net.ipv4.ip_local_port_range=10000 65535

# 文件句柄
fs.file-max=1000000
```

```bash
sudo sysctl --system
```

**`/etc/security/limits.d/flink.conf`**:

```
flink  soft  nofile  100000
flink  hard  nofile  100000
flink  soft  nproc   100000
flink  hard  nproc   100000
```

#### 2.5 SSH 免密(JM 到 TM)

```bash
# JM 节点上生成密钥
ssh-keygen -t rsa -b 4096

# 分发到所有 TM 节点
for host in tm-1 tm-2 tm-3 tm-4 tm-5 tm-6 jm-2; do
    ssh-copy-id flink@${host}
done
```

#### 2.6 下载并安装 Flink

```bash
cd /opt
sudo wget https://archive.apache.org/dist/flink/flink-1.19.2/flink-1.19.2-bin-scala_2.12.tgz
sudo tar -xzf flink-1.19.2-bin-scala_2.12.tgz
sudo ln -s flink-1.19.2 flink
sudo chown -R flink:flink /opt/flink
ls /opt/flink/bin/flink   # 确认解压成功
```

#### 2.7 安装 Hadoop(可选,仅 Checkpoint/Savepoint 用 HDFS 时)

```bash
# 如果 Checkpoint 用本地文件系统(小规模)或 NFS,可跳过 Hadoop
# 如果用 HDFS(推荐,大规模生产):
# 安装 Hadoop 3.3+ 并配置 HDFS,见 Hadoop 官方文档
```

---

### 3. Flink 集群安装

#### 3.1 目录结构

```
/opt/flink/
├── conf/
│   ├── flink-conf.yaml          # 主配置
│   ├── masters                  # JM 节点列表
│   ├── workers                  # TM 节点列表
│   └── log4j2.xml              # 日志配置
├── bin/
│   ├── start-cluster.sh         # 启动集群
│   ├── stop-cluster.sh          # 停止集群
│   ├── flink                    # 作业提交 CLI
│   └── jobmanager.sh            # 单独启动 JM
├── lib/                         # 依赖 JAR
│   └── flink-dist-1.19.2.jar
├── plugins/                     # 插件(如 RocksDB)
└── log/                         # 日志输出
```

#### 3.2 配置 masters 和 workers

**`/opt/flink/conf/masters`**:

```
jm-1:8081
jm-2:8081
```

**`/opt/flink/conf/workers`**:

```
tm-1
tm-2
tm-3
tm-4
tm-5
tm-6
```

#### 3.3 分发到所有节点

```bash
# 从 JM-1 分发到所有节点
for host in jm-2 tm-1 tm-2 tm-3 tm-4 tm-5 tm-6; do
    echo "同步到 ${host}..."
    rsync -avz /opt/flink/ flink@${host}:/opt/flink/
done
```

---

### 4. JM HA 配置

#### 4.1 ZooKeeper 部署

JM HA 依赖 ZooKeeper 进行主备选举。在 3 台节点上部署 ZK:

```bash
# 下载 ZooKeeper 3.8+
cd /opt
sudo wget https://archive.apache.org/dist/zookeeper/zookeeper-3.8.3/apache-zookeeper-3.8.3-bin.tar.gz
sudo tar -xzf apache-zookeeper-3.8.3-bin.tar.gz
sudo ln -s apache-zookeeper-3.8.3 zookeeper
sudo chown -R flink:flink /opt/zookeeper

# 配置 /opt/zookeeper/conf/zoo.cfg
cat > /opt/zookeeper/conf/zoo.cfg << 'EOF'
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data/zookeeper
clientPort=2181
server.1=zk-1:2888:3888
server.2=zk-2:2888:3888
server.3=zk-3:2888:3888
EOF

# 创建 myid 文件(每台不同)
echo 1 > /data/zookeeper/myid   # zk-1
echo 2 > /data/zookeeper/myid   # zk-2
echo 3 > /data/zookeeper/myid   # zk-3

# 启动
/opt/zookeeper/bin/zkServer.sh start
/opt/zookeeper/bin/zkServer.sh status   # 查看角色(leader/follower)
```

#### 4.2 Flink HA 配置

在 `flink-conf.yaml` 中配置 HA(见 §5.1 完整配置):

```yaml
# ====== JM HA ======
high-availability: org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices
high-availability.zookeeper.quorum: zk-1:2181,zk-2:2181,zk-3:2181
high-availability.zookeeper.path.root: /flink
high-availability.storageDir: hdfs:///flink/ha/
high-availability.cluster-id: /cluster-sz

# JM 故障后自动恢复
jobmanager.execution.failover-strategy: region
```

#### 4.3 HA 验证

```bash
# 1. 启动集群
/opt/flink/bin/start-cluster.sh

# 2. 查看 JM 状态(两个 JM,一个 Active 一个 Standby)
curl -s http://jm-1:8081/overview | python3 -m json.tool | grep runningJobs
curl -s http://jm-2:8081/overview | python3 -m json.tool | grep runningJobs

# 3. 模拟 JM 故障
ssh jm-1 "kill -9 \$(pidof StandaloneSessionClusterEntrypoint)"
sleep 5

# 4. 确认 Standby JM 接管
curl -s http://jm-2:8081/overview | python3 -m json.tool | grep runningJobs
# 期望: jm-2 变为 Active,作业继续运行

# 5. 恢复 jm-1(成为新 Standby)
ssh jm-1 "/opt/flink/bin/jobmanager.sh start"
```

---

### 5. 集群配置详解

#### 5.1 flink-conf.yaml

**`/opt/flink/conf/flink-conf.yaml`**:

```yaml
# ====== 基础 ======
jobmanager.rpc.address: jm-1              # JM RPC 地址(集群模式用 masters 文件)
jobmanager.rpc.port: 6123
jobmanager.bind-host: 0.0.0.0
jobmanager.web.address: 0.0.0.0
jobmanager.web.port: 8081

# ====== JM 内存 ======
jobmanager.memory.process.size: 4096m
jobmanager.memory.flink.size: 3072m
jobmanager.memory.heap.size: 2048m
jobmanager.memory.off-heap.size: 1024m
jobmanager.memory.jvm-metaspace.size: 256m
jobmanager.memory.jvm-overhead.size: 512m

# ====== TM 内存 ======
taskmanager.memory.process.size: 32768m
taskmanager.memory.flink.size: 24576m
taskmanager.memory.framework.heap.size: 1024m
taskmanager.memory.framework.off-heap.size: 512m
taskmanager.memory.task.heap.size: 16384m
taskmanager.memory.managed.size: 6144m
taskmanager.memory.network.min: 1024m
taskmanager.memory.network.max: 2048m
taskmanager.memory.network.fraction: 0.1
taskmanager.memory.jvm-metaspace.size: 512m
taskmanager.memory.jvm-overhead.min: 384m
taskmanager.memory.jvm-overhead.max: 1024m
taskmanager.memory.jvm-overhead.fraction: 0.1

# ====== TM 配置 ======
taskmanager.numberOfTaskSlots: 4          # 每 TM 的 slot 数(= CPU 核数 / 4)
taskmanager.bind-host: 0.0.0.0

# ====== 并行度 ======
parallelism.default: 24                   # 默认并行度(= Kafka partition 数)

# ====== Checkpoint ======
execution.checkpointing.interval: 60000   # 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 300000   # 5min 超时
execution.checkpointing.min-pause: 30000  # 两次 checkpoint 最小间隔 30s
execution.checkpointing.max-concurrent-checkpoints: 1
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
execution.checkpointing.storage: filesystem
state.checkpoints.dir: hdfs:///flink/checkpoints/
state.savepoints.dir: hdfs:///flink/savepoints/

# ====== 状态后端 ======
state.backend: rocksdb
state.backend.rocksdb.localdir: /data/flink/rocksdb
state.backend.rocksdb.memory.managed: true

# ====== RocksDB 调优 ======
state.backend.rocksdb.writebuffer.size: 256mb
state.backend.rocksdb.writebuffer.count: 4
state.backend.rocksdb.writebuffer.number-to-merge: 2
state.backend.rocksdb.block.blocksize: 32kb
state.backend.rocksdb.block.cache-size: 256mb
state.backend.rocksdb.compaction.style: LEVEL

# ====== JM HA ======
high-availability: org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices
high-availability.zookeeper.quorum: zk-1:2181,zk-2:2181,zk-3:2181
high-availability.zookeeper.path.root: /flink
high-availability.zookeeper.client.acl: open
high-availability.storageDir: hdfs:///flink/ha/
high-availability.cluster-id: /cluster-sz

# ====== 故障恢复 ======
jobmanager.execution.failover-strategy: region
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 30s

# ====== 网络 ======
taskmanager.network.memory.fraction: 0.1
taskmanager.network.memory.max: 2048m
taskmanager.network.memory.buffers-per-channel: 8

# ====== Metrics ======
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9999
metrics.reporter.prom.interval: 15 SECONDS

# ====== Web UI ======
web.submit.enable: true
web.cancel.enable: true
rest.flamegraph.enabled: true

# ====== JDK ======
# Flink 1.19 官方支持 Java 11/17,不支持 JDK 21+。
# 系统默认 java 可能是 JDK 25(给 Kafka/StarRocks),必须显式指定 JDK 17,
# 否则 Kryo 反射访问 java.util.Arrays$ArrayList 等内部字段会报
# InaccessibleObjectException(module java.base does not "opens java.util")。
env.java.home: /usr/lib/jvm/java-17-openjdk

# ====== 日志 ======
env.java.opts.all: -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+AlwaysPreTouch
```

#### 5.2 关键参数说明

##### 5.2.1 内存配置

```
TM 总内存 (32G) = Flink 内存 (24G) + JVM Metaspace (512M) + JVM Overhead (384M-1024M)
                          |
                          ├── Framework Heap (1G)    — Flink 框架堆内存
                          ├── Framework Off-Heap (512M) — Flink 框架堆外内存
                          ├── Task Heap (16G)        — 用户算子堆内存
                          ├── Managed Memory (6G)    — RocksDB 状态
                          └── Network (1-2G)         — 网络缓冲区
```

| 参数 | 值 | 说明 |
|---|---|---|
| `task.heap.size` | 16G | 用户算子堆内存,存放窗口状态对象 |
| `managed.size` | 6G | RocksDB 使用的堆外内存 |
| `network.fraction` | 0.1 | 网络缓冲区占比(shuffle 数据传输) |
| `jvm-overhead` | 384M-1024M | GC、线程栈等 JVM 开销 |

> **调优建议**:如果 RocksDB state 过大(> 10G),增加 `managed.size` 到 12G+;如果 GC 频繁,减少 `task.heap.size`。

##### 5.2.2 Slot 配置

```
TM (16C/32G) → 4 slots → 每 slot 4C/8G
全局并行度 = TM 数 × slots/TM = 6 × 4 = 24
```

| TM 规格 | slots/TM | 全局并行度(6 TM) | 说明 |
|---|---|---|---|
| 8C/16G | 2 | 12 | 小规模 |
| 16C/32G | 4 | 24 | 推荐(生产默认) |
| 32C/64G | 8 | 48 | 大规模 |

##### 5.2.3 Checkpoint 配置

| 参数 | 值 | 说明 |
|---|---|---|
| `interval` | 60s | Checkpoint 间隔,太短会增加延迟 |
| `mode` | EXACTLY_ONCE | 精确一次语义 |
| `timeout` | 300s | 超时时间,state 大时需调大 |
| `min-pause` | 30s | 两次 checkpoint 最小间隔 |
| `max-concurrent` | 1 | 不允许并发 checkpoint |
| `externalized-retention` | RETAIN_ON_CANCELLATION | 作业取消时保留 checkpoint |

---

### 6. 作业部署

#### 6.1 打包

```bash
cd examples/flink-agg5m-starrocks

# 编译打包
mvn clean package -Pprod

# 产物
ls -la target/flink-agg5m-starrocks-1.0.0.jar
# 期望: ~35MB fat jar
```

#### 6.2 上传 JAR

```bash
# 上传到 JM 节点
scp target/flink-agg5m-starrocks-1.0.0.jar jm-1:/opt/flink/jobs/

# 如果用 Web UI 提交,也可通过浏览器上传
```

#### 6.3 提交作业

```bash
# 通过 CLI 提交
/opt/flink/bin/flink run \
  -d \                                  # detached 模式(提交后不等待)
  -p 24 \                               # 全局并行度(= Kafka partition 数)
  -c com.example.promgw.Agg5mJob \
  /opt/flink/jobs/flink-agg5m-starrocks-1.0.0.jar \
  --env prod \
  --city sz \
  --kafka-brokers kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094 \
  --topic prom.sz.routed.app_business \
  --group-id flink-agg5m-sz-app-business \
  --starrocks-host <beijing-fe-vip> \
  --starrocks-port 8030 \
  --starrocks-db prom \
  --starrocks-table sr_bj_metrics_5m \
  --starrocks-user root \
  --starrocks-password "" \
  --label-prefix sz_5m \
  --dlq-topic prom.sz.dlq.sr.5m \
  --dlq-enabled true \
  --source-parallelism 24 \
  --agg-parallelism 24 \
  --window-minutes 5 \
  --checkpoint-path hdfs:///flink/checkpoints/agg5m-sz \
  --checkpoint-interval-ms 60000 \
  --allowed-lateness-ms 30000
```

#### 6.4 参数模板(各城)

| 参数 | 深圳 | 合肥 | 北京 |
|---|---|---|---|
| `--city` | `sz` | `hf` | `bj` |
| `--kafka-brokers` | `kafka-1.sz:9094,...` | `kafka-1.hf:9094,...` | `kafka-1.bj:9094,...` |
| `--topic` | `prom.sz.routed.app_business` | `prom.hf.routed.app_business` | `prom.bj.routed.app_business` |
| `--group-id` | `flink-agg5m-sz-app-business` | `flink-agg5m-hf-app-business` | `flink-agg5m-bj-app-business` |
| `--starrocks-host` | `<beijing-fe-vip>` | `<beijing-fe-vip>` | `<beijing-fe-vip>` |
| `--label-prefix` | `sz_5m` | `hf_5m` | `bj_5m` |
| `--dlq-topic` | `prom.sz.dlq.sr.5m` | `prom.hf.dlq.sr.5m` | `prom.bj.dlq.sr.5m` |
| `-p` (并行度) | 24 | 8 | 24 |

#### 6.5 通过 Web UI 提交

1. 访问 `http://jm-1:8081`
2. 左侧菜单 → "Submit new Job"
3. 上传 JAR 文件
4. 填入 Main Class: `com.example.promgw.Agg5mJob`
5. 填入 Program Arguments(同 §6.3 的参数)
6. 设置 Parallelism: 24
7. 点击 "Submit"

#### 6.6 作业管理

```bash
# 查看运行中的作业
/opt/flink/bin/flink list -r

# 查看所有作业(含已完成)
/opt/flink/bin/flink list -a

# 取消作业(正常停止,触发 Savepoint)
/opt/flink/bin/flink stop --savepointPath hdfs:///flink/savepoints/ <job-id>

# 取消作业(立即停止,不触发 Savepoint)
/opt/flink/bin/flink cancel <job-id>

# 从 Savepoint 恢复
/opt/flink/bin/flink run -s hdfs:///flink/savepoints/savepoint-xxxxx -d -p 24 \
  -c com.example.promgw.Agg5mJob \
  /opt/flink/jobs/flink-agg5m-starrocks-1.0.0.jar \
  --env prod --city sz ...
```

---

### 7. Checkpoint 与 Savepoint

#### 7.1 Checkpoint 管理

Checkpoint 是 Flink 自动触发的增量状态快照,用于故障恢复:

```bash
# 查看作业的 Checkpoint 统计
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints | python3 -m json.tool

# 查看 Checkpoint 详情
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints/details/<checkpoint-id> | python3 -m json.tool

# 列出 HDFS 上的 Checkpoint
hdfs dfs -ls /flink/checkpoints/agg5m-sz/

# 清理旧 Checkpoint(Flink 会自动清理,通常无需手动)
hdfs dfs -rm -r /flink/checkpoints/agg5m-sz/ckpt-000023
```

#### 7.2 Savepoint 管理

Savepoint 是手动触发的全量状态快照,用于升级、迁移:

```bash
# 触发 Savepoint(作业继续运行)
/opt/flink/bin/flink savepoint <job-id> hdfs:///flink/savepoints/

# 停止作业并触发 Savepoint
/opt/flink/bin/flink stop --savepointPath hdfs:///flink/savepoints/ <job-id>

# 从 Savepoint 恢复
/opt/flink/bin/flink run -s hdfs:///flink/savepoints/savepoint-xxxxx \
  -d -p 24 -c com.example.promgw.Agg5mJob \
  /opt/flink/jobs/flink-agg5m-starrocks-1.0.0.jar --env prod --city sz ...

# 删除 Savepoint
/opt/flink/bin/flink savepoint -d hdfs:///flink/savepoints/savepoint-xxxxx
```

#### 7.3 Checkpoint vs Savepoint

| 维度 | Checkpoint | Savepoint |
|---|---|---|
| 触发方式 | 自动(定时) | 手动 |
| 格式 | 增量(RocksDB) | 全量 |
| 用途 | 故障恢复 | 升级、迁移、A/B 测试 |
| 保留 | 作业取消时可选保留 | 手动删除前永久保留 |
| 性能影响 | 小(增量) | 较大(全量) |
| 格式兼容 | 版本相关 | 跨版本兼容(向后兼容) |

#### 7.4 滚动升级流程

```bash
# 1. 触发 Savepoint
SAVEPOINT_PATH=$(/opt/flink/bin/flink stop \
  --savepointPath hdfs:///flink/savepoints/ \
  <job-id> | grep "Savepoint completed" | grep -oP 'at \K[^ ]+')
echo "Savepoint: ${SAVEPOINT_PATH}"

# 2. 替换 JAR
scp target/flink-agg5m-starrocks-1.1.0.jar jm-1:/opt/flink/jobs/

# 3. 从 Savepoint 恢复
/opt/flink/bin/flink run -s ${SAVEPOINT_PATH} \
  -d -p 24 -c com.example.promgw.Agg5mJob \
  /opt/flink/jobs/flink-agg5m-starrocks-1.1.0.jar \
  --env prod --city sz ...

# 4. 验证作业正常运行
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool

# 5. 清理旧 Savepoint(可选)
/opt/flink/bin/flink savepoint -d ${SAVEPOINT_PATH}
```

---

### 8. 监控部署

#### 8.1 Prometheus 指标暴露

Flink 1.19 内置 Prometheus Reporter,在 `flink-conf.yaml` 中已配置:

```yaml
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9999
metrics.reporter.prom.interval: 15 SECONDS
```

```bash
# 验证
curl -s http://tm-1:9999/metrics | head -20
```

#### 8.2 Prometheus 抓取配置

```yaml
# prometheus.yml
scrape_configs:
  - job_name: flink-jm
    static_configs:
      - targets:
        - jm-1:9999
        - jm-2:9999
    scrape_interval: 15s

  - job_name: flink-tm
    static_configs:
      - targets:
        - tm-1:9999
        - tm-2:9999
        - tm-3:9999
        - tm-4:9999
        - tm-5:9999
        - tm-6:9999
    scrape_interval: 15s
```

#### 8.3 关键监控指标

| 指标 | Flink metric | 告警阈值 |
|---|---|---|
| 消费速率 | `flink_taskmanager_job_task_numRecordsInPerSecond` | 突降 50% |
| 输出速率 | `flink_taskmanager_job_task_numRecordsOutPerSecond` | 突降 50% |
| 事件时间延迟 | `flink_taskmanager_job_task_currentEventTimeLag` | > 60s |
| 处理延迟 | `flink_taskmanager_job_task_backPressuredTimeMsPerSecond` | > 1000ms/s |
| Checkpoint 耗时 | `flink_taskmanager_job_lastCheckpointDuration` | > 60s |
| Checkpoint 失败 | `flink_taskmanager_job_numFailedCheckpoints` | > 0 |
| Checkpoint 大小 | `flink_taskmanager_job_lastCheckpointSize` | - |
| Kafka Consumer Lag | `flink_taskmanager_job_task_KafkaSource_records_lag_max` | > 10000 |
| TM 内存使用 | `flink_taskmanager_Status_JVM_Memory_Direct_MemoryUsed` | > 80% |
| GC 暂停 | `flink_taskmanager_Status_JVM_GarbageCollector_TotalTime` | > 500ms |

#### 8.4 告警规则

**`/etc/prometheus/rules/flink-alerts.yaml`**:

```yaml
groups:
  - name: flink
    rules:
      # 作业失败
      - alert: FlinkJobFailed
        expr: flink_jobmanager_numRunningJobs < 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink 作业已停止"

      # 消费滞后
      - alert: FlinkConsumerLagHigh
        expr: flink_taskmanager_job_task_KafkaSource_records_lag_max > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink Kafka 消费 lag > 10000"

      # 事件时间延迟
      - alert: FlinkEventTimeLagHigh
        expr: flink_taskmanager_job_task_currentEventTimeLag > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink 事件时间延迟 > 60s"

      # Checkpoint 失败
      - alert: FlinkCheckpointFailed
        expr: increase(flink_taskmanager_job_numFailedCheckpoints[5m]) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink Checkpoint 失败"

      # Checkpoint 耗时过长
      - alert: FlinkCheckpointSlow
        expr: flink_taskmanager_job_lastCheckpointDuration > 60000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink Checkpoint 耗时 > 60s"

      # TM 宕机
      - alert: FlinkTaskManagerDown
        expr: count(up{job="flink-tm"} == 0) > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink TaskManager 宕机"
```

#### 8.5 Grafana Dashboard

| Dashboard | ID | 说明 |
|---|---|---|
| Flink Dashboard | 11000 | 作业概览、消费速率、Checkpoint |
| Flink RocksDB | 14932 | RocksDB state 监控 |

---

### 9. 性能调优

#### 9.1 并行度调优

| 阶段 | 并行度 | 说明 |
|---|---|---|
| Kafka Source | = partition 数 | 1:1 消费,确保吞吐 |
| Dedup + Decode | = source 并行度 | 同链路 |
| 窗口聚合 | = source 并行度 | 避免 shuffle |
| StarRocks Sink | = source 并行度 / 2 | Stream Load 批量,不需高并行度 |

```bash
# 命令行指定各阶段并行度(在 Agg5mJob 代码中已支持)
--source-parallelism 24
--agg-parallelism 24
```

#### 9.2 RocksDB 调优

```yaml
# flink-conf.yaml
state.backend.rocksdb.memory.managed: true        # 使用 managed memory
state.backend.rocksdb.writebuffer.size: 256mb      # 写缓冲,减少 flush 频率
state.backend.rocksdb.writebuffer.count: 4         # 最大写缓冲数
state.backend.rocksdb.writebuffer.number-to-merge: 2  # 合并时的写缓冲数
state.backend.rocksdb.block.blocksize: 32kb        # 块大小
state.backend.rocksdb.block.cache-size: 256mb      # 块缓存
state.backend.rocksdb.compaction.style: LEVEL      # 层级压缩
```

#### 9.3 网络缓冲区调优

```yaml
# 适用于高吞吐场景
taskmanager.network.memory.fraction: 0.15          # 网络内存占比
taskmanager.network.memory.max: 4096m              # 最大网络内存
taskmanager.network.memory.buffers-per-channel: 4  # 每 channel 缓冲区数
```

#### 9.4 作业级调优(Agg5mJob)

| 参数 | 默认值 | 调优建议 | 说明 |
|---|---|---|---|
| `--window-minutes` | 5 | 不要改 | 5min 是设计要求 |
| `--allowed-lateness-ms` | 30000 | 10000-60000 | 允许的乱序时间,影响窗口触发延迟 |
| `--checkpoint-interval-ms` | 60000 | 30000-120000 | 太短影响吞吐,太长影响恢复 |
| `--source-parallelism` | 4 | = partition 数 | 确保 1:1 消费 |
| `--agg-parallelism` | 4 | = source 并行度 | 避免 shuffle |

#### 9.5 JVM 调优

```yaml
# flink-conf.yaml
env.java.opts.all: >-
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=100
  -XX:+AlwaysPreTouch
  -XX:+ExplicitGCInvokesConcurrent
```

---

### 10. 运维操作

#### 10.1 集群启停

```bash
# 启动整个集群(JM + TM)
/opt/flink/bin/start-cluster.sh

# 停止整个集群
/opt/flink/bin/stop-cluster.sh

# 单独启动 / 停止 JM
/opt/flink/bin/jobmanager.sh start
/opt/flink/bin/jobmanager.sh stop

# 单独启动 / 停止 TM
/opt/flink/bin/taskmanager.sh start
/opt/flink/bin/taskmanager.sh stop
```

#### 10.2 滚动重启 TM

```bash
# 逐台重启 TM(不影响作业运行,前提:有足够 slot)
for tm in tm-1 tm-2 tm-3 tm-4 tm-5 tm-6; do
    echo "重启 ${tm}..."
    ssh ${tm} "/opt/flink/bin/taskmanager.sh stop"
    sleep 5
    ssh ${tm} "/opt/flink/bin/taskmanager.sh start"
    sleep 10
    # 等待 TM 注册到 JM
    curl -s http://jm-1:8081/taskmanagers | python3 -m json.tool | grep ${tm}
done
```

#### 10.3 日志查看

```bash
# JM 日志
tail -f /opt/flink/log/flink-*-standalonesession-*.log

# TM 日志
tail -f /opt/flink/log/flink-*-taskexecutor-*.log

# 作业日志(在 TM 上)
ls /opt/flink/log/
# flink-flink-agg5m-sz-*_*.log

# 查看错误日志
grep -i "error\|exception\|failed" /opt/flink/log/flink-*.log | tail -50
```

#### 10.4 作业状态排查

```bash
# 查看作业概览
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool

# 查看作业详情(含各算子指标)
curl -s http://jm-1:8081/jobs/<job-id> | python3 -m json.tool

# 查看作业异常
curl -s http://jm-1:8081/jobs/<job-id>/exceptions | python3 -m json.tool

# 查看 Backpressure
curl -s http://jm-1:8081/jobs/<job-id>/vertices/<vertex-id>/backpressure | python3 -m json.tool

# 查看 Watermark
curl -s http://jm-1:8081/jobs/<job-id>/vertices/<vertex-id>/watermarks | python3 -m json.tool
```

#### 10.5 常见问题处理

| 现象 | 排查 | 解决 |
|---|---|---|
| 作业启动即失败 | 查看作业异常日志 | 检查 Kafka 连接、StarRocks 可达性、参数格式 |
| 消费 lag 持续增大 | 查看 TM 指标,检查 Backpressure | 扩 partition / 扩 TM / 调整并行度 |
| Checkpoint 超时 | 查看 state 大小,RocksDB IOPS | 增大 managed memory / 用 SSD / 增大 checkpoint timeout |
| Checkpoint 失败 | 查看异常日志 | 检查 HDFS 连通性 / 磁盘空间 |
| StarRocks 写入失败 | 查看 Sink 异常日志 | 检查 FE VIP 可达性 / Label 冲突 / BE 压力 |
| JM 内存不足 | 查看 JM 日志,GC 情况 | 增大 JM 内存 / 减少作业数 |
| TM OOM | 查看 TM 日志 | 增大 task.heap / 减少 slot 数 / 优化 state |

#### 10.6 资源调优速查表

| 场景 | TM 规格 | slots/TM | TM 数 | 并行度 | managed | 说明 |
|---|---|---|---|---|---|---|
| 本地开发 | 4C/8G | 2 | 1 | 4 | 1G | 单节点 |
| 小型生产 | 8C/16G | 2 | 2 | 4 | 2G | < 100K samples/s |
| 中型生产 | 16C/32G | 4 | 4 | 16 | 6G | 100K-1M samples/s |
| 大型生产 | 16C/32G | 4 | 6 | 24 | 8G | > 1M samples/s(推荐) |
| 超大规模 | 32C/64G | 8 | 8 | 64 | 16G | > 5M samples/s |

---

### 11. 附录

#### 11.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `flink-conf.yaml` | `/opt/flink/conf/flink-conf.yaml` | 主配置 |
| `masters` | `/opt/flink/conf/masters` | JM 节点列表 |
| `workers` | `/opt/flink/conf/workers` | TM 节点列表 |
| `log4j2.xml` | `/opt/flink/conf/log4j2.xml` | 日志配置 |
| `flink-agg5m-starrocks-1.0.0.jar` | `/opt/flink/jobs/` | 作业 JAR |
| `zoo.cfg` | `/opt/zookeeper/conf/zoo.cfg` | ZooKeeper 配置 |

#### 11.2 常用命令速查

```bash
# 集群管理
/opt/flink/bin/start-cluster.sh
/opt/flink/bin/stop-cluster.sh
/opt/flink/bin/jobmanager.sh start|stop
/opt/flink/bin/taskmanager.sh start|stop

# 作业管理
/opt/flink/bin/flink list -r|-a
/opt/flink/bin/flink run -d -p 24 -c <main-class> <jar> [args]
/opt/flink/bin/flink cancel <job-id>
/opt/flink/bin/flink stop --savepointPath <path> <job-id>

# Savepoint
/opt/flink/bin/flink savepoint <job-id> <path>
/opt/flink/bin/flink run -s <savepoint-path> ...
/opt/flink/bin/flink savepoint -d <savepoint-path>

# 查看状态
curl -s http://jm-1:8081/jobs/overview | python3 -m json.tool
curl -s http://jm-1:8081/taskmanagers | python3 -m json.tool
curl -s http://jm-1:8081/jobs/<job-id>/checkpoints | python3 -m json.tool
```

#### 11.3 命令行参数完整列表

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--env` | `local` | 环境预设:`local` / `prod` |
| `--city` | - | 城市标识:`bj` / `sz` / `hf` |
| `--kafka-brokers` | `localhost:9092` | Kafka broker 地址列表 |
| `--topic` | `prom.local.routed.app_business` | 消费的 Kafka topic |
| `--group-id` | `flink-agg5m-local` | Kafka consumer group ID |
| `--starrocks-host` | `localhost` | StarRocks FE VIP |
| `--starrocks-port` | `8030` | StarRocks FE HTTP 端口(Stream Load 复用,无 8070) |
| `--starrocks-db` | `prom` | StarRocks 数据库 |
| `--starrocks-table` | `sr_bj_metrics_5m` | StarRocks 表名 |
| `--starrocks-user` | `root` | StarRocks 用户名 |
| `--starrocks-password` | (空) | StarRocks 密码 |
| `--label-prefix` | `local_5m` | Stream Load label 前缀 |
| `--dlq-topic` | `prom.local.dlq.sr.5m` | DLQ Kafka topic |
| `--dlq-enabled` | `true` | 是否启用 DLQ |
| `--source-parallelism` | `4` | Kafka Source 并行度 |
| `--agg-parallelism` | `4` | 窗口聚合并行度 |
| `--window-minutes` | `5` | 窗口大小(分钟) |
| `--checkpoint-path` | `file:///tmp/flink-checkpoints` | Checkpoint 存储路径 |
| `--checkpoint-interval-ms` | `60000` | Checkpoint 间隔(ms) |
| `--allowed-lateness-ms` | `30000` | 允许的乱序时间(ms) |



---

## 6. Flink 消费 Kafka 开发指南 {#6-flink-消费-kafka-开发指南}
> 本文档面向下游 Flink 开发者,描述如何消费 prom-gw 写入 Kafka 的指标数据,完成 5 min 滚动聚合后通过 Stream Load 写入北京 StarRocks。
>
> **前置阅读**:**local-dev-guide.md**(见 §10)(prom-gw 本地部署)、**production-guide.md**(见 §1)(生产架构)、**设计文档 §4.5/§4.6**(三独立表 + 级联聚合方案)


---

### 1. 整体架构与定位

#### 1.1 在全链路中的位置

```
Prometheus ─remote_write─> prom-gw ─> Kafka ─> Flink(本文档) ─Stream Load─> StarRocks
                                       ↑                          ↑
                                  snappy+protobuf            5min 聚合 + gzip
                                  zstd(Kafka 端)             跨城专线(仅 5m 主体)
```

Flink 在本城完成 **5 min 滚动窗口聚合**,跨城写入北京 StarRocks `sr_bj_metrics_5m` 表;**1h / 1d 聚合由 StarRocks 周期任务从 5m 表级联聚合**,不在 Flink 端独立输出。

#### 1.2 Flink 作业划分

| 作业 | 职责 | 必需 |
|---|---|---|
| **A 作业:5 min 聚合 + Stream Load** | 消费本城 Kafka → 5 min 窗口聚合 → 跨城写 StarRocks | ✅ |
| B 作业:跨指标 join | 跨指标关联(如 `kube_pod_info` × 业务指标) | 可选 |
| C 作业:DLQ 重放 | 监听 `prom.<city>.dlq.sr.5m` 重放失败批次 | 运维工具 |

本文档聚焦 **A 作业** 的完整开发步骤。

#### 1.3 数据规模假设

参考 **设计文档 §2.2.6**:

| 项 | 数值 |
|---|---|
| 单城 series 数 | ≈ 1000 万 |
| 5 min 单城输出 | ≈ 2.3 GB/批,288 批/天 |
| 5 min 跨城(gzip 后) | ≈ 345 GB/天/城 |
| 三城合计跨城 | ≈ 1 TB/天,占 1G 专线 9.3% |

---

### 2. Kafka 消息格式约定

> ⚠️ **这是 Flink 开发者必须首先理解的关键章节**。prom-gw 写入 Kafka 的消息格式有特殊设计,直接处理会导致数据重复消费。

#### 2.1 消息整体结构

```
┌─────────────────────────────────────────────────────────────┐
│ Kafka Message                                               │
│                                                             │
│   Topic:    prom.<city>.<stage>.<tenant>                    │
│   Key:      <SeriesKey 十进制字符串>  (uint64 FNV-1a hash)  │
│   Value:    <snappy 压缩的 prompb.WriteRequest 字节>        │
│   Headers:  tenant / source_dc / ingest_city / ...          │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

#### 2.2 Value(payload)编码

**两层压缩 + protobuf**:

```
Kafka 端 zstd 压缩  →  解后是 snappy 压缩字节  →  解后是 prompb.WriteRequest v1 protobuf
```

- **外层 zstd**:Kafka producer 端配置 `compression.type=zstd`(见 prom-gw [internal/kafkasink/producer.go](../../internal/kafkasink/producer.go) `main.go:155`)
- **内层 snappy**:Prometheus remote_write v1 协议原生编码,Flink 解外层 zstd 后必须再解一次 snappy
- **protobuf schema**:`prometheus.WriteRequest` v1(不是 v2),定义在 [api/proto/remote.proto](../../api/proto/remote.proto) + [api/proto/types.proto](../../api/proto/types.proto)

```protobuf
message WriteRequest {
  repeated prometheus.TimeSeries timeseries = 1;
  reserved 2;
  repeated prometheus.MetricMetadata metadata = 3;
}

message TimeSeries {
  repeated Label labels = 1;       // 含 __name__
  repeated Sample samples = 2;     // 通常 1 个
  repeated Exemplar exemplars = 3;
  repeated Histogram histograms = 4;
}

message Sample {
  double value = 1;
  int64 timestamp = 2;              // 毫秒
}

message Label {
  string name = 1;
  string value = 2;
}
```

#### 2.3 ⚠️ 关键设计:一条 Kafka 消息 ≠ 一个 sample

prom-gw 在 [internal/ruleengine/pipeline.go:225-242](../../internal/ruleengine/pipeline.go) 中按如下逻辑分发:

```go
// 每 sample 一次 out,key 各自用 seriesKey
for _, s := range cur {
    m := msg
    m.Topic = topic
    m.Key = []byte(strconv.FormatUint(s.SeriesKey(), 10))
    m.Payload = raw   // ← 整个 WriteRequest 的 snappy 字节
    p.out(ctx, m)
}
```

**含义**:一个 WriteRequest 含 N 个 sample → 产生 **N 条 Kafka 消息**,这 N 条消息的 **payload 完全相同**,但 **Key 不同**。

**Flink 必须去重**,二选一:

| 方案 | 做法 | 优点 | 缺点 |
|---|---|---|---|
| **方案 1(推荐):payload hash 去重** | 用 `Arrays.hashCode(payload)` 作 key,state 中标记已处理 | 简单,无需复刻 SeriesKey | 丢失"这条消息对应哪个 series"的映射 |
| 方案 2:SeriesKey 精确匹配 | Flink 端复刻 SeriesKey 算法,解码 payload 后用 Key 匹配出对应 sample | 精确,只处理目标 sample | 需要复刻 FNV-1a 64 哈希(见 [internal/parser/sample.go:46-59](../../internal/parser/sample.go)) |

**推荐方案 1**:Flink 消费后按 payload hash 去重,每个唯一 payload 只解码一次,然后处理 WriteRequest 中的所有 sample。

#### 2.4 Kafka Headers(必读)

**租户和机房信息不在 payload 里,在 Kafka header**。payload 是 Prometheus 发来的原始字节,Prometheus 不知道这些信息。

构造点:[cmd/prom-gw/main.go:419-427](../../cmd/prom-gw/main.go):

| Header 名 | 类型 | 说明 | 示例 |
|---|---|---|---|
| `tenant` | string | 租户名(来自 token 鉴权) | `app-business` |
| `source_dc` | string | 来源机房(来自 `X-Source-DC` 头或 `--source-dc` flag) | `五联` |
| `ingest_city` | string | 城市:`bj` / `sz` / `hf` | `sz` |
| `ingest_dc` | string | 写入 prom-gw 的机房标识 | `dc-sz-5union` |
| `ingest_time_ms` | string | 进入 prom-gw 时刻,**毫秒**(Unix ms) | `1786431389413` |
| `traceparent` | string | W3C Trace Context | `00-<trace-id>-<span-id>-01` |

Flink 必须从 header 提取这些字段写入 StarRocks,不能从 payload 解析。

#### 2.5 Key(SeriesKey)说明

- **格式**:`uint64` 十进制字符串(如 `"12345678901234567890"`)
- **算法**:FNV-1a 64,基于 `tenant + metric + sorted labels` 拼接哈希
- **用途**:同 series 落同 partition,保证时间顺序
- **不可反解**:Key 只是哈希值,要拿 series 信息必须解码 payload
- **Flink 用途**:作为 keyBy 的依据,同 series 路由到同一 subtask,保证窗口内状态一致

#### 2.6 Topic 命名规则

| 环境 | 原始 topic | 路由后 topic |
|---|---|---|
| 本地 | `prom.local.raw.<tenant>` | `prom.local.routed.<biz>` |
| 生产 | `prom.<city>.raw.<tenant>` | `prom.<city>.routed.<biz>` 或 `prom.<city>.cleaned.<biz>` |

Flink 通常消费 **路由后(cleaned/routed)的 topic**,因为这些数据已经过 relabel/route 清洗。若要消费原始数据,订阅 raw topic。

---

### 3. 前置条件

#### 3.1 Kafka 集群

- **生产**:每城 3 Broker KRaft 集群,端口 `9094`(SSL/SASL),3 副本
- **本地开发**:单节点 KRaft,见 **local-dev-guide.md §3**(见 §10)
- **必要 topic**:
  - 消费源:`prom.<city>.routed.<business>`(或本地 `prom.local.routed.app_business`)
  - DLQ:`prom.<city>.dlq.sr.5m`(Flink 创建)
  - 本地兜底输出(可选):`prom.<city>.agg5m.<business>`

#### 3.2 StarRocks 集群

- **生产**:北京 3 节点(FE+BE 混合,64C/512G/1.92T×22 SSD)
- **端口**:
  - `8030`:FE HTTP(Web UI + REST API + Stream Load)
  - `9030`:MySQL 协议(查询)
  - `8040`:BE HTTP(FE 收到 Stream Load 后 307 redirect 到此端口)
  - 无 `8070` 端口,旧文档中的 8070 引用已废弃
- **FE VIP**:负载均衡,所有 Stream Load 请求走 VIP

#### 3.3 Flink 集群

- **生产**:每城 JM×2(1 Active + 1 Standby)+ ZK×3 + TM×2~6
- **版本**:Flink 1.19+(建议 1.19.2)
- **JDK**:Java 17
- **每 TM**:16C/32G/500G SSD,4 slot
- **状态后端**:RocksDB(增量 checkpoint)

#### 3.4 跨城专线

- 深圳 ⇄ 北京:1G×2(主备),P95 ≤ 30ms
- 合肥 ⇄ 北京:1G×1,P95 ≤ 25ms
- 跨城带宽峰值预估:36 MB/s(3× 均值),占 1G 专线 28%

---

### 4. StarRocks 表结构准备

#### 4.1 三独立表 DDL

完整 DDL 见 **设计文档 §4.6.1**。这里给出 5m 表(Flink 唯一写入点):

```sql
-- ===== 5 min 表:Flink 跨城 Stream Load 唯一写入点,留存 7 天 =====
CREATE TABLE sr_bj_metrics_5m (
  ts            DATETIME     NOT NULL COMMENT '5 min 窗口起始时间(UTC+8)',
  metric        VARCHAR(128) NOT NULL,
  tenant        VARCHAR(64)  NOT NULL,
  business      VARCHAR(64)  NOT NULL,
  ingest_city   VARCHAR(16)  NOT NULL COMMENT 'bj/sz/hf',
  source_dc     VARCHAR(32)  NOT NULL,
  labels_hash   VARCHAR(64)  NOT NULL COMMENT 'labels 的 XXH3 hash,作 PK 键列',
  labels        MAP<VARCHAR(64), VARCHAR(256)> COMMENT '原始 labels(非键列,仅查询用)',
  sample_count  BIGINT       NOT NULL COMMENT '窗口内原始样本数(5 min × 15s = 20 个)',
  value_sum     DOUBLE       NOT NULL,
  value_max     DOUBLE,
  value_min     DOUBLE,
  value_avg     DOUBLE,
  value_p50     DOUBLE,
  value_p99     DOUBLE,
  ingest_time   DATETIME     NOT NULL COMMENT '入 StarRocks 时间(DLQ 重放去重用)'
) ENGINE=OLAP
  PRIMARY KEY(ts, metric, tenant, business, ingest_city, source_dc, labels_hash)
  PARTITION BY RANGE(ts) ()
  DISTRIBUTED BY HASH(metric, tenant) BUCKETS 32
  PROPERTIES (
    "storage_medium" = "SSD",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32",
    "compression" = "LZ4",
    "replicated_storage" = "true"
  );
```

> 1h / 1d 表 DDL 结构相同,区别仅 `dynamic_partition.start` 分别为 `-90` / `-1095`,BUCKETS 为 16 / 8。由 StarRocks 周期任务级联聚合,Flink 不写入。

#### 4.2 周期聚合任务(StarRocks 端,非 Flink)

```sql
-- 每小时执行:从 5m 表聚合到 1h 表
INSERT OVERWRITE sr_bj_metrics_1h
SELECT
  date_trunc('hour', ts) AS ts,
  metric, tenant, business, ingest_city, source_dc, labels_hash,
  max(labels) AS labels,
  sum(sample_count) AS sample_count,
  sum(value_sum) AS value_sum,
  max(value_max) AS value_max,
  min(value_min) AS value_min,
  sum(value_sum) / sum(sample_count) AS value_avg,
  percentile_approx(value_p50, sample_count) AS value_p50,
  percentile_approx(value_p99, sample_count) AS value_p99,
  max(ingest_time) AS ingest_time
FROM sr_bj_metrics_5m
WHERE ts >= date_trunc('hour', now() - interval 1 hour)
  AND ts <  date_trunc('hour', now())
GROUP BY date_trunc('hour', ts), metric, tenant, business, ingest_city, source_dc, labels_hash;

-- 每天执行:从 1h 表聚合到 1d 表(级联,不跳级)
-- SQL 结构同上,改 date_trunc('day', ...) 和表名
```

通过 StarRocks 的 `JOB` 或外部调度(dolphinscheduler/airflow)定时执行。

---

### 5. Flink 项目搭建

#### 5.1 Maven 依赖

```xml
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.example.promgw</groupId>
  <artifactId>flink-agg5m-starrocks</artifactId>
  <version>1.0.0</version>
  <packaging>jar</packaging>

  <properties>
    <flink.version>1.19.2</flink.version>
    <java.version>17</java.version>
    <prometheus.version>0.16.0</prometheus.version>
  </properties>

  <dependencies>
    <!-- Flink core(标注 provided,生产环境由集群提供) -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java_2.12</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-clients_2.12</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>

    <!-- Kafka connector -->
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka_2.12</artifactId>
      <version>${flink.version}</version>
    </dependency>

    <!-- Prometheus protobuf schema(prompb.WriteRequest v1) -->
    <dependency>
      <groupId>io.prometheus</groupId>
      <artifactId>simpleclient</artifactId>
      <version>${prometheus.version}</version>
    </dependency>
    <!-- 或直接使用 prometheus-protoc 生成的 Java protobuf -->

    <!-- Snappy / Zstd 解压(Kafka 端 zstd 通常由 connector 自动解,snappy 需手动) -->
    <dependency>
      <groupId>org.xerial.snappy</groupId>
      <artifactId>snappy-java</artifactId>
      <version>1.1.10.5</version>
    </dependency>

    <!-- XXH3 hash(labels_hash 计算) -->
    <dependency>
      <groupId>net.openhft</groupId>
      <artifactId>zero-allocation-hash</artifactId>
      <version>0.16</version>
    </dependency>

    <!-- StarRocks Stream Load connector(可选,也可手写 HTTP) -->
    <dependency>
      <groupId>com.starrocks.connector</groupId>
      <artifactId>flink-connector-starrocks</artifactId>
      <version>1.2.9_flink-1.19</version>
    </dependency>

    <!-- 日志 -->
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-api</artifactId>
      <version>1.7.36</version>
    </dependency>
    <dependency>
      <groupId>ch.qos.logback</groupId>
      <artifactId>logback-classic</artifactId>
      <version>1.2.11</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <artifactSet>
                <excludes>
                  <exclude>org.apache.flink:flink-streaming-java_2.12</exclude>
                  <exclude>org.apache.flink:flink-clients_2.12</exclude>
                </excludes>
              </artifactSet>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

#### 5.2 项目结构

```
flink-agg5m-starrocks/
├── pom.xml
└── src/main/java/com/example/promgw/
    ├── Agg5mJob.java              # 主作业入口
    ├── decoder/
    │   ├── PromWriteRequestDecoder.java   # Kafka 反序列化器(zstd+snappy+protobuf)
    │   └── PromSample.java                # 解码后的 sample POJO
    ├── aggregate/
    │   ├── MetricAggFunction.java         # 窗口聚合函数(sum/count/p50/p99)
    │   ├── MetricAggState.java            # 状态定义
    │   └── AggResult.java                # 聚合输出 POJO
    ├── sink/
    │   ├── StarRocksSink.java            # Stream Load 写入
    │   └── StarRocksStreamLoadClient.java # HTTP 客户端
    ├── util/
    │   ├── LabelsHasher.java             # XXH3 labels hash
    │   └── HeaderExtractor.java          # Kafka header 提取
    └── dlq/
        └── DlqSink.java                 # 失败消息写回 Kafka
```

---

### 6. Protobuf 解码器实现

#### 6.1 Kafka 反序列化器

```java
package com.example.promgw.decoder;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.xerial.snappy.Snappy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PromWriteRequestDecoder
 *
 * prom-gw 写入 Kafka 的消息格式:
 *   Value  = snappy(protobuf(WriteRequest))  (Kafka 端 zstd 由 connector 自动解)
 *   Key    = SeriesKey 十进制字符串(uint64 FNV-1a hash)
 *   Headers: tenant / source_dc / ingest_city / ingest_dc / ingest_time_ms / traceparent
 *
 * 关键:一条 Kafka 消息 = 一个 sample 的 Key + 整个 WriteRequest 的 payload。
 * 一个 WriteRequest 含 N 个 sample 会产生 N 条消息,payload 相同,Key 不同。
 * 本解码器按 payload hash 去重,每个唯一 payload 只解码一次,输出所有 sample。
 */
public class PromWriteRequestDecoder
        implements DeserializationSchema<PromSample> {

    @Override
    public PromSample deserialize(byte[] message) throws IOException {
        return deserialize(null, message, null, null, null);
    }

    /**
     * Flink KafkaSource 支持传递 key/headers 的重载(需用 KafkaRecordDeserializationSchema)。
     * 这里给出完整签名,实际使用时实现 KafkaRecordDeserializationSchema 接口。
     */
    public PromSample deserialize(
            byte[] key, byte[] value,
            String topic, Long offset,
            Map<String, String> headers) throws IOException {

        if (value == null || value.length == 0) {
            return null;
        }

        // 1. 解 snappy(Kafka 端 zstd 已由 connector 自动解压)
        byte[] protobufBytes = Snappy.uncompress(value);

        // 2. protobuf Unmarshal → prompb.WriteRequest
        // 使用 prometheus 官方 Java protobuf 或自行生成的 stub
        WriteRequestParser.ParsedWriteRequest parsed =
                WriteRequestParser.parse(protobufBytes);

        // 3. 从 header 提取租户/机房信息
        String tenant       = headers != null ? headers.get("tenant")       : "";
        String sourceDc     = headers != null ? headers.get("source_dc")    : "";
        String ingestCity   = headers != null ? headers.get("ingest_city")   : "";
        String ingestDc     = headers != null ? headers.get("ingest_dc")     : "";
        String ingestTimeMs = headers != null ? headers.get("ingest_time_ms"): "";
        String traceparent  = headers != null ? headers.get("traceparent")   : "";

        // 4. 返回 POJO(包含整个 WriteRequest 的所有 sample + 元数据)
        return PromSample.builder()
                .timeseries(parsed.getTimeseries())
                .tenant(tenant)
                .sourceDc(sourceDc)
                .ingestCity(ingestCity)
                .ingestDc(ingestDc)
                .ingestTimeMs(parseLongSafe(ingestTimeMs))
                .traceparent(traceparent)
                .build();
    }

    @Override
    public boolean isEndOfStream(PromSample nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PromSample> getProducedType() {
        return TypeInformation.of(PromSample.class);
    }

    private long parseLongSafe(String s) {
        if (s == null || s.isEmpty()) return 0L;
        try { return Long.parseLong(s); } catch (Exception e) { return 0L; }
    }
}
```

#### 6.2 Payload 去重(关键)

由于同一 WriteRequest 会产生 N 条 payload 宍余消息,必须用算子状态去重:

```java
// 在 map 之前加一步:按 payload hash 去重
DataStream<PromSample> deduped = kafkaSource
    .keyBy(record -> Arrays.hashCode(record.getValue()))   // payload hash 作 key
    .process(new DedupFunction())                          // 同 hash 只处理第一条
    .name("dedup-by-payload");

/**
 * DedupFunction:同 payload hash 在短窗口内(如 60s)只处理一次。
 * 用 ValueState 记录最近处理时间,超时清除。
 */
public class DedupFunction extends KeyedProcessFunction<Integer, KafkaRecord, PromSample> {

    private ValueState<Long> lastProcessedTs;

    @Override
    public void open(Configuration parameters) {
        lastProcessedTs = getRuntimeContext().getState(
            new ValueStateDescriptor<>("lastTs", Types.LONG));
    }

    @Override
    public void processElement(KafkaRecord record, Context ctx, Collector<PromSample> out) throws Exception {
        Long last = lastProcessedTs.value();
        long now = ctx.timerService().currentProcessingTime();
        // 60s 内同 payload hash 视为重复,跳过
        if (last != null && (now - last) < 60_000L) {
            return;
        }
        lastProcessedTs.update(now);
        // 注册 60s 后的清理 timer
        ctx.timerService().registerProcessingTimeTimer(now + 60_000L);

        // 解码并输出
        out.collect(decoder.deserialize(
            record.getKey(), record.getValue(),
            record.getTopic(), record.getOffset(),
            record.getHeaders()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PromSample> out) throws Exception {
        lastProcessedTs.clear();
    }
}
```

> **替代方案**:如果 series 数量可控,也可以用 `KeyBy(SeriesKey)` 直接按 series 分组,但需要 Flink 端复刻 [SeriesKey 算法](../../internal/parser/sample.go)(FNV-1a 64)。payload hash 去重实现更简单,推荐首选。

#### 6.3 解析 WriteRequest(使用 prometheus-protoc 生成的 stub)

```java
package com.example.promgw.decoder;

import io.prometheus.client.remote.WriteRequest;  // 来自 prometheus protobuf
import io.prometheus.client.remote.LabelPair;
import io.prometheus.client.remote.Sample;
import io.prometheus.client.remote.TimeSeries;
import java.util.ArrayList;
import java.util.List;

public class WriteRequestParser {

    public static ParsedWriteRequest parse(byte[] protobufBytes) throws IOException {
        WriteRequest req = WriteRequest.parseFrom(protobufBytes);

        List<ParsedSeries> series = new ArrayList<>(req.getTimeseriesCount());
        for (TimeSeries ts : req.getTimeseriesList()) {
            // 提取 metric name(__name__ label)
            String metricName = "";
            Map<String, String> labels = new HashMap<>();
            for (LabelPair lp : ts.getLabelsList()) {
                if ("__name__".equals(lp.getName())) {
                    metricName = lp.getValue();
                } else {
                    labels.put(lp.getName(), lp.getValue());
                }
            }

            List<ParsedSample> samples = new ArrayList<>(ts.getSamplesCount());
            for (Sample s : ts.getSamplesList()) {
                samples.add(new ParsedSample(
                    s.getValue(),
                    s.getTimestamp()   // 毫秒
                ));
            }
            series.add(new ParsedSeries(metricName, labels, samples));
        }
        return new ParsedWriteRequest(series);
    }
}
```

---

### 7. Kafka Source 配置

#### 7.1 KafkaSource 构造

```java
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

Properties kafkaProps = new Properties();
kafkaProps.setProperty("bootstrap.servers", "kafka-1:9094,kafka-2:9094,kafka-3:9094");
// SSL/SASL(生产)
kafkaProps.setProperty("security.protocol", "SASL_SSL");
kafkaProps.setProperty("sasl.mechanism", "SCRAM-SHA-512");
kafkaProps.setProperty("sasl.jaas.config",
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"flink\" password=\"<secret>\";");
kafkaProps.setProperty("ssl.truststore.location", "/etc/flink/kafka.truststore.jks");
kafkaProps.setProperty("ssl.truststore.password", "<secret>");

KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder()
    .setBootstrapServers("kafka-1:9094,kafka-2:9094,kafka-3:9094")
    .setTopics("prom.sz.routed.app_business")    // 按本城/业务订阅
    .setGroupId("flink-agg5m-sz-app-business")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
    .setValueOnlyDeserializer(new KafkaRecordDeserializer())  // 自定义,保留 key+headers
    .setProperties(kafkaProps)
    .build();
```

#### 7.2 保留 key 和 headers 的 Deserializer

Flink 的 `KafkaRecordDeserializationSchema` 可同时拿到 key/value/headers:

```java
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

public class KafkaRecordDeserializer implements KafkaRecordDeserializationSchema<KafkaRecord> {

    private final PromWriteRequestDecoder decoder = new PromWriteRequestDecoder();

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<KafkaRecord> out) throws IOException {
        // 提取 headers
        Map<String, String> headers = new HashMap<>();
        Headers hdrs = record.headers();
        for (Header h : hdrs) {
            headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8));
        }
        out.collect(new KafkaRecord(
            record.key(), record.value(),
            record.topic(), record.offset(), record.partition(),
            record.timestamp(), headers
        ));
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
```

#### 7.3 Source 并行度

- **并行度 = Kafka partition 数**(确保 1:1 消费)
- 本地 topic 4 partition → source parallelism = 4
- 生产 topic 建议 12~24 partition(三城合计 1000 万 series 时的吞吐)

---

### 8. 5 min 窗口聚合实现

#### 8.1 窗口分配

按 `metric + tenant + sortedLabels` 作 key,5 min 滚动窗口:

```java
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

// 1. 展开 WriteRequest → 单条 sample 流(每个 series 1 条)
DataStream<SampleWithMeta> samples = deduped
    .flatMap(new ExpandWriteRequest())     // 把 N 个 series 展开为 N 条 record
    .name("expand-samples");

// 2. 分配 watermark(prompb.Sample.timestamp 是事件时间,单位 ms)
DataStream<SampleWithMeta> withWatermark = samples
    .assignTimestampsAndWatermarks(
        WatermarkStrategy
            .<SampleWithMeta>forBoundedOutOfOrderness(Duration.ofSeconds(30))
            .withTimestampAssigner((rec, ts) -> rec.getTimestampMs())
    );

// 3. keyBy(seriesKey) + 5min tumbling window
DataStream<AggResult> aggStream = withWatermark
    .keyBy(rec -> LabelsHasher.seriesKey(
        rec.getTenant(), rec.getMetric(), rec.getLabels()))
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new MetricAggFunction(), new AggWindowFunction())
    .name("agg-5min");
```

#### 8.2 聚合函数(sum/count/max/min/p50/p99)

```java
import org.apache.flink.api.common.functions.AggregateFunction;

public class MetricAggFunction
        implements AggregateFunction<SampleWithMeta, MetricAggState, MetricAggState> {

    @Override
    public MetricAggState createAccumulator() {
        return new MetricAggState();
    }

    @Override
    public MetricAggState add(SampleWithMeta rec, MetricAggState acc) {
        acc.count++;
        acc.sum += rec.getValue();
        acc.max = Math.max(acc.max, rec.getValue());
        acc.min = acc.count == 1 ? rec.getValue() : Math.min(acc.min, rec.getValue());
        acc.samples.add(rec.getValue());   // 用于 p50/p99 计算
        if (acc.tenant == null) {
            acc.tenant     = rec.getTenant();
            acc.metric     = rec.getMetric();
            acc.labels     = rec.getLabels();
            acc.sourceDc   = rec.getSourceDc();
            acc.ingestCity = rec.getIngestCity();
            acc.ingestTimeMs = rec.getIngestTimeMs();
        }
        return acc;
    }

    @Override
    public MetricAggState getResult(MetricAggState acc) {
        // 计算 p50/p99(简单排序,series 内 5min 通常 ≤ 20 个 sample)
        if (!acc.samples.isEmpty()) {
            Collections.sort(acc.samples);
            acc.p50 = percentile(acc.samples, 0.50);
            acc.p99 = percentile(acc.samples, 0.99);
        }
        acc.avg = acc.sum / acc.count;
        return acc;
    }

    @Override
    public MetricAggState merge(MetricAggState a, MetricAggState b) {
        a.count += b.count;
        a.sum += b.sum;
        a.max = Math.max(a.max, b.max);
        a.min = Math.min(a.min, b.min);
        a.samples.addAll(b.samples);
        return a;
    }

    private double percentile(List<Double> sorted, double p) {
        int idx = (int) Math.ceil(p * sorted.size()) - 1;
        return sorted.get(Math.max(0, idx));
    }
}
```

> **大规模优化**:若单 series 5min 内 sample 数很多(>100),用 t-digest 替代排序(参考设计文档 §2.2.6 state 估算)。Java 库可用 [Caffeine t-digest](https://github.com/tdunning/t-digest)。

#### 8.3 窗口函数(组装 AggResult)

```java
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AggWindowFunction
        extends ProcessWindowFunction<MetricAggState, AggResult, String, TimeWindow> {

    @Override
    public void process(
            String seriesKey,
            Context ctx,
            Iterable<MetricAggState> states,
            Collector<AggResult> out) throws Exception {
        MetricAggState s = states.iterator().next();
        if (s.count == 0) return;

        AggResult r = new AggResult();
        // 窗口起始时间(UTC+8,转 datetime)
        r.setTs(windowStartToLocalDateTime(ctx.window().getStart()));
        r.setMetric(s.metric);
        r.setTenant(s.tenant);
        r.setBusiness(extractBusiness(s.labels));   // 从 labels 提取 business 字段
        r.setIngestCity(s.ingestCity);
        r.setSourceDc(s.sourceDc);
        r.setLabelsHash(LabelsHasher.xxh3(s.labels)); // XXH3 hash
        r.setLabels(s.labels);
        r.setSampleCount(s.count);
        r.setValueSum(s.sum);
        r.setValueMax(s.max);
        r.setValueMin(s.min);
        r.setValueAvg(s.avg);
        r.setValueP50(s.p50);
        r.setValueP99(s.p99);
        r.setIngestTime(new java.util.Date());       // 写入 StarRocks 时刻
        out.collect(r);
    }
}
```

---

### 9. Stream Load 写入 StarRocks

#### 9.1 方案选择

| 方案 | 实现 | 适用 |
|---|---|---|
| **官方 connector** | `flink-connector-starrocks` | 推荐,自动批量 + 重试 + at-least-once |
| 手写 HTTP | 直接调 `http://<fe-vip>:8030/api/<db>/<table>/_stream_load` | 精细控制场景 |

#### 9.2 官方 connector 配置

```java
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

StarRocksSinkOptions options = StarRocksSinkOptions.builder()
    .withProperty("jdbc-url", "jdbc:mysql://<fe-vip>:9030")
    .withProperty("load-url", "http://<fe-vip>:8030")
    .withProperty("database-name", "default_cluster:prom")
    .withProperty("table-name", "sr_bj_metrics_5m")
    .withProperty("username", "root")
    .withProperty("password", "")
    .withProperty("sink.label-prefix", "sz_5m")     // 每城唯一,避免跨城冲突
    .withProperty("sink.properties.format", "json")
    .withProperty("sink.properties.strip_outer_array", "true")
    .withProperty("sink.properties.columns",
        "ts,metric,tenant,business,ingest_city,source_dc,labels_hash," +
        "labels,sample_count,value_sum,value_max,value_min,value_avg," +
        "value_p50,value_p99,ingest_time")
    .withProperty("sink.buffer-flush.max-rows", "50000")
    .withProperty("sink.buffer-flush.max-bytes", "104857600")  // 100MB
    .withProperty("sink.buffer-flush.interval-ms", "30000")
    .withProperty("sink.max-retries", "3")
    .withProperty("sink.connect.timeout-ms", "60000")
    .build();

aggStream.addSink(StarRocksSink.sink(options));
```

#### 9.3 手写 Stream Load(精细控制场景)

```java
package com.example.promgw.sink;

import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StarRocksStreamLoadClient
 *
 * Stream Load 接口:HTTP PUT 到 /api/<db>/<table>/_stream_load
 * - Label 必须全局唯一(同 label 重试会幂等去重)
 * - 支持 gzip 压缩(Content-Encoding: gzip),跨城带宽减半
 * - PK 模型表用 MERGE 模式,自动按主键去重
 */
public class StarRocksStreamLoadClient {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksStreamLoadClient.class);

    private final String loadUrl;
    private final String user;
    private final String password;

    public StarRocksStreamLoadClient(String feVip, int port,
                                      String db, String table,
                                      String user, String password) {
        this.loadUrl = String.format("http://%s:%d/api/%s/%s/_stream_load",
            feVip, port, db, table);
        this.user = user;
        this.password = password;
    }

    public String load(String label, String jsonBody, boolean gzip) throws Exception {
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPut put = new HttpPut(loadUrl);
            put.setHeader("Authorization", basicAuth(user, password));
            put.setHeader("Label", label);
            put.setHeader("Format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("Expect", "100-continue");

            byte[] body = jsonBody.getBytes(StandardCharsets.UTF_8);
            if (gzip) {
                body = gzipCompress(body);
                put.setHeader("Content-Encoding", "gzip");
            }
            put.setEntity(new ByteArrayEntity(body));

            return client.execute(put, resp -> {
                String result = EntityUtils.toString(resp.getEntity());
                int code = resp.getStatusLine().getStatusCode();
                if (code != 200) {
                    throw new IOException("Stream Load failed: HTTP " + code + ", " + result);
                }
                return result;
            });
        }
    }

    private String basicAuth(String user, String pwd) {
        String token = user + ":" + pwd;
        return "Basic " + Base64.getEncoder().encodeToString(token.getBytes());
    }

    private byte[] gzipCompress(byte[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(data);
        }
        return bos.toByteArray();
    }
}
```

#### 9.4 Label 命名规则(关键)

Stream Load 的 `Label` 是全局唯一的去重 key。建议格式:

```
<city>_<window_start>_<business>_<labels_hash_short>
```

例:`sz_20260811_1430_app_business_a3f5e1c2`

- 同窗口重试 → 同 label,StarRocks 自动去重(at-least-once 语义)
- 不同城 → 前缀不同,避免跨城冲突
- 不同窗口 → label 不同,各自独立

---

### 10. DLQ 与重试机制

#### 10.1 DLQ Topic 设计

```
prom.<city>.dlq.sr.5m     # Stream Load 失败的批次写回本城 Kafka,等待重放
```

每城独立 DLQ,由 C 作业(重放工具)定期消费并重新写 StarRocks。

#### 10.2 失败处理策略

```java
public class StarRocksSinkWithRetry extends RichSinkFunction<AggResult> {

    private transient StarRocksStreamLoadClient client;
    private transient Producer<byte[], byte[]> dlqProducer;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        client = new StarRocksStreamLoadClient(feVip, 8030, "prom", "sr_bj_metrics_5m", user, pwd);
        // DLQ producer
        Properties p = new Properties();
        p.put("bootstrap.servers", localKafkaBrokers);
        p.put("key.serializer", "ByteArraySerializer");
        p.put("value.serializer", "ByteArraySerializer");
        dlqProducer = new KafkaProducer<>(p);
        mapper = new ObjectMapper();
    }

    @Override
    public void invoke(AggResult result, Context context) throws Exception {
        String json = mapper.writeValueAsString(result);
        String label = buildLabel(result);

        int maxRetry = 3;
        for (int i = 0; i <= maxRetry; i++) {
            try {
                String resp = client.load(label, json, true);
                LOG.debug("Stream Load ok: label={}, resp={}", label, resp);
                return;
            } catch (Exception e) {
                LOG.warn("Stream Load retry {}/{}: label={}, err={}", i, maxRetry, label, e.getMessage());
                if (i == maxRetry) {
                    // 最终失败,写 DLQ
                    sendToDlq(result, label, e.getMessage());
                    return;
                }
                Thread.sleep(1000L * (1 << i));  // 指数退避
            }
        }
    }

    private void sendToDlq(AggResult result, String label, String error) throws Exception {
        DlqMessage msg = DlqMessage.builder()
            .original(result)
            .label(label)
            .error(error)
            .retryCount(0)
            .timestamp(System.currentTimeMillis())
            .build();
        byte[] value = mapper.writeValueAsBytes(msg);
        dlqProducer.send(new ProducerRecord<>("prom.sz.dlq.sr.5m", label.getBytes(), value));
    }

    private String buildLabel(AggResult r) {
        return String.format("%s_%s_%s_%s",
            r.getIngestCity(),
            r.getTs().format("yyyyMMdd_HHmm"),
            r.getBusiness(),
            r.getLabelsHash().substring(0, 8));
    }
}
```

#### 10.3 DLQ 重放作业(运维工具)

```java
// 简单实现:消费 DLQ topic,重试 N 次,成功则提交 offset,失败则累加 retry_count
// 超过 max_retry(如 5 次)发到 dead-letter-syslog 告警
```

---

### 11. 本地开发与测试

#### 11.1 本地环境

参照 **local-dev-guide.md**(见 §10) 部署本地 Kafka + prom-gw + Prometheus,并验证 Kafka 已有数据:

```bash
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 5 --timeout-ms 10000 | xxd | head -20
# 期望:看到二进制数据(zstd + snappy + protobuf)
```

#### 11.2 Flink 本地运行

```java
// Agg5mJob.java 的 main 方法支持本地执行
public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
        new Configuration());

    // 本地配置
    String kafkaBrokers = "localhost:9092";
    String topic        = "prom.local.routed.app_business";
    String starrocksUrl = "http://localhost:8030";  // 本地 StarRocks(可选 Docker)

    buildPipeline(env, kafkaBrokers, topic, starrocksUrl, "local_5m");

    env.execute("flink-agg5m-local");
}
```

#### 11.3 验证步骤

```bash
# 1. 启动本地 Kafka + prom-gw + Prometheus(见 local-dev-guide.md §6.1)
# 2. 验证 Kafka 有数据
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic prom.local.routed.app_business
# 期望:offset > 0

# 3. 本地运行 Flink
mvn clean package
java -jar target/flink-agg5m-starrocks-1.0.0.jar --env local

# 4. 验证 StarRocks 收到数据
mysql -h <starrocks-fe> -P 9060 -u root -e "
  SELECT count(*), min(ts), max(ts)
  FROM prom.sr_bj_metrics_5m
  WHERE ingest_city = 'local';"

# 5. 验证聚合正确性(对比 Prometheus 原始值)
curl -s 'http://localhost:9090/api/v1/query?query=up' | jq
# 对比 StarRocks 中近 5 分钟的 value_avg
```

#### 11.4 单元测试

```java
public class PromWriteRequestDecoderTest {

    @Test
    void testDecodeSnappyProtobuf() throws Exception {
        // 构造一个 WriteRequest → snappy 编码
        byte[] raw = WriteRequest.newBuilder()
            .addTimeseries(TimeSeries.newBuilder()
                .addLabels(LabelPair.newBuilder().setName("__name__").setValue("up").build())
                .addLabels(LabelPair.newBuilder().setName("job").setValue("prometheus").build())
                .addSamples(Sample.newBuilder().setValue(1.0).setTimestamp(1786431389000L).build())
                .build())
            .build().toByteArray();
        byte[] snappyBytes = Snappy.compress(raw);

        // 模拟 Kafka header
        Map<String, String> headers = Map.of(
            "tenant", "app-business",
            "source_dc", "dc-local-dev",
            "ingest_city", "local",
            "ingest_time_ms", "1786431389413"
        );

        PromSample sample = new PromWriteRequestDecoder()
            .deserialize(null, snappyBytes, "test", 0L, headers);

        assertEquals("app-business", sample.getTenant());
        assertEquals("local", sample.getIngestCity());
        assertEquals(1, sample.getTimeseries().size());
        assertEquals("up", sample.getTimeseries().get(0).getMetricName());
        assertEquals(1.0, sample.getTimeseries().get(0).getSamples().get(0).getValue(), 0.001);
    }
}
```

---

### 12. 生产部署

#### 12.1 集群部署

```bash
# 1. 打包
mvn clean package -Pprod
# 产物:target/flink-agg5m-starrocks-1.0.0.jar

# 2. 提交到 Flink 集群
flink run \
  -d \                                  # detached 模式
  -p 24 \                               # 全局并行度
  -c com.example.promgw.Agg5mJob \
  /opt/flink/jobs/flink-agg5m-starrocks-1.0.0.jar \
  --env prod \
  --city sz \
  --kafka-brokers kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094 \
  --topic prom.sz.routed.app_business \
  --starrocks-url http://<beijing-fe-vip>:8030 \
  --label-prefix sz_5m \
  --dlq-topic prom.sz.dlq.sr.5m

# 3. 配置 JM HA(见生产部署文档)
```

#### 12.2 参数模板

| 参数 | 深圳示例 | 合肥示例 |
|---|---|---|
| `--city` | `sz` | `hf` |
| `--kafka-brokers` | `kafka-1.sz:9094,...` | `kafka-1.hf:9094,...` |
| `--topic` | `prom.sz.routed.app_business` | `prom.hf.routed.app_business` |
| `--starrocks-url` | `http://<beijing-fe-vip>:8030` | 同 |
| `--label-prefix` | `sz_5m` | `hf_5m` |
| `--dlq-topic` | `prom.sz.dlq.sr.5m` | `prom.hf.dlq.sr.5m` |
| 并行度 | 24(按 12 partition × 2 TM) | 8 |

#### 12.3 Checkpoint 配置(关键)

```java
env.enableCheckpointing(60_000L);  // 1min
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
env.getCheckpointConfig().setCheckpointTimeout(300_000L);
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/checkpoints/agg5m-sz");
env.setStateBackend(new EmbeddedRocksDBStateBackend());
```

#### 12.4 资源调优建议

| 维度 | 建议值 | 说明 |
|---|---|---|
| TM 内存 | 32G | t-digest state 较大 |
| TM slot | 4 | 每 TM 跑 4 个 subtask |
| RocksDB write buffer | 256MB | 减少 SST flush 频率 |
| t-digest compression | 50 | state 减半,精度可接受(见设计文档 §2.2.6) |
| 窗口允许延迟 | 30s | 超过则丢弃,走 DLQ |
| Kafka offset 提交 | 关闭自动提交,checkpoint 时提交 | at-least-once 语义 |

---

### 13. 监控与告警

#### 13.1 关键指标

| 指标 | 来源 | 告警阈值 |
|---|---|---|
| `flink_job_numRecordsInPerSecond` | Flink metrics | 突降 50% 告警 |
| `flink_job_numRecordsOutPerSecond` | Flink metrics | 突降 50% 告警 |
| `flink_job_currentEventTimeLag` | Flink metrics | > 60s 告警(消费滞后) |
| `flink_job_lastCheckpointDuration` | Flink metrics | > 60s 告警 |
| `flink_job_numFailedCheckpoints` | Flink metrics | > 0 告警 |
| Kafka consumer lag | Kafka exporter | > 10000 告警 |
| Stream Load 成功率 | 自定义 metric | < 99% 告警 |
| DLQ 消息数 | 自定义 metric | > 0 告警(需重放) |
| StarRocks 写入 QPS | StarRocks FE | 突降 50% 告警 |

#### 13.2 Prometheus 抓取配置

```yaml
- job_name: flink
  static_configs:
    - targets:
      - 'flink-jm-1.sz:9999'
      - 'flink-jm-2.sz:9999'
  metrics_path: /prom
```

#### 13.3 Grafana 看板

参考 [deploy/grafana/dashboards/prom-gw.json](../../deploy/grafana/dashboards/prom-gw.json),新增 "Flink 消费链路" 面板组,包含:
- Kafka 消费速率 + lag
- 窗口触发频率
- Stream Load 成功率 / 延迟
- Checkpoint 耗时 / 失败率
- DLQ 队列深度

---

### 14. 常见问题

| 现象 | 排查 |
|---|---|
| Flink 解码报 `Snappy decoding failed` | Kafka connector 未启用 zstd 自动解压,或消息 payload 不是 snappy 编码。确认 prom-gw 端 `compression.type=zstd` 且 Flink `value.deserializer` 正确 |
| 数据重复入 StarRocks | (1) payload hash 去重未生效;(2) Stream Load label 重复导致幂等去重未命中。检查 label 命名规则 |
| 数据丢失 | (1) checkpoint 未启用 → 重启后 offset 回滚;(2) DLQ 未消费 → 失败批次积压 |
| `currentEventTimeLag` 持续增大 | (1) Kafka 消费滞后 → 扩 partition / TM;(2) watermark 策略过严 → 调整 `boundedOutOfOrderness` |
| Checkpoint 超时 | (1) state 过大 → 调小 t-digest compression;(2) RocksDB IOPS 不足 → 用 SSD |
| Stream Load 失败率上升 | (1) StarRocks BE 压力大 → 扩 BE;(2) FE VIP 不可达 → 检查专线;(3) label 碰撞 → 调整命名 |
| 跨城专线带宽告警 | 5 min 聚合 gzip 后仍 > 专线 30% → 降级为 1h 跨城(见设计文档 §4.5) |
| Prometheus 指标值与 StarRocks 不一致 | (1) 窗口触发延迟 → 看 watermark;(2) sample stage 采样(prom-gw 端 `rate=0.1`)→ 检查 ruleset;(3) downsample stage 修改了 value → 看 ruleset 是否含 downsample |
| Kafka header 缺失 | (1) 消费 raw topic 而非 routed topic(老消息可能没 header);(2) prom-gw 版本过旧,升级到最新 |
| `labels_hash` 碰撞 | XXH3 64 位 hash 碰撞概率 < 10⁻¹⁵,实际可忽略。若必须避免,改用 SHA-1(labels 全量拼接) |

---

### 附录

#### A. 数据流完整时序

```
T+0s      Prometheus 抓取 → remote_write
T+0.1s    prom-gw 接收 → 鉴权 → ruleset 清洗 → Kafka
T+0.2s    Kafka 落盘(zstd 压缩)
T+0.3s    Flink 消费 → 解码 → 展开为 sample
T+0.3s    Flink keyBy(seriesKey) → 状态累加
T+5min    5min 窗口触发 → 计算 p50/p99
T+5min+1s Stream Load gzip → 北京 StarRocks
T+5min+2s StarRocks PK 模型 REPLACE 去重 → 落盘
T+1h      StarRocks 周期任务:5m → 1h 表
T+1d      StarRocks 周期任务:1h → 1d 表
```



---

## 7. 高可用与负载均衡 {#7-高可用与负载均衡}
> 本文档覆盖 prom-gw 生产环境的高可用架构设计、Nginx/HAProxy 负载均衡配置、Keepalived VIP 高可用、健康检查、SSL/TLS、多机房容灾、故障切换测试和运维操作。
>
> 配套文档:**生产部署指南**(见 §1)(含 LVS 方案)、**压力测试指南**(见 §8)、**SLO 指标**(见 §12)、**故障剧本**(见 §11)


---

### 1. 高可用架构设计

#### 1.1 设计目标

| 目标 | 指标 | 说明 |
|---|---|---|
| 实例可用性 | 99.95% 月度 | 单实例故障不影响服务 |
| 端到端可用性 | 99.9% 月度 | 含 Kafka 链路 |
| 故障切换时间 | < 5s | LB 健康检查 + 自动摘流 |
| 数据零丢失 | 100% | Kafka 故障降级 WAL |
| 水平扩展 | 线性 | 单机房支持 2-10 实例 |

#### 1.2 高可用分层

```
┌─────────────────────────────────────────────────────────┐
│                    客户端 (Prometheus)                     │
│                  remote_write → VIP:19201                │
└──────────────────────┬──────────────────────────────────┘
                       │
            ┌──────────▼──────────┐
            │  Keepalived VIP     │  ← 主备自动切换
            │  (10.0.1.100)       │
            └──────────┬──────────┘
                       │
         ┌─────────────▼─────────────┐
         │   Nginx / HAProxy (LB)    │  ← 4 层负载均衡
         │   nginx-lb-1 / nginx-lb-2 │  ← 主备双机
         └─────────────┬─────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼────┐
   │prom-gw-1│   │prom-gw-2│   │prom-gw-N│  ← 无状态,水平扩展
   │ :19201  │   │ :19201  │   │ :19201  │
   └────┬────┘   └────┬────┘   └────┬────┘
        │              │              │
        └──────────────┼──────────────┘
                       │
              ┌────────▼────────┐
              │  Kafka 集群     │  ← 3 副本 KRaft
              │  (3 Broker)     │
              └─────────────────┘
```

#### 1.3 无状态设计

prom-gw 设计为**无状态服务**,所有实例对等:

| 维度 | 说明 |
|---|---|
| 请求处理 | 每个实例独立处理 RemoteWrite 请求,无 session 粘性需求 |
| 配置同步 | 所有实例加载相同 ruleset/token(本地文件或 Nacos) |
| 数据投递 | 每个实例独立写 Kafka,producer 幂等 + acks=all 保证不重复 |
| WAL | 每个实例本地 WAL,故障切换时未 drain 的数据由本机启动时 replay |
| 唯一有状态部分 | downsample/deadvalue stage 的 series 状态,实例故障后重建(可接受) |

> **关键**:因 prom-gw 无状态,LB 可使用最简单的轮询策略,无需会话保持。

#### 1.4 多节点时序保证机制

负载均衡模式下,多个 prom-gw 实例并发处理同一 Prometheus 的不同请求,可能引发时序问题。prom-gw 不做全局排序,而是通过**分层保证**让时序问题在下游可解。

##### 1.4.1 同 series 顺序保证(Kafka partition 亲和)

每条 sample 的 Kafka message key 使用 `SeriesKey()`(FNV-1a 64 位 hash,对 tenant + metric + sorted labels 计算):

```
SeriesKey = FNV-1a64(tenant + \x00 + metric + \x00 + label1=val1 + \x00 + label2=val2 + \x00 + ...)
```

pipeline 逐 sample 投递时把 SeriesKey 作为 Kafka key,使同一 series 的所有 sample 落到同一 partition,partition 内严格有序。即使不同 prom-gw 节点处理了同一 series 的不同请求,Kafka 的 partition 亲和性保证最终顺序。

相关代码:
- [internal/parser/sample.go](../../internal/parser/sample.go) `SeriesKey()` 方法
- [internal/ruleengine/pipeline.go](../../internal/ruleengine/pipeline.go) 逐 sample 设置 `m.Key = seriesKey`

##### 1.4.2 跨节点重复消除(幂等 producer)

Kafka producer 默认开启幂等写(`enable.idempotence=true`),配合 `acks=all` + `retries=10`,网络重试不产生重复消息。多节点并发写同一 Kafka 不会重复。

相关代码:[internal/kafkasink/producer.go](../../internal/kafkasink/producer.go) `Idempotent` 配置(默认 true)。

##### 1.4.3 WAL 降级时的顺序保证

Kafka 不可用时降级到本地 WAL,WAL 按 segment mtime 顺序重放,Kafka 恢复后按写入顺序 drain,不破坏时序。

相关代码:[internal/wal/wal.go](../../internal/wal/wal.go) `Replay()` 方法。

##### 1.4.4 下游消费侧去重(Flink)

同一 WriteRequest 可能被 LB 分发到不同 prom-gw 节点,各节点都写入 Kafka。下游 Flink 作业按 payload hash 去重,60s 窗口内同 hash 视为重复。

相关代码:[examples/flink-agg5m-starrocks/.../DedupFunction.java](../../examples/flink-agg5m-starrocks/src/main/java/com/example/promgw/decoder/DedupFunction.java)。

##### 1.4.5 时序保证矩阵

| 场景 | 机制 | 保证级别 | 相关组件 |
|---|---|---|---|
| 同 series 的 sample 顺序 | SeriesKey → 同 partition | partition 内严格有序 | prom-gw + Kafka |
| 多节点写入重复 | Kafka 幂等 producer(默认开启) | 不重复 | prom-gw producer |
| Kafka 故障期间数据 | WAL 按 segment mtime 顺序重放 | 不丢不重 | prom-gw WAL |
| 跨 series 顺序 | 无需保证 | 各 series 独立时间戳 | - |
| 同请求多节点重复 | Flink DedupFunction(60s 窗口) | 下游去重 | Flink 消费侧 |
| 迟到数据 | Flink watermark + 窗口聚合 | 窗口内容忍 | Flink 消费侧 |

##### 1.4.6 关键结论

- **多节点 LB 部署不会引入时序问题**:每个 sample 自带 `Timestamp`(毫秒),时序由数据本身决定,不依赖到达顺序
- **不需要会话粘性**:即使同一 Prometheus 的两个请求被分到不同 prom-gw 节点,只要 SeriesKey 相同(同一 series),最终都落同一 partition,Kafka 保证顺序
- **不同 series 之间无顺序依赖**:各自独立时间线,无需全局排序
- **下游职责**:prom-gw 只保证"同 series 落同 partition 且不重复",全局排序和迟到数据处理交给 Flink(watermark + 窗口聚合)

#### 1.5 故障切换矩阵

| 故障场景 | 影响 | 切换机制 | 恢复时间 |
|---|---|---|---|
| 单个 prom-gw 实例宕机 | 流量自动分摊到其他实例 | LB 健康检查摘流 | 5-10s |
| LB 主节点宕机 | VIP 漂移到备节点 | Keepalived VRRP | 1-3s |
| Kafka Broker 宕机 | 数据写入其他副本 | Kafka 自动 leader 选举 | 10-30s |
| Kafka 集群不可用 | prom-gw 降级 WAL | 自动降级 + 恢复后 drain | 即时降级 |
| 机房故障 | 切换到灾备机房 | DNS / 全局 LB | 5-15min |
| 磁盘满 | WAL 硬拒绝 503 | 告警 + 扩容 | 人工介入 |

---

### 2. 部署拓扑

#### 2.1 单机房标准拓扑(推荐)

每机房部署:**2 台 LB(主备)+ 2-4 台 prom-gw**

```
机房 (BJ)
┌──────────────────────────────────────────────────┐
│                                                  │
│  Prometheus ──> VIP:19201 (Keepalived)           │
│                    │                             │
│          ┌─────────┴─────────┐                   │
│          │                   │                   │
│    Nginx-LB-1          Nginx-LB-2               │
│    (MASTER)           (BACKUP)                  │
│    10.0.1.101          10.0.1.102               │
│          │                   │                   │
│    ┌─────┼───────────────────┼─────┐            │
│    │     │                   │     │            │
│  prom-gw-1  prom-gw-2  prom-gw-3  prom-gw-4     │
│  10.0.1.11  10.0.1.12  10.0.1.13  10.0.1.14    │
│    │           │           │           │        │
│    └───────────┴───────────┴───────────┘        │
│                    │                             │
│              Kafka 集群 (3 Broker)               │
│              10.0.1.21/22/23                     │
└──────────────────────────────────────────────────┘
```

#### 2.2 资源规划

| 角色 | 规格 | 数量 | 说明 |
|---|---|---|---|
| Nginx LB | 4C/8G/50G | 2 | 主备,Keepalived VIP |
| prom-gw | 8C/16G/100G SSD | 2-4 | 按流量扩展,WAL 独立盘 |
| Kafka | 64C/512G/12×16T | 3 | KRaft 模式,3 副本 |

#### 2.3 端口规划

| 端口 | 组件 | 暴露范围 | LB 转发 |
|---|---|---|---|
| 19201 | prom-gw RemoteWrite | LB 后端 | VIP:19201 → 后端 :19201 |
| 8080 | prom-gw metrics | Prometheus 抓取 | 不经 LB(直连) |
| 8081 | prom-gw healthz | LB 健康检查 | LB 主动探测 |
| 8082 | prom-gw Admin | 运维网段 | 不经 LB(直连,白名单) |
| 8443 | Nginx 管理 UI(可选) | 运维网段 | - |

#### 2.4 网络隔离

```
Prometheus 网段 (10.0.1.0/28)    → 只能访问 VIP:19201
LB 网段 (10.0.1.16/28)           → 能访问 prom-gw :19201/:8081
prom-gw 网段 (10.0.1.32/27)      → 能访问 Kafka :9094
Kafka 网段 (10.0.1.64/28)        → 仅 prom-gw + Flink 可访问
运维网段 (10.0.0.0/24)           → 能访问 Admin :8082、SSH
```

---

### 3. Nginx 负载均衡配置

#### 3.1 Nginx 安装

```bash
# 安装 Nginx(需包含 stream 模块)
sudo yum install -y nginx              # CentOS/RHEL
# 或
sudo apt install -y nginx              # Ubuntu/Debian

# 验证 stream 模块
nginx -V 2>&1 | grep -o 'stream'       # 应输出 stream
```

> **编译安装**(官方源未含 stream 时):
> ```bash
> ./configure --with-stream --with-stream_ssl_module --with-http_ssl_module
> make && sudo make install
> ```

#### 3.2 4 层负载均衡(RemoteWrite TCP 转发)

prom-gw 的 RemoteWrite 是 TCP 协议(protobuf + snappy over HTTP),使用 Nginx stream 模块做 4 层转发,性能最优。

**`/etc/nginx/nginx.conf`**:

```nginx
user nginx;
worker_processes auto;
worker_rlimit_nofile 65535;

events {
    worker_connections 16384;
    use epoll;
    multi_accept on;
}

# ====== 4 层负载均衡 (RemoteWrite) ======
stream {
    log_format remote_write '$remote_addr [$time_local] '
                           'protocol=$protocol status=$status '
                           'bytes_sent=$bytes_sent bytes_received=$bytes_received '
                           'session_time=$session_time '
                           'upstream_addr=$upstream_addr '
                           'upstream_connect_time=$upstream_connect_time';

    access_log /var/log/nginx/remote_write.access.log remote_write;
    error_log  /var/log/nginx/remote_write.error.log warn;

    # upstream: prom-gw 实例池
    upstream prom_gw_backend {
        # least_conn: 最少连接数调度(比 rr 更均衡)
        least_conn;
        # 超时与失败判定
        server 10.0.1.11:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.12:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.13:19201 max_fails=3 fail_timeout=10s;
        server 10.0.1.14:19201 max_fails=3 fail_timeout=10s;
    }

    # 健康检查(主动探测,需 nginx-plus 或 nginx_upstream_check_module)
    # 开源方案:用 max_fails + fail_timeout 被动检查 + 外部 consul-template
    # 如已安装 nginx_upstream_check_module:
    # check interval=3000 rise=2 fall=3 timeout=2000 type=tcp;
    # check_http_send "GET /healthz HTTP/1.0\r\n\r\n";
    # check_http_expect_alive http_2xx;

    server {
        listen 19201;                    # 监听 RemoteWrite 端口
        proxy_pass prom_gw_backend;
        proxy_connect_timeout 3s;        # 连接 prom-gw 超时
        proxy_timeout 60s;               # 单请求超时(RemoteWrite batch 可能较大)
        proxy_buffer_size 16k;
        proxy_next_upstream on;          # 连接失败时尝试下一台
        proxy_next_upstream_tries 2;     # 最多重试 2 次
        proxy_next_upstream_timeout 5s;  # 重试总超时
    }
}
```

**关键参数说明**:

| 参数 | 值 | 说明 |
|---|---|---|
| `worker_connections` | 16384 | 单 worker 最大连接数,1.5M samples/s 约需 8000 并发连接 |
| `least_conn` | - | 最少连接数调度,比 round-robin 更均衡 |
| `max_fails` | 3 | 10s 内失败 3 次标记为 down |
| `fail_timeout` | 10s | 标记 down 后 10s 再重试 |
| `proxy_connect_timeout` | 3s | 连接后端超时,快速失败切换 |
| `proxy_timeout` | 60s | 单请求超时,大 batch 需要足够时间 |
| `proxy_next_upstream` | on | 连接失败自动重试下一台 |

#### 3.3 7 层负载均衡(Admin API / Metrics)

Admin API 和 metrics 不建议经 LB(直连更安全),但如需统一入口可用 http 模块:

```nginx
# 追加到 /etc/nginx/nginx.conf 的 http 块(与 stream 块同级)
http {
    # Admin API 负载均衡(仅运维网段访问)
    upstream prom_gw_admin {
        server 10.0.1.11:8082;
        server 10.0.1.12:8082;
        server 10.0.1.13:8082;
        server 10.0.1.14:8082;
    }

    server {
        listen 8082;
        # IP 白名单(仅运维网段)
        allow 10.0.0.0/24;
        deny all;

        location / {
            proxy_pass http://prom_gw_admin;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_connect_timeout 3s;
            proxy_read_timeout 10s;
        }
    }

    # Metrics 负载均衡(Prometheus 抓取,通常直连不经 LB)
    # 如需经 LB,用以下配置(但建议直连,便于定位单实例问题)
    upstream prom_gw_metrics {
        server 10.0.1.11:8080;
        server 10.0.1.12:8080;
        server 10.0.1.13:8080;
        server 10.0.1.14:8080;
    }

    server {
        listen 8080;
        allow 10.0.0.0/24;   # 仅 Prometheus 抓取网段
        deny all;

        location /metrics {
            proxy_pass http://prom_gw_metrics/metrics;
        }
    }
}
```

#### 3.4 Nginx 性能调优

**`/etc/sysctl.d/99-nginx.conf`**:

```ini
# 网络栈调优
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
net.ipv4.ip_local_port_range = 10000 65535

# 连接追踪
net.netfilter.nf_conntrack_max = 262144
net.ipv4.tcp_max_tw_buckets = 262144

# 缓冲区
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216

# 文件句柄
fs.file-max = 1048576
```

```bash
sudo sysctl --system
```

**Nginx worker 亲和性**(可选,高负载场景):

```nginx
worker_processes auto;
worker_cpu_affinity auto;          # 自动绑定 CPU
```

**`/etc/security/limits.d/nginx.conf`**:

```
nginx  soft  nofile  65535
nginx  hard  nofile  65535
```

#### 3.5 Nginx systemd 服务

**`/etc/systemd/system/nginx.service`**(或使用系统自带):

```ini
[Unit]
Description=Nginx Load Balancer for prom-gw
After=network.target

[Service]
Type=forking
PIDFile=/run/nginx.pid
ExecStartPre=/usr/sbin/nginx -t -c /etc/nginx/nginx.conf
ExecStart=/usr/sbin/nginx -c /etc/nginx/nginx.conf
ExecReload=/usr/sbin/nginx -s reload
ExecStop=/usr/sbin/nginx -s stop
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nginx
sudo nginx -t                    # 验证配置
```

#### 3.6 验证

```bash
# 1. Nginx 配置语法检查
sudo nginx -t

# 2. 通过 VIP 写入(模拟 Prometheus)
curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://10.0.1.100:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_prod" \
  --data-binary @payload.bin
# 期望: 200

# 3. 查看 LB 转发日志
tail -f /var/log/nginx/remote_write.access.log

# 4. 查看后端连接分布
ss -tnp | grep 19201 | awk '{print $5}' | sort | uniq -c
# 期望: 4 个后端 IP 连接数大致相等
```

---

### 4. Keepalived 高可用配置

#### 4.1 双机主备架构

```
          VIP: 10.0.1.100
            │
    ┌───────┴───────┐
    │               │
Nginx-LB-1      Nginx-LB-2
(MASTER)        (BACKUP)
priority=100    priority=90
10.0.1.101      10.0.1.102
```

- 正常时:VIP 在 LB-1,所有流量走 LB-1
- LB-1 故障:VIP 漂移到 LB-2,< 3s 切换
- LB-1 恢复:根据 `preempt` 配置决定是否抢占

#### 4.2 安装 Keepalived

```bash
sudo yum install -y keepalived     # CentOS/RHEL
# 或
sudo apt install -y keepalived     # Ubuntu/Debian

keepalived -v                      # 验证版本
```

#### 4.3 MASTER 节点配置

**`/etc/keepalived/keepalived.conf`(LB-1 / MASTER)**:

```nginx
global_defs {
    router_id NGINX_LB_BJ         # 路由标识,按机房修改
    enable_script_security         # 脚本需 root 权限
    script_user root
}

# Nginx 健康检查脚本
vrrp_script check_nginx {
    script "/etc/keepalived/check_nginx.sh"
    interval 2                     # 每 2s 检查一次
    timeout 2                      # 超时 2s
    fall 2                         # 连续失败 2 次标记 down
    rise 2                         # 连续成功 2 次标记 up
}

vrrp_instance VI_1 {
    state MASTER                   # 主节点
    interface eth0                 # 物理网卡
    virtual_router_id 51           # VRRP 组 ID(主备必须一致)
    priority 100                   # 优先级(主 > 备)
    advert_int 1                   # VRRP 通告间隔

    authentication {
        auth_type PASS
        auth_pass PromGw@2026     # 认证密码(主备一致,≤8 字符)
    }

    virtual_ipaddress {
        10.0.1.100/24              # VIP
    }

    track_script {
        check_nginx                # 关联健康检查脚本
    }

    # VIP 切换时触发通知
    notify_master "/etc/keepalived/notify.sh master"
    notify_backup "/etc/keepalived/notify.sh backup"
    notify_fault  "/etc/keepalived/notify.sh fault"
}
```

#### 4.4 BACKUP 节点配置

**`/etc/keepalived/keepalived.conf`(LB-2 / BACKUP)**:

```nginx
global_defs {
    router_id NGINX_LB_BJ
    enable_script_security
    script_user root
}

vrrp_script check_nginx {
    script "/etc/keepalived/check_nginx.sh"
    interval 2
    timeout 2
    fall 2
    rise 2
}

vrrp_instance VI_1 {
    state BACKUP                   # 备节点
    interface eth0
    virtual_router_id 51           # 必须与 MASTER 一致
    priority 90                    # 低于 MASTER
    advert_int 1

    authentication {
        auth_type PASS
        auth_pass PromGw@2026
    }

    virtual_ipaddress {
        10.0.1.100/24
    }

    track_script {
        check_nginx
    }

    notify_master "/etc/keepalived/notify.sh master"
    notify_backup "/etc/keepalived/notify.sh backup"
    notify_fault  "/etc/keepalived/notify.sh fault"
}
```

#### 4.5 健康检查脚本

**`/etc/keepalived/check_nginx.sh`**:

```bash
#!/bin/bash
# 检查 Nginx 进程是否存活
# Keepalived 通过 exit code 判断:0=健康,1=不健康

if pgrep -x nginx > /dev/null 2>&1; then
    exit 0
else
    # 尝试拉起 Nginx
    systemctl restart nginx
    sleep 2
    if pgrep -x nginx > /dev/null 2>&1; then
        exit 0
    fi
    exit 1
fi
```

```bash
sudo chmod +x /etc/keepalived/check_nginx.sh
```

#### 4.6 通知脚本

**`/etc/keepalived/notify.sh`**:

```bash
#!/bin/bash
# VIP 切换通知脚本
# 在 MASTER/BACKUP 切换时触发,发送告警

STATE=$1
HOSTNAME=$(hostname)
VIP="10.0.1.100"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

MESSAGE="[${DATE}] ${HOSTNAME} VRRP state changed to ${STATE} (VIP=${VIP})"

# 写日志
echo "${MESSAGE}" >> /var/log/keepalived-notify.log

# 发送告警(示例:调用企业微信/钉钉 webhook)
# WEBHOOK_URL="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx"
# curl -s -X POST "${WEBHOOK_URL}" \
#   -H "Content-Type: application/json" \
#   -d "{\"msgtype\":\"text\",\"text\":{\"content\":\"${MESSAGE}\"}}"

# 发送邮件(可选)
# echo "${MESSAGE}" | mail -s "Keepalived VIP switch" ops@example.com

exit 0
```

```bash
sudo chmod +x /etc/keepalived/notify.sh
```

#### 4.7 Keepalived systemd 服务

```bash
sudo systemctl enable --now keepalived
sudo systemctl status keepalived
```

#### 4.8 验证 VIP

```bash
# 在 LB-1 (MASTER) 上查看 VIP
ip addr show eth0 | grep 10.0.1.100
# 期望: inet 10.0.1.100/24 scope global eth0

# 在 LB-2 (BACKUP) 上确认无 VIP
ip addr show eth0 | grep 10.0.1.100
# 期望: 无输出

# 通过 VIP 访问
curl -sS http://10.0.1.100:8081/healthz
# 期望: {"status":"ok"}

# 模拟 MASTER 故障(在 LB-1 上)
sudo systemctl stop nginx
sleep 5

# 在 LB-2 上确认 VIP 已漂移
ip addr show eth0 | grep 10.0.1.100
# 期望: inet 10.0.1.100/24 scope global eth0

# 恢复 LB-1
sudo systemctl start nginx
sleep 5
# VIP 回到 LB-1(如配置了 preempt,默认启用)
```

#### 4.9 Keepalived 参数速查

| 参数 | MASTER | BACKUP | 说明 |
|---|---|---|---|
| `state` | MASTER | BACKUP | 初始角色 |
| `priority` | 100 | 90 | 优先级,高者获得 VIP |
| `virtual_router_id` | 51 | 51 | VRRP 组 ID,主备一致 |
| `advert_int` | 1 | 1 | 通告间隔(秒) |
| `auth_pass` | 一致 | 一致 | 认证密码 |
| `preempt` | 默认启用 | 默认启用 | MASTER 恢复后是否抢占 VIP |
| `preempt_delay` | 0 | 0 | 抢占延迟(秒),避免抖动 |

---

### 5. HAProxy 替代方案

#### 5.1 适用场景

| 方案 | 优势 | 劣势 | 推荐场景 |
|---|---|---|---|
| Nginx | 生态成熟、stream+http 统一 | 4 层健康检查需第三方模块 | 通用场景(推荐) |
| HAProxy | 原生健康检查、统计面板、4 层最强 | 无 7 层静态文件能力 | 纯 4 层高并发场景 |
| LVS | 内核态、性能最高、DR 模式 | 配置复杂、需 ARP 抑制 | 超高吞吐(>10Gbps) |

#### 5.2 HAProxy 配置

**`/etc/haproxy/haproxy.cfg`**:

```haproxy
global
    log /dev/log local0
    maxconn 65535
    user haproxy
    group haproxy
    daemon
    stats socket /run/haproxy/admin.sock mode 660 level admin

defaults
    log global
    mode tcp
    option tcplog
    option dontlognull
    retries 3
    timeout connect 3s
    timeout client  60s
    timeout server  60s
    timeout check   2s

# 4 层负载均衡: prom-gw RemoteWrite
frontend prom_gw_write
    bind *:19201
    default_backend prom_gw_backend

backend prom_gw_backend
    # 最少连接调度
    balance leastconn
    # 主动健康检查(每 2s 探测 8081/healthz)
    option httpchk GET /healthz
    http-check expect status 200
    server prom-gw-1 10.0.1.11:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-2 10.0.1.12:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-3 10.0.1.13:19201 check port 8081 inter 2s rise 2 fall 3
    server prom-gw-4 10.0.1.14:19201 check port 8081 inter 2s rise 2 fall 3

# 统计面板
listen stats
    bind *:8404
    mode http
    stats enable
    stats uri /
    stats auth admin:PromGw@2026
    stats refresh 5s
```

```bash
sudo systemctl enable --now haproxy

# 访问统计面板
# http://lb-ip:8404/  (用户名: admin, 密码: PromGw@2026)
```

#### 5.3 HAProxy + Keepalived

HAProxy 与 Keepalived 配合方式与 Nginx 完全相同,只需修改 `check_nginx.sh` 为 `check_haproxy.sh`:

```bash
#!/bin/bash
if pgrep -x haproxy > /dev/null 2>&1; then
    exit 0
else
    systemctl restart haproxy
    sleep 2
    pgrep -x haproxy > /dev/null 2>&1 && exit 0 || exit 1
fi
```

---

### 6. 健康检查机制

#### 6.1 三层健康检查

```
┌─────────────────────────────────────────────────────┐
│  Layer 1: Keepalived → Nginx 进程存活检查            │
│  (每 2s 执行 check_nginx.sh)                        │
├─────────────────────────────────────────────────────┤
│  Layer 2: Nginx → prom-gw 实例健康检查               │
│  (被动: max_fails=3 fail_timeout=10s)               │
│  (主动: nginx_upstream_check_module,可选)           │
├─────────────────────────────────────────────────────┤
│  Layer 3: prom-gw → Kafka 连通性检查                 │
│  (prom-gw 内部:Kafka 失败自动降级 WAL)              │
└─────────────────────────────────────────────────────┘
```

#### 6.2 Nginx 被动健康检查(默认)

Nginx 开源版默认使用被动健康检查:

| 参数 | 作用 | 配置 |
|---|---|---|
| `max_fails=3` | 10s 内失败 3 次标记 down | `server 10.0.1.11:19201 max_fails=3` |
| `fail_timeout=10s` | 标记 down 后 10s 重试 | `fail_timeout=10s` |
| `proxy_next_upstream` | 连接失败时重试下一台 | `proxy_next_upstream on` |

**缺点**:只有有流量时才能检测到故障,低流量时检测延迟。

#### 6.3 Nginx 主动健康检查(推荐)

安装 `nginx_upstream_check_module` 实现主动探测:

```nginx
upstream prom_gw_backend {
    least_conn;
    server 10.0.1.11:19201;
    server 10.0.1.12:19201;
    server 10.0.1.13:19201;
    server 10.0.1.14:19201;

    # 主动健康检查
    check interval=3000 rise=2 fall=3 timeout=2000 type=http;
    check_http_send "GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n";
    check_http_expect_alive http_2xx;

    # 健康检查面板(可选)
    # check_status;
}
```

安装方法:

```bash
# 下载模块
cd /tmp
wget https://github.com/yaoweibin/nginx_upstream_check_module/archive/refs/tags/v0.5.0.tar.gz
tar -xzf v0.5.0.tar.gz

# 重新编译 Nginx
cd /path/to/nginx-source
patch -p1 < /tmp/nginx_upstream_check_module-0.5.0/check_1.20.1+.patch
./configure --with-stream --add-module=/tmp/nginx_upstream_check_module-0.5.0
make && sudo make install
```

#### 6.4 外部健康检查(Consul / Prometheus)

大型部署可用 Consul + consul-template 动态管理 upstream:

```bash
# consul-template 模板
cat > /etc/consul-template/nginx-upstream.ctmpl << 'EOF'
upstream prom_gw_backend {
    least_conn;
    {{ range service "prom-gw" }}
    server {{ .Address }}:{{ .Port }} max_fails=3 fail_timeout=10s;
    {{ end }}
}
EOF

# consul-template 守护进程
consul-template \
    -consul-addr 10.0.0.10:8500 \
    -template "/etc/consul-template/nginx-upstream.ctmpl:/etc/nginx/conf.d/upstream.conf:nginx -s reload"
```

prom-gw 注册到 Consul:

```json
{
  "service": {
    "name": "prom-gw",
    "address": "10.0.1.11",
    "port": 19201,
    "check": {
      "http": "http://10.0.1.11:8081/healthz",
      "interval": "5s",
      "timeout": "2s",
      "deregister_critical_service_after": "30s"
    }
  }
}
```

---

### 7. SSL/TLS 配置

#### 7.1 mTLS 双向认证(高安全场景)

如需对 RemoteWrite 链路加密,使用 Nginx 终止 TLS:

```nginx
stream {
    upstream prom_gw_backend {
        least_conn;
        server 10.0.1.11:19201;
        server 10.0.1.12:19201;
        server 10.0.1.13:19201;
        server 10.0.1.14:19201;
    }

    server {
        listen 19201 ssl;                    # TLS 监听

        # 服务端证书
        ssl_certificate     /etc/nginx/ssl/server.crt;
        ssl_certificate_key /etc/nginx/ssl/server.key;
        ssl_protocols       TLSv1.2 TLSv1.3;
        ssl_ciphers         ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384;
        ssl_session_cache   shared:SSL:10m;
        ssl_session_timeout 10m;

        # 客户端证书验证(mTLS)
        ssl_client_certificate /etc/nginx/ssl/ca.crt;
        ssl_verify_client on;
        ssl_verify_depth 2;

        proxy_pass prom_gw_backend;
    }
}
```

#### 7.2 证书生成

```bash
# 1. 生成 CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt \
    -subj "/CN=prom-gw-ca"

# 2. 生成服务端证书
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr \
    -subj "/CN=nginx-lb"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key \
    -set_serial 01 -out server.crt

# 3. 生成客户端证书(Prometheus 侧)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr \
    -subj "/CN=prometheus"
openssl x509 -req -days 365 -in client.csr -CA ca.crt -CAkey ca.key \
    -set_serial 02 -out client.crt

# 4. 部署证书
sudo mkdir -p /etc/nginx/ssl
sudo cp ca.crt server.crt server.key /etc/nginx/ssl/
sudo chmod 600 /etc/nginx/ssl/server.key
```

#### 7.3 Prometheus 侧 mTLS 配置

```yaml
# prometheus.yml
remote_write:
  - url: https://10.0.1.100:19201/api/v1/write    # 注意 https
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    tls_config:
      ca_file: /etc/prometheus/ssl/ca.crt
      cert_file: /etc/prometheus/ssl/client.crt
      key_file: /etc/prometheus/ssl/client.key
      server_name: nginx-lb
      insecure_skip_verify: false
```

#### 7.4 性能影响

| 模式 | 吞吐影响 | 延迟增加 | CPU 开销 |
|---|---|---|---|
| 无 TLS(默认) | 基线 | 基线 | 基线 |
| TLS 终止(Nginx) | -10~15% | +2-5ms | Nginx CPU +20% |
| mTLS | -15~20% | +3-8ms | Nginx CPU +30% |

> **建议**:内网环境(同一 VPC / 专线)无需 TLS,性能优先。跨不可信网络时启用 mTLS。

---

### 8. 安全加固

#### 8.1 Nginx 限流

防止恶意流量打垮 prom-gw:

```nginx
http {
    # 限制每 IP 请求速率(RemoteWrite 流量较大,设宽松值)
    limit_req_zone $binary_remote_addr zone=remote_write:10m rate=1000r/s;

    server {
        listen 8082;
        location / {
            limit_req zone=remote_write burst=2000 nodelay;
            proxy_pass http://prom_gw_admin;
        }
    }
}
```

#### 8.2 IP 白名单

```nginx
# stream 模块 4 层 IP 白名单(需 nginx 1.19+)
stream {
    server {
        listen 19201;
        # 仅允许 Prometheus 和 LB 网段
        allow 10.0.1.0/28;      # Prometheus 网段
        allow 10.0.1.16/28;     # LB 网段
        deny all;
        proxy_pass prom_gw_backend;
    }
}
```

#### 8.3 DDoS 防护

```nginx
http {
    # 限制并发连接数
    limit_conn_zone $binary_remote_addr zone=conn_limit:10m;

    server {
        listen 8082;
        location / {
            limit_conn conn_limit 100;       # 每 IP 最多 100 并发
            limit_req zone=remote_write burst=2000 nodelay;
            proxy_pass http://prom_gw_admin;
        }
    }
}
```

#### 8.4 安全清单

| 项 | 配置 | 说明 |
|---|---|---|
| Nginx 版本隐藏 | `server_tokens off;` | 不暴露版本号 |
| 超时设置 | `proxy_connect_timeout 3s` | 快速失败 |
| 请求体限制 | `client_max_body_size 10m;` | 限制 payload 大小 |
| 日志脱敏 | 不记录 Authorization header | 避免泄露 token |
| 证书权限 | `chmod 600 *.key` | 私钥仅 root 可读 |
| 防火墙 | iptables / 安全组 | 仅开放必要端口 |

---

### 9. 多机房容灾

#### 9.1 同城双活

同城两个可用区(AZ),每个 AZ 独立部署 prom-gw 集群:

```
同城 (BJ)
┌──────────────── AZ-1 ────────────────┐  ┌──────────────── AZ-2 ────────────────┐
│                                      │  │                                      │
│  Prometheus-AZ1                      │  │  Prometheus-AZ2                      │
│      │                               │  │      │                               │
│  Nginx-LB-AZ1 (VIP-AZ1)              │  │  Nginx-LB-AZ2 (VIP-AZ2)              │
│      │                               │  │      │                               │
│  prom-gw-1, prom-gw-2               │  │  prom-gw-3, prom-gw-4               │
│      │                               │  │      │                               │
│  Kafka-AZ1, Kafka-AZ2 (同城副本)     │  │  Kafka-AZ1, Kafka-AZ2 (同城副本)     │
└──────────────────────────────────────┘  └──────────────────────────────────────┘
                        │                                │
                        └────────── Kafka 跨 AZ 复制 ─────┘
```

Prometheus 配置双 remote_write(主备):

```yaml
remote_write:
  - url: http://vip-az1:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
  - url: http://vip-az2:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
```

> prom-gw 的 Kafka producer 开启幂等写,双写消息在 Kafka 端去重。

#### 9.2 跨城灾备

三城部署,每城独立 prom-gw + Kafka,跨城专线同步:

```
北京 (主)              深圳 (灾备)            合肥 (灾备)
┌──────────┐          ┌──────────┐          ┌──────────┐
│ Prometheus│          │ Prometheus│          │ Prometheus│
│ prom-gw×4 │          │ prom-gw×4 │          │ prom-gw×2 │
│ Kafka×3   │ ←专线→   │ Kafka×3   │ ←专线→   │ Kafka×3   │
└──────────┘          └──────────┘          └──────────┘
      │                      │                      │
      └──────── Flink 跨城汇聚 → StarRocks (北京) ────┘
```

**DNS 全局负载均衡**(跨城切换):

```bash
# 正常:prom-gw.example.com → 北京 VIP
prom-gw.example.com.  60  IN  A  10.0.1.100    # 北京 VIP

# 灾备切换:DNS 指向深圳
prom-gw.example.com.  60  IN  A  10.2.1.100    # 深圳 VIP
```

#### 9.3 容灾切换流程

| 步骤 | 操作 | 负责人 | 耗时 |
|---|---|---|---|
| 1 | 确认主机房故障(健康检查 + 人工确认) | on-call | 2min |
| 2 | DNS 切换到灾备机房 VIP | 运维 | 1min |
| 3 | 等待 DNS 生效(TTL 60s) | - | 1-5min |
| 4 | 验证灾备机房 prom-gw + Kafka 健康 | 运维 | 2min |
| 5 | 验证数据链路(Prometheus → prom-gw → Kafka) | 运维 | 2min |
| 6 | 通告相关团队 | on-call | 即时 |

**总计**:5-15min(取决于 DNS TTL 和人工确认速度)

---

### 10. 故障切换测试

#### 10.1 测试矩阵

| 测试场景 | 操作 | 期望结果 | 恢复时间 |
|---|---|---|---|
| prom-gw 单实例宕机 | `systemctl stop prom-gw@bj` | LB 自动摘流,其他实例接管 | 5-10s |
| Nginx MASTER 宕机 | `systemctl stop nginx`(LB-1) | VIP 漂移到 LB-2 | 1-3s |
| Kafka 单 Broker 宕机 | `systemctl stop kafka`(Broker-1) | Kafka 自动 leader 选举 | 10-30s |
| Kafka 集群不可用 | 停所有 Kafka | prom-gw 降级 WAL,返回 200 | 即时 |
| 磁盘满 | 写满 /data/wal | prom-gw 返回 503,告警触发 | 即时 |

#### 10.2 测试脚本

**测试 1:prom-gw 单实例宕机**

```bash
#!/bin/bash
# test_failover_single_instance.sh
set -e

VIP=10.0.1.100
TARGET=prom-gw-1  # 10.0.1.11

echo "=== 测试 1: prom-gw 单实例宕机 ==="

# 1. 持续压测(后台)
go run ./test/loadgen \
    --url=http://${VIP}:19201/api/v1/write \
    --token=tk_app_business_prod \
    --rate=500000 --duration=120s &
LOADGEN_PID=$!
echo "loadgen started (pid=$LOADGEN_PID)"

# 2. 30s 后停掉一个 prom-gw 实例
sleep 30
echo ">>> 停止 ${TARGET}"
ssh ${TARGET} "sudo systemctl stop prom-gw@bj"

# 3. 观察 30s(应有短暂错误,然后恢复)
sleep 30
echo ">>> 恢复 ${TARGET}"
ssh ${TARGET} "sudo systemctl start prom-gw@bj"

# 4. 等待压测结束
wait $LOADGEN_PID

echo "=== 测试完成 ==="
echo "检查 loadgen 输出:err_batches 应在故障期间有少量增长,恢复后归零"
```

**测试 2:Nginx MASTER 宕机**

```bash
#!/bin/bash
# test_failover_nginx_master.sh
set -e

VIP=10.0.1.100
LB1=nginx-lb-1

echo "=== 测试 2: Nginx MASTER 宕机 ==="

# 1. 确认 VIP 在 LB-1
echo ">>> VIP 位置:"
ssh ${LB1} "ip addr show eth0 | grep ${VIP}" || echo "VIP not on ${LB1}"

# 2. 持续写入
for i in $(seq 1 60); do
    CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
        -X POST http://${VIP}:19201/api/v1/write \
        -H "Content-Type: application/x-protobuf" \
        -H "Content-Encoding: snappy" \
        -H "Authorization: Bearer tk_app_business_prod" \
        --data-binary @payload.bin 2>/dev/null || echo "000")
    echo "$(date +%H:%M:%S) HTTP=${CODE}"
    sleep 1
done &
WRITER_PID=$!

# 3. 10s 后停 LB-1
sleep 10
echo ">>> 停止 ${LB1} 的 nginx"
ssh ${LB1} "sudo systemctl stop nginx"

# 4. 等 VIP 漂移
sleep 5
echo ">>> 检查 VIP"
ssh nginx-lb-2 "ip addr show eth0 | grep ${VIP}" && echo "VIP moved to nginx-lb-2"

# 5. 恢复 LB-1
sleep 10
echo ">>> 恢复 ${LB1}"
ssh ${LB1} "sudo systemctl start nginx"

wait $WRITER_PID
echo "=== 测试完成 ==="
echo "期望:VIP 漂移期间有 1-3s 的连接失败,之后恢复"
```

#### 10.3 验收标准

| 测试项 | 通过标准 |
|---|---|
| 单实例宕机 | 错误率 < 1%,恢复时间 < 10s |
| LB 主备切换 | VIP 漂移 < 3s,丢连接 < 5 个 |
| Kafka 故障 | prom-gw 自动降级 WAL,无 5xx |
| 磁盘满 | 返回 503,告警触发 |
| 全链路恢复 | 5min 内所有指标恢复正常 |

---

### 11. 监控与告警

#### 11.1 Nginx 监控

安装 `nginx-prometheus-exporter`:

```bash
# 部署 exporter
docker run -d --name nginx-exporter \
    -p 9113:9113 \
    nginx/nginx-prometheus-exporter:0.11 \
    -nginx.scrape-uri=http://nginx-lb-1:8080/stub_status
```

Nginx 开启 stub_status(需编译 `--with-http_stub_status_module`):

```nginx
http {
    server {
        listen 8080;
        location /stub_status {
            stub_status;
            allow 10.0.0.0/24;   # 仅 Prometheus 抓取
            deny all;
        }
    }
}
```

#### 11.2 关键监控指标

| 指标 | PromQL | 告警阈值 |
|---|---|---|
| Nginx 活跃连接 | `nginx_connections_active` | > 10000 |
| Nginx 请求速率 | `rate(nginx_connections_total[1m])` | - |
| 后端响应时间 | `nginx_upstream_response_time_seconds` | p99 > 1s |
| 后端可用性 | `nginx_upstream_servers{state="up"}` | < 实例总数 |
| VIP 状态 | Keepalived 日志 / 自定义脚本 | VIP 不在任意节点 |
| prom-gw 错误率 | `rate(gateway_errors_total[1m])` | > 0.01% |
| prom-gw 背压 | `rate(gateway_backpressure_rejected_total[1m])` | > 0 |

#### 11.3 告警规则

**`/etc/prometheus/rules/ha-lb-alerts.yaml`**:

```yaml
groups:
  - name: ha-lb
    rules:
      # Nginx 后端实例 down
      - alert: NginxBackendDown
        expr: nginx_upstream_servers{state="up"} < 2
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Nginx 后端可用实例数 < 2"

      # Nginx 活跃连接过高
      - alert: NginxHighConnections
        expr: nginx_connections_active > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Nginx 活跃连接数 > 10000"

      # prom-gw 实例 healthz 不可达
      - alert: PromGwInstanceDown
        expr: up{job="prom-gw"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "prom-gw 实例 {{ $labels.instance }} 不可达"

      # VIP 不可达(所有 LB 均无法响应)
      - alert: VIPUnreachable
        expr: |
          count(up{job="nginx-lb"} == 0) == count(up{job="nginx-lb"})
        for: 30s
        labels:
          severity: critical
        annotations:
          summary: "所有 Nginx LB 均不可达,VIP 可能不可用"
```

#### 11.4 Grafana 大盘

导入以下 dashboard:

| Dashboard | ID | 说明 |
|---|---|---|
| Nginx Overview | 1120 | Nginx 连接/请求/响应时间 |
| HAProxy Overview | 2428 | HAProxy 统计(如使用) |
| prom-gw | 仓库内 `deploy/grafana/dashboards/prom-gw.json` | prom-gw 全量指标 |

---

### 12. 运维操作

#### 12.1 滚动升级 prom-gw

```bash
#!/bin/bash
# rolling_update_prom_gw.sh
# 逐台升级 prom-gw,LB 自动摘流

INSTANCES="prom-gw-1 prom-gw-2 prom-gw-3 prom-gw-4"
NEW_BIN=/tmp/prom-gw

for host in $INSTANCES; do
    echo "=== 升级 ${host} ==="

    # 1. LB 自动摘流(Nginx max_fails=3 fail_timeout=10s 会标记 down)
    # 2. 停实例
    ssh ${host} "sudo systemctl stop prom-gw@bj"

    # 3. 等待 in-flight 请求处理完(30s 优雅停机)
    sleep 35

    # 4. 替换二进制
    scp ${NEW_BIN} ${host}:/tmp/prom-gw
    ssh ${host} "sudo cp /tmp/prom-gw /opt/prom-gw/bin/prom-gw"

    # 5. 启动
    ssh ${host} "sudo systemctl start prom-gw@bj"

    # 6. 等待健康检查通过
    for i in $(seq 1 30); do
        if ssh ${host} "curl -fsS http://127.0.0.1:8081/healthz" 2>/dev/null; then
            echo "  ✓ ${host} healthy"
            break
        fi
        sleep 1
    done

    # 7. 观察 30s
    echo "  观察 30s..."
    sleep 30
done

echo "=== 全部升级完成 ==="
```

#### 12.2 Nginx 配置热加载

```bash
# 修改配置后(如增减后端实例)
sudo vim /etc/nginx/nginx.conf

# 语法检查
sudo nginx -t

# 热加载(不断连接)
sudo nginx -s reload
# 或
sudo systemctl reload nginx
```

#### 12.3 添加/移除 prom-gw 实例

**添加实例**:

```bash
# 1. 部署新实例(见 production-guide.md §5)
sudo systemctl enable --now prom-gw@bj

# 2. Nginx upstream 添加 server
sudo vim /etc/nginx/nginx.conf
# 在 upstream prom_gw_backend 中添加:
#   server 10.0.1.15:19201 max_fails=3 fail_timeout=10s;

# 3. 热加载
sudo nginx -t && sudo nginx -s reload

# 4. 验证连接
curl -sS http://10.0.1.15:8081/healthz
```

**移除实例**:

```bash
# 1. Nginx upstream 标记为 down(先摘流)
sudo vim /etc/nginx/nginx.conf
#   server 10.0.1.11:19201 down;
sudo nginx -s reload

# 2. 等待连接排空(观察 Nginx 日志,确认无新连接)
sleep 60

# 3. 停实例
ssh prom-gw-1 "sudo systemctl stop prom-gw@bj"

# 4. 从 upstream 删除
sudo vim /etc/nginx/nginx.conf
# 删除 server 10.0.1.11:19201 down;
sudo nginx -s reload
```

#### 12.4 VIP 手动切换

```bash
# 在 MASTER 上手动放弃 VIP(触发漂移到 BACKUP)
ssh nginx-lb-1 "sudo systemctl stop keepalived"

# 确认 VIP 已漂移
ssh nginx-lb-2 "ip addr show eth0 | grep 10.0.1.100"

# 恢复(如需切回)
ssh nginx-lb-1 "sudo systemctl start keepalived"
# 默认 preempt 模式下,VIP 会自动回到 LB-1
```

#### 12.5 常用排查命令

```bash
# 查看 VIP 位置
for lb in nginx-lb-1 nginx-lb-2; do
    echo -n "${lb}: "
    ssh ${lb} "ip addr show eth0 | grep 10.0.1.100" || echo "no VIP"
done

# 查看 Nginx 后端连接分布
ssh nginx-lb-1 "ss -tnp | grep ':19201' | awk '{print \$5}' | sort | uniq -c"

# 查看 Nginx upstream 健康状态(需 check_module)
curl -s http://nginx-lb-1:8080/upstream_status | jq .

# 查看 Keepalived 状态
ssh nginx-lb-1 "systemctl status keepalived"
ssh nginx-lb-1 "journalctl -u keepalived --since '5min ago'"

# 查看所有 prom-gw 实例健康
for host in prom-gw-1 prom-gw-2 prom-gw-3 prom-gw-4; do
    echo -n "${host}: "
    curl -fsS -m 2 http://${host}:8081/healthz 2>/dev/null && echo " OK" || echo " FAIL"
done
```

---

### 13. 附录

#### 13.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `nginx.conf` | `/etc/nginx/nginx.conf` | Nginx 主配置(stream + http) |
| `keepalived.conf` | `/etc/keepalived/keepalived.conf` | Keepalived VRRP 配置 |
| `check_nginx.sh` | `/etc/keepalived/check_nginx.sh` | Nginx 健康检查脚本 |
| `notify.sh` | `/etc/keepalived/notify.sh` | VIP 切换通知脚本 |
| `haproxy.cfg` | `/etc/haproxy/haproxy.cfg` | HAProxy 配置(替代方案) |
| `prom-gw@.service` | `/etc/systemd/system/prom-gw@.service` | prom-gw systemd template |
| `sysctl.conf` | `/etc/sysctl.d/99-nginx.conf` | 内核网络参数 |

#### 13.2 Nginx vs HAProxy vs LVS 对比

| 维度 | Nginx | HAProxy | LVS |
|---|---|---|---|
| 工作层 | 4 层 + 7 层 | 4 层 + 7 层 | 4 层(内核态) |
| 性能 | 高(10Gbps) | 高(10Gbps) | 极高(40Gbps+) |
| 健康检查 | 被动(主动需模块) | 原生主动 + 面板 | 有限 |
| 配置复杂度 | 中 | 低 | 高(需 ARP 抑制) |
| 统计面板 | 需第三方 | 内置 | 需 ipvsadm |
| TLS 终止 | 支持 | 支持 | 不支持 |
| 会话保持 | cookie(7层) | cookie(7层) | source_hash |
| 适用规模 | 中大型 | 中大型 | 超大型 |
| 推荐度 | ★★★★★ | ★★★★ | ★★★(需超高吞吐) |

#### 13.3 端口与防火墙速查

```bash
# iptables 规则示例(Nginx LB 节点)
sudo iptables -A INPUT -p tcp --dport 19201 -s 10.0.1.0/28 -j ACCEPT  # Prometheus
sudo iptables -A INPUT -p tcp --dport 19201 -s 10.0.0.0/24 -j ACCEPT  # 运维网段
sudo iptables -A INPUT -p tcp --dport 19201 -j DROP
sudo iptables -A INPUT -p tcp --dport 8082 -s 10.0.0.0/24 -j ACCEPT  # Admin
sudo iptables -A INPUT -p tcp --dport 8082 -j DROP

# 保存规则
sudo iptables-save > /etc/sysconfig/iptables
```



---

## 8. 压力测试指南 {#8-压力测试指南}
> 本文档定义 prom-gw 的压力测试方法论、数据生成方式、执行步骤和报告模板,用于发版前性能回归、容量规划和性能瓶颈定位。
>
> 配套文档:**SLO 指标**(见 §12)、**配置参数**(见 §9)、**本地部署**(见 §10)、**生产部署**(见 §1)


---

### 1. 概述

#### 1.1 测试目标

| 目标 | 说明 |
|---|---|
| 性能基线回归 | 每次发版前验证单实例吞吐 ≥ 1.5M samples/s |
| 容量规划 | 确定不同负载下所需实例数,指导扩缩容 |
| 瓶颈定位 | 通过 pprof + metrics 定位 CPU / 内存 / GC 瓶颈 |
| 稳定性验证 | 长时间运行(1h+)无内存泄漏、无 FD 泄漏、无 goroutine 泄漏 |
| 故障行为验证 | Kafka 不可用时降级 WAL、磁盘满时 503 拒绝 |

#### 1.2 SLO 基线

压力测试的判定标准依据 **SLO 文档**(见 §12):

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 吞吐 | ≥ 1.5M samples/s 单实例 | `rate(gateway_samples_total{stage="parse",status="ok"})[1m]` |
| p99 延迟 | < 500ms | `histogram_quantile(0.99, rate(gateway_request_duration_seconds_bucket[1m]))` |
| p50 延迟 | < 50ms | 同上,quantile=0.50 |
| 错误率 | < 0.01% | `rate(gateway_errors_total) / rate(gateway_samples_total)` |
| 背压拒绝率 | < 0.1% | `rate(gateway_backpressure_rejected_total) / rate(gateway_samples_total)` |
| CPU | < 70% | `gateway_cpu_ratio` |
| 内存 | < 8 GB | `gateway_mem_bytes` |
| Goroutines | < 5000 | `gateway_goroutines` |

#### 1.3 测试工具

prom-gw 自带两个压测工具,无需引入第三方依赖:

| 工具 | 路径 | 用途 |
|---|---|---|
| loadgen | [test/loadgen/main.go](../../test/loadgen/main.go) | 自研 Prometheus RemoteWrite 协议压测客户端,精确控制每请求 sample 数 |
| profile.sh | [test/perf/profile.sh](../../test/perf/profile.sh) | 一键执行压测 + CPU/heap profile 采集 + metrics 抓取 |

---

### 2. 测试方法

#### 2.1 测试类型

| 类型 | 目的 | 负载 | 时长 | 使用场景 |
|---|---|---|---|---|
| 冒烟测试 | 验证基本功能可用 | 50K samples/s | 30s | CI / 本地开发 |
| 基线回归 | 验证 SLO 达标 | 1.5M samples/s | 5min | 发版前 |
| 容量阶梯 | 寻找性能拐点 | 100K → 500K → 1M → 1.5M → 2M | 每档 3min | 容量规划 |
| 稳定性测试 | 检测内存/FD/goroutine 泄漏 | 1.5M samples/s | 1h+ | 发版前 |
| 故障注入 | 验证降级行为 | 1M samples/s | 5min | 发版前 |
| 多租户测试 | 验证限流与隔离 | 多 token 并发 | 5min | 上线前 |

#### 2.2 测试环境要求

##### 2.2.1 硬件要求

| 资源 | 最低 | 建议 | 备注 |
|---|---|---|---|
| CPU | 4 核 | 8 核 | 1.5M 吞吐需要 ≥ 4 核 |
| 内存 | 8 GB | 16 GB | SLO 上限 8G,建议预留余量 |
| 磁盘 | 50 GB SSD | 100 GB SSD | WAL 目录需独立盘,NVMe 最佳 |
| 网络 | 1 Gbps | 10 Gbps | 1.5M samples/s 压缩后约 150-200 Mbps |

##### 2.2.2 软件要求

| 组件 | 版本 | 备注 |
|---|---|---|
| Go | ≥ 1.21 | 编译 prom-gw 和 loadgen |
| Kafka | ≥ 3.4(KRaft) | 可选,WAL-only 模式可跳过 |
| curl | 任意 | 健康检查和 metrics 抓取 |
| Go pprof | 内置 | CPU/heap profile 分析 |

##### 2.2.3 网络拓扑

```
压测机 (loadgen)                被测机 (prom-gw)              下游
┌──────────────┐    HTTP     ┌──────────────────────┐    ┌────────┐
│  loadgen     │ ──────────> │  receiver :19201     │    │ Kafka  │
│  (8 并发)    │             │  metrics  :8080      │ ─> │ :9092  │
│              │             │  health   :8081      │    └────────┘
│              │             │  admin    :8082      │
│              │             │  pprof    :9090      │
└──────────────┘             └──────────────────────┘
```

> 生产环境压测时,loadgen 和 prom-gw 应部署在不同机器,避免争抢 CPU。本机测试可同机部署,但需注意 loadgen 自身 CPU 开销。

#### 2.3 采集指标

压测过程中需采集以下指标,分三类:

##### 2.3.1 客户端指标(loadgen 输出)

| 指标 | 来源 | 说明 |
|---|---|---|
| rate | loadgen stdout | 实际发送 samples/s |
| sent_batches | loadgen stdout | 已发送 batch 总数 |
| err_batches | loadgen stdout | 错误 batch 数 |
| latency p50/p95/p99/max | loadgen stdout | HTTP 请求延迟分布 |
| bytes_sent | loadgen stdout | 已发送字节数 |

##### 2.3.2 服务端指标(/metrics)

| 指标 | PromQL | 说明 |
|---|---|---|
| 解析吞吐 | `rate(gateway_samples_total{stage="parse",status="ok"}[1m])` | GW 实际处理 samples/s |
| 请求延迟 | `histogram_quantile(0.99, rate(gateway_request_duration_seconds_bucket[1m]))` | GW 侧 p99 延迟 |
| 错误计数 | `rate(gateway_errors_total[1m])` | 错误速率 |
| 背压拒绝 | `rate(gateway_backpressure_rejected_total[1m])` | 503 拒绝速率 |
| 限流拒绝 | `rate(gateway_rate_limit_rejected_total[1m])` | 429 拒绝速率 |
| WAL 占用 | `gateway_wal_bytes` | WAL 当前字节数 |
| Goroutines | `gateway_goroutines` | goroutine 数 |
| 内存 | `gateway_mem_bytes` | 驻留内存 |
| CPU | `gateway_cpu_ratio` | CPU 使用率(0-1) |

##### 2.3.3 系统指标(pprof + OS)

| 指标 | 采集方法 | 说明 |
|---|---|---|
| CPU profile | `go tool pprof http://localhost:8080/debug/pprof/profile?seconds=60` | 函数级 CPU 热点 |
| Heap profile | `curl http://localhost:8080/debug/pprof/heap -o heap.pprof` | 堆分配热点 |
| Goroutine profile | `curl http://localhost:8080/debug/pprof/goroutine -o goroutine.pprof` | goroutine 堆栈 |
| FD 数 | `lsof -p <pid> \| wc -l` | 文件描述符数 |
| 磁盘 IO | `iostat -x 1` | WAL 写入吞吐 |

#### 2.4 判定标准

| 判定项 | 通过标准 | 阻断发版 |
|---|---|---|
| 吞吐 | 持续 ≥ 1.5M samples/s | 是 |
| p99 延迟 | < 500ms | 是 |
| p50 延迟 | < 50ms | 否(告警) |
| 错误率 | < 0.01% | 是 |
| 背压拒绝率 | < 0.1% | 是 |
| 内存 | < 8 GB | 是 |
| CPU | < 70% | 否(告警) |
| Goroutines | < 5000 | 否(告警) |
| 1h 内存增长 | < 5% | 是(泄漏) |
| 1h FD 增长 | < 100 | 是(泄漏) |

---

### 3. 数据生成方式

#### 3.1 loadgen 工具

prom-gw 使用自研 loadgen 而非 vegeta/wrk,原因:

- **精确控制 sample 数**:每个 WriteRequest 携带指定数量 sample,模拟真实 Prometheus RemoteWrite 负载
- **协议完整**:构造 protobuf + snappy 压缩的 WriteRequest,与真实 Prometheus 行为一致
- **series 滚动**:预生成 series 池,worker 轮转选取,模拟真实指标滚动

#### 3.2 数据结构

loadgen 生成的每条 TimeSeries 包含以下标签:

| 标签 | 取值 | 说明 |
|---|---|---|
| `__name__` | `metric_0` ~ `metric_{series_count-1}` | 指标名 |
| `instance` | `host-{0-99}.{0-999}.example.com` | 实例标识 |
| `job` | `node` / `app` / `db` / `kafka` / `redis` | 作业名(随机 5 选 1) |

每个 sample 包含:
- `value`: 0-1000 的随机浮点数
- `timestamp`: 当前时间戳(毫秒)

#### 3.3 负载参数

loadgen 通过以下 flag 控制负载模型:

| 参数 | 默认值 | 说明 | 调优建议 |
|---|---|---|---|
| `--rate` | 100000 | 目标 samples/s | 基线测试设 1500000 |
| `--samples-per-batch` | 500 | 每个 WriteRequest 的 sample 数 | 500(模拟真实 Prometheus),大 batch 提升吞吐但增加延迟 |
| `--concurrency` | 4 | 并发 worker 数 | 4-8,过多会争抢 CPU |
| `--duration` | 30s | 压测时长 | 冒烟 30s,基线 5min,稳定性 1h |
| `--series-count` | 10000 | series 池大小 | 10000(模拟中型集群),高基数测试设 100000 |
| `--token` | `tk_app_business_dev` | Bearer token | 多租户测试切换不同 token |
| `--url` | `http://127.0.0.1:19201/api/v1/write` | RemoteWrite URL | 指向被测实例 |
| `--metrics-url` | (空) | GW metrics URL | 填写后压测结束自动拉取 GW 指标 |

#### 3.4 payload 计算

单个 WriteRequest 的 payload 大小计算:

```
series_per_batch = 10 (固定)
samples_per_series = samples_per_batch / series_per_batch

raw_bytes ≈ series_per_batch × (labels_size + samples_per_series × 16)
compressed_bytes ≈ raw_bytes × 0.4  (snappy 压缩比约 40%)
```

典型配置(`--samples-per-batch=500`):

| 项 | 值 |
|---|---|
| series_per_batch | 10 |
| samples_per_series | 50 |
| raw_bytes | ~8 KB |
| compressed_bytes | ~3-4 KB |

1.5M samples/s 时的网络带宽:

```
batches_per_sec = 1500000 / 500 = 3000
bandwidth = 3000 × 4KB = 12 MB/s ≈ 96 Mbps
```

#### 3.5 负载场景

##### 场景 1:标准基线负载

模拟真实 Prometheus 默认配置:

```bash
--rate=1500000 --samples-per-batch=500 --concurrency=8 --series-count=10000 --duration=300s
```

##### 场景 2:高基数负载

模拟大量 instance/metric,测试内存和 series 跟踪:

```bash
--rate=1000000 --samples-per-batch=500 --concurrency=8 --series-count=100000 --duration=300s
```

##### 场景 3:大 batch 负载

模拟低频大包(如联邦集群),测试单请求处理延迟:

```bash
--rate=1000000 --samples-per-batch=5000 --concurrency=4 --series-count=10000 --duration=300s
```

##### 场景 4:小 batch 高频负载

模拟边缘节点高频小包,测试 HTTP 连接和调度开销:

```bash
--rate=500000 --samples-per-batch=50 --concurrency=16 --series-count=5000 --duration=300s
```

##### 场景 5:多租户负载

模拟多租户并发,需开多个 loadgen 进程:

```bash
# 终端 1: app-business 租户
go run ./test/loadgen --token=tk_app_business_dev --rate=800000 --duration=300s &

# 终端 2: infra 租户
go run ./test/loadgen --token=tk_infra_dev --rate=500000 --duration=300s &
```

---

### 4. 测试步骤

#### 4.1 前置准备

##### 4.1.1 编译 prom-gw

```bash
cd /path/to/prom-gw
make build
# 产物: ./bin/prom-gw
```

##### 4.1.2 准备配置

使用 app-business ruleset(含 relabel + route + sample 三个 stage,覆盖典型处理链路):

```bash
# 配置文件: configs/rules/app-business.yaml
# Token 文件: configs/tokens/local.yaml
```

> 如需测试纯 WAL 模式(无 Kafka),跳过 KAFKA_BROKERS 环境变量即可。

##### 4.1.3 准备 Kafka(可选)

```bash
# 启动单节点 Kafka(KRaft 模式)
# 详见 local-dev-guide.md 第 3 章
export KAFKA_BROKERS=localhost:9092
```

##### 4.1.4 确认端口可用

| 端口 | 用途 | 检查 |
|---|---|---|
| 19201 | RemoteWrite | `lsof -i:19201` 应为空 |
| 8080 | metrics + pprof | `lsof -i:8080` 应为空 |
| 8081 | healthz | `lsof -i:8081` 应为空 |
| 8082 | admin | `lsof -i:8082` 应为空 |

#### 4.2 一键压测(profile.sh)

最便捷的方式,自动完成构建、启动、压测、采集、汇总:

```bash
# 冒烟测试(30s)
bash test/perf/profile.sh

# 基线回归(1.5M samples/s × 5min)
RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 带 Kafka 的完整压测
KAFKA_BROKERS=localhost:9092 RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 高基数测试
RATE=1000000 DURATION=300s SERIES=100000 bash test/perf/profile.sh
```

`profile.sh` 支持的环境变量:

| 变量 | 默认值 | 说明 |
|---|---|---|
| `RATE` | 500000 | 目标 samples/s |
| `DURATION` | 60s | 压测时长 |
| `CONCURRENCY` | 4 | loadgen 并发数 |
| `BATCH` | 500 | 每 batch sample 数 |
| `SERIES` | 10000 | series 池大小 |
| `GW_BIN` | `./bin/prom-gw` | prom-gw 二进制路径 |
| `CFG` | `configs/rules/app-business.yaml` | ruleset 配置 |
| `TOKENS` | `configs/tokens/local.yaml` | token 配置 |
| `WAL_DIR` | 随机临时目录 | WAL 数据目录 |
| `OUT_DIR` | `./perf-out/<timestamp>` | 输出目录 |
| `WRITE_PORT` | 19201 | RemoteWrite 端口 |
| `METRICS_PORT` | 8080 | metrics 端口 |
| `HEALTH_PORT` | 8081 | healthz 端口 |
| `ADMIN_PORT` | 8082 | admin 端口 |
| `PPROF_PORT` | 9090 | pprof 端口 |
| `KEEP_WAL` | 0 | 是否保留 WAL 数据(1=保留) |

输出文件结构:

```
perf-out/20260812-143000/
├── prom-gw.log          # prom-gw 运行日志
├── loadgen.log          # loadgen 压测输出
├── cpu.pprof            # CPU profile
├── heap.pprof           # Heap profile
├── metrics.txt          # /metrics 全量快照
└── admin-stats.json     # admin /v1/stats 快照
```

#### 4.3 手动分步压测

需要精细控制时,可手动执行各步骤:

##### 步骤 1:启动 prom-gw

```bash
# WAL-only 模式(无 Kafka)
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-wal \
    --write-addr=:19201 \
    --metrics-addr=:8080 \
    --health-addr=:8081 \
    --admin-addr=:8082 \
    --admin-allow-cidr=127.0.0.1/32 \
    --source-dc=dc-perf \
    > /tmp/prom-gw.log 2>&1 &

# 记录 PID
echo $! > /tmp/prom-gw.pid

# 等待启动
for i in $(seq 1 50); do
    curl -fsS http://127.0.0.1:8081/healthz && break
    sleep 0.2
done
```

##### 步骤 2:启动 CPU profile 采集(后台)

```bash
go tool pprof -proto -seconds=300 \
    "http://127.0.0.1:8080/debug/pprof/profile?seconds=300" \
    > /tmp/cpu.pprof &
```

##### 步骤 3:执行压测

```bash
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1500000 \
    --samples-per-batch=500 \
    --duration=300s \
    --concurrency=8 \
    --series-count=10000 \
    --metrics-url=http://127.0.0.1:8080/metrics \
    2>&1 | tee /tmp/loadgen.log
```

##### 步骤 4:采集 profile 和 metrics

```bash
# Heap profile
curl -sS http://127.0.0.1:8080/debug/pprof/heap -o /tmp/heap.pprof

# Goroutine profile
curl -sS http://127.0.0.1:8080/debug/pprof/goroutine -o /tmp/goroutine.pprof

# /metrics 全量
curl -sS http://127.0.0.1:8080/metrics -o /tmp/metrics.txt

# Admin stats
curl -sS http://127.0.0.1:8082/v1/stats -o /tmp/admin-stats.json
```

##### 步骤 5:分析 profile

```bash
# CPU 热点 top 20
go tool pprof -top -cum -nodecount=20 /tmp/cpu.pprof

# Heap 分配热点 top 20
go tool pprof -top -nodecount=20 -sample_index=alloc_space /tmp/heap.pprof

# 交互式火焰图
go tool pprof -http=:9999 /tmp/cpu.pprof
```

##### 步骤 6:清理

```bash
kill $(cat /tmp/prom-gw.pid)
rm -rf /tmp/perf-wal
```

#### 4.4 容量阶梯测试

逐步加压,找到性能拐点:

```bash
for rate in 100000 500000 1000000 1500000 2000000; do
    echo "===== RATE=$rate ====="
    RATE=$rate DURATION=180s OUT_DIR=./perf-out/staircase-$rate \
        bash test/perf/profile.sh
    sleep 10  # 冷却
done
```

每档记录以下数据,绘制吞吐-延迟曲线:

- 实际吞吐(samples/s)
- p99 延迟
- CPU 使用率
- 内存占用

拐点判定:p99 延迟开始指数增长或错误率 > 0.01% 时的 rate。

#### 4.5 稳定性测试

长时间运行,检测资源泄漏:

```bash
# 1h 稳定性测试
RATE=1500000 DURATION=3600s OUT_DIR=./perf-out/soak-1h \
    bash test/perf/profile.sh
```

压测期间每 5 分钟采集一次资源指标:

```bash
# 后台采集脚本
while true; do
    ts=$(date +%H:%M:%S)
    goroutines=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_goroutines | awk '{print $2}')
    mem=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_mem_bytes | awk '{print $2}')
    fd=$(lsof -p $(cat /tmp/prom-gw.pid) | wc -l)
    echo "$ts goroutines=$goroutines mem=$mem fd=$fd"
    sleep 300
done
```

判定标准:
- 1h 内存增长 < 5%
- 1h FD 增长 < 100
- goroutine 数稳定不持续增长

#### 4.6 故障注入测试

##### 4.6.1 Kafka 不可用降级

```bash
# 1. 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 ./bin/prom-gw ... &

# 2. 启动压测
go run ./test/loadgen --rate=1000000 --duration=300s &

# 3. 压测中途停止 Kafka
kill $(pidof kafka)

# 4. 观察:gateway_samples_total{stage="wal"} 应增长,errors 不增
# 5. 重启 Kafka,观察 WAL drain(gateway_wal_bytes 应下降)
```

##### 4.6.2 磁盘满硬拒绝

```bash
# 1. 启动 prom-gw(WAL 目录设为小盘)
./bin/prom-gw --wal-dir=/tmp/small-wal --wal-max-bytes=1073741824 ... &

# 2. 压测并停止 Kafka(迫使数据落 WAL)
# 3. 观察 WAL 填满后返回 503
# 4. metrics: gateway_wal_hard_reject_total 应 > 0
```

#### 4.7 多租户限流测试

```bash
# 启动 prom-gw(token 配置:app-business=80K/s, infra=50K/s)
./bin/prom-gw --tokens=configs/tokens/local.yaml ... &

# 同时压两个租户,各超过其限流
go run ./test/loadgen --token=tk_app_business_dev --rate=120000 --duration=60s &
go run ./test/loadgen --token=tk_infra_dev --rate=80000 --duration=60s &

# 观察:gateway_rate_limit_rejected_total{tenant="app-business"} 应增长
# app-business 超过 80K/s 的部分被 429 拒绝
```

---

### 5. 压力测试报告

#### 5.1 报告模板

每次压测完成后,按以下模板填写报告:

```markdown
# prom-gw 压力测试报告

## 测试信息
| 项 | 值 |
|---|---|
| 测试日期 | YYYY-MM-DD |
| 测试人员 | |
| prom-gw 版本 | |
| Git commit | |
| 测试类型 | 冒烟 / 基线 / 容量阶梯 / 稳定性 / 故障 |

## 环境信息
| 项 | 值 |
|---|---|
| 机器规格 | |
| 操作系统 | |
| Go 版本 | |
| Kafka 版本 | |
| 网络 | 本机 / 跨机 |

## 压测参数
| 参数 | 值 |
|---|---|
| rate | |
| samples-per-batch | |
| concurrency | |
| series-count | |
| duration | |
| token | |
| ruleset | |

## 测试结果
| 指标 | 目标 | 实测 | 判定 |
|---|---|---|---|
| 吞吐 (samples/s) | ≥ 1,500,000 | | PASS/FAIL |
| p50 延迟 | < 50ms | | PASS/FAIL |
| p99 延迟 | < 500ms | | PASS/FAIL |
| 错误率 | < 0.01% | | PASS/FAIL |
| 背压拒绝率 | < 0.1% | | PASS/FAIL |
| CPU | < 70% | | PASS/FAIL |
| 内存 | < 8 GB | | PASS/FAIL |
| Goroutines | < 5000 | | PASS/FAIL |

## Profile 分析
(CPU/Heap 热点 top 10 截图或文本)

## 结论与建议
(是否通过 / 瓶颈分析 / 改进建议)
```

#### 5.2 基线回归报告(示例)

以下为基于设计目标和 SLO 的示例报告,实际数值以压测实测为准:

---

##### prom-gw 压力测试报告 — 基线回归

**测试信息**

| 项 | 值 |
|---|---|
| 测试日期 | 2026-08-12 |
| 测试类型 | 基线回归 |
| prom-gw 版本 | v1.0.0 |
| Git commit | a1b2c3d |
| 测试模式 | WAL-only(无 Kafka) |

**环境信息**

| 项 | 值 |
|---|---|
| 机器规格 | 8C 16G,100GB SSD |
| 操作系统 | macOS 14 / Linux 8 |
| Go 版本 | 1.22 |
| 网络 | 本机回环 |

**压测参数**

| 参数 | 值 |
|---|---|
| rate | 1,500,000 samples/s |
| samples-per-batch | 500 |
| concurrency | 8 |
| series-count | 10,000 |
| duration | 300s |
| token | tk_app_business_dev |
| ruleset | app-business(relabel + route + sample) |

**测试结果**

| 指标 | 目标 | 实测 | 判定 |
|---|---|---|---|
| 吞吐 (samples/s) | ≥ 1,500,000 | 1,502,400 | PASS |
| p50 延迟 | < 50ms | 12ms | PASS |
| p95 延迟 | - | 85ms | - |
| p99 延迟 | < 500ms | 180ms | PASS |
| max 延迟 | - | 420ms | - |
| 错误率 | < 0.01% | 0.000% | PASS |
| 背压拒绝率 | < 0.1% | 0.000% | PASS |
| CPU | < 70% | 58% | PASS |
| 内存 | < 8 GB | 2.1 GB | PASS |
| Goroutines | < 5000 | 320 | PASS |

**loadgen 输出摘要**

```
=== Final ===
duration=5m0s sent_batches=9000 err_batches=0 (0.0000%) bytes=33120000000
rate=1502400 samples/s
latency p50=12ms p95=85ms p99=180ms max=420ms
samples_sent=4500000
```

**GW 侧关键指标**

```
gateway_samples_total{stage="parse",status="ok"}   4507200
gateway_request_duration_seconds{quantile="0.99"}  0.180
gateway_errors_total                               0
gateway_backpressure_rejected_total                0
gateway_goroutines                                 320
gateway_mem_bytes                                  2254857830  (~2.1GB)
gateway_cpu_ratio                                  0.58
gateway_wal_bytes                                  0           (WAL-only 无积压)
```

**Profile 分析**

CPU top 5:

```
Showing nodes accounting for 4.20s, 85.71% of 4.90s total
      flat  flat%   sum%        cum   cum%
     1.20s 24.49% 24.49%      1.80s 36.73%  github.com/lynnyq/bigdata/internal/parser.parseSeries
     0.80s 16.33% 40.82%      1.20s 24.49%  github.com/lynnyq/bigdata/internal/decoder.Decode
     0.60s 12.24% 53.06%      0.60s 12.24%  snappy.Decode
     0.40s  8.16% 61.22%      0.50s 10.20%  github.com/lynnyq/bigdata/internal/ruleengine.(*compiledStage).Apply
     0.30s  6.12% 67.35%      0.30s  6.12%  runtime.mallocgc
```

Heap top 5(alloc_space):

```
Showing nodes accounting for 8.50GB, 78.70% of 10.80GB total
      flat  flat%   sum%        cum   cum%
    3.20GB 29.63% 29.63%     3.20GB 29.63%  github.com/prometheus/prometheus/prompb.(*WriteRequest).Marshal
    2.10GB 19.44% 49.07%     2.10GB 19.44%  github.com/lynnyq/bigdata/internal/parser.parseSeries
    1.50GB 13.89% 62.96%     1.50GB 13.89%  snappy.Encode
    1.00GB  9.26% 72.22%     1.00GB  9.26%  net/http.(*Server).Serve
    0.70GB  6.48% 78.70%     0.70GB  6.48%  runtime.malg
```

**瓶颈分析**

1. **CPU 热点**:parser.parseSeries 占 24.49%,是 protobuf 反序列化的主要开销,属正常水平
2. **内存分配**:WriteRequest.Marshal 占 29.63%,可通过 sync.Pool 优化(当前未启用)
3. **无背压**:pipeline buffer 65535 足够,无 channel 满拒绝

**结论**

- 全部 SLO 指标 PASS,可发版
- 内存有余量(2.1G / 8G),可支持更高 series 基数
- 建议:后续考虑对 WriteRequest 启用 sync.Pool 降低 GC 压力

---

#### 5.3 容量阶梯报告(示例)

| rate (samples/s) | 实际吞吐 | p99 延迟 | CPU | 内存 | 错误率 | 判定 |
|---|---|---|---|---|---|---|
| 100,000 | 100,200 | 8ms | 8% | 0.8 GB | 0% | PASS |
| 500,000 | 500,100 | 25ms | 22% | 1.2 GB | 0% | PASS |
| 1,000,000 | 1,001,500 | 95ms | 42% | 1.7 GB | 0% | PASS |
| 1,500,000 | 1,502,400 | 180ms | 58% | 2.1 GB | 0% | PASS |
| 2,000,000 | 1,850,000 | 850ms | 89% | 3.5 GB | 0.12% | FAIL |

**拐点分析**:rate=2M 时 CPU 达到 89%,实际吞吐无法跟上目标(1.85M < 2M),p99 超过 500ms。单实例性能上限约 1.8-1.9M samples/s。

---

#### 5.4 稳定性报告(示例)

| 时间点 | 吞吐 (samples/s) | p99 延迟 | 内存 (GB) | Goroutines | FD |
|---|---|---|---|---|---|
| 0min | 1,502,000 | 175ms | 2.1 | 318 | 42 |
| 15min | 1,501,800 | 178ms | 2.2 | 320 | 44 |
| 30min | 1,502,100 | 172ms | 2.2 | 319 | 44 |
| 45min | 1,501,900 | 180ms | 2.3 | 321 | 45 |
| 60min | 1,502,000 | 176ms | 2.3 | 320 | 45 |

**判定**:
- 内存增长:(2.3-2.1)/2.1 = 9.5% > 5% → 需关注
- goroutine 稳定:320 ± 2 → PASS
- FD 稳定:42 → 45(+3)→ PASS

> 注:内存增长 9.5% 可能是 series 状态缓存预热,建议延长到 2h 观察是否稳定。

---

### 6. 性能分析与优化指南

#### 6.1 Profile 分析方法

##### 6.1.1 CPU profile 分析

压测结束后,CPU profile 是定位性能热点的首要工具:

```bash
# 文本模式:按累积耗时(cum)排序,展示 top 20
go tool pprof -top -cum -nodecount=20 perf-out/<latest>/cpu.pprof

# 火焰图:浏览器交互式分析
go tool pprof -http=:9999 perf-out/<latest>/cpu.pprof

# 对比两次 profile(检测回归)
go tool pprof -base perf-out/old/cpu.pprof perf-out/new/cpu.pprof
```

**CPU profile 输出解读**:

```
Showing nodes accounting for 4.20s, 85.71% of 4.90s total
      flat  flat%   sum%        cum   cum%
     1.20s 24.49% 24.49%      1.80s 36.73%  parser.parseSeries
```

| 列 | 含义 | 关注点 |
|---|---|---|
| `flat` | 函数自身耗时(不含子调用) | 高 flat = 函数本身是热点 |
| `flat%` | flat 占总 CPU 百分比 | > 20% 需重点关注 |
| `cum` | 函数含子调用的累积耗时 | 高 cum + 低 flat = 子函数是瓶颈 |
| `cum%` | cum 占总 CPU 百分比 | 用于定位调用链 |

**正常分布**(参考基线):

| 函数 | 预期 flat% | 说明 |
|---|---|---|
| `parser.parseSeries` | 20-30% | protobuf 反序列化,主要 CPU 开销 |
| `decoder.Decode` | 10-20% | snappy 解压 + protobuf 解码 |
| `snappy.Decode` | 5-15% | snappy 解压缩 |
| `ruleengine.(*Stage).Apply` | 5-10% | 规则引擎处理 |
| `runtime.mallocgc` | 3-8% | GC 开销,> 15% 需优化 |
| `net/http.(*Server).Serve` | 2-5% | HTTP 框架开销 |

**异常信号**:

| 信号 | 可能原因 | 排查方向 |
|---|---|---|
| `runtime.mallocgc` > 15% | 对象分配过多 | 检查 heap profile,考虑 sync.Pool |
| `runtime.gcBgMarkWorker` > 10% | GC 压力大 | 调整 GOGC 或减少分配 |
| `runtime.lock` / `semacquire` > 5% | 锁竞争 | 检查 mutex profile |
| `syscall.Read` / `Write` > 20% | IO 瓶颈 | 检查磁盘 / 网络 |

##### 6.1.2 Heap profile 分析

```bash
# 按分配空间排序(找出分配最多的函数)
go tool pprof -top -nodecount=20 -sample_index=alloc_space perf-out/<latest>/heap.pprof

# 按当前驻留内存排序(找出内存泄漏)
go tool pprof -top -nodecount=20 -sample_index=inuse_space perf-out/<latest>/heap.pprof

# 火焰图
go tool pprof -http=:9999 -sample_index=alloc_space perf-out/<latest>/heap.pprof
```

**关键指标**:

| sample_index | 用途 | 说明 |
|---|---|---|
| `alloc_space` | 累计分配字节数 | 找出分配热点,优化 GC |
| `inuse_space` | 当前驻留内存 | 找出内存泄漏 |
| `alloc_objects` | 累计分配对象数 | 找出小对象频繁分配 |
| `inuse_objects` | 当前驻留对象数 | 找出未释放的对象 |

**泄漏判定**:对比压测开始和结束的 `inuse_space`,如果持续增长且不回落,可能是泄漏。

##### 6.1.3 Goroutine profile 分析

```bash
# 查看 goroutine 堆栈
go tool pprof -top perf-out/<latest>/goroutine.pprof

# 查看阻塞原因(需开启 mutex/Block profile)
curl -s "http://127.0.0.1:8080/debug/pprof/block?seconds=60" -o block.pprof
go tool pprof -top block.pprof
```

**正常状态**:goroutine 数稳定在 200-500,不随流量增长。

**异常状态**:goroutine 数持续增长,可能是 channel 阻塞或 goroutine 泄漏。

#### 6.2 GC 调优

##### 6.2.1 GOGC 参数

Go GC 触发频率由 `GOGC` 控制(默认 100 = 堆翻倍时触发):

| GOGC | 效果 | 适用场景 |
|---|---|---|
| 50 | GC 频繁,CPU 开销大,内存占用低 | 内存受限 |
| 100(默认) | 平衡 | 通用 |
| 200 | GC 少,CPU 开销小,内存占用高 | 吞吐优先 |
| 400 | GC 很少,内存占用翻 4 倍 | 延迟优先(大内存机器) |
| off | 关闭 GC | 仅短时压测,生产禁用 |

**prom-gw 推荐配置**:

```bash
# 生产环境:吞吐优先,内存充裕
GOGC=200

# 内存受限环境(如 8G 机器跑满负载)
GOGC=100

# 低延迟场景(p99 < 100ms)
GOGC=150  + GOMEMLIMIT=6GiB
```

##### 6.2.2 GOMEMLIMIT 参数

Go 1.19+ 引入 `GOMEMLIMIT`,硬性限制 Go 堆上限:

```bash
# systemd 配置(已在 prom-gw@.service 中)
Environment=GOMEMLIMIT=6GiB

# 或环境变量
GOMEMLIMIT=6GiB ./bin/prom-gw ...
```

> `GOMEMLIMIT` 应设为 cgroup 内存限制的 80-90%,留余量给非 Go 内存(stack、CGO、mmap)。

##### 6.2.3 GC 监控

```bash
# 查看 GC 频率和耗时
curl -s http://127.0.0.1:8080/metrics | grep -E "go_gc_duration_seconds|go_memstats_gc"

# 关键指标
go_gc_duration_seconds{quantile="0"}     # 最小 GC 耗时
go_gc_duration_seconds{quantile="0.5"}   # 中位 GC 耗时
go_gc_duration_seconds{quantile="1"}     # 最大 GC 耗时(< 10ms 为佳)
go_memstats_gc_cpu_ratio                 # GC CPU 占比(< 0.05 为佳)
```

**GC 调优判定**:

| 指标 | 正常 | 需优化 |
|---|---|---|
| GC p99 耗时 | < 10ms | > 50ms |
| GC CPU 占比 | < 5% | > 15% |
| GC 频率 | < 1/s | > 10/s |
| Stop-the-world | < 1ms | > 10ms |

#### 6.3 CPU 调优

##### 6.3.1 GOMAXPROCS

```bash
# 默认 = CPU 核数,生产环境建议显式设置
Environment=GOMAXPROCS=8   # 在 prom-gw@.service 中

# 或环境变量
GOMAXPROCS=8 ./bin/prom-gw ...
```

**推荐值**:

| CPU 核数 | GOMAXPROCS | 说明 |
|---|---|---|
| 4 核 | 4 | 默认即可 |
| 8 核 | 8 | 1.5M samples/s 推荐 |
| 16 核 | 12-16 | 留余量给 OS / Kafka client |
| cgroup 限制 | = limit | 容器环境必须显式设置 |

##### 6.3.2 pipeline buffer 调优

prom-gw 内部 pipeline 使用 channel buffer,大小影响延迟和内存:

```yaml
# configs/rules/app-business.yaml
global:
  channel_buffer: 65535    # 默认 65535
```

| channel_buffer | 延迟 | 内存 | 背压风险 | 适用场景 |
|---|---|---|---|---|
| 1024 | 低 | 低 | 高(易 503) | 低流量 |
| 16384 | 中 | 中 | 中 | 通用 |
| 65535(默认) | 略高 | 中 | 低 | 高吞吐(推荐) |
| 262144 | 高 | 高 | 极低 | 超高吞吐 + 大内存 |

#### 6.4 内存优化

##### 6.4.1 series 基数控制

downsample / deadvalue 等状态型 stage 会缓存 series 状态,series 数直接影响内存:

```
预估内存 = series_count × avg_labels_bytes × 1.5(含 map 开销)
```

**优化建议**:

| 场景 | series 数 | 预估内存 | 建议 |
|---|---|---|---|
| 小型集群 | < 10K | < 500MB | 无需优化 |
| 中型集群 | 10K-100K | 500MB-5GB | relabel 删除无用标签 |
| 大型集群 | > 100K | > 5GB | sample 降采样 + 分租户 |

**relabel 优化示例**(减少 series 基数):

```yaml
stages:
  - type: relabel
    drop_labels:
      - instance          # 删除高基数标签
      - pod               # 删除高频变化标签
      - container_id
    keep_labels:
      - __name__
      - job
      - team
```

##### 6.4.2 采样降负载

```yaml
stages:
  - type: sample
    rate: 0.1             # 保留 10%,减少 90% 下游负载
```

| sample rate | 吞吐影响 | 内存影响 | 适用场景 |
|---|---|---|---|
| 1.0(不采样) | 基线 | 基线 | 精确监控 |
| 0.5 | -50% | -50% | 告警 + 部分监控 |
| 0.1 | -90% | -90% | 历史趋势 |
| 0.01 | -99% | -99% | 大盘统计 |

#### 6.5 性能优化决策树

```
压测未达标?
│
├─ 吞吐不足 (< 1.5M)
│   ├─ CPU > 90%? → 检查 CPU profile,优化热点函数
│   ├─ CPU < 50%? → 检查锁竞争(mutex profile)、IO 瓶颈
│   └─ 错误率 > 0? → 检查背压(pipeline buffer)、限流配置
│
├─ p99 延迟高 (> 500ms)
│   ├─ GC 耗时大? → 调整 GOGC=200、检查 heap profile
│   ├─ WAL 积压? → 检查 Kafka 连通性、磁盘 IO
│   ├─ 大 batch? → 降低 samples-per-batch
│   └─ 锁竞争? → 检查 mutex profile
│
├─ 内存超限 (> 8GB)
│   ├─ series 基数大? → 添加 relabel 删除无用标签
│   ├─ buffer 过大? → 降低 channel_buffer
│   └─ 泄漏? → 对比 heap profile,inuse_space 持续增长
│
├─ goroutine 泄漏 (> 5000)
│   ├─ channel 阻塞? → 检查 goroutine profile
│   ├─ HTTP 连接泄漏? → 检查 MaxIdleConns 配置
│   └─ safego 未退出? → 检查 context cancel
│
└─ 错误率高 (> 0.01%)
    ├─ 4xx? → token 鉴权问题、payload 格式错误
    ├─ 429? → 限流配置过低
    ├─ 503? → 背压(pipeline 满)、WAL 硬拒绝
    └─ 5xx? → 内部错误,查看日志
```

#### 6.6 调优参数速查表

| 参数 | 位置 | 默认值 | 调优方向 | 影响 |
|---|---|---|---|---|
| `GOMAXPROCS` | systemd env | CPU 核数 | = 核数 | 并行度 |
| `GOGC` | systemd env | 100 | 200(吞吐) | GC 频率 |
| `GOMEMLIMIT` | systemd env | 无 | 6GiB(8G 机器) | 内存上限 |
| `channel_buffer` | ruleset yaml | 65535 | 16384-65535 | 延迟 vs 背压 |
| `rate_limit` | token yaml | 80000 | 按租户调整 | 限流阈值 |
| `--wal-max-bytes` | CLI flag | 50GB | 100GB | WAL 容量 |
| `--wal-disk-used-ratio` | CLI flag | 0.80 | 0.85 | 磁盘硬拒绝阈值 |
| `samples-per-batch` | loadgen | 500 | 100-1000 | 压测负载模型 |
| `concurrency` | loadgen | 4 | 4-8 | 压测并发数 |

---

### 7. 更多测试场景

#### 7.1 Kafka 端到端压测

验证 prom-gw + Kafka 全链路吞吐(非 WAL-only):

##### 7.1.1 前置条件

```bash
# 启动 Kafka(单节点 KRaft,见 local-dev-guide.md 第 3 章)
export KAFKA_BROKERS=localhost:9092

# 创建测试 topic
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.perf.raw.app_business \
    --partitions 12 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.perf.routed.core \
    --partitions 12 --replication-factor 1
```

##### 7.1.2 执行压测

```bash
# 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-kafka-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 3

# 压测(1M samples/s × 3min,留余量给 Kafka)
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 \
    --samples-per-batch=500 \
    --duration=180s \
    --concurrency=8 \
    --metrics-url=http://127.0.0.1:8080/metrics
```

##### 7.1.3 验证 Kafka 消费

```bash
# 查看 topic offset(确认数据写入)
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic prom.perf.raw.app_business
# 期望: 各 partition offset 总和 ≈ rate × duration / samples_per_batch

# 消费速率测试
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic prom.perf.raw.app_business \
    --max-messages 100 \
    --timeout-ms 10000 | wc -c
# 期望: 消费到数据,每条约 3-4KB

# 清理
kill $GW_PID
rm -rf /tmp/perf-kafka-wal
```

##### 7.1.4 判定标准

| 指标 | 目标 | 说明 |
|---|---|---|
| GW 侧吞吐 | ≥ 1M samples/s | Kafka 写入不拖慢 GW |
| Kafka 写入延迟 | p99 < 50ms | `gateway_stage_duration_seconds{stage="kafka"}` |
| WAL 积压 | 0 | Kafka 正常时不应有 WAL 积压 |
| Kafka produce 错误 | 0 | `gateway_produce_errors_total` |
| 端到端延迟 | < 2s | Prometheus 写入到 Kafka 可消费 |

#### 7.2 规则引擎压测

验证 relabel + route + sample + downsample 多 stage 串联的性能影响:

##### 7.2.1 配置多 stage ruleset

```yaml
# configs/rules/perf-heavy.yaml
rulesets:
  - name: perf-heavy
    tenant: app-business
    default_topic: prom.perf.routed.app_business
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels: [env, instance, pod, container_id]
        keep_labels: [__name__, job, team, cluster]
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.perf.routed.core
          - match: { team: "infra" }
            topic: prom.perf.routed.infra
          - match: { team: "data" }
            topic: prom.perf.routed.data

      - type: sample
        rate: 0.5              # 保留 50%

      - type: downsample
        interval: 60s          # 1 分钟降采样
        aggregation: avg

      - type: deadvalue
        max_age: 300s          # 5 分钟无更新判定为死值

global:
  rate_limit_per_instance: 2000000
  channel_buffer: 65535
```

##### 7.2.2 执行压测

```bash
# 对比:空 ruleset vs 重 ruleset
echo "=== 空 ruleset ==="
CFG=configs/rules/default.yaml RATE=1500000 DURATION=120s \
    bash test/perf/profile.sh

echo "=== 重 ruleset (5 stage) ==="
CFG=configs/rules/perf-heavy.yaml RATE=1500000 DURATION=120s \
    bash test/perf/profile.sh

# 对比两次 CPU profile
go tool pprof -base perf-out/<空ruleset>/cpu.pprof \
    perf-out/<重ruleset>/cpu.pprof
```

##### 7.2.3 判定标准

| 场景 | 预期吞吐下降 | 预期 CPU 增量 | 说明 |
|---|---|---|---|
| 空 ruleset | 基线 | 基线 | 纯 parse + forward |
| relabel only | < 5% | +3-5% | 标签操作轻量 |
| relabel + route | < 8% | +5-8% | 路由匹配开销 |
| + sample | < 10% | +5-8% | 采样减少下游负载 |
| + downsample | < 20% | +10-15% | 状态维护开销大 |
| + deadvalue | < 25% | +15-20% | series 跟踪内存 + CPU |

#### 7.3 WAL drain 压测

验证 Kafka 恢复后 WAL 积压数据的 drain 速率:

##### 7.3.1 构造 WAL 积压

```bash
# 1. 启动 prom-gw(连接不存在的 Kafka,强制降级到 WAL)
KAFKA_BROKERS=127.0.0.1:9999 \
./bin/prom-gw \
    --config=configs/rules/default.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-drain-wal \
    --wal-max-bytes=5368709120 \
    --source-dc=dc-perf &

GW_PID=$!
sleep 3

# 2. 压测 60s,数据全部落 WAL
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=500000 \
    --duration=60s \
    --concurrency=4

# 3. 记录 WAL 积压量
WAL_BYTES_BEFORE=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes | awk '{print $2}')
echo "WAL 积压: ${WAL_BYTES_BEFORE} bytes"
```

##### 7.3.2 启动 Kafka 并观察 drain

```bash
# 4. 启动 Kafka(或恢复 Kafka 连接)
kafka-server-start.sh config/local.properties &
sleep 5

# 5. 重启 prom-gw,连接真实 Kafka
kill $GW_PID
sleep 2

KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
    --config=configs/rules/default.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-drain-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 2

# 6. 监控 WAL drain 过程(每 5s 采样)
START_TIME=$(date +%s)
while true; do
    WAL_BYTES=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes | awk '{print $2}')
    ELAPSED=$(( $(date +%s) - START_TIME ))
    echo "t=${ELAPSED}s wal_bytes=${WAL_BYTES}"
    if [ "$WAL_BYTES" = "0" ]; then
        echo "WAL drain 完成,耗时 ${ELAPSED}s"
        break
    fi
    sleep 5
done

# 7. 计算 drain 速率
DRAIN_RATE=$(( WAL_BYTES_BEFORE / ELAPSED ))
echo "WAL drain 速率: ${DRAIN_RATE} bytes/s"
```

##### 7.3.3 判定标准

| 指标 | 目标 | 说明 |
|---|---|---|
| drain 速率 | ≥ 50 MB/s | SSD + Kafka 正常 |
| drain 期间错误率 | 0 | drain 不影响新请求 |
| drain 期间 p99 延迟 | < 1s | drain 期间延迟会升高但可接受 |
| WAL 完全清空 | 是 | `gateway_wal_bytes` 归零 |

#### 7.4 配置热更新压测

验证高负载下 ruleset 热切换不影响请求处理:

```bash
# 1. 启动 prom-gw + 持续压测(后台)
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-hotreload-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 2

# 2. 持续压测(后台,5min)
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 --duration=300s --concurrency=8 &
LOADGEN_PID=$!

# 3. 压测中途热更新配置(修改 sample rate)
sleep 60
cp configs/rules/app-business.yaml /tmp/rules-v2.yaml
sed -i 's/rate: 0.1/rate: 0.05/' /tmp/rules-v2.yaml

# 通过 Admin API 热更新
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
    -H "Content-Type: application/yaml" \
    --data-binary @/tmp/rules-v2.yaml

echo "配置已热更新,观察压测是否中断..."

# 4. 再次热更新(改回)
sleep 60
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
    -H "Content-Type: application/yaml" \
    --data-binary @configs/rules/app-business.yaml

# 5. 等待压测结束
wait $LOADGEN_PID

# 6. 检查结果
echo "检查 loadgen 输出:热更新期间不应有错误或延迟突增"

# 清理
kill $GW_PID
rm -rf /tmp/perf-hotreload-wal /tmp/rules-v2.yaml
```

**判定标准**:
- 热更新期间错误率 = 0
- 热更新瞬间 p99 延迟毛刺 < 100ms
- `gateway_ruleset_switch_total` 计数 +1
- `gateway_config_reload_total{status="ok"}` 计数 +1

#### 7.5 长连接稳定性测试

验证 HTTP keep-alive 长连接在长时间运行下的稳定性:

```bash
# loadgen 默认使用 keep-alive(MaxIdleConns=200)
# 运行 30min,观察连接是否被重置
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 \
    --duration=1800s \
    --concurrency=8 \
    --metrics-url=http://127.0.0.1:8080/metrics
```

**监控项**:

```bash
# HTTP 连接数(应稳定,不持续增长)
ss -tnp | grep :19201 | wc -l

# TIME_WAIT 连接(应 < 1000)
ss -tn state time-wait | wc -l

# FD 数(应稳定)
lsof -p $(pidof prom-gw) | wc -l
```

---

### 8. CI/CD 性能回归集成

#### 8.1 集成方案

将性能冒烟测试集成到 GitHub Actions CI,每次 PR 自动运行,防止性能回归:

```
PR 提交
  │
  ├─ lint + unit test (已有)
  ├─ build (已有)
  └─ perf smoke test (新增)
       │
       ├─ 构建 prom-gw (带 pprof)
       ├─ 启动 prom-gw (WAL-only)
       ├─ 跑 loadgen (100K samples/s × 30s)
       ├─ 采集 metrics + profile
       └─ 判定:
            ├─ 吞吐 ≥ 100K samples/s → PASS
            ├─ p99 < 500ms → PASS
            ├─ 错误率 < 0.01% → PASS
            └─ 任一不达标 → FAIL(阻断合并)
```

#### 8.2 GitHub Actions Workflow

在 `.github/workflows/ci.yml` 追加 perf job:

```yaml
  perf:
    name: perf smoke
    runs-on: ubuntu-latest
    needs: build
    # 仅 main 分支和 PR 时触发,避免资源浪费
    if: github.event_name == 'pull_request' || github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
          cache: true

      # 下载 build job 产出的二进制
      - uses: actions/download-artifact@v4
        with:
          name: prom-gw-linux-amd64
          path: bin/

      - name: chmod +x
        run: chmod +x bin/prom-gw

      # 设置 Go 缓存目录(sandbox 兼容)
      - name: set go cache
        run: |
          echo "GOCACHE=/tmp/gocache-prom-gw" >> $GITHUB_ENV
          echo "GOMODCACHE=/tmp/gomodcache" >> $GITHUB_ENV

      # 启动 prom-gw(WAL-only 模式)
      - name: start prom-gw
        run: |
          ./bin/prom-gw \
            --config=configs/rules/app-business.yaml \
            --tokens=configs/tokens/local.yaml \
            --wal-dir=/tmp/perf-wal \
            --write-addr=:19201 \
            --metrics-addr=:8080 \
            --health-addr=:8081 \
            --admin-addr=:8082 \
            --admin-allow-cidr=127.0.0.1/32 \
            --source-dc=dc-ci \
            > /tmp/prom-gw.log 2>&1 &

          echo $! > /tmp/prom-gw.pid

          # 等待启动
          for i in $(seq 1 30); do
            curl -fsS http://127.0.0.1:8081/healthz && break
            sleep 0.5
          done

      # 执行压测
      - name: run loadgen
        id: loadgen
        run: |
          go run ./test/loadgen \
            --url=http://127.0.0.1:19201/api/v1/write \
            --token=tk_app_business_dev \
            --rate=100000 \
            --samples-per-batch=500 \
            --duration=30s \
            --concurrency=4 \
            --metrics-url=http://127.0.0.1:8080/metrics \
            2>&1 | tee /tmp/loadgen.log

      # 解析结果并判定
      - name: parse results
        run: |
          # 从 loadgen 输出解析最终结果
          RATE=$(grep "^rate=" /tmp/loadgen.log | tail -1 | awk -F'[= ]' '{print $2}')
          P99=$(grep "^latency" /tmp/loadgen.log | tail -1 | grep -oP 'p99=\K[0-9.]+')
          ERR=$(grep "err_batches" /tmp/loadgen.log | tail -1 | grep -oP '\(\K[0-9.]+')
          SENT=$(grep "sent_batches" /tmp/loadgen.log | tail -1 | grep -oP 'sent_batches=\K[0-9]+')

          echo "::notice::rate=${RATE} samples/s p99=${P99} err_rate=${ERR}%"

          # 判定
          PASS=true
          if [ -z "$RATE" ] || [ "$RATE" -lt 90000 ]; then
            echo "::error::吞吐不足: ${RATE} < 90000 samples/s"
            PASS=false
          fi
          if [ -z "$ERR" ] || (( $(echo "$ERR > 0.01" | bc -l) )); then
            echo "::error::错误率过高: ${ERR}%"
            PASS=false
          fi

          if [ "$PASS" != "true" ]; then
            echo "::error::性能冒烟测试未通过"
            exit 1
          fi
          echo "::notice::性能冒烟测试通过"

      # 上传 profile artifact(便于离线分析)
      - name: upload artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: perf-profiles
          path: |
            /tmp/prom-gw.log
            /tmp/loadgen.log

      # 清理
      - name: cleanup
        if: always()
        run: |
          kill $(cat /tmp/prom-gw.pid) 2>/dev/null || true
          rm -rf /tmp/perf-wal
```

#### 8.3 性能门禁策略

| 检查项 | CI 阈值 | 发版阈值 | 说明 |
|---|---|---|---|
| 吞吐 | ≥ 90K samples/s | ≥ 1.5M samples/s | CI 资源有限,用低阈值 |
| p99 延迟 | < 1s | < 500ms | CI 共享 runner 延迟偏高 |
| 错误率 | < 0.1% | < 0.01% | CI 允许略高 |
| 进程崩溃 | 无 | 无 | 任何 panic 阻断 |

> **注意**:CI runner 是共享虚拟机,CPU/内存/磁盘性能远不如生产物理机。CI 性能阈值远低于 SLO,仅用于检测**回归**(如代码改动导致吞吐下降 50%),不用于验证 SLO 达标。SLO 验证应在专用压测环境执行。

#### 8.4 本地 pre-push 性能检查

在本地提交前快速验证,避免 CI 等待:

```bash
# ~/.git/hooks/pre-push (创建后 chmod +x)
#!/bin/bash
echo ">>> 运行性能冒烟测试..."
make build

./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/pre-push-wal \
    --source-dc=dc-local > /tmp/prom-gw.log 2>&1 &

GW_PID=$!
sleep 2

go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=50000 --duration=10s --concurrency=2 \
    2>&1 | tee /tmp/loadgen.log

kill $GW_PID
rm -rf /tmp/pre-push-wal

# 检查是否有错误
if grep -q "err_batches=0" /tmp/loadgen.log; then
    echo ">>> 性能冒烟通过"
    exit 0
else
    echo ">>> 性能冒烟失败,检查 /tmp/loadgen.log"
    exit 1
fi
```

---

### 9. 压测报告自动化

#### 9.1 自动化脚本

以下脚本自动解析 `profile.sh` 输出,生成 Markdown 格式的压测报告:

```bash
#!/bin/bash
# scripts/gen-perf-report.sh
# 用法: bash scripts/gen-perf-report.sh <perf-out-dir>
# 生成: <perf-out-dir>/REPORT.md

set -euo pipefail

DIR="${1:-$(ls -td perf-out/*/ | head -1)}"
DIR="${DIR%/}"

if [ ! -f "$DIR/loadgen.log" ]; then
    echo "错误: $DIR/loadgen.log 不存在"
    exit 1
fi

LOG="$DIR/loadgen.log"
METRICS="$DIR/metrics.txt"
GW_LOG="$DIR/prom-gw.log"
REPORT="$DIR/REPORT.md"

# ===== 解析 loadgen 输出 =====
FINAL_LINE=$(grep "=== Final ===" -A 10 "$LOG" | tail -n +2)
RATE=$(echo "$FINAL_LINE" | grep "^rate=" | awk '{print $1}' | cut -d= -f2)
SENT_BATCHES=$(echo "$FINAL_LINE" | grep "sent_batches" | grep -oP 'sent_batches=\K[0-9]+')
ERR_BATCHES=$(echo "$FINAL_LINE" | grep "err_batches" | grep -oP 'err_batches=\K[0-9]+')
ERR_RATE=$(echo "$FINAL_LINE" | grep "err_batches" | grep -oP '\(\K[0-9.]+')
BYTES=$(echo "$FINAL_LINE" | grep "bytes=" | grep -oP 'bytes=\K[0-9]+')
LATENCY=$(echo "$FINAL_LINE" | grep "^latency")
P50=$(echo "$LATENCY" | grep -oP 'p50=\K[0-9a-z]+')
P95=$(echo "$LATENCY" | grep -oP 'p95=\K[0-9a-z]+')
P99=$(echo "$LATENCY" | grep -oP 'p99=\K[0-9a-z]+')
MAX_LAT=$(echo "$LATENCY" | grep -oP 'max=\K[0-9a-z]+')
SAMPLES_SENT=$(echo "$FINAL_LINE" | grep "samples_sent" | grep -oP 'samples_sent=\K[0-9]+')

# ===== 解析 GW metrics =====
GW_PARSE_OK=""
GW_REQ_P99=""
GW_ERRORS=""
GW_BACKPRESSURE=""
GW_GOROUTINES=""
GW_MEM=""
GW_CPU=""
GW_WAL_BYTES=""

if [ -f "$METRICS" ]; then
    GW_PARSE_OK=$(grep 'gateway_samples_total{stage="parse",status="ok"' "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_GOROUTINES=$(grep "^gateway_goroutines" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_MEM=$(grep "^gateway_mem_bytes" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_CPU=$(grep "^gateway_cpu_ratio" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_WAL_BYTES=$(grep "^gateway_wal_bytes" "$METRICS" | awk '{print $2}' | tail -1 || echo "N/A")
fi

# ===== 解析 prom-gw 版本 =====
VERSION=$(grep "starting prom-gw" "$GW_LOG" 2>/dev/null | grep -oP 'version=\\?"?\K[^" ]+' || echo "unknown")
START_TIME=$(grep "starting prom-gw" "$GW_LOG" 2>/dev/null | grep -oP 'ts=\\?"?\K[^" ]+' || echo "unknown")

# ===== 格式化辅助 =====
fmt_bytes() {
    local b=$1
    if [ -z "$b" ] || [ "$b" = "N/A" ]; then echo "N/A"; return; fi
    if [ "$b" -gt 1073741824 ]; then
        echo "$(echo "scale=2; $b / 1073741824" | bc) GB"
    elif [ "$b" -gt 1048576 ]; then
        echo "$(echo "scale=2; $b / 1048576" | bc) MB"
    elif [ "$b" -gt 1024 ]; then
        echo "$(echo "scale=2; $b / 1024" | bc) KB"
    else
        echo "$b B"
    fi
}

fmt_mem() {
    local b=$1
    if [ -z "$b" ] || [ "$b" = "N/A" ]; then echo "N/A"; return; fi
    echo "$(echo "scale=2; $b / 1073741824" | bc) GB"
}

fmt_cpu() {
    local r=$1
    if [ -z "$r" ] || [ "$r" = "N/A" ]; then echo "N/A"; return; fi
    echo "$(echo "scale=1; $r * 100" | bc)%"
}

# ===== 判定 =====
PASS=true
[ -n "$RATE" ] && [ "$RATE" -ge 1500000 ] 2>/dev/null || PASS=false
[ -n "$ERR_RATE" ] && (( $(echo "$ERR_RATE < 0.01" | bc -l) )) 2>/dev/null || PASS=false
RESULT=$([ "$PASS" = "true" ] && echo "PASS" || echo "FAIL")

# ===== 生成报告 =====
cat > "$REPORT" << EOF
# prom-gw 压力测试报告(自动生成)

## 测试信息

| 项 | 值 |
|---|---|
| 生成时间 | $(date '+%Y-%m-%d %H:%M:%S') |
| prom-gw 版本 | ${VERSION} |
| 启动时间 | ${START_TIME} |
| 数据目录 | ${DIR} |

## 压测参数

| 参数 | 值 |
|---|---|
| 目标 rate | ${RATE} samples/s |
| sent_batches | ${SENT_BATCHES} |
| err_batches | ${ERR_BATCHES} (${ERR_RATE}%) |
| bytes_sent | $(fmt_bytes "$BYTES") |
| samples_sent | ${SAMPLES_SENT} |

## 客户端结果(loadgen)

| 指标 | 值 |
|---|---|
| 实际吞吐 | ${RATE} samples/s |
| p50 延迟 | ${P50} |
| p95 延迟 | ${P95} |
| p99 延迟 | ${P99} |
| max 延迟 | ${MAX_LAT} |
| 错误率 | ${ERR_RATE}% |

## 服务端指标(/metrics)

| 指标 | 值 |
|---|---|
| 解析成功 sample 数 | ${GW_PARSE_OK} |
| Goroutines | ${GW_GOROUTINES} |
| 内存 | $(fmt_mem "$GW_MEM") |
| CPU | $(fmt_cpu "$GW_CPU") |
| WAL 积压 | $(fmt_bytes "$GW_WAL_BYTES") |

## 判定结果

**${RESULT}**

| 判定项 | 阈值 | 实测 | 结果 |
|---|---|---|---|
| 吞吐 | ≥ 1,500,000 | ${RATE} | $([ -n "$RATE" ] && [ "$RATE" -ge 1500000 ] 2>/dev/null && echo "✅" || echo "❌") |
| 错误率 | < 0.01% | ${ERR_RATE}% | $(( $(echo "${ERR_RATE:-1} < 0.01" | bc -l) ) && echo "✅" || echo "❌") |

## Profile 文件

| 文件 | 路径 |
|---|---|
| prom-gw 日志 | ${DIR}/prom-gw.log |
| loadgen 日志 | ${DIR}/loadgen.log |
| CPU profile | ${DIR}/cpu.pprof |
| Heap profile | ${DIR}/heap.pprof |
| metrics 快照 | ${DIR}/metrics.txt |

## 分析命令

\`\`\`bash
# 查看 CPU 火焰图
go tool pprof -http=:9999 ${DIR}/cpu.pprof

# 查看 Heap 分配热点
go tool pprof -top -nodecount=20 -sample_index=alloc_space ${DIR}/heap.pprof

# 对比基线
go tool pprof -base <baseline>/cpu.pprof ${DIR}/cpu.pprof
\`\`\`
EOF

echo "报告已生成: $REPORT"
echo "判定结果: $RESULT"
```

#### 9.2 使用方式

```bash
# 方式 1:压测后自动生成(集成到 profile.sh)
bash test/perf/profile.sh
bash scripts/gen-perf-report.sh perf-out/<latest>/

# 方式 2:指定目录生成
bash scripts/gen-perf-report.sh perf-out/20260812-143000/

# 方式 3:生成后直接查看
cat perf-out/20260812-143000/REPORT.md
```

#### 9.3 批量对比脚本

对比多次压测结果,生成趋势报告:

```bash
#!/bin/bash
# scripts/perf-compare.sh
# 用法: bash scripts/perf-compare.sh perf-out/run-a/ perf-out/run-b/

RUN_A="${1:?usage: $0 <run-a> <run-b>}"
RUN_B="${2:?usage: $0 <run-a> <run-b>}"

echo "# 性能对比报告"
echo ""
echo "| 指标 | Run A ($(basename $RUN_A)) | Run B ($(basename $RUN_B)) | 变化 |"
echo "|---|---|---|---|"

# 提取各指标的函数
extract_metric() {
    local dir=$1
    local pattern=$2
    grep "$pattern" "$dir/loadgen.log" | tail -1 | grep -oP "$3" || echo "N/A"
}

RATE_A=$(extract_metric "$RUN_A" "^rate=" '[0-9]+')
RATE_B=$(extract_metric "$RUN_B" "^rate=" '[0-9]+')
P99_A=$(extract_metric "$RUN_A" "latency" 'p99=\K[0-9a-z]+')
P99_B=$(extract_metric "$RUN_B" "latency" 'p99=\K[0-9a-z]+')
ERR_A=$(extract_metric "$RUN_A" "err_batches" '\(\K[0-9.]+')
ERR_B=$(extract_metric "$RUN_B" "err_batches" '\(\K[0-9.]+')

# 计算变化率
if [ "$RATE_A" != "N/A" ] && [ "$RATE_B" != "N/A" ] && [ "$RATE_A" -gt 0 ]; then
    RATE_DELTA=$(echo "scale=1; ($RATE_B - $RATE_A) * 100 / $RATE_A" | bc)
    echo "| 吞吐 | $RATE_A | $RATE_B | ${RATE_DELTA}% |"
else
    echo "| 吞吐 | $RATE_A | $RATE_B | N/A |"
fi

echo "| p99 延迟 | $P99_A | $P99_B | - |"
echo "| 错误率 | ${ERR_A}% | ${ERR_B}% | - |"

echo ""
echo "## Profile 对比"
echo ""
echo '```bash'
echo "# CPU profile diff"
echo "go tool pprof -base $RUN_A/cpu.pprof $RUN_B/cpu.pprof"
echo ""
echo "# Heap profile diff"
echo "go tool pprof -base $RUN_A/heap.pprof -sample_index=alloc_space $RUN_B/heap.pprof"
echo '```'
```

**使用**:

```bash
# 对比两次发版的性能
bash scripts/perf-compare.sh perf-out/v1.0-baseline/ perf-out/v1.1-candidate/

# 输出示例:
# | 指标 | Run A (v1.0-baseline) | Run B (v1.1-candidate) | 变化 |
# |---|---|---|---|
# | 吞吐 | 1502400 | 1485000 | -1.2% |
# | p99 延迟 | 180ms | 195ms | - |
# | 错误率 | 0.000% | 0.000% | - |
```

#### 9.4 性能回归判定

| 变化幅度 | 判定 | 动作 |
|---|---|---|
| 吞吐下降 < 5% | 正常波动 | 无需处理 |
| 吞吐下降 5-10% | 需关注 | 检查 profile,定位原因 |
| 吞吐下降 > 10% | 性能回归 | 阻断发版,必须修复 |
| p99 延迟增加 < 20% | 正常波动 | 无需处理 |
| p99 延迟增加 > 50% | 性能回归 | 阻断发版 |
| 内存增长 > 20% | 可能泄漏 | 阻断发版,检查 heap profile |

---

### 10. 附录

#### 10.1 常用命令速查

```bash
# 冒烟压测(30s)
bash test/perf/profile.sh

# 基线回归(1.5M × 5min)
RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 稳定性测试(1.5M × 1h)
RATE=1500000 DURATION=3600s bash test/perf/profile.sh

# 容量阶梯
for r in 100000 500000 1000000 1500000 2000000; do
    RATE=$r DURATION=180s bash test/perf/profile.sh; sleep 10
done

# 查看 CPU 火焰图
go tool pprof -http=:9999 perf-out/<latest>/cpu.pprof

# 实时查看 GW 指标
watch -n 1 'curl -s http://127.0.0.1:8080/metrics | grep -E "gateway_samples_total|gateway_goroutines|gateway_mem_bytes"'

# 查看 admin stats
curl -s http://127.0.0.1:8082/v1/stats | jq .
```

#### 10.2 故障排查

| 现象 | 可能原因 | 解决方法 |
|---|---|---|
| loadgen 报 `connection refused` | prom-gw 未启动或端口错误 | 检查 `curl http://127.0.0.1:8081/healthz` |
| 实际吞吐远低于 rate | CPU 打满或限流 | 检查 `gateway_cpu_ratio`、`gateway_rate_limit_rejected_total` |
| 错误率 > 0 | payload 格式错误或 token 无效 | 检查 loadgen 日志和 prom-gw 日志 |
| p99 延迟突增 | GC stop-the-world 或 WAL 积压 | 查看 `gateway_wal_bytes`、heap profile |
| 内存持续增长 | series 泄漏或 buffer 未释放 | 对比 heap profile,grep `runtime.malg` |
| goroutine 持续增长 | channel 泄漏或 goroutine 未退出 | `go tool pprof goroutine.pprof` 查看堆栈 |

#### 10.3 输出目录归档

建议按日期归档压测结果,便于版本对比:

```bash
# 归档
tar -czf perf-archive/$(date +%Y%m%d)-baseline.tar.gz \
    -C perf-out/<latest> .

# 对比两次 CPU profile
go tool pprof -base perf-archive/20260805-baseline/cpu.pprof \
    perf-out/20260812-baseline/cpu.pprof
```



---

## 9. 配置参数参考 {#9-配置参数参考}
> 本文档覆盖 prom-gw 的**全部启动参数、环境变量、配置文件(ruleset / token)、内部模块参数**,提供字段类型、默认值、取值范围、配置示例和注意事项。
>
> 配套文档:**local-dev-guide.md**(见 §10)(本地部署)、**production-guide.md**(见 §1)(生产部署)、**ruleset-reference.md**(ruleset 字段)


---

### 1. 启动命令行参数

#### 1.1 参数总览

| 参数 | 类型 | 默认值 | 必填 | 说明 |
|---|---|---|---|---|
| `--config` | string | `configs/rules/default.yaml` | 否 | ruleset 配置文件路径 |
| `--tokens` | string | `configs/tokens/local.yaml` | 否 | token 配置文件路径 |
| `--metrics-addr` | string | `:8080` | 否 | Prometheus self-export 监听地址 |
| `--health-addr` | string | `:8081` | 否 | healthz / readyz 监听地址 |
| `--write-addr` | string | `:19201` | 否 | Prometheus RemoteWrite 接入地址 |
| `--admin-addr` | string | `:8082` | 否 | Admin API 监听地址 |
| `--admin-allow-cidr` | string | `127.0.0.1/32,10.0.0.0/8` | 否 | Admin API IP 白名单(逗号分隔 CIDR) |
| `--source-dc` | string | `dc-unknown` | 否 | 本实例所属机房标识 |
| `--ingest-city` | string | `dc-unknown` | 否 | 城市标识(bj/sz/hf) |
| `--wal-dir` | string | `/data/wal` | 否 | WAL 数据目录 |
| `--wal-max-bytes` | int64 | `53687091200`(50GB) | 否 | WAL 总字节上限 |
| `--wal-disk-used-ratio` | float64 | `0.80` | 否 | WAL 所在磁盘使用率阈值(0-1) |
| `--nacos-addr` | string | `""`(空) | 否 | Nacos 服务端列表(逗号分隔) |
| `--nacos-namespace` | string | `""` | 否 | Nacos namespace id |
| `--nacos-username` | string | `""` | 否 | Nacos 用户名 |
| `--nacos-password` | string | `""` | 否 | Nacos 密码 |
| `--nacos-data-id` | string | `prom-gw-rules` | 否 | Nacos dataId |
| `--nacos-group` | string | `GATEWAY` | 否 | Nacos group |
| `--nacos-snapshot-path` | string | `/data/nacos_snapshot.json` | 否 | Nacos last-good snapshot 持久化路径 |
| `--version` | bool | `false` | 否 | 打印版本后退出 |

#### 1.2 详细说明

##### `--config`

- **类型**:string(文件路径)
- **默认**:`configs/rules/default.yaml`
- **环境变量覆盖**:`PROM_GW_CONFIG`
- **说明**:ruleset 配置文件路径,定义 prom-gw 如何清洗、路由、采样、下采样指标。支持热更新(fsnotify 5s 检测)。
- **生产建议**:按城市分目录 `configs/rules/<city>/default.yaml`
- **示例**:
  ```bash
  --config=configs/rules/bj/default.yaml
  ```

##### `--tokens`

- **类型**:string(文件路径)
- **默认**:`configs/tokens/local.yaml`
- **环境变量覆盖**:`PROM_GW_TOKENS`
- **说明**:token 鉴权配置,定义 token → tenant 映射、默认 topic、限流。支持 SIGHUP 热重载。
- **示例**:
  ```bash
  --tokens=configs/tokens/production.yaml
  ```

##### `--metrics-addr`

- **类型**:string(`host:port`)
- **默认**:`:8080`
- **说明**:暴露 `/metrics`(Prometheus 抓取)和 `/debug/pprof/*`(性能分析)。
- **生产建议**:仅对 Prometheus 和运维网段开放
- **示例**:
  ```bash
  --metrics-addr=:8080
  ```

##### `--health-addr`

- **类型**:string(`host:port`)
- **默认**:`:8081`
- **说明**:暴露 `/healthz`(200)和 `/readyz`(204)。LVS / Keepalived 探测此端口。
- **示例**:
  ```bash
  --health-addr=:8081
  ```

##### `--write-addr`

- **类型**:string(`host:port`)
- **默认**:`:19201`
- **说明**:Prometheus remote_write 的接入点,完整路径为 `http://<addr>/api/v1/write`。
- **生产建议**:通过 LVS VIP 暴露给 Prometheus
- **示例**:
  ```bash
  --write-addr=:19201
  ```

##### `--admin-addr`

- **类型**:string(`host:port`)
- **默认**:`:8082`
- **说明**:Admin API 监听地址,提供 ruleset 热更新、stats、tenants 等查询。
- **安全**:**必须**通过 `--admin-allow-cidr` 限制来源 IP,默认仅本机
- **示例**:
  ```bash
  --admin-addr=:8082 --admin-allow-cidr=127.0.0.1/32,10.0.0.0/8
  ```

##### `--admin-allow-cidr`

- **类型**:string(逗号分隔 CIDR)
- **默认**:`127.0.0.1/32,10.0.0.0/8`
- **说明**:Admin API 白名单。仅匹配 CIDR 的来源 IP 可访问。
- **生产建议**:收紧为运维网段,如 `10.10.0.0/16`
- **示例**:
  ```bash
  --admin-allow-cidr=127.0.0.1/32,10.10.0.0/16
  ```

##### `--source-dc`

- **类型**:string
- **默认**:`dc-unknown`
- **说明**:本实例所属机房标识,写入每条消息的 Kafka header `source_dc` 和 `ingest_dc`。也用于指标 `gateway_*{source_dc=...}`。
- **生产建议**:按实际机房命名,如 `dc-bj-dongba`、`dc-sz-wulian`
- **示例**:
  ```bash
  --source-dc=dc-bj-dongba
  ```

##### `--ingest-city`

- **类型**:string
- **默认**:`dc-unknown`
- **环境变量覆盖**:`INGEST_CITY`
- **说明**:城市标识(`bj`/`sz`/`hf`),写入 Kafka header `ingest_city`,用于指标分片和 StarRocks 城市切片。
- **生产建议**:由 systemd 通过 `Environment=INGEST_CITY=bj` 注入,无需 flag 显式指定
- **示例**:
  ```bash
  --ingest-city=bj
  # 或环境变量
  INGEST_CITY=bj ./prom-gw
  ```

##### `--wal-dir`

- **类型**:string(目录路径)
- **默认**:`/data/wal`
- **说明**:Kafka 故障时数据落盘目录。建议挂载独立 SSD,`noatime` 挂载。
- **本地开发**:`/tmp/prom-gw-local-wal`
- **生产建议**:`/data/wal`(独立盘)
- **示例**:
  ```bash
  --wal-dir=/data/wal
  ```

##### `--wal-max-bytes`

- **类型**:int64(字节数)
- **默认**:`53687091200`(50GB)
- **说明**:WAL 总字节上限。达到上限后,**新请求返回 HTTP 503**(背压)。
- **本地开发**:`1073741824`(1GB)
- **生产建议**:50GB(默认),Kafka 恢复后自动 drain
- **示例**:
  ```bash
  --wal-max-bytes=53687091200
  ```

##### `--wal-disk-used-ratio`

- **类型**:float64(0.0-1.0)
- **默认**:`0.80`(80%)
- **说明**:WAL 所在磁盘使用率硬阈值。达到后切硬拒绝(503)。与 `--wal-max-bytes` 为**双阈值机制**,任一触发即拒绝。
- **生产建议**:0.80(默认),磁盘紧张可调到 0.70
- **示例**:
  ```bash
  --wal-disk-used-ratio=0.80
  ```

##### `--nacos-addr`

- **类型**:string(逗号分隔 `ip:port`)
- **默认**:`""`(空,不启用)
- **说明**:Nacos 配置中心地址。空则仅用本地文件源。配置后 ruleset 可从 Nacos 远程拉取并热更新。
- **示例**:
  ```bash
  --nacos-addr=10.0.0.1:8848,10.0.0.2:8848,10.0.0.3:8848
  ```

##### `--nacos-namespace` / `--nacos-username` / `--nacos-password`

- **类型**:string
- **默认**:空
- **说明**:Nacos 鉴权信息。生产环境必须配置账号密码。
- **示例**:
  ```bash
  --nacos-namespace=production --nacos-username=prom-gw --nacos-password=<secret>
  ```

##### `--nacos-data-id` / `--nacos-group`

- **类型**:string
- **默认**:`prom-gw-rules` / `GATEWAY`
- **说明**:Nacos 中 ruleset 配置的 dataId 和 group。
- **示例**:
  ```bash
  --nacos-data-id=prom-gw-rules-bj --nacos-group=GATEWAY
  ```

##### `--nacos-snapshot-path`

- **类型**:string(文件路径)
- **默认**:`/data/nacos_snapshot.json`
- **说明**:last-good snapshot 持久化路径。Nacos 拉取成功后写本地快照,Nacos 不可用时从快照恢复。空则不持久化。
- **示例**:
  ```bash
  --nacos-snapshot-path=/data/nacos_snapshot.json
  ```

##### `--version`

- **类型**:bool
- **默认**:`false`
- **说明**:打印版本后退出。版本号由 Makefile 通过 `-ldflags` 注入。
- **示例**:
  ```bash
  ./prom-gw --version
  # 输出: prom-gw v1.2.3
  ```

---

### 2. 环境变量

#### 2.1 环境变量总览

| 环境变量 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `KAFKA_BROKERS` | string | 空 | Kafka broker 列表(逗号分隔)。**空 = 进入 WAL-only 模式** |
| `INGEST_CITY` | string | `dc-unknown` | 城市标识(bj/sz/hf),可被 `--ingest-city` 覆盖 |
| `PROM_GW_CONFIG` | string | `configs/rules/default.yaml` | ruleset 配置路径,可被 `--config` 覆盖 |
| `PROM_GW_TOKENS` | string | `configs/tokens/local.yaml` | token 配置路径,可被 `--tokens` 覆盖 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | 空 | OpenTelemetry OTLP 接收端。空 = tracing 降级为 noop |

#### 2.2 详细说明

##### `KAFKA_BROKERS`

- **类型**:string(逗号分隔 `host:port`)
- **默认**:空
- **行为**:
  - 空 → 进入 **WAL-only 模式**(数据只落本地 WAL,不投递 Kafka)
  - 非空但连不上 → 自动降级到 WAL-only,日志输出 `kafka connect failed`
  - 非空且连上 → 正常模式,故障时降级到 WAL
- **示例**:
  ```bash
  KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 ./prom-gw
  # 生产(SSL/SASL)
  KAFKA_BROKERS=kafka-1:9094,kafka-2:9094,kafka-3:9094 ./prom-gw
  ```

##### `INGEST_CITY`

- **类型**:string
- **默认**:`dc-unknown`
- **说明**:由 systemd 通过 `Environment=INGEST_CITY=bj` 注入,可被 `--ingest-city` flag 覆盖(flag 优先)。
- **取值**:`bj` / `sz` / `hf` / 自定义
- **示例**:
  ```bash
  # systemd 单元
  [Service]
  Environment=INGEST_CITY=bj
  ```

##### `PROM_GW_CONFIG` / `PROM_GW_TOKENS`

- **类型**:string(文件路径)
- **说明**:与 `--config` / `--tokens` flag 等价。flag 优先级 > env。
- **用途**:容器化部署时避免改启动命令
- **示例**:
  ```bash
  PROM_GW_CONFIG=/etc/prom-gw/rules.yaml \
  PROM_GW_TOKENS=/etc/prom-gw/tokens.yaml \
  ./prom-gw
  ```

##### `OTEL_EXPORTER_OTLP_ENDPOINT`

- **类型**:string(URL)
- **默认**:空
- **说明**:OpenTelemetry OTLP/gRPC 接收端,如 `http://otel-collector:4317`。空时 tracing 降级为 noop,不发送 span。
- **示例**:
  ```bash
  OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability:4317 ./prom-gw
  ```

---

### 3. Ruleset 配置文件

#### 3.1 顶层结构

```yaml
rulesets:        # 规则集列表(支持多 ruleset 并行)
  - name: ...
    ...
global:          # 全局参数
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

#### 3.2 顶层字段

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `rulesets` | array | 否 | `[]` | 规则集列表。空数组 = 透传模式(只接收不清洗) |
| `global` | object | 否 | 见下 | 全局参数 |

#### 3.3 `global` 字段

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `rate_limit_per_instance` | int | `100000` | 单实例 samples/s 上限。超过返回 429 |
| `channel_buffer` | int | `65535` | sink pipeline 内部 channel 容量。满了返回 503 |

**示例**:
```yaml
global:
  rate_limit_per_instance: 200000   # 高吞吐场景调高
  channel_buffer: 131072            # 缓解背压
```

#### 3.4 `rulesets[]` 字段

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `name` | string | ✓ | — | ruleset 唯一名,用于 Admin API 路径 |
| `tenant` | string | 否 | `""` | 适用租户(多租户预留,v1 全局生效) |
| `input_topic` | string | 否 | `""` | 输入 topic 标记(仅文档用,运行期不参与逻辑) |
| `default_topic` | string | ✓ | — | 没路由命中时的兜底 topic |
| `match` | object | 否 | `{}`(全量) | metric 命中条件 |
| `stages` | array | 否 | `[]`(透传) | 处理阶段列表 |
| `version` | int | ✓ | — | 单调递增版本号 |

#### 3.5 `match` 字段

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `metric_prefix` | string | `""` | metric 名称前缀匹配。空 = 全量接收 |
| `metric_exact` | string | `""` | metric 精确匹配。**优先级 > metric_prefix** |

**匹配规则**:
- 两个字段都空 → 全量接收
- `metric_exact` 非空 → 仅当 `metric == metric_exact` 时接管
- 否则 `metric_prefix` 非空 → 仅当 `metric.HasPrefix(prefix)` 时接管

**示例**:
```yaml
match:
  metric_prefix: "app_"      # 接管 app_* 指标
  # metric_exact: "up"       # 仅接管 up 指标
```

#### 3.6 `stages[]` 字段

每个 stage 有:
- `type`:阶段类型(见下表)
- 其他字段:按 type 不同而不同,**支持 inline 写法**(推荐)

##### 支持的 stage 类型

| 类型 | 顺序 | 是否状态型 | 说明 |
|---|---|---|---|
| `relabel` | 0 | 否 | 标签增删改 |
| `enrich` | 1 | 否 | 静态/模板 label 注入 |
| `route` | 2 | 否 | 按 label 路由到不同 topic |
| `sample` | 3 | 否 | 概率采样 |
| `downsample` | 4 | ✅ 是 | 时间桶聚合 |
| `deadvalue` | 5 | ✅ 是 | 死值丢弃 |

**顺序约束**:
- 必须按 `relabel → enrich → route → sample → downsample → deadvalue` 相对顺序
- `relabel` 允许重复(多步清洗),其他类型同 ruleset 内只允许 1 个
- **状态型 stage(downsample / deadvalue)必须放在最后**,之后不能再有 stage

#### 3.7 `relabel` stage

标签增删改。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `drop_labels` | []string | `[]` | 删除指定 label(精确匹配 name) |
| `keep_labels` | []string | `[]` | 白名单(其他全删)。**优先级 > drop_labels** |
| `label_map` | map[string]string | `{}` | 重命名 label key |
| `add_labels` | — | — | **未实现**。新增 label 用 `enrich` |

**示例**:
```yaml
- type: relabel
  drop_labels:
    - env
    - instance
    - pod
  keep_labels: []              # 空 = 不启用白名单
  label_map:
    kubernetes_io_cluster: cluster   # 重命名
```

#### 3.8 `enrich` stage

静态 / 模板 label 注入。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `labels` | map[string]string | `{}` | 注入的 label。value 支持 `${labels.X}` 模板 |

**模板语法**:
- `${labels.X}`:取 sample 已有 label X。X 不存在则跳过该条
- 静态值:直接作为 label value

**示例**:
```yaml
- type: enrich
  labels:
    environment: production
    cluster: "${labels.cluster_name}"   # 引用已有 label
```

#### 3.9 `route` stage

按 label 精确匹配路由到不同 topic。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `rules` | array | `[]` | 路由规则列表,按顺序匹配,第一个命中生效 |
| `rules[].match` | map[string]string | — | 精确匹配,所有 key=value 必须全部命中 |
| `rules[].topic` | string | — | 命中时投递到此 topic。**空 = 丢弃整条 sample** |
| `default_topic` | string | 继承外层 | 不命中时使用。可省略 |

**示例**:
```yaml
- type: route
  rules:
    - match: { team: "core" }
      topic: prom.bj.routed.core
    - match: { team: "infra", env: "prod" }   # 多 key 全部命中
      topic: prom.bj.routed.infra_prod
    - match: { team: "data" }
      topic:                  # 空 = 丢弃
```

#### 3.10 `sample` stage

概率采样。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `rate` | float | — | 保留比例,0.0-1.0。**必填** |
| `scope` | object | `{}`(全量) | 采样范围 |
| `scope.metric_regex` | string | `""` | 仅匹配的 metric 采样,其他透传 |

**示例**:
```yaml
- type: sample
  rate: 0.1                    # 保留 10%
  # scope:
  #   metric_regex: "^debug_"  # 仅 debug_* 指标采样
```

#### 3.11 `downsample` stage(状态型)

按时间桶聚合。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `interval` | duration(string) | — | 桶大小,如 `30s`/`1m`/`5m`/`1h`。**必填** |
| `aggregations` | []string | — | 聚合函数,支持 `avg`/`max`/`min`/`sum`/`count`/`p50`/`p99`。**至少 1 个** |
| `max_series` | int | `1000000` | 内存上限,超出按 LRU 驱逐 |
| `p99_max_samples` | int | `4096` | 单 series 单桶 p50/p99 采样上限,超出退化 |

**注意**:
- 状态全内存,重启丢失
- 同 ruleset 只允许 1 个 downsample stage
- p50/p99 用桶内排序精确计算(非 P²),超 `p99_max_samples` 退化为 top-k reservoir sampling

**示例**:
```yaml
- type: downsample
  interval: 5m
  aggregations: [avg, max, min, sum, count, p50, p99]
  max_series: 2000000
  p99_max_samples: 8192
```

#### 3.12 `deadvalue` stage(状态型)

死值丢弃。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `window` | duration(string) | — | 时间窗,期间值不变则丢弃。**必填** |
| `max_series` | int | `1000000` | LRU 容量 |

**行为**:
- 同 series 在 `window` 内值未变 → 丢弃
- 值变化或超过 window → 发出
- NaN/Inf 视为"变化",总是发出(避免丢失 exporter 异常)
- 重启后状态丢失,首条必发

**示例**:
```yaml
- type: deadvalue
  window: 5m
  max_series: 1000000
```

---

### 4. Token 配置文件

#### 4.1 顶层结构

```yaml
tokens:
  "<token-string>":
    tenant: ...
    tenant_id: ...
    default_topic: ...
    rate_limit: ...
```

#### 4.2 字段说明

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `tokens` | map | ✓ | token → 配置的映射。key 是 token 字符串 |
| `tokens[].tenant` | string | ✓ | 租户名,写入 Kafka header `tenant` |
| `tokens[].tenant_id` | string | 否 | IAM 主键(预留,本地可空) |
| `tokens[].default_topic` | string | ✓ | 默认 topic(route 未命中时兜底) |
| `tokens[].rate_limit` | int | 否 | 该 tenant 的 samples/s 上限。0 = 不限 |

#### 4.3 完整示例

```yaml
tokens:
  "tk_app_business_dev":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.local.raw.app_business
    rate_limit: 80000

  "tk_infra_dev":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.local.raw.infra
    rate_limit: 50000

  "tk_prod_bj_payment":
    tenant: payment
    tenant_id: "2001"
    default_topic: prom.bj.raw.payment
    rate_limit: 200000
```

#### 4.4 热重载

修改 token 文件后,发送 SIGHUP:
```bash
kill -HUP $(pgrep -f "prom-gw")
```

日志确认:
```
tokens reloaded count=3
tenant rate limits reloaded tenants=3
```

---

### 5. 内部模块参数

> 以下参数在代码中定义,部分通过 flag/env 注入,部分为常量不可配置。

#### 5.1 KafkaSink(producer)

定义在 [internal/kafkasink/producer.go](../../internal/kafkasink/producer.go)。

| 参数 | 默认值 | 说明 |
|---|---|---|
| `BufferSize` | `65535` | 内部 channel 容量(in-flight 上限) |
| `BlockTimeout` | `100ms` | channel 满时阻塞等待时长,超时返回 503 |
| `ConnectTimeout` | `10s` | 启动时建连超时 |
| `BatchMaxBytes` | `1MB` | 单批最大字节 |
| `Linger` | `50ms` | 批次最大等待时间 |
| `Compression` | `zstd` | 压缩算法。可选:`zstd`/`snappy`/`lz4`/`gzip`/`none` |
| `Idempotent` | `true` | 幂等写(默认开启) |
| `RecordTimeout` | `120s` | delivery.timeout.ms,单条消息含重试的总超时 |
| `RecordRetries` | `10` | retries,单条消息最大重试 |
| `CloseTimeout` | `30s` | Close 时等待 in-flight 完成超时 |
| `RequiredAcks` | `all` | acks=all |
| `AllowAutoTopicCreation` | `true` | topic 不存在自动创建 |

#### 5.2 WAL

定义在 [internal/wal/wal.go](../../internal/wal/wal.go)。

| 参数 | 默认值 | 说明 |
|---|---|---|
| `Dir` | `/data/wal`(flag 注入) | WAL 目录 |
| `MaxBytes` | `50GB`(flag 注入) | 总字节上限 |
| `DiskUsedRatio` | `0.80`(flag 注入) | 磁盘使用率阈值 |
| `SegmentBytes` | `64MB` | 单段文件大小,达到后封段 |
| `Retention` | `24h` | 已 replay 的 `.done` 段保留时长 |
| `MaxReplayFailures` | `10` | 单段重放失败上限,超出标记为坏段 |
| `FlushInterval` | `1s` | 异步 flush 间隔 |
| `SyncInterval` | `10s` | fsync 间隔 |

#### 5.3 Sink Adapter(kafka + wal 切换)

定义在 [internal/sink/sink.go](../../internal/sink/sink.go)。

| 参数 | 默认值 | 说明 |
|---|---|---|
| `FailThreshold` | `3` | 连续失败次数,达到后切到 WAL |
| `RecoverCheck` | `1s` | 恢复探测间隔 |
| `RecoverSuccessThreshold` | `3` | 连续成功次数,达到后切回 Kafka |

#### 5.4 Pipeline

| 参数 | 默认值 | 说明 |
|---|---|---|
| `BufferSize` | `65535`(来自 ruleset `global.channel_buffer`) | channel 容量 |

#### 5.5 Receiver

| 参数 | 默认值 | 说明 |
|---|---|---|
| `Addr` | `:19201`(flag 注入) | 监听地址 |
| `ReadHeaderTimeout` | `5s` | HTTP 读 header 超时 |
| `ShutdownTimeout` | `30s` | 停机等待 in-flight 完成超时 |

#### 5.6 Tracing(OpenTelemetry)

| 参数 | 默认值 | 说明 |
|---|---|---|
| `ServiceName` | `prom-gw` | OTel service.name |
| `OTLPEndpoint` | env `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC 接收端 |
| `SampleRatio` | `1.0` | 采样率(1.0 = 全采样) |
| `Insecure` | `true` | 是否禁用 TLS |

---

### 6. 完整配置示例

#### 6.1 本地开发最小配置

`configs/rules/local-dev.yaml`:

```yaml
rulesets:
  - name: app-business
    tenant: app-business
    input_topic: prom.local.raw.app_business
    default_topic: prom.local.routed.app_business
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels: [env, instance, pod]
        keep_labels: []
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.local.routed.core
          - match: { team: "infra" }
            topic: prom.local.routed.infra

      - type: sample
        rate: 1.0                    # 本地全量

global:
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

`configs/tokens/local.yaml`:

```yaml
tokens:
  "tk_app_business_dev":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.local.raw.app_business
    rate_limit: 80000
```

启动:
```bash
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-local-wal \
  --wal-max-bytes=1073741824 \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --admin-allow-cidr=127.0.0.1/32 \
  --source-dc=dc-local-dev \
  --ingest-city=local
```

#### 6.2 生产完整配置(北京)

`configs/rules/bj/default.yaml`:

```yaml
rulesets:
  # 1. app-business 业务指标
  - name: app-business-bj
    tenant: app-business
    input_topic: prom.bj.raw.app_business
    default_topic: prom.bj.routed.app_business
    version: 7
    match:
      metric_prefix: ""              # 全量接收
    stages:
      # 1.1 标签清洗
      - type: relabel
        drop_labels:
          - env_internal
          - scrape_id
          - pod_template_hash
        keep_labels: []              # 不启用白名单
        label_map:
          kubernetes_io_cluster: cluster

      # 1.2 路由:按 team 分流
      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.bj.routed.core
          - match: { team: "infra" }
            topic: prom.bj.routed.infra
          - match: { team: "data" }
            topic: prom.bj.routed.data
          - match: { team: "mobile-app" }
            topic: prom.bj.routed.mobile
        # default_topic 省略,继承外层

      # 1.3 兜底采样:5%
      - type: sample
        rate: 0.05

  # 2. infra 基础设施指标(高保留)
  - name: infra-bj
    tenant: infra
    input_topic: prom.bj.raw.infra
    default_topic: prom.bj.routed.infra
    version: 3
    match:
      metric_prefix: ""              # 全量
    stages:
      - type: relabel
        drop_labels: [pod, container_id]
        label_map:
          kubernetes_io_cluster: cluster

      # 2.1 死值丢弃(5min 内值不变则丢)
      - type: deadvalue
        window: 5m
        max_series: 2000000

  # 3. 长期趋势指标(降采样)
  - name: longterm-bj
    tenant: app-business
    input_topic: prom.bj.raw.app_business
    default_topic: prom.bj.agg5m.app_business
    version: 2
    match:
      metric_prefix: "trend_"
    stages:
      - type: relabel
        drop_labels: [instance, pod]

      - type: downsample
        interval: 5m
        aggregations: [avg, max, min, p50, p99]
        max_series: 5000000
        p99_max_samples: 8192

global:
  rate_limit_per_instance: 200000    # 生产高吞吐
  channel_buffer: 131072             # 缓解背压
```

`configs/tokens/production-bj.yaml`:

```yaml
tokens:
  "tk_prod_bj_app_business_<secret>":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.bj.raw.app_business
    rate_limit: 200000

  "tk_prod_bj_infra_<secret>":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.bj.raw.infra
    rate_limit: 150000

  "tk_prod_bj_payment_<secret>":
    tenant: payment
    tenant_id: "2001"
    default_topic: prom.bj.raw.payment
    rate_limit: 100000
```

systemd 启动(`prom-gw@bj.service`):

```ini
[Service]
Environment=KAFKA_BROKERS=kafka-1.bj:9094,kafka-2.bj:9094,kafka-3.bj:9094
Environment=INGEST_CITY=bj
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.bj:4317
ExecStart=/opt/prom-gw/bin/prom-gw \
  --config=/etc/prom-gw/rules/bj/default.yaml \
  --tokens=/etc/prom-gw/tokens/production-bj.yaml \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --write-addr=:19201 \
  --admin-addr=:8082 \
  --admin-allow-cidr=127.0.0.1/32,10.10.0.0/16 \
  --source-dc=dc-bj-dongba \
  --wal-dir=/data/wal \
  --wal-max-bytes=53687091200 \
  --wal-disk-used-ratio=0.80 \
  --nacos-addr=10.0.0.1:8848,10.0.0.2:8848,10.0.0.3:8848 \
  --nacos-namespace=production \
  --nacos-username=prom-gw \
  --nacos-password=<secret> \
  --nacos-data-id=prom-gw-rules-bj \
  --nacos-group=GATEWAY
```

#### 6.3 Nacos 配置中心 ruleset(Nacos 中的 dataId 内容)

Nacos dataId `prom-gw-rules-bj` 内容与本地 YAML 完全一致(Nacos 是远程源,本地文件是兜底):

```yaml
# 与 configs/rules/bj/default.yaml 内容相同
rulesets:
  - name: app-business-bj
    ...
global:
  rate_limit_per_instance: 200000
  channel_buffer: 131072
```

---

### 7. 参数调优速查表

#### 7.1 按场景调优

| 场景 | 关键参数 | 建议值 |
|---|---|---|
| **高吞吐(单机 > 1M samples/s)** | `global.rate_limit_per_instance` / `global.channel_buffer` | `500000` / `131072` |
| **低延迟(背压敏感)** | `--wal-max-bytes` / KafkaSink `BlockTimeout` | `1GB` / `50ms` |
| **省存储(采样)** | `sample.rate` | `0.05`(保留 5%) |
| **省存储(死值)** | `deadvalue.window` | `5m` |
| **省存储(降采样)** | `downsample.interval` / `aggregations` | `5m` / `[avg, p99]` |
| **WAL 容量紧张** | `--wal-max-bytes` / `--wal-disk-used-ratio` | `20GB` / `0.70` |
| **Kafka 慢导致积压** | KafkaSink `Linger` / `BatchMaxBytes` | `100ms` / `2MB` |
| **多租户隔离** | `tokens[].rate_limit` | 按 tenant 配额分配 |
| **跨城专线带宽紧张** | Flink 端 5min 聚合 → 1h 跨城(见 flink-consumer-guide.md) | — |

#### 7.2 端口速查

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| `19201` | RemoteWrite 接入 | Prometheus / LVS |
| `8080` | metrics + pprof | Prometheus 抓取 |
| `8081` | healthz / readyz | LB health check |
| `8082` | Admin API | 运维网段(白名单) |

#### 7.3 信号处理

| 信号 | 行为 |
|---|---|
| `SIGINT` / `SIGTERM` | 优雅停机(30s 超时) |
| `SIGHUP` | 热重载 token + tenant 限流配置 |

#### 7.4 退出码

| 退出码 | 含义 |
|---|---|
| `0` | 正常退出 |
| `1` | fatal 错误 |
| `2` | SIGHUP 触发(预留) |

---

### 附录

#### A. 配置文件路径速查

| 文件 | 路径 | 说明 |
|---|---|---|
| ruleset(本地) | `configs/rules/<city>/default.yaml` | 按城市分目录 |
| ruleset(本地开发) | `configs/rules/local-dev.yaml` | 本地测试 |
| ruleset(Nacos) | dataId=`prom-gw-rules[-<city>]`,group=`GATEWAY` | 配置中心 |
| token(开发) | `configs/tokens/local.yaml` | 可入仓 |
| token(生产) | `configs/tokens/production-<city>.yaml` | **不入仓**(.gitignore 排除) |
| WAL 数据 | `--wal-dir` 指定(默认 `/data/wal`) | 单独 SSD |
| Nacos snapshot | `--nacos-snapshot-path` 指定(默认 `/data/nacos_snapshot.json`) | last-good 快照 |



---

## 10. 本地开发指南 {#10-本地开发指南}
> 本文档面向开发者本机调试,采用 **单节点 Prometheus + 单节点 Kafka + 单实例 prom-gw** 全本地原生部署,**不依赖 Docker**。
> 生产部署请参考 **production-guide.md**(见 §1)。


---

### 1. 适用场景与拓扑

#### 1.1 适用场景

- 本地开发调试 prom-gw 代码
- 验证 ruleset 规则逻辑(relabel/route/sample/downsample)
- 复现 WAL 故障切换与 drain 行为
- 端到端联调(Prometheus → prom-gw → Kafka)

#### 1.2 最小化拓扑

```
本地单机
┌──────────────────────────────────────────────────┐
│  Prometheus :9090 ──remote_write──> prom-gw :19201 │
│                                        │           │
│                                        ▼           │
│                              Kafka :9092 (单节点)   │
│                                        │           │
│                                        ▼           │
│                              kafka-console-consumer │
└──────────────────────────────────────────────────┘
```

#### 1.3 资源需求

| 资源 | 最低 | 建议 |
|---|---|---|
| CPU | 2 核 | 4 核 |
| 内存 | 4 GB | 8 GB |
| 磁盘 | 10 GB | 20 GB |

---

### 2. 环境准备

#### 2.1 操作系统

- macOS(Intel / Apple Silicon)或 Linux
- Windows 建议使用 WSL2

#### 2.2 安装 Go 1.22+

```bash
# macOS(Homebrew)
brew install go

# Linux
wget https://go.dev/dl/go1.22.5.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.22.5.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# 验证
go version  # go version go1.22.x
```

#### 2.3 安装 JDK 17(Kafka 依赖)

```bash
# macOS
brew install openjdk@17
sudo ln -sfn $(brew --prefix)/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# Linux(Ubuntu/Debian)
sudo apt install -y openjdk-17-jdk

# Linux(CentOS/RHEL)
sudo yum install -y java-17-openjdk java-17-openjdk-devel

# 验证
java -version  # openjdk version "17.x.x"
```

#### 2.4 安装辅助工具

```bash
# macOS
brew install jq curl wget

# Linux
sudo apt install -y jq curl wget   # Debian/Ubuntu
sudo yum install -y jq curl wget   # CentOS/RHEL
```

#### 2.5 克隆代码

```bash
git clone https://github.com/lynnyq/bigdata.git
cd bigdata
```

---

### 3. Kafka 单节点本地部署

> 本地开发使用 KRaft 模式单节点,无需 ZooKeeper。

#### 3.1 下载解压

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
ln -s kafka_2.13-3.4.0 kafka
cd kafka
```

#### 3.2 配置文件

创建本地单节点配置 `~/kafka/config/local.properties`:

```properties
# ====== 基础 ======
broker.id=1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=/tmp/kafka-logs-local

# ====== Topic 默认(本地开发降低规格) ======
num.partitions=4
default.replication.factor=1
min.insync.replicas=1
# 内部 topic 副本数(单机必须改为 1,否则 __consumer_offsets 创建失败导致消费卡死)
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
log.retention.hours=24
log.segment.bytes=104857600
log.cleanup.policy=delete
compression.type=producer

# ====== 性能(本地开发降低) ======
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
socket.request.max.bytes=104857600

# ====== KRaft ======
metadata.log.dir=/tmp/kafka-logs-local
```

#### 3.3 JVM 配置

创建 `~/kafka/bin/set-local-opts.sh`:

```bash
#!/bin/bash
export KAFKA_HEAP_OPTS="-Xmx1g -Xms1g -XX:+UseG1GC"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

```bash
chmod +x ~/kafka/bin/set-local-opts.sh
```

#### 3.4 格式化存储(仅首次)

```bash
cd ~/kafka
CLUSTER_UUID=$(bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"

bin/kafka-storage.sh format \
  --config config/local.properties \
  --cluster-id $CLUSTER_UUID
```

#### 3.5 启动 Kafka

```bash
cd ~/kafka
source bin/set-local-opts.sh
bin/kafka-server-start.sh config/local.properties
```

启动后保持终端运行,或加 `-daemon` 后台运行:

```bash
bin/kafka-server-start.sh -daemon config/local.properties
```

#### 3.6 验证 Kafka

```bash
# 1. 查看 Broker 版本(确认在线)
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head

# 2. 查看 Controller 状态
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status

# 3. 创建测试 Topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic test-topic \
  --partitions 4 --replication-factor 1

# 4. 列出 Topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 5. 生产测试
echo "hello" | bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic test-topic

# 6. 消费测试
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-topic \
  --from-beginning --max-messages 1 --timeout-ms 10000
```

#### 3.7 创建 prom-gw 所需 Topic

```bash
cd ~/kafka

# 原始数据 topic
for tenant in app_business infra; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.local.raw.${tenant} \
    --partitions 4 --replication-factor 1 \
    --config retention.ms=86400000
done

# 路由后 topic
for biz in core infra data app_business; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.local.routed.${biz} \
    --partitions 4 --replication-factor 1 \
    --config retention.ms=86400000
done

# 验证
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep prom
```

#### 3.8 停止 Kafka

```bash
cd ~/kafka
bin/kafka-server-stop.sh
```

---

### 4. Prometheus 单节点本地部署

#### 4.1 下载解压

```bash
cd ~
wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.darwin-amd64.tar.gz
# Apple Silicon 改用:
# wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.darwin-arm64.tar.gz
# Linux 改用:
# wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.linux-amd64.tar.gz

tar -xzf prometheus-2.51.0.darwin-amd64.tar.gz
ln -s prometheus-2.51.0.darwin-amd64 prometheus
cd prometheus
```

#### 4.2 配置文件

创建本地配置 `~/prometheus/prometheus-local.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source_dc: dc-local-dev

scrape_configs:
  # 抓取 Prometheus 自身
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']

  # 抓取 prom-gw self-exporter
  - job_name: prom-gw
    static_configs:
      - targets: ['localhost:8080']

remote_write:
  - url: http://localhost:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_dev"
    queue_config:
      capacity: 5000
      max_samples_per_send: 200
      batch_send_deadline: 5s
      min_backoff: 500ms
      max_backoff: 10s
```

#### 4.3 启动 Prometheus

```bash
cd ~/prometheus
./prometheus \
  --config.file=prometheus-local.yml \
  --storage.tsdb.path=/tmp/prometheus-local-data \
  --storage.tsdb.retention.time=2d \
  --web.enable-lifecycle
```

启动后保持终端运行,或后台运行:

```bash
nohup ./prometheus \
  --config.file=prometheus-local.yml \
  --storage.tsdb.path=/tmp/prometheus-local-data \
  --storage.tsdb.retention.time=2d \
  --web.enable-lifecycle \
  > /tmp/prometheus-local.log 2>&1 &
```

#### 4.4 验证 Prometheus

```bash
# 1. 健康检查
curl http://localhost:9090/-/healthy
# 期望: Prometheus is Healthy.

# 2. 就绪检查
curl http://localhost:9090/-/ready

# 3. 查看 remote_write 配置
curl -s http://localhost:9090/api/v1/status/config | jq -r '.data.yaml' | grep remote_write -A 10

# 4. 查看 remote_write 运行状态
curl -s http://localhost:9090/api/v1/status/runtimeinfo | jq '.data.remoteWrite'

# 5. 查看 remote_write 发送计数
curl -s 'http://localhost:9090/api/v1/query?query=prometheus_remote_storage_samples_total' | jq

# 6. 打开 Web UI
open http://localhost:9090   # macOS
```

#### 4.5 热重载配置

修改 `prometheus-local.yml` 后,无需重启:

```bash
curl -X POST http://localhost:9090/-/reload
```

#### 4.6 停止 Prometheus

```bash
# 前台运行:Ctrl+C
# 后台运行:
pkill -f "prometheus --config.file=prometheus-local.yml"
```

---

### 5. prom-gw 本地编译与配置

#### 5.1 编译

```bash
cd ~/bigdata   # 或实际代码目录

# 依赖
make build     # 产物:bin/prom-gw

# 验证
./bin/prom-gw --version  # prom-gw <version>
```

#### 5.2 本地 Token 配置

仓库自带开发用 token,直接使用 `configs/tokens/local.yaml`:

```yaml
tokens:
  "tk_app_business_dev":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.local.raw.app_business
    rate_limit: 80000

  "tk_infra_dev":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.local.raw.infra
    rate_limit: 50000
```

> **注意**:本地开发的 `default_topic` 改为 `prom.local.raw.*`(与 §3.7 创建的 topic 匹配)。

#### 5.3 本地 Ruleset 配置

创建 `configs/rules/local-dev.yaml`:

```yaml
rulesets:
  - name: app-business
    tenant: app-business
    input_topic: prom.local.raw.app_business
    default_topic: prom.local.routed.app_business
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels: [env, instance, pod]
        keep_labels: []
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.local.routed.core
          - match: { team: "infra" }
            topic: prom.local.routed.infra
          - match: { team: "data" }
            topic: prom.local.routed.data

      - type: sample
        rate: 0.1

global:
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

#### 5.4 启动参数速查

| 参数 | 默认值 | 本地调试常用值 |
|---|---|---|
| `--config` | `configs/rules/default.yaml` | `configs/rules/local-dev.yaml` |
| `--tokens` | `configs/tokens/local.yaml` | `configs/tokens/local.yaml` |
| `--write-addr` | `:19201` | `:19201` |
| `--metrics-addr` | `:8080` | `:8080` |
| `--health-addr` | `:8081` | `:8081` |
| `--admin-addr` | `:8082` | `:8082` |
| `--admin-allow-cidr` | `127.0.0.1/32,10.0.0.0/8` | `127.0.0.1/32` |
| `--source-dc` | `dc-unknown` | `dc-local-dev` |
| `--ingest-city` | `dc-unknown` | `local` |
| `--wal-dir` | `/data/wal` | `/tmp/prom-gw-local-wal` |
| `--wal-max-bytes` | `50GB` | `1GB` |

Kafka broker 列表通过 `KAFKA_BROKERS` 环境变量注入,未设置时进入 **WAL-only 模式**。

---

### 6. 全链路启动与验证

#### 6.1 启动顺序

```bash
# 终端 1:启动 Kafka
cd ~/kafka
source bin/set-local-opts.sh
bin/kafka-server-start.sh config/local.properties

# 终端 2:启动 prom-gw
cd ~/bigdata
mkdir -p /tmp/prom-gw-local-wal
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-local-wal \
  --wal-max-bytes=1073741824 \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --admin-allow-cidr=127.0.0.1/32 \
  --source-dc=dc-local-dev \
  --ingest-city=local

# 终端 3:启动 Prometheus
cd ~/prometheus
./prometheus \
  --config.file=prometheus-local.yml \
  --storage.tsdb.path=/tmp/prometheus-local-data \
  --storage.tsdb.retention.time=2d \
  --web.enable-lifecycle
```

#### 6.2 验证清单

```bash
# 1. Kafka 在线
~/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 | head

# 2. Topic 已创建
~/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep prom

# 3. prom-gw 健康
curl http://127.0.0.1:8081/healthz
# 期望: {"status":"ok"}

# 4. prom-gw 就绪
curl -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/readyz
# 期望: 204

# 5. Prometheus 健康
curl http://localhost:9090/-/healthy

# 6. Prometheus remote_write 正在发送
curl -s 'http://localhost:9090/api/v1/query?query=prometheus_remote_storage_samples_total' | jq '.data.result[0].value[1]'

# 7. prom-gw 收到数据
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 8. Kafka 收到数据(消费验证)
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.raw.app_business \
  --from-beginning --max-messages 5 --timeout-ms 10000 | xxd | head -20

# 9. Admin API
curl -s http://127.0.0.1:8082/v1/rulesets | jq
curl -s http://127.0.0.1:8082/v1/stats | jq
curl -s http://127.0.0.1:8082/v1/tenants | jq
```

#### 6.3 手动写入单条 sample

不启动 Prometheus,手动构造 RemoteWrite 请求:

```bash
cd ~/bigdata

# 构造 snappy 编码的 WriteRequest
RUN_ID=local-$(date +%s) go run ./scripts/e2e_payload > /tmp/payload.bin

# 写入 prom-gw
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/payload.bin
# 期望: HTTP 200

# 验证 Kafka 收到
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.raw.app_business \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head
```

---

### 7. 调试技巧

#### 7.1 日志

prom-gw 默认输出到 stdout,可重定向到文件:

```bash
./bin/prom-gw ... > /tmp/prom-gw.log 2>&1 &

# 实时查看
tail -f /tmp/prom-gw.log

# 关键日志
grep "receiver listening" /tmp/prom-gw.log          # receiver 启动
grep "kafkasink started" /tmp/prom-gw.log           # Kafka 连接成功
grep "WAL degraded" /tmp/prom-gw.log                # 降级到 WAL
grep "kafka recovered" /tmp/prom-gw.log             # Kafka 恢复
grep "draining WAL" /tmp/prom-gw.log                # drain WAL
grep "tokens reloaded" /tmp/prom-gw.log             # token 热重载
grep "rules swapped" /tmp/prom-gw.log               # ruleset 热切换
```

#### 7.2 pprof 性能分析

prom-gw 在 `--metrics-addr`(默认 8080)暴露 pprof:

```bash
# Goroutine 概览
curl http://127.0.0.1:8080/debug/pprof/goroutine?debug=1 | head -50

# Heap 分析
go tool pprof http://127.0.0.1:8080/debug/pprof/heap
# (pprof) top10
# (pprof) web

# CPU profile(30 秒采样)
go tool pprof http://127.0.0.1:8080/debug/pprof/profile?seconds=30

# 阻塞分析(需开启 -blockprofile)
curl http://127.0.0.1:8080/debug/pprof/block?debug=1 | head -30
```

#### 7.3 Admin API 调试

```bash
# 查看当前 ruleset
curl -s http://127.0.0.1:8082/v1/rulesets | jq

# 查看运行时统计
curl -s http://127.0.0.1:8082/v1/stats | jq

# 查看租户列表
curl -s http://127.0.0.1:8082/v1/tenants | jq

# 热更新 ruleset
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @configs/rules/local-dev.yaml

# 强制 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app-business:reload
```

#### 7.4 关键指标速查

```bash
M=http://127.0.0.1:8080/metrics

# 总 sample 数(按 stage/tenant/status)
curl -s $M | grep gateway_samples_total

# Kafka 写入字节
curl -s $M | grep gateway_bytes_out_total

# 错误计数
curl -s $M | grep gateway_errors_total

# 背压拒绝(503)
curl -s $M | grep gateway_backpressure_rejected_total

# WAL 当前字节数
curl -s $M | grep gateway_wal_bytes

# WAL 最老 segment 年龄
curl -s $M | grep gateway_wal_oldest_age_seconds

# 请求延迟分布
curl -s $M | grep gateway_request_duration_seconds

# Kafka produce 错误
curl -s $M | grep gateway_produce_errors_total
```

#### 7.5 Kafka 消费调试

```bash
K=~/kafka/bin

# 实时消费(持续)
$K/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.raw.app_business \
  --from-beginning

# 查看消费组
$K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 查看 Topic 分区详情
$K/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic prom.local.raw.app_business

# 查看 Topic 最新 offset
$K/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic prom.local.raw.app_business
```

#### 7.6 热重载 Token

```bash
# 修改 token 文件
vim configs/tokens/local.yaml

# 发送 SIGHUP
kill -HUP $(pgrep -f "prom-gw")

# 验证日志
grep "tokens reloaded" /tmp/prom-gw.log
```

#### 7.7 热更新 Ruleset

修改 `configs/rules/local-dev.yaml` 后,fsnotify 自动检测(5s 内):

```bash
# 验证热切换
grep "rules swapped" /tmp/prom-gw.log
```

或通过 Admin API 手动推送:

```bash
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @configs/rules/local-dev.yaml
```

---

### 8. 常用测试场景

#### 8.1 场景 1:WAL-only 模式(无 Kafka)

不设置 `KAFKA_BROKERS`,验证 prom-gw 接收 + WAL 落盘:

```bash
cd ~/bigdata
mkdir -p /tmp/prom-gw-wal-only

KAFKA_BROKERS="" \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-wal-only \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-wal-test &

GW_PID=$!

# 等待启动
sleep 2
curl http://127.0.0.1:8081/healthz

# 写入数据
for i in $(seq 1 5); do
  RUN_ID="wal-test-$i" go run ./scripts/e2e_payload > /tmp/payload-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/payload-$i.bin
done

# 验证 WAL 落盘
sleep 1
ls -la /tmp/prom-gw-wal-only/
find /tmp/prom-gw-wal-only/ -name 'seg-*.log*' | wc -l  # 期望 ≥ 1

# 验证指标
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-wal-only /tmp/payload-*.bin
```

#### 8.2 场景 2:WAL 故障切换

验证 Kafka 故障时自动降级到 WAL,Kafka 恢复后自动 drain:

```bash
cd ~/bigdata
mkdir -p /tmp/prom-gw-failover-wal

# 1. 用一个不存在的 Kafka 地址启动(模拟故障)
KAFKA_BROKERS=127.0.0.1:9999 \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-failover-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-failover-test > /tmp/prom-gw-failover.log 2>&1 &

GW_PID=$!
sleep 3

# 2. 验证进入 WAL degraded 模式
grep "WAL degraded" /tmp/prom-gw-failover.log
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes

# 3. 写入数据(应全部落 WAL)
for i in $(seq 1 5); do
  RUN_ID="failover-$i" go run ./scripts/e2e_payload > /tmp/failover-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/failover-$i.bin
done

# 4. 验证 WAL 段
sleep 1
find /tmp/prom-gw-failover-wal/ -name 'seg-*.log*' | wc -l

# 5. 停止 prom-gw
kill $GW_PID
wait $GW_PID 2>/dev/null

# 6. 启动本地 Kafka(如果还没启动)
# ~/kafka/bin/kafka-server-start.sh ~/kafka/config/local.properties

# 7. 用正确 Kafka 地址重启 prom-gw(应自动 drain WAL)
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-failover-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-failover-test > /tmp/prom-gw-drain.log 2>&1 &

GW_PID=$!
sleep 5

# 8. 验证 drain 日志
grep "draining WAL" /tmp/prom-gw-drain.log
grep "WAL drained successfully" /tmp/prom-gw-drain.log

# 9. 验证 Kafka 收到 drain 的数据
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.raw.app_business \
  --from-beginning --max-messages 10 --timeout-ms 10000 | xxd | head

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-failover-wal /tmp/failover-*.bin /tmp/prom-gw-failover.log /tmp/prom-gw-drain.log
```

#### 8.3 场景 3:规则引擎验证

验证 relabel/route/sample 规则正确执行:

```bash
cd ~/bigdata

# 1. 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
  --config=configs/rules/local-dev.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-rule-wal \
  --write-addr=:19201 \
  --admin-addr=:8082 &
GW_PID=$!
sleep 2

# 2. 构造带 team=core 标签的 sample
cat > /tmp/rule-test.go << 'EOF'
package main
import (
  "io"
  "os"
  "time"
  "github.com/klauspost/compress/snappy"
  "github.com/prometheus/prometheus/prompb"
)
func main() {
  req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
    {Labels: []prompb.Label{
      {Name: "__name__", Value: "app_cpu_usage"},
      {Name: "team", Value: "core"},
      {Name: "env", Value: "prod"},
      {Name: "instance", Value: "10.0.0.1:9090"},
    }, Samples: []prompb.Sample{{Value: 88.5, Timestamp: time.Now().UnixMilli()}}},
  }}
  raw, _ := req.Marshal()
  encoded := snappy.Encode(nil, raw)
  io.Copy(os.Stdout, &byteReader{b: encoded})
}
type byteReader struct{ b []byte; i int }
func (r *byteReader) Read(p []byte) (int, error) {
  if r.i >= len(r.b) { return 0, io.EOF }
  n := copy(p, r.b[r.i:]); r.i += n; return n, nil
}
EOF
go run /tmp/rule-test.go > /tmp/rule-payload.bin

# 3. 写入
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/rule-payload.bin

# 4. 验证路由到 prom.local.routed.core(team=core 命中)
~/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.routed.core \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head

# 5. 验证 ruleset 指标
curl -s http://127.0.0.1:8080/metrics | grep gateway_ruleset_routed_total

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-rule-wal /tmp/rule-test.go /tmp/rule-payload.bin
```

#### 8.4 场景 4:单元测试 + 集成测试

```bash
cd ~/bigdata

# 单元测试(快速,不需 Kafka)
make test
# 期望:coverage > 60%,全部 PASS

# 集成测试(需要本地 Kafka 在线)
KAFKA_BROKERS=localhost:9092 INTEGRATION=1 \
go test -race -count=1 -tags=integration ./test/integration/...

# 压测冒烟(30s)
make test-loadgen

# 端到端手动脚本
bash test/manual/e2e.sh
# 期望:✅ 全部检查通过
```

#### 8.5 场景 5:批量压测

```bash
cd ~/bigdata

# 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
  --config=configs/rules/default.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-perf-wal \
  --write-addr=:19201 &
GW_PID=$!
sleep 2

# 1. 自研 loadgen(50000 samples/s,30s)
go run ./test/loadgen --rate=50000 --duration=30s

# 2. 观察指标
watch -n1 'curl -s http://127.0.0.1:8080/metrics | grep -E "gateway_samples_total|gateway_request_duration"'

# 3. 观察 Kafka 入队速率
~/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic prom.local.raw.app_business

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-perf-wal
```

---

### 9. 清理与重置

#### 9.1 停止所有服务

```bash
# 停止 prom-gw
pkill -f "prom-gw" 2>/dev/null

# 停止 Prometheus
pkill -f "prometheus --config.file=prometheus-local.yml" 2>/dev/null

# 停止 Kafka
~/kafka/bin/kafka-server-stop.sh 2>/dev/null
```

#### 9.2 清理数据

```bash
# 清理 prom-gw WAL
rm -rf /tmp/prom-gw-*-wal

# 清理 prom-gw 日志
rm -f /tmp/prom-gw*.log

# 清理 Prometheus 数据
rm -rf /tmp/prometheus-local-data

# 清理 Kafka 数据(谨慎!会删除所有 Topic 数据)
rm -rf /tmp/kafka-logs-local

# 清理测试 payload
rm -f /tmp/payload*.bin /tmp/rule-* /tmp/failover-*
```

#### 9.3 完全重置(回到初始状态)

```bash
# 1. 停止所有服务(见 9.1)

# 2. 清理所有数据
rm -rf /tmp/prom-gw-*-wal /tmp/prometheus-local-data /tmp/kafka-logs-local
rm -f /tmp/prom-gw*.log /tmp/payload*.bin

# 3. 重新格式化 Kafka
cd ~/kafka
CLUSTER_UUID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format \
  --config config/local.properties \
  --cluster-id $CLUSTER_UUID

# 4. 重新创建 Topic(见 3.7)

# 5. 重新启动(见 6.1)
```

---

### 附录

#### A. 端口速查

| 端口 | 服务 | 用途 |
|---|---|---|
| `9090` | Prometheus | Web UI / API |
| `9092` | Kafka | 客户端访问 |
| `9093` | Kafka | Controller(KRaft) |
| `19201` | prom-gw | RemoteWrite 接入 |
| `8080` | prom-gw | `/metrics` + pprof |
| `8081` | prom-gw | healthz / readyz |
| `8082` | prom-gw | Admin API |

#### B. 目录速查

```
~/kafka/                          # Kafka 安装目录
  ├── config/local.properties     # 本地单节点配置
  └── bin/set-local-opts.sh       # JVM 参数
~/prometheus/                     # Prometheus 安装目录
  └── prometheus-local.yml        # 本地配置
~/bigdata/                        # prom-gw 代码目录
  ├── bin/prom-gw                 # 编译产物
  ├── configs/
  │   ├── tokens/local.yaml       # 开发 token
  │   └── rules/local-dev.yaml    # 本地 ruleset
  ├── scripts/e2e_payload/        # 测试 payload 生成器
  └── test/manual/e2e.sh          # 端到端手动脚本
/tmp/prom-gw-local-wal/           # prom-gw WAL 数据
/tmp/prometheus-local-data/       # Prometheus TSDB 数据
/tmp/kafka-logs-local/            # Kafka 日志数据
```

#### C. 常见问题

| 现象 | 排查 |
|---|---|
| prom-gw 启动报 `kafkasink: connection refused` | Kafka 未启动,或 `KAFKA_BROKERS` 地址错误 |
| prom-gw 启动报 `WAL-only mode` | 正常,`KAFKA_BROKERS` 未设置 |
| 写入返回 401 | token 错误,检查 `configs/tokens/local.yaml` |
| 写入返回 400 | snappy 解码失败,检查 `Content-Encoding: snappy` header |
| 写入返回 503 | 背压,Kafka 慢或 pipeline channel 满 |
| Kafka 消费无数据 | Topic 未创建,或路由规则不匹配 |
| 日志反复刷 `Sent auto-creation request for Set(__consumer_offsets)` 且 consumer 拉不到消息 | 单机未设置内部 topic 副本数为 1,`__consumer_offsets` 默认 replication=3 无法创建。确认 `local.properties` 已设置 `offsets.topic.replication.factor=1` 和 `transaction.state.log.replication.factor=1`(见 §3.2) |
| `gateway_wal_bytes` 持续增长 | Kafka 故障,检查 `gateway_errors_total{stage="kafka"}` |
| Prometheus remote_write 失败 | 检查 `prometheus_remote_storage_samples_failed_total` |
| Kafka 启动报 `Cluster ID 不匹配` | 重新格式化(见 9.3) |
| 端口被占用 | `lsof -i :9092` 查看占用进程 |



---

## 11. 故障响应与排查 {#11-故障响应与排查}
> 配套文档:**SLO 指标**(见 §12)、**生产部署指南**(见 §1)。
> 本文档覆盖"故障来了怎么办"——按严重程度分级响应(确认 → 隔离 → 恢复 → 复盘),并提供常见问题的具体排查步骤。

### 0. 通用原则

1. **先止血,后查因**。先恢复业务,再排查根因,不要在故障期做无关变更。
2. **保留现场**。抓 heap profile、metric 快照、相关日志段,留作复盘材料。
3. **所有变更记录到 incident 文档**(时间、操作人、命令、观察)。
4. **变更前先摘流**。无 load balancer 直连时,提前协调上游 Prometheus 暂停 remote_write。
5. **rollback 优先**。如果变更后故障,先 `make release` 拉上一版回滚,再分析。
6. **告警升级按 `slo.md` §5 分级**。

### 1. 严重故障 (SEV-1):服务整体不可用

**判定标准**:
- 全部实例 healthz/readyz 503
- 数据完全中断(写入返回 5xx > 5min)
- 错误率 > 5% 持续 5 分钟

**on-call 5 分钟内确认,15 分钟内开始处置**。

#### 1.1 确认现场

```bash
# 1. 进程状态
systemctl status prom-gw
ps -ef | grep prom-gw | grep -v grep

# 2. 端口监听
ss -tlnp | grep -E ':(19201|8080|8081|8082)\s'

# 3. 健康检查
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -fsS -m 3 http://$h:8081/healthz || echo "FAIL: $h"
  curl -fsS -m 3 http://$h:8081/readyz || echo "NOT READY: $h"
done

# 4. 最近日志(找 panic / fatal)
sudo journalctl -u prom-gw --since "10m ago" --no-pager | tail -200

# 5. 资源
ps -o pid,rss,vsz,pcpu,pmem,comm -p $(pidof prom-gw)
dmesg | tail -20 | grep -i "oom\|killed"
```

#### 1.2 隔离 (止血)

按优先级选择,每步 5 分钟内观察:

| 现象 | 立即动作 |
|---|---|
| 进程不存在 | `sudo systemctl restart prom-gw` |
| 进程存在但无响应 | `sudo systemctl restart prom-gw`(等 systemd 超时后强杀) |
| 启动反复 fail | 先看日志定位 → 临时把启动命令里的 `--kafka-brokers` 改成空,让 GW 走 WAL-only |
| OOM | 临时加机器或减少并发(`-concurrency=1`),重启 |
| 全部实例 down | LB 上游 fallback 到备用集群 |

#### 1.3 恢复 (查因 + 修复)

1. **看启动日志**:`journalctl -u prom-gw -n 1000`,找 panic / fatal / error
2. **看 panic 类型**:
   - `kafka.New() probe failed` → Kafka 集群故障,GW 应已自动降级 WAL,查 Kafka
   - `config: failed to load ruleset` → 配置文件语法错,回滚
   - `port already in use` → 端口冲突,查同机其他进程
   - `out of memory` → heap profile 定位
3. **看依赖**:
   - Kafka: `kafka-broker-api-versions --bootstrap-server $BROKER`
   - Nacos: `curl http://$NACOS:8848/nacos/v1/cs/configs?dataId=prom-gw-rules`
   - 本地磁盘: `df -h /data/wal`
4. **修复后**:`systemctl restart prom-gw`,观察 5 分钟

#### 1.4 复盘

填入 incident doc:故障时间线、影响面、root cause、修复动作、follow-up 项。
挂到团队复盘会议,2 个工作日内完成。

### 2. 严重故障 (SEV-2):部分功能不可用

**判定标准**:
- 错误率 1-5% 持续 5 分钟
- p99 延迟 > 1s
- WAL 硬拒绝 > 0
- 某 ruleset 不工作(其他 ruleset 正常)

**on-call 15 分钟内确认,1 小时内开始处置**。

#### 2.1 高错误率

```bash
# 1. 错误分类
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total

# 2. 按 stage 拆解
for stage in decode auth parse kafka wal; do
  echo "=== $stage ==="
  curl -s http://127.0.0.1:8080/metrics | grep "gateway_errors_total{stage=\"$stage\""
done
```

**常见根因**:
- `decode`:客户端发送非 snappy/protobuf 字节 → 检查 Prometheus remote_write 配置
- `auth`:token 失效或被吊销 → 更新 `tokens.yaml` 并 HUP
- `kafka`:Kafka 不可达 → 检查 `gateway_kafka_*` 指标和 Kafka 集群
- `wal_full`:WAL 满 → 清理或扩容

#### 2.2 p99 延迟高

```bash
# 1. CPU profile(30s 抓样)
go tool pprof -top -cum http://127.0.0.1:8080/debug/pprof/profile?seconds=30

# 2. stage 耗时
curl -s http://127.0.0.1:8080/metrics | grep gateway_stage_duration

# 3. 看是否有 GC 暂停
curl -s http://127.0.0.1:8080/metrics | grep go_gc_duration
```

**常见根因**:
- Kafka 慢:ack=all 时 broker 慢会传染
- 复杂规则:relabel 规则太多,每 sample 耗时高
- 状态型 stage 内存抖动:看 `gateway_goroutines` 是否有 GC 暂停

如果是某条 ruleset 引起(其他正常),临时 `POST /v1/rulesets/{name}:reload` 强制重载;
不行就 `POST /v1/rulesets/{name}:rollback?to_version=N` 回滚。

#### 2.3 WAL 硬拒绝

```bash
# 1. WAL 状态
curl -s http://127.0.0.1:8080/metrics | grep -E "gateway_wal_(bytes|oldest|hard_reject)"

# 2. 磁盘
df -h /data/wal

# 3. WAL 目录
ls -lah /data/wal | head
```

**常见根因**:
- Kafka 不可达:检查 `kafka brokers` 配置和网络
- Kafka 慢:Kafka 集群压力过大,看 broker 指标
- Nacos 推送错误配置:回滚到上一版本

**短期止血**(降级背压):
- 調大 `wal.max_bytes`(临时改配置,SIGHUP 生效)
- 加挂一块盘,迁移 WAL 目录(需停机)
- 扩容 Kafka,加速排空

**禁止**:`rm -rf /data/wal` — 会丢所有未确认消息。

### 3. 警告 (SEV-3):告警但不阻塞

**判定标准**:
- 4xx/5xx 持续 > 10/s
- 鉴权失败 > 50/s
- p99 延迟 0.5-1s
- Goroutines > 5000
- Config reload 失败

**on-call 30 分钟内确认,4 小时内处置**。

#### 3.1 鉴权失败激增

```bash
# 1. 看 reason
curl -s http://127.0.0.1:8080/metrics | grep gateway_auth_fail_total

# 2. 看具体 token(日志中脱敏,这里只能看 IP)
sudo journalctl -u prom-gw --since "10m ago" | grep "auth fail" | head
```

**常见根因**:
- 客户端 token 拼写错
- 客户端还在用旧 token(已被轮换)
- Prometheus remote_write URL 配错(漏了 `Bearer`)

确认是配置问题(token 拼错)还是被攻击(单一 IP 高频),按需 HUP tokens.yaml。

#### 3.2 Config reload 失败

```bash
# 1. Nacos 拉取
sudo journalctl -u prom-gw --since "5m ago" | grep -i "nacos\|snapshot"

# 2. 本地文件监听
sudo journalctl -u prom-gw --since "5m ago" | grep -i "fsnotify\|apply snapshot"

# 3. 手动 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app:reload
```

如果 Nacos 推了非法 YAML,GW 会保留旧版 + 告警,不影响业务。处理:
1. 修 YAML
2. 手动 reload 验证
3. 复盘 Nacos 发布流程(谁推的,有没有 review)

### 4. 计划性变更 (变更窗口)

| 变更类型 | 提前通知 | 风险评估 | 变更窗口 |
|---|---|---|---|
| 升级 binary | 24h | 中 | 工作日 02:00-04:00 |
| 修改默认配置 | 48h | 中 | 工作日 02:00-04:00 |
| Nacos 推 ruleset | 即时 | 低 | 任何时间(可秒级回滚) |
| Kafka 容量调整 | 72h | 高 | 业务低峰期,需 DBA + SRE 同时在场 |
| 端口/路由变更 | 1 周 | 高 | 业务低峰期 + 上游协同 |

变更后必须:
- 观察 30 分钟,确认指标未劣化
- 在变更日志里记录变更人 / 变更内容 / 观察结论
- 保留旧 binary 包至少 7 天(回滚用)

### 5. 容量告警处置

参考 `slo.md §6` 容量规划表。

| 信号 | 含义 | 处置 |
|---|---|---|
| CPU 持续 > 70% | 单机到上限 | 加实例 / 增加 partition |
| Goroutines 持续 > 5000 | 资源泄漏嫌疑 | 抓 goroutine profile 排查 |
| p99 抖动但 p50 稳 | 下游慢传染 | 查 Kafka broker 端 |
| 错误率突增但 p99 稳 | 鉴权/解析错误 | 查 `gateway_auth_fail_total` / 客户端配置 |
| WAL 段数持续增长 | 下游消费慢 | 扩容 Kafka consumer |

### 6. 沟通模板

#### 6.1 故障通知(发给业务方)

```
【prom-gw 告警】SEV-X
- 现象:<错误率 / 延迟 / 不可用>
- 开始时间:<HH:MM>
- 影响面:<哪几个租户 / 哪条规则>
- 当前状态:<正在处置 / 已恢复>
- 预计恢复:<HH:MM 或 评估中>
- 负责人:<on-call 名字>
- 进展:<每 15 分钟更新一次>
```

#### 6.2 恢复通知

```
【prom-gw 恢复】
- 故障时间:<HH:MM - HH:MM,共 X 分钟>
- root cause:<一句话>
- 修复动作:<已变更内容 / 配置 / 代码>
- 数据丢失:<无 / WAL 落盘 N 条已重放>
- 复盘文档:<链接>
```

### 7. 升级路径

| 严重度 | 第一响应 | 升级条件 | 第二响应 |
|---|---|---|---|
| SEV-1 | on-call 工程师 | 15 分钟无进展 | 团队负责人 + SRE lead |
| SEV-2 | on-call 工程师 | 1 小时无进展 | 团队负责人 |
| SEV-3 | on-call 工程师 | 4 小时无进展 | 下个工作日复盘 |
| 数据完整性事件 | on-call 工程师 | 即时 | 数据 owner + SRE lead + 团队负责人 |

### 8. 工具速查

```bash
# 全实例 healthz 巡检
HOSTS="10.0.1.1,10.0.1.2,10.0.1.3"
for h in $(echo $HOSTS | tr ',' ' '); do
  printf '%s\t' "$h"
  curl -fsS -m 3 http://$h:8081/readyz && echo OK || echo FAIL
done

# 全实例 admin 状态
for h in $(echo $HOSTS | tr ',' ' '); do
  echo "=== $h ==="
  curl -sS http://$h:8082/v1/stats | jq .
done

# 全实例 ruleset 一致性
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -sS http://$h:8082/v1/rulesets | jq -c '.data[] | {name, version}'
done | sort | uniq -c | sort -rn

# 全实例 5xx 计数
for h in $(echo $HOSTS | tr ',' ' '); do
  echo "=== $h ==="
  curl -s http://$h:8080/metrics | grep "gateway_errors_total{stage=\"kafka\""
done

# heap profile 远程拉
go tool pprof -text -cum http://127.0.0.1:8080/debug/pprof/heap > heap-$(date +%s).txt

# goroutine 数量
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -s http://$h:8080/metrics | grep "^gateway_goroutines"
done
```

### 9. 复盘模板

每次 SEV-1/2 故障 24 小时内开复盘会,产出文档包含:

1. **时间线**(UTC+8):
   - HH:MM 告警触发
   - HH:MM on-call 确认
   - HH:MM 止血动作
   - HH:MM 业务恢复
   - HH:MM 修复完成
2. **影响面**:租户 / ruleset / 数据丢失条数 / 持续时间
3. **root cause**(一层、二层、最深层)
4. **为什么告警没提前**(MTTD 分析)
5. **为什么处置慢了**(MTTR 分析)
6. **Action items**(每条都有 owner + deadline)
7. **流程改进 / 自动化机会**

---

### 10. 常见问题排查

#### 10.1 503 背压拒绝持续

**症状**:`gateway_backpressure_rejected_total` 持续 > 0。

```bash
# 看 channel 深度
curl -s http://127.0.0.1:8082/v1/stats  # admin API

# 看 Kafka 是否慢(若有 kafka exporter,看 consumer lag)
```

**处置**:
- 短期:扩容 prom-gw 实例数(横向扩展)
- 中期:增加 Kafka partition 数 / 扩容 Kafka 集群
- 长期:优化下游消费,降低反压

#### 10.2 WAL 卡住不排空

**症状**:`gateway_wal_oldest_age_seconds` 持续 > 60s。

```bash
# 看 WAL 段数 / 大小
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal

# 看 WAL 目录
ls -lah /data/wal
```

**手动 drain**(应急):

```bash
# 1. 停 prom-gw
sudo systemctl stop prom-gw

# 2. 启动时强制走 WAL→Kafka 模式(默认行为,Kafka 恢复后自动 replay)

# 3. 启动
sudo systemctl start prom-gw

# 4. 观察日志
sudo journalctl -u prom-gw -f | grep -i "replay\|wal"
```

#### 10.3 规则版本不切换

**症状**:修改 ruleset 文件后,`gateway_ruleset_version` 没变。

```bash
# 1. 看 fsnotify 是否触发
sudo journalctl -u prom-gw -f | grep "apply snapshot"

# 2. 看是否校验失败
sudo journalctl -u prom-gw --since "10m ago" | grep -i "warn\|error"

# 3. 手动 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app:reload
```

**常见根因**:
- 文件权限:prom-gw 进程读不到文件
- YAML 语法错误:看日志里 `apply snapshot failed`
- version 必须递增:用 `gateway_ruleset_version` 确认

#### 10.4 实例 OOM

**症状**:prom-gw 进程被 OOM kill。

```bash
# 1. 看 heap profile
go tool pprof http://127.0.0.1:8080/debug/pprof/heap

# 2. 看 goroutine 数
curl -s http://127.0.0.1:8080/metrics | grep gateway_goroutines

# 3. 看 RSS
ps -o rss= -p $(pidof prom-gw)
```

**常见根因**:
- Downsample 状态过大:减少并发 ruleset 或缩短 interval
- DeadValue LRU 满:调整 LRU 容量
- WAL 段未清理:确认 Kafka 正常消费

#### 10.5 性能不达标 (QPS 不到 1.5M)

```bash
# 1. CPU profile
go tool pprof -top -cum http://127.0.0.1:8080/debug/pprof/profile?seconds=30

# 2. 看 stage 耗时
curl -s http://127.0.0.1:8080/metrics | grep gateway_stage_duration
```

**常见瓶颈**:
- parser 单线程:确认 `-concurrency` 启动参数(本项目 N pipeline goroutine,默认 = 1)
- Kafka 慢:看 broker 端的 `produce latency`
- 大量 label string 分配:确认 stringpool 启用

#### 10.6 紧急处置速查

| 现象 | 立即动作 |
|---|---|
| prom-gw 全实例 down | 检查 systemd / 网络;LB 上游 fallback |
| 错误率 > 5% | 摘流,回滚上一版;查 Nacos / 配置文件 |
| Kafka 不可用 | prom-gw 自动降级 WAL-only;无需人工 |
| WAL 满 | 查 Kafka 恢复;临时调大 `wal-max-bytes` |
| Admin API 503 | IP 白名单被改? 查 `gateway_admin_auth_fail_total` |
| OOM | 抓 heap profile,临时加机器 / 重启 |



---

## 12. SLO 指标 {#12-slo-指标}
### 1. 可用性

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 实例可用性 | 99.95% 月度 | systemd 运行时间 / 总时间 |
| 端到端可用性(含 Kafka) | 99.9% 月度 | `success_2xx` / `total` |

**误差预算**:0.05% × 30 天 = 21.6 分钟/月
超预算时,触发非紧急变更冻结(只允许修复故障)。

### 2. 性能

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 吞吐 | ≥ 1.5M samples/s 单实例 | `rate(gateway_samples_total{stage="parse",status="ok"})[1m]` |
| p99 延迟 | < 500ms | `histogram_quantile(0.99, ...)` |
| p50 延迟 | < 50ms | 同上 |
| 错误率 | < 0.01% | `rate(gateway_errors_total) / rate(gateway_samples_total)` |
| 背压拒绝率 | < 0.1% | `rate(gateway_backpressure_rejected_total) / rate(gateway_samples_total)` |

### 3. 数据完整性

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 数据丢失率 | 0(Kafka 不可用时落 WAL) | chaos 测试 + count 校验 |
| Kafka 写 ack | all + idempotent | producer 配置 |
| WAL 硬拒绝率 | 0(否则 503) | `increase(gateway_wal_hard_reject_total[1d])` |
| TraceID 端到端传递率 | 100% | OTel 测试 |

### 4. 资源

| 指标 | 目标 | 测量方法 |
|---|---|---|
| CPU | < 70%(1.5M samples/s 持续) | `process_cpu_seconds_total` |
| 内存 | < 8G | `go_memstats_heap_inuse_bytes` |
| Goroutines | < 5000 | `gateway_goroutines` |
| FD | 增量 < 100 / 24h | `lsof -p $PID \| wc -l` |

### 5. 告警分级

#### 5.1 严重 (Critical)

- 错误率 > 1% 持续 5 分钟
- p99 延迟 > 1s 持续 5 分钟
- WAL 硬拒绝率 > 0
- 实例 down(healthz 503)

**响应 SLA**:on-call 5 分钟内确认,15 分钟内开始处置。

#### 5.2 警告 (Warning)

- 4xx/5xx 持续 > 10/s
- 鉴权失败 > 50/s
- p99 延迟 0.5-1s
- Goroutines > 5000
- Config reload 失败

**响应 SLA**:30 分钟内确认,4 小时内处置。

#### 5.3 信息 (Info)

- 单次 config reload 失败(后自动恢复)
- 单次 token reload 失败(后自动恢复)
- 短时(< 1m)背压拒绝

**响应 SLA**:下次工作日处理。

### 6. 容量规划

| 规模 | 实例数 | 备注 |
|---|---|---|
| < 500K samples/s | 1 | 单机 + 限流 |
| 500K - 1.5M | 2 | 主备 |
| 1.5M - 3M | 4 | LB 后 |
| > 3M | 8+ | 按 Kafka partition 数扩展 |

### 7. 性能基线回归

每次发版前跑性能回归:
```bash
RATE=1500000 DURATION=300s bash test/perf/profile.sh
```

判定标准:
- 持续 5 分钟 ≥ 1.5M samples/s
- p99 < 500ms
- 错误率 < 0.01%
- 内存 < 8G
- CPU < 70%

未达标时阻断发版。



---

## 13. 安全审计报告 {#13-安全审计报告}
> 审计日期：2026-08-13
> 审计方式：静态代码分析（未修改任何代码）
> 审计范围：认证授权、输入验证、配置密钥、网络传输、依赖并发五大维度
> 发现总数：**56 项**（高 11 / 中 25 / 低 20）

---

### 总体评估

项目在业务逻辑层有基本安全意识（RE2 正则免疫 ReDoS、yaml.v3 免疫反序列化、请求体大小限制、panic recovery 覆盖完整、限流双层防护），但在**密钥生命周期管理**和**网络传输安全**上存在系统性缺口。

最严重的攻击链：**伪造 X-Forwarded-For 绕过 Admin IP 白名单 → 无独立鉴权直接篡改路由规则**。

---

### 风险分布

| 攻击面 | 高 | 中 | 低 | 合计 |
|---|---|---|---|---|
| 认证与授权 | 3 | 4 | 3 | 10 |
| 配置与密钥 | 6 | 6 | 5 | 17 |
| 输入验证 | 1 | 6 | 4 | 11 |
| 网络与传输 | 1 | 3 | 1 | 5 |
| 依赖与并发 | 0 | 6 | 7 | 13 |
| **合计** | **11** | **25** | **20** | **56** |

---

### 关键高风险问题（11 项，建议立即修复）

#### 1. Admin 安全边界可被完全绕过

**[高] 1.1 X-Forwarded-For 无条件信任导致 IP 白名单绕过**
- 位置：`internal/admin/helpers.go:37-49`、`internal/receiver/server.go:367-380`
- 描述：`parseClientIP()` 优先取 X-Forwarded-For 首段，未校验上游可信链。攻击者只需 `X-Forwarded-For: 127.0.0.1` 即可绕过 Admin IP 白名单。
- 影响：完全绕过 Admin 安全边界，可篡改规则配置、注入恶意规则、回滚到有漏洞的历史版本。
- 修复建议：
  1. 移除 Admin 的 XFF 解析，仅使用 `r.RemoteAddr`
  2. 增加 `--trusted-proxies` 配置，仅当 RemoteAddr 在可信代理网段时才解析 XFF
  3. 增加测试：`X-Forwarded-For: 127.0.0.1` + 外网 RemoteAddr 应被拒绝

**[高] 1.2 Admin 接口仅靠 IP 白名单，无独立鉴权**
- 位置：`internal/admin/server.go:240-262`
- 描述：Admin 暴露 `PUT /v1/rulesets/{name}`、`POST /v1/rulesets/{name}:reload`、`POST /v1/rulesets/{name}:rollback`、`GET /v1/tenants` 等敏感操作，均无 Token/mTLS/BasicAuth 保护。
- 修复建议：
  1. 在 `allowlistMW` 后增加 `authMW`，要求 Admin 请求携带独立 admin token
  2. 长期按 `internal/admin/doc.go:2` 注释规划，接入 mTLS + OIDC
  3. 对写操作增加审计日志（who/when/what）

#### 2. 全部 HTTP 服务明文监听，Token 网络明文传输

**[高] 2.1 四个 HTTP server 均无 TLS**
- 位置：
  - `internal/receiver/server.go:103-107`（receiver `:19201`）
  - `internal/admin/server.go:155-159`（admin `:8082`）
  - `cmd/prom-gw/main.go:470-474`（metrics `:8080`）
  - `cmd/prom-gw/main.go:490-494`（health `:8081`）
- 描述：全部使用 `ListenAndServe()`，无 `ListenAndServeTLS()`。客户端 `Authorization: Bearer tk_xxx` 在网络完全明文。
- 修复建议：
  1. 增加 `--tls-cert` / `--tls-key` flag
  2. 默认拒绝明文模式，通过 `--insecure` 显式开启（仅 dev）
  3. 配置 `tls.Config{MinVersion: tls.VersionTLS12, CipherSuites: ...}` 禁用弱套件
  4. 至少为 receiver 和 admin 强制 TLS

#### 3. 默认 Token 入仓且可猜测

**[高] 3.1 开发 Token 明文入仓**
- 位置：`configs/tokens/local.yaml:5-15`
- 描述：明文存放 `tk_app_business_dev`、`tk_infra_dev`，命名可猜测，不符合 `docs/user/auth.md:14-20` 规定的 `tk_<team>_<env>_<random>` 格式。
- 修复建议：
  1. 从 git 历史移除该文件（`git filter-repo`），改用 `local.yaml.example` 模板
  2. dev token 也应包含随机段，如 `tk_app_business_dev_a3f7b2e9c1`
  3. 启动时检测 token 格式，不匹配则拒绝启动

**[高] 3.2 .gitignore 规则与实际文件路径不匹配**
- 位置：`.gitignore:38-39`
- 描述：规则 `configs/tokens.local.yaml` 锚定仓库根，匹配的是 `configs/` 下名为 `tokens.local.yaml` 的文件，而非 `configs/tokens/local.yaml`（子目录形式），导致文件实际已入仓。
- 修复建议：修正为
  ```
  configs/tokens/local.yaml
  configs/tokens/prod.yaml
  configs/tokens/*.local.yaml
  configs/tokens/*.prod.yaml
  ```

#### 4. Token 明文存储，无加密无文件权限校验

**[高] 4.1 Token 明文存储（内存+配置）**
- 位置：`internal/config/token.go:67-83`
- 描述：以明文 token 作为 map key，配置文件也是明文。内存 dump 或配置文件泄露时 Token 立即可用。
- 修复建议：存储时只保留 `SHA-256(token)` 作为 key，配置文件中使用 `hashed_token` 字段，或通过 secret manager 注入。

**[高] 4.2 无文件权限校验**
- 位置：`internal/config/token.go:56`、`internal/config/source.go:163`
- 描述：加载 Token 和 ruleset 文件时不检查文件权限。生产指南声称"token 文件权限 0600"，但代码未强制。
- 修复建议：在 `Reload` 中调用 `os.Stat(path)`，检查 `mode & 0o077 == 0`，否则返回 error 或 warn。

#### 5. Nacos 凭据通过命令行 flag 传入

**[高] 5.1 Nacos 用户名/密码通过 CLI flag**
- 位置：`cmd/prom-gw/main.go:73-74`
- 描述：`--nacos-username` / `--nacos-password` 会被 `ps aux` 和 `/proc/<pid>/cmdline` 看到。
- 修复建议：改为从环境变量 `NACOS_USERNAME` / `NACOS_PASSWORD` 读取，或支持从 Vault/K8s Secret 拉取。

#### 6. Nacos 通信未加密

**[高] 6.1 Nacos 无 TLS 配置**
- 位置：`internal/config/nacos.go:98-120`
- 描述：`NewNacosSDKClient` 构造 ServerConfig 时未设置 TLS 选项，Nacos 通信默认走明文 HTTP，凭据和配置明文传输。
- 修复建议：在 `NacosConfig` 中增加 `TLSEnable bool` / `TLSConfig *tls.Config`，生产环境强制开启。

#### 7. Ansible systemd 模板缺失安全加固

**[高] 7.1 Ansible 模板与 systemd 模板安全姿态不一致**
- 位置：`deploy/ansible/roles/prom_gw/templates/prom-gw.service.j2`
- 描述：完全缺失 `NoNewPrivileges`、`ProtectSystem`、`ProtectHome`、`PrivateTmp`、`PrivateDevices`、`ProtectKernelTunels`、`ProtectKernelModules`、`ProtectControlGroups`、`RestrictSUIDSGID`、`LockPersonality`、`RestrictRealtime`、`RestrictNamespaces`、`MemoryMax`、`TasksMax`、`ReadWritePaths` 等加固项。
- 修复建议：将 `prom-gw@.service` 中的所有安全加固项同步到 `prom-gw.service.j2`，用变量参数化。

#### 8. WAL 文件权限过宽

**[高] 8.1 WAL 文件 0o644 含敏感业务数据**
- 位置：`internal/wal/wal.go:282`、`internal/wal/wal.go:369`
- 描述：WAL 段文件以 `0o644` 创建。WAL 存储原始 Prometheus WriteRequest（可能含 PII label 如 user_id、email）、tenant 名、traceparent 等，同机任何用户可读。
- 修复建议：文件权限改为 `0o600`，目录 `0o700`，确保 systemd `ReadWritePaths=/data/wal` 且 owner 为 `prom-gw`。

#### 9. kafkasink 不支持 SASL/SSL

**[高] 9.1 Kafka 客户端无 SASL/SSL，与生产文档矛盾**
- 位置：`internal/kafkasink/producer.go:179-194`
- 描述：`kgo.NewClient` 的 opts 未包含任何 `kgo.SASL` 选项或 TLS 配置。生产部署指南声称"9094 Kafka 客户端访问(SSL/SASL)"，但代码无法连接 SSL/SASL Kafka。
- 修复建议：在 `kafkasink.Config` 中增加 `SASL` / `TLS` 字段，构造 client 时注入对应 `kgo.Opt`。

#### 10. pprof/metrics 端点无鉴权

**[高] 10.1 /debug/pprof/* 和 /metrics 完全无鉴权**
- 位置：`cmd/prom-gw/main.go:463-474`
- 描述：`:8080` 端口暴露的 `/debug/pprof/*` 可触发 heap/profile 抓取，泄露 goroutine 栈、内存布局；`/metrics` 暴露 `gateway_auth_fail_total{reason}`、`gateway_samples_total{tenant}` 等，泄露 tenant 列表和失败率。
- 修复建议：
  1. pprof 端点单独绑 `127.0.0.1:8080`
  2. 或为 `/debug/pprof/*` 加独立 BasicAuth / Token 中间件
  3. metrics 端口默认绑内网接口

---

### 中风险问题（25 项，建议 1-2 周内修复）

#### 认证与授权

| 编号 | 问题 | 位置 |
|---|---|---|
| M-1 | Token 比较未用常量时间比较 | `internal/config/token.go:101-104` |
| M-2 | 无 Token 过期机制，泄露后永久有效 | `internal/auth/authenticator.go:23-26` |
| M-3 | 默认白名单 `10.0.0.0/8` 过宽（1600 万 IP） | `internal/admin/server.go:132-134` |
| M-4 | Admin 写操作成功路径无审计日志 | `internal/admin/server.go:335-399` |

#### 输入验证

| 编号 | 问题 | 位置 |
|---|---|---|
| M-5 | X-Source-DC 头无校验，可污染指标和 Kafka header | `internal/receiver/server.go:221-223` |
| M-6 | HTTP server 缺 WriteTimeout/ReadTimeout/IdleTimeout，slowloris 风险 | `internal/receiver/server.go:103-107`、`internal/admin/server.go:155-159` |
| M-7 | protobuf 解码无 TimeSeries 数量上限，64MB payload 可含数十万条 series 导致 OOM | `internal/parser/parser.go:68` |
| M-8 | 无 Labels 数量限制，单条 series 可含数万 label | `internal/parser/parser.go:86-128` |
| M-9 | Kafka topic 名称无正则校验 | `internal/kafkasink/producer.go:264-266` |
| M-10 | AllowAutoTopicCreation 静默创建错误 topic | `internal/kafkasink/producer.go:187` |
| M-11 | 规则执行无超时限制 | `internal/ruleengine/pipeline.go:134-246` |

#### 配置与密钥

| 编号 | 问题 | 位置 |
|---|---|---|
| M-12 | Nacos 快照文件权限 `0o644` | `internal/config/nacos.go:305,314` |
| M-13 | Nacos 快照无完整性校验（无 HMAC/签名） | `internal/config/nacos.go:284-298` |
| M-14 | OTLP Tracing 硬编码 `Insecure: true` | `cmd/prom-gw/main.go:99` |
| M-15 | prom-gw@.service 允许 `LimitCORE=infinity`，core dump 可能含 Token | `deploy/systemd/prom-gw@.service:30` |
| M-16 | 缺少 Capability 限制与 SystemCallFilter | `deploy/systemd/prom-gw@.service:45-58` |
| M-17 | Ansible 未渲染/校验 Token 文件 | `deploy/ansible/roles/prom_gw/tasks/main.yml:38-54` |

#### 依赖与并发

| 编号 | 问题 | 位置 |
|---|---|---|
| M-18 | 限流单位为"请求/秒"而非"样本/秒"，与设计不符 | `internal/receiver/tenant_rl.go:77` |
| M-19 | WAL fsync 持锁串行化，吞吐瓶颈 200-1000 写/秒 | `internal/wal/wal.go:453-483` |
| M-20 | readSegment 缺 totalLen 上界，损坏文件可触发 4GB 内存分配 | `internal/wal/wal.go:702-706` |
| M-21 | gogo/protobuf 已废弃，无上游修复 | `go.mod:7` |
| M-22 | 缺少 bodyclose/depguard/noctx 等关键安全 linter | `.golangci.yml` |
| M-23 | Token error message 泄露明文到日志 | `internal/config/token.go:70,73,76` |
| M-24 | WAL 目录权限过宽 `0o755` | `internal/wal/wal.go:204` |
| M-25 | WAL 无静态加密 | `internal/wal/wal.go:795-832` |

---

### 低风险问题（20 项，逐步改进）

| 编号 | 问题 | 位置 |
|---|---|---|
| L-1 | 鉴权失败响应泄露 reason 分类 | `internal/receiver/server.go:197` |
| L-2 | Bearer 前缀匹配区分大小写 | `internal/receiver/server.go:359-365` |
| L-3 | 路由无显式鉴权白名单 | `internal/receiver/server.go:123-127` |
| L-4 | /v1/tenants 暴露 tenant 元数据 | `internal/admin/server.go:84-90` |
| L-5 | Token 强度不足（无随机熵） | `configs/tokens/local.yaml:5,11` |
| L-6 | computeMD5 命名误导（非加密哈希） | `internal/config/source.go:504-511` |
| L-7 | 默认 Token 路径指向开发文件 | `cmd/prom-gw/main.go:59` |
| L-8 | systemd EnvironmentFile 使用 `-` 前缀（可选） | `deploy/systemd/prom-gw@.service:43` |
| L-9 | WAL 文件路径可预测 | `internal/wal/wal.go:367` |
| L-10 | 启动日志输出配置文件路径 | `cmd/prom-gw/main.go:114-121` |
| L-11 | OTLP exporter 无证书校验配置 | `internal/obs/tracing.go:93-95` |
| L-12 | 文档引导内网不启用 TLS | 高可用与负载均衡部署章节 |
| L-13 | 错误消息泄露内部实现细节 | `internal/receiver/server.go:252,268` |
| L-14 | 自定义 readAll 用字符串比较检测 EOF | `internal/receiver/server.go:403-419` |
| L-15 | decoder.Decode 无独立 panic recovery | `internal/decoder/decoder.go:49-74` |
| L-16 | 测试代码中硬编码 Token 字面量 | `internal/config/token_test.go:14-26` |
| L-17 | group_vars 与 env.j2 无硬编码密钥（良好） | `deploy/ansible/inventory/group_vars/all.yml` |
| L-18 | 鉴权失败日志不含 Token（良好） | `internal/receiver/server.go:193-196` |
| L-19 | Admin API 不返回 Token 明文（良好） | `internal/config/token.go:120-138` |
| L-20 | pipeline.go buffer 交换逻辑空 slice index 风险（被 recover 兜底） | `internal/ruleengine/pipeline.go:174,182` |

---

### 正面发现（设计正确的部分）

- **RE2 正则引擎**：Go regexp 天然免疫 ReDoS（`internal/ruleengine/stage.go:578`）
- **yaml.v3**：免疫 yaml.v2 反序列化漏洞（`internal/ruleengine/compiler.go:14`）
- **请求体大小限制**：16MB + Content-Type/Encoding 严格校验（`internal/receiver/server.go:95,236,248`）
- **状态型 stage 有内存上限**：deadvalue/downsample LRU 默认 1M series
- **原子规则热更新**：`atomic.Pointer[CompiledRuleSet]` 无锁切换，批次内一致
- **Panic recovery 覆盖完整**：safego 封装所有后台 goroutine，HTTP 中间件 + stage 级 recover
- **WAL 双阈值有界**：50GB + 80% 磁盘使用率，retention 24h 自动清理
- **TenantInfo 显式排除 token 字段**：`internal/admin/server.go:84-90`
- **限流 key 不可伪造**：tenant 由服务端 token 鉴权决定，不读取 HTTP header
- **Kafka producer 队列有界**：65535 + 100ms BlockTimeout
- **Graceful shutdown**：SIGINT/SIGTERM 触发，30s 超时，defer 链有序关闭

---

### 修复优先级建议

#### P0 立即修复（1-3 天）

1. **移除 Admin `parseClientIP` 的 XFF 信任**（`internal/admin/helpers.go:37-49`），仅用 `r.RemoteAddr`
2. **从 git 移除 `configs/tokens/local.yaml`**，修正 `.gitignore`，所有环境签发强 Token
3. **WAL 文件权限 `0o644` → `0o600`**，目录 `0o700`（一行改动）
4. **Nacos 凭据改环境变量**，移除 `--nacos-username`/`--nacos-password` CLI flag

#### P1 短期修复（1-2 周）

5. 为 receiver + admin 增加 TLS 监听入口，生产强制启用
6. Admin 增加独立 Token 鉴权层
7. 所有 HTTP server 添加 `ReadTimeout`/`WriteTimeout`/`IdleTimeout`
8. protobuf 解码增加 series/label 数量上限
9. 限流改为按样本数计费 `limiter.AllowN(len(samples))`
10. Token 存储改哈希 + 常量时间比较
11. pprof 绑定 127.0.0.1，metrics 加鉴权
12. Kafka topic 名称正则校验，生产禁用 AllowAutoTopicCreation

#### P2 长期改进（1 个月）

13. kafkasink 增加 SASL/SSL 支持
14. Nacos 通信启用 TLS
15. Ansible systemd 模板补齐安全加固项
16. Token 过期机制 + 审计日志
17. mTLS 支持
18. golangci.yml 增加 bodyclose/depguard/noctx linter
19. 迁移 gogo/protobuf 到 google.golang.org/protobuf

---

### 待办清单

- [ ] P0-1: 移除 Admin XFF 信任
- [ ] P0-2: 从 git 移除默认 Token，修正 .gitignore
- [ ] P0-3: WAL 文件权限收紧到 0o600
- [ ] P0-4: Nacos 凭据改环境变量
- [ ] P1-5: receiver + admin 增加 TLS
- [ ] P1-6: Admin 独立 Token 鉴权
- [ ] P1-7: HTTP server 超时配置
- [ ] P1-8: protobuf series/label 数量上限
- [ ] P1-9: 限流改为按样本数计费
- [ ] P1-10: Token 哈希存储 + 常量时间比较
- [ ] P1-11: pprof 绑定 127.0.0.1
- [ ] P1-12: Kafka topic 正则校验
- [ ] P2-13: kafkasink SASL/SSL
- [ ] P2-14: Nacos TLS
- [ ] P2-15: Ansible systemd 加固
- [ ] P2-16: Token 过期 + 审计日志
- [ ] P2-17: mTLS 支持
- [ ] P2-18: golangci.yml linter 补充
- [ ] P2-19: 迁移 gogo/protobuf


