# 生产部署与使用指南

> 本文档覆盖 Prometheus + Kafka + prom-gw 全链路生产部署、配置说明、测试验证与运维操作。
> 配套文档:`deploy.md`(部署速查)、`runbook.md`(故障剧本)、`slo.md`(SLO 指标)、`ruleset-reference.md`(规则配置)。

## 目录

1. [架构概述](#1-架构概述)
2. [环境要求与资源规划](#2-环境要求与资源规划)
3. [Kafka 集群部署](#3-kafka-集群部署)
4. [Prometheus 部署与配置](#4-prometheus-部署与配置)
5. [prom-gw 编译与部署](#5-prom-gw-编译与部署)
6. [LVS 负载均衡部署](#6-lvs-负载均衡部署)
7. [端到端测试验证](#7-端到端测试验证)
8. [监控与告警接入](#8-监控与告警接入)
9. [Admin API 使用](#9-admin-api-使用)
10. [运维操作](#10-运维操作)
11. [故障排查](#11-故障排查)
12. [安全加固](#12-安全加固)

---

## 1. 架构概述

### 1.1 整体拓扑

```
三城同城采集 → 同城清洗聚合 → 跨城汇聚到北京 StarRocks

DC-A Prometheus ─┐                        ┌─> Kafka BJ ─> Flink BJ ─┐
DC-B Prometheus ─┼─> LVS → prom-gw (各机房) ─┤                        ├─> StarRocks (北京)
DC-C Prometheus ─┘                        └─> Kafka SZ ─> Flink SZ ─┘
                                           └─> Kafka HF ─> Flink HF ─┘
```

### 1.2 组件职责

| 组件 | 职责 | 部署形态 |
|---|---|---|
| **Prometheus** | 采集本地业务指标,通过 `remote_write` 上报 | 每机房 1~2 套(已有) |
| **LVS** | 4 层负载均衡,DR 模式转发到 prom-gw | 每机房 2 台主备(Keepalived) |
| **prom-gw** | RemoteWrite 网关,鉴权/限流/规则清洗/Kafka 投递/WAL 故障切换 | 每机房 2~4 个实例(VM) |
| **Kafka** | 同城消息队列,KRaft 模式,3 副本 | 每机房 3 Broker(物理机) |
| **Flink** | 同城 5min 聚合,跨城 Stream Load 写 StarRocks | 每机房 JM×2 + TM×2~6 |
| **StarRocks** | 统一查询分析层,3 独立物理表 + 级联聚合 | 北京 3 节点(物理机) |
| **Nacos**(可选) | 配置中心,ruleset 热推送 | 北京 3 节点 |

### 1.3 数据可靠性保证

- Kafka producer:`acks=all` + `enable.idempotence=true` + `delivery.timeout.ms=120000` + `retries=10`
- Kafka 故障时 prom-gw 自动降级到本地 WAL(`/data/wal`),恢复后自动 drain 回灌
- WAL 使用 segment + CRC32 校验,启动时 replay 未 `.done` 的段
- 跨城仅传 5min 聚合数据(1TB/天,占 1G 专线 9.3%),原始 sample 明细严禁跨城

---

## 2. 环境要求与资源规划

### 2.1 硬件资源清单

| 角色 | 形态 | 单台规格 | 数量(BJ/SZ/HF) | 小计 | 备注 |
|---|---|---|---|---|---|
| **Prometheus** | 已有 | 8C/16G | 2/2/1 | 5 | 已在生产运行 |
| **LVS (Keepalived)** | VM | 8C/16G/200G | 2/2/2 | 6 | 每机房主备 |
| **prom-gw** | VM | 16C/32G/500G SSD | 4/4/2 | 10 | `prom-gw@<city>.service` |
| **Kafka Broker (KRaft)** | 物理机 | 64C/512G/12×16T HDD JBOD | 3/3/3 | 9 | 3 副本,3 天留存 |
| **Flink JobManager** | VM | 32C/64G/1T | 2/2/2 | 6 | 1 Active + 1 Standby |
| **Flink Zookeeper** | VM | 8C/16G/200G | 3/3/3 | 9 | HA 选主 |
| **Flink TaskManager** | VM | 16C/32G/500G SSD | 6/4/2 | 12 | 每 TM 4 slot |
| **StarRocks (FE+BE)** | 物理机 | 64C/512G/1.92T×22 SSD | 3(全北京) | 3 | 混合部署 |
| **Nacos** | VM | 16C/32G/1T | 3(北京) | 3 | 配置中心 |

### 2.2 操作系统

- Linux(x86_64),内核 ≥ 4.19
- systemd ≥ 245(支持 `MemoryMax` / `TasksMax`)
- 文件系统:ext4 或 xfs(WAL/Kafka 目录建议 `noatime` 挂载)
- 时间同步:全集群 `chrony` 对齐北京 NTP 源(北斗 + GPS)

### 2.3 网络规划

| 链路 | 带宽 | 延迟 | 说明 |
|---|---|---|---|
| Prom → LVS | 10G 同城 LAN | < 1ms | `remote_write` 到 LVS VIP |
| LVS → prom-gw | 10G 内网 | < 1ms | DR 模式直接转发 |
| prom-gw → Kafka | 10G 内网 | < 1ms | Kafka `advertised.listeners` 绑内网 |
| Flink → StarRocks | 走 HTTP 8070 Stream Load | — | FE VIP 负载均衡 |
| 深圳 ⇄ 北京专线 | 1G×2(主备) | P95 ≤ 30ms | 跨城仅传 5min 聚合 |
| 合肥 ⇄ 北京专线 | 1G×1 | P95 ≤ 25ms | 故障时降级本地 ClickHouse |

### 2.4 端口规划

| 端口 | 组件 | 用途 | 暴露范围 |
|---|---|---|---|
| `9090` | Prometheus | Web UI / API | 运维网段 |
| `9094` | Kafka | 客户端访问(SSL/SASL) | prom-gw + Flink |
| `9093` | Kafka | Controller(KRaft) | Kafka 内部 |
| `19201` | prom-gw | RemoteWrite 接入 | Prometheus / LVS |
| `8080` | prom-gw | `/metrics` + pprof | Prometheus 抓取 |
| `8081` | prom-gw | healthz / readyz | LB health check |
| `8082` | prom-gw | Admin API | 运维网段(白名单) |
| `8030` | StarRocks FE | Web UI | 运维网段 |
| `8070` | StarRocks FE | Stream Load | Flink |
| `9060` | StarRocks FE | 查询服务 | 应用层 |

---

## 3. Kafka 集群部署

### 3.1 前置准备(JDK / 用户 / 系统调优 / 安装)

> 三台 Broker 物理机均执行以下步骤。

**JDK 17 安装**(Kafka 3.4+ KRaft 要求 JDK 11/17):

```bash
# CentOS / RHEL
sudo yum install -y java-17-openjdk java-17-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-17-jdk
java -version   # 确认输出 openjdk version "17.x.x"
```

**创建 Kafka 用户与目录**:

```bash
sudo useradd -r -m -d /opt/kafka -s /sbin/nologin kafka
sudo mkdir -p /opt/kafka /data/kafka /var/log/kafka
sudo chown -R kafka:kafka /opt/kafka /data/kafka /var/log/kafka
```

**内核参数调优**(`/etc/sysctl.d/99-kafka.conf`):

```ini
vm.swappiness=1
vm.max_map_count=262144
vm.dirty_ratio=10
vm.dirty_background_ratio=2
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
fs.file-max=1000000
```

```bash
sudo sysctl --system
```

**文件句柄**(`/etc/security/limits.d/kafka.conf`):

```
kafka  soft  nofile  100000
kafka  hard  nofile  100000
kafka  soft  nproc   100000
kafka  hard  nproc   100000
```

**下载并安装 Kafka**(3.4.0, Scala 2.13):

```bash
cd /opt
sudo wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
sudo tar -xzf kafka_2.13-3.4.0.tgz
sudo ln -s kafka_2.13-3.4.0 kafka
sudo chown -R kafka:kafka /opt/kafka
ls /opt/kafka/bin/kafka-server-start.sh   # 确认解压成功
```

### 3.2 集群规划

每机房部署 3 Broker KRaft 模式(无 ZooKeeper 依赖),跨机架 `2+1` 分布在 2 个 AZ:

```
AZ-1: Kafka-1, Kafka-2
AZ-2: Kafka-3
```

### 3.3 磁盘挂载(JBOD)

每台 Kafka 物理机 12 × 16T HDD,JBOD 模式(不做 RAID):

```bash
# /etc/fstab 挂载 12 块盘
/dev/sdb1 /data/kafka/log-0  ext4 noatime,nodiratime 0 2
/dev/sdc1 /data/kafka/log-1  ext4 noatime,nodiratime 0 2
/dev/sdd1 /data/kafka/log-2  ext4 noatime,nodiratime 0 2
# ... 依此类推到 log-11
```

```bash
sudo mkdir -p /data/kafka/log-{0..11}
sudo chown -R kafka:kafka /data/kafka
```

### 3.4 Kafka 配置

**`/opt/kafka/config/server.properties`(Broker 1 示例)**:

```properties
# ====== 基础 ======
broker.id=1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
listeners=PLAINTEXT://:9094,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://kafka-1:9094
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=/data/kafka/log-0,/data/kafka/log-1,/data/kafka/log-2,/data/kafka/log-3,/data/kafka/log-4,/data/kafka/log-5,/data/kafka/log-6,/data/kafka/log-7,/data/kafka/log-8,/data/kafka/log-9,/data/kafka/log-10,/data/kafka/log-11

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

# ====== Rack awareness ======
broker.rack=az-1                          # Kafka-1/Kafka-2 = az-1, Kafka-3 = az-2
replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector

# ====== KRaft ======
log.dirs=/data/kafka/log-0,/data/kafka/log-1,/data/kafka/log-2,/data/kafka/log-3,/data/kafka/log-4,/data/kafka/log-5,/data/kafka/log-6,/data/kafka/log-7,/data/kafka/log-8,/data/kafka/log-9,/data/kafka/log-10,/data/kafka/log-11
metadata.log.dir=/data/kafka/log-0
```

### 3.5 JVM 配置

**`/opt/kafka/bin/kafka-server-start.sh` 修改 `KAFKA_HEAP_OPTS`**:

```bash
export KAFKA_HEAP_OPTS="-Xms32g -Xmx32g -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+ExplicitGCInvokesConcurrent -XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

### 3.6 systemd 管理

**`/etc/systemd/system/kafka.service`**:

```ini
[Unit]
Description=Apache Kafka (KRaft mode)
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms32g -Xmx32g -XX:+UseG1GC"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=5
LimitNOFILE=100000
LimitNPROC=100000
MemoryMax=48G

# 安全
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/data/kafka

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now kafka
```

### 3.7 格式化存储(KRaft 首次启动)

```bash
# 生成 Cluster UUID
CLUSTER_UUID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"

# 每台 Broker 格式化
/opt/kafka/bin/kafka-storage.sh format \
  --config /opt/kafka/config/server.properties \
  --cluster-id $CLUSTER_UUID \
  --add-scram
```

### 3.8 创建 Topic

```bash
# 原始数据 topic(每城每个 tenant 一个)
for city in bj sz hf; do
  for tenant in app_business infra; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 \
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
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 \
      --create --topic prom.${city}.routed.${biz} \
      --partitions 64 \
      --replication-factor 3 \
      --config retention.ms=259200000
  done
done

# 验证
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --list
```

### 3.9 Kafka 验证

```bash
# 1. Broker 状态
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9094 | head

# 2. Controller 选举
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9094 describe --status

# 3. Topic 列表
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --list | grep prom

# 4. 消费测试
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9094 \
  --topic prom.bj.raw.app_business \
  --from-beginning --max-messages 5 --timeout-ms 10000

# 5. 生产测试
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9094 \
  --topic prom.bj.raw.app_business
```

---

## 4. Prometheus 部署与配置

### 4.1 Prometheus 安装(全新部署,已有环境可跳过)

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

### 4.2 remote_write 配置对接 prom-gw

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

### 4.3 高可用配置(多实例)

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

### 4.4 Prometheus 验证

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

## 5. prom-gw 编译与部署

### 5.1 编译

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

### 5.2 Token 配置

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

### 5.3 Ruleset 配置

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

### 5.4 systemd template 部署

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
KAFKA_BROKERS=kafka-1:9094,kafka-2:9094,kafka-3:9094
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

### 5.5 启动参数速查

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

## 6. LVS 负载均衡部署

### 6.1 LVS + Keepalived 配置

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

### 6.2 prom-gw 实例配置 VIP

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

## 7. 端到端测试验证

### 7.1 测试环境准备

```bash
# 1. 确认 Kafka 可达
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9094 | head

# 2. 确认 Topic 已创建
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --list | grep prom

# 3. 编译 prom-gw
make build

# 4. 确认配置文件
cat configs/tokens/local.yaml
cat configs/rules/app-business.yaml
```

### 7.2 测试 1:WAL-only 模式冒烟测试(无 Kafka)

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

### 7.3 测试 2:完整端到端测试(Kafka + prom-gw)

> 验证数据从 Prometheus → prom-gw → Kafka 全链路。

**启动 prom-gw(连接 Kafka)**:

```bash
KAFKA_BROKERS=kafka-1:9094,kafka-2:9094,kafka-3:9094 \
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
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9094 \
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

### 7.4 测试 3:WAL 故障切换测试

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

### 7.5 测试 4:规则引擎验证

> 验证 relabel/route/sample 规则正确执行。

```bash
# 使用 app-business ruleset(包含 relabel + route + sample)
KAFKA_BROKERS=kafka-1:9094 \
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
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9094 \
  --topic prom.bj.routed.core \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head

# 验证 ruleset 指标
curl -s http://127.0.0.1:8080/metrics | grep gateway_ruleset_routed_total

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-rule-wal /tmp/rule-test.go /tmp/rule-payload.bin
```

### 7.6 测试 5:单元测试 + 集成测试

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

### 7.7 测试 6:全链路验证清单

部署完成后,按以下清单逐项验证:

| 序号 | 验证项 | 命令 | 期望结果 |
|---|---|---|---|
| 1 | Kafka Broker 状态 | `kafka-broker-api-versions.sh --bootstrap-server kafka-1:9094` | 3 个 Broker 在线 |
| 2 | Topic 列表 | `kafka-topics.sh --list --bootstrap-server kafka-1:9094 \| grep prom` | 包含 raw/routed topic |
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

## 8. 监控与告警接入

### 8.1 Prometheus 抓取配置

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
        - kafka-1:9094
        - kafka-2:9094
        - kafka-3:9094
```

### 8.2 关键指标速查

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

### 8.3 告警规则

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

### 8.4 Grafana 大盘

导入 `deploy/grafana/dashboards/prom-gw.json`,选 Prometheus 数据源。

---

## 9. Admin API 使用

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

## 10. 运维操作

### 10.1 热重载 Token

```bash
sudo vim /etc/prom-gw/tokens.yaml
sudo kill -HUP $(pidof prom-gw)
sudo journalctl -u prom-gw@bj --since "1m ago" | grep "tokens reloaded"
```

### 10.2 热更新 Ruleset

**文件监听(自动)**:修改 `/etc/prom-gw/config-<city>.yaml` 后 fsnotify 5s 内自动检测。

**Admin API(手动)**:

```bash
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @new-ruleset.yaml
```

### 10.3 滚动升级

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

### 10.4 优雅停机顺序

prom-gw 收到 SIGTERM 后(spec §6.5):

1. 停止接收新请求(receiver Shutdown,超时 30s)
2. drain WAL → Kafka(超时 30s)
3. 关闭 WAL(确保 pending 数据落盘)
4. 关闭 Kafka producer(等待 in-flight 消息 ack,超时 30s)
5. 关闭 Admin API
6. 关闭 tracing exporter

### 10.5 Ansible 一键部署/回滚

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

## 11. 故障排查

### 11.1 常见问题速查

| 现象 | 可能原因 | 排查命令 |
|---|---|---|
| 写入 401 | token 错误/过期 | `journalctl -u prom-gw \| grep "auth failed"` |
| 写入 403 | IP 不在白名单 | `journalctl -u prom-gw \| grep "source ip"` |
| 写入 503 | 背压(Kafka 满/WAL 满) | `curl /metrics \| grep backpressure` |
| p99 延迟高 | Kafka 慢/规则引擎慢 | `go tool pprof http://:8080/debug/pprof/profile` |
| WAL 硬拒绝 | 磁盘满/WAL 超限 | `df -h /data/wal` |
| OOM | 状态型 stage series 过多 | `go tool pprof http://:8080/debug/pprof/heap` |
| Kafka 消费无数据 | topic 未创建/路由错误 | `kafka-topics.sh --list` |

### 11.2 日志关键字

| 关键字 | 含义 |
|---|---|
| `receiver listening` | receiver 启动成功 |
| `kafkasink started` | Kafka producer 启动成功 |
| `sink adapter: switched to WAL degraded mode` | Kafka 故障,降级 WAL |
| `sink adapter: kafka recovered, switching back` | Kafka 恢复,切回 + drain |
| `sink adapter: draining WAL to Kafka` | 正在 drain WAL |
| `rule engine: rules swapped` | ruleset 热切换成功 |
| `tokens reloaded` | token 热重载成功 |

### 11.3 全实例巡检

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

## 12. 安全加固

### 12.1 systemd 安全选项(已配置)

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

### 12.2 Token 管理

- token 文件权限 `0600`,owner `root:prom-gw`
- 生产 token 不入仓,通过 Ansible vault 或 secret manager 分发
- token 定期轮换(SIGHUP 热重载,不中断服务)

### 12.3 网络隔离

- RemoteWrite 端口(`:19201`)仅对 Prometheus/LVS 开放
- Metrics 端口(`:8080`)仅对 Prometheus 抓取实例开放
- Admin 端口(`:8082`)仅对运维网段开放
- Kafka 端口(`:9094`)仅对 prom-gw + Flink 开放
- 跨机房走专线,不开公网

---

## 附录

### A. 文件布局

```
/opt/prom-gw/bin/prom-gw                    # prom-gw 二进制
/etc/prom-gw/
  ├── tokens.yaml                           # token 配置
  ├── prom-gw.env                           # 环境变量(KAFKA_BROKERS 等)
  ├── config-bj.yaml                        # 北京 ruleset
  ├── config-sz.yaml                        # 深圳 ruleset
  └── config-hf.yaml                        # 合肥 ruleset
/data/wal/                                  # prom-gw WAL 数据
/opt/kafka/                                 # Kafka 安装目录
/data/kafka/log-{0..11}/                    # Kafka 数据(JBOD 12 盘)
/etc/systemd/system/
  ├── prom-gw@.service                      # prom-gw template unit
  └── kafka.service                         # Kafka service
```

### B. 相关文档

- [设计文档](../superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md)
- [部署速查](deploy.md)
- [故障剧本](runbook.md)
- [SLO 指标](slo.md)
- [排障手册](troubleshooting.md)
- [高可用与负载均衡部署指南](ha-lb-deployment.md)
- [Kafka 生产部署](kafka-production-deployment.md) — SASL/SSL、监控、运维、扩缩容
- [Flink 生产部署](flink-production-deployment.md) — JM HA、作业管理、Checkpoint
- [Ruleset 配置参考](../user/ruleset-reference.md)
- [5 分钟接入](../user/quickstart.md)
- [鉴权说明](../user/auth.md)
