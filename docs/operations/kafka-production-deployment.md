# Kafka 生产部署完整步骤

> 本文档覆盖 prom-gw 配套 Kafka 集群的生产环境完整部署,包括 KRaft 集群搭建、SASL/SSL 安全认证、监控告警、Topic 管理、性能调优、扩缩容、备份恢复和灾难恢复。
>
> 基础安装步骤( JDK / 系统调优 / KRaft 格式化 / Topic 创建)见 [生产部署指南 §3](production-guide.md#3-kafka-集群部署),本文档聚焦**安全、监控、运维和调优**,与 §3 互补。
>
> 配套文档:[生产部署指南](production-guide.md)、[高可用与负载均衡](ha-lb-deployment.md)、[Flink 生产部署](flink-production-deployment.md)、[压力测试指南](stress-test-guide.md)、[故障剧本](runbook.md)

## 目录

1. [部署架构](#1-部署架构)
2. [前置准备](#2-前置准备)
3. [KRaft 集群安装](#3-kraft-集群安装)
4. [SASL/SSL 安全配置](#4-saslssl-安全配置)
5. [Topic 管理与最佳实践](#5-topic-管理与最佳实践)
6. [监控部署](#6-监控部署)
7. [性能调优](#7-性能调优)
8. [运维操作](#8-运维操作)
9. [扩容与缩容](#9-扩容与缩容)
10. [备份与恢复](#10-备份与恢复)
11. [灾难恢复](#11-灾难恢复)
12. [附录](#12-附录)

---

## 1. 部署架构

### 1.1 单机房标准拓扑

每机房部署 3 Broker KRaft 模式(无 ZooKeeper),跨 2 个 AZ 分布:

```
机房 (深圳)
┌──────────────── AZ-1 ────────────────┐  ┌──────── AZ-2 ─────────┐
│                                      │  │                       │
│  Kafka-1 (broker+controller)         │  │  Kafka-3              │
│  broker.id=1, rack=az-1             │  │  broker.id=3           │
│  10.0.1.21:9094(PLAINTEXT)          │  │  rack=az-2            │
│  10.0.1.21:9093(CONTROLLER)         │  │  10.0.1.23            │
│                                      │  │                       │
│  Kafka-2 (broker+controller)         │  │                       │
│  broker.id=2, rack=az-1             │  │                       │
│  10.0.1.22                          │  │                       │
│                                      │  │                       │
└──────────────────────────────────────┘  └───────────────────────┘

                   │
                   │ prom-gw / Flink 消费
                   │ SASL_SSL → 9094
                   ▼
          ┌─────────────────┐
          │  prom-gw × 4    │
          │  Flink × 2-6 TM │
          └─────────────────┘
```

### 1.2 端口规划

| 端口 | 协议 | 用途 | 暴露范围 |
|---|---|---|---|
| 9092 | PLAINTEXT | 本地调试(仅 dev) | 127.0.0.1 |
| 9093 | CONTROLLER | KRaft 控制器间通信 | Kafka 节点间 |
| 9094 | SASL_SSL | 生产客户端访问 | prom-gw / Flink 网段 |

### 1.3 资源规划

| 角色 | 规格 | 数量 | 磁盘 |
|---|---|---|---|
| Kafka Broker | 64C/512G | 3 | 12×16T HDD(JBOD) |
| prom-gw(客户端) | 8C/16G | 2-4 | 100G SSD(WAL) |
| Flink TM(客户端) | 16C/32G | 2-6 | 500G SSD(state) |

---

## 2. 前置准备

### 2.1 操作系统

```bash
# CentOS / RHEL 8+
cat /etc/redhat-release

# Ubuntu / Debian 22+
cat /etc/os-release
```

### 2.2 JDK 17 安装

```bash
# CentOS / RHEL
sudo yum install -y java-17-openjdk java-17-openjdk-devel
# Ubuntu / Debian
sudo apt install -y openjdk-17-jdk

java -version   # 期望: openjdk version "17.x.x"
```

### 2.3 创建 Kafka 用户与目录

```bash
sudo useradd -r -m -d /opt/kafka -s /sbin/nologin kafka
sudo mkdir -p /opt/kafka /data/kafka /var/log/kafka
sudo chown -R kafka:kafka /opt/kafka /data/kafka /var/log/kafka
```

### 2.4 内核参数调优

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

### 2.5 文件句柄限制

**`/etc/security/limits.d/kafka.conf`**:

```
kafka  soft  nofile  100000
kafka  hard  nofile  100000
kafka  soft  nproc   100000
kafka  hard  nproc   100000
```

### 2.6 磁盘挂载(JBOD)

每台 Kafka 物理机 12 × 16T HDD,JBOD 模式(不做 RAID,Kafka 自带副本冗余):

```bash
# /etc/fstab
/dev/sdb1 /data/kafka/log-0  ext4 noatime,nodiratime 0 2
/dev/sdc1 /data/kafka/log-1  ext4 noatime,nodiratime 0 2
# ... 依此类推到 log-11

sudo mkdir -p /data/kafka/log-{0..11}
sudo chown -R kafka:kafka /data/kafka
```

> **为什么不用 RAID?** Kafka 通过副本机制保证数据可靠性,JBOD 模式下单盘故障只影响该盘上的 partition,其他盘不受影响。RAID 会增加写放大和性能开销。

### 2.7 下载并安装 Kafka

```bash
cd /opt
sudo wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
sudo tar -xzf kafka_2.13-3.4.0.tgz
sudo ln -s kafka_2.13-3.4.0 kafka
sudo chown -R kafka:kafka /opt/kafka
ls /opt/kafka/bin/kafka-server-start.sh   # 确认解压成功
```

---

## 3. KRaft 集群安装

> 基础安装(JDK / 系统调优 / Kafka 下载)见 [生产部署指南 §3.1-§3.3](production-guide.md#31-前置准备jdk--用户--系统调优--安装),本节聚焦安全配置和 KRaft 格式化。

### 3.1 集群规划

| Broker | node.id | AZ | 角色 | rack |
|---|---|---|---|---|
| kafka-1 (10.0.1.21) | 1 | AZ-1 | broker+controller | az-1 |
| kafka-2 (10.0.1.22) | 2 | AZ-1 | broker+controller | az-1 |
| kafka-3 (10.0.1.23) | 3 | AZ-2 | broker+controller | az-2 |

> 3 节点 KRaft 可容忍 1 个节点故障(Quorum 需要 2/3 存活)。跨 2 个 AZ 分布,单 AZ 故障不影响集群。

### 3.2 Broker 配置

**`/opt/kafka/config/server.properties`(Broker 1 示例,Broker 2/3 修改 broker.id / node.id / advertised.listeners / rack)**:

```properties
# ====== 基础 ======
broker.id=1
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093

# ====== 监听器 ======
# PLAINTEXT://9094 用于 SASL_SSL 客户端通信
# CONTROLLER://9093 用于 KRaft 控制器间通信
listeners=SASL_SSL://:9094,CONTROLLER://:9093
advertised.listeners=SASL_SSL://kafka-1:9094
controller.listener.names=CONTROLLER
inter.broker.listener.name=SASL_SSL

# ====== 监听器安全协议 ======
listener.security.protocol.map=CONTROLLER:SASL_SSL,SASL_SSL:SASL_SSL
control.plane.listener.name=CONTROLLER

# ====== SASL 配置 ======
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.mechanism.controller.protocol=SCRAM-SHA-512
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer

# ====== SSL 配置 ======
ssl.keystore.location=/opt/kafka/ssl/kafka.keystore.jks
ssl.keystore.password=<keystore-password>
ssl.key.password=<key-password>
ssl.truststore.location=/opt/kafka/ssl/kafka.truststore.jks
ssl.truststore.password=<truststore-password>
ssl.client.auth=required
ssl.endpoint.identification.algorithm=HTTPS

# ====== 日志目录(JBOD) ======
log.dirs=/data/kafka/log-0,/data/kafka/log-1,/data/kafka/log-2,/data/kafka/log-3,/data/kafka/log-4,/data/kafka/log-5,/data/kafka/log-6,/data/kafka/log-7,/data/kafka/log-8,/data/kafka/log-9,/data/kafka/log-10,/data/kafka/log-11
metadata.log.dir=/data/kafka/log-0

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

### 3.3 JVM 配置

修改 `/opt/kafka/bin/kafka-server-start.sh` 或通过 systemd `Environment` 传入:

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

### 3.4 systemd 服务

**`/etc/systemd/system/kafka.service`**:

```ini
[Unit]
Description=Apache Kafka (KRaft mode)
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xms32g -Xmx32g -XX:+UseG1GC -XX:MaxGCPauseMillis=20"
Environment="KAFKA_JVM_PERFORMANCE_OPTS=-XX:+AlwaysPreTouch -Djava.awt.headless=true"
Environment="KAFKA_OPTS=-Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf"
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=always
RestartSec=5
LimitNOFILE=100000
LimitNPROC=100000
MemoryMax=48G
TimeoutStopSec=300

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/data/kafka /var/log/kafka /tmp

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
```

### 3.5 KRaft 格式化(首次启动)

```bash
# 1. 生成 Cluster UUID(所有 Broker 共用一个)
CLUSTER_UUID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"
# 将此 UUID 同步到所有 Broker 节点

# 2. 每台 Broker 格式化(在 3 台机器上分别执行)
/opt/kafka/bin/kafka-storage.sh format \
  --config /opt/kafka/config/server.properties \
  --cluster-id $CLUSTER_UUID \
  --add-scram

# 3. 添加 inter-broker SCRAM 用户(在 Broker-1 上执行一次)
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=KafkaInterBroker@2026]' \
  --entity-type users \
  --entity-name interbroker

# 4. 启动所有 Broker
sudo systemctl start kafka

# 5. 验证
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties | head
```

> **注意**:格式化前确保 `/data/kafka/log-*` 为空,否则会报 Cluster ID mismatch。

---

## 4. SASL/SSL 安全配置

### 4.1 生成 SSL 证书

```bash
# 1. 生成 CA
openssl genrsa -out ca-key 4096
openssl req -new -x509 -key ca-key -out ca-cert -days 3650 \
    -subj "/CN=Kafka-CA/O=prom-gw/C=CN"

# 2. 为每个 Broker 生成密钥库(Broker 1 示例)
keytool -keystore kafka-1.keystore.jks -alias kafka-1 \
    -validity 3650 -genkey -keyalg RSA \
    -dname "CN=kafka-1,O=prom-gw,C=CN" \
    -ext SAN=DNS:kafka-1,DNS:kafka-1.sz,IP:10.0.1.21

# 3. 生成 CSR
keytool -keystore kafka-1.keystore.jks -alias kafka-1 \
    -certreq -file kafka-1.csr

# 4. 用 CA 签名
openssl x509 -req -CA ca-cert -CAkey ca-key -in kafka-1.csr \
    -out kafka-1-cert -days 3650 -CAcreateserial \
    -extfile <(echo "subjectAltName=DNS:kafka-1,DNS:kafka-1.sz,IP:10.0.1.21")

# 5. 导入 CA 和签名证书到密钥库
keytool -keystore kafka-1.keystore.jks -alias CARoot \
    -import -file ca-cert -noprompt
keytool -keystore kafka-1.keystore.jks -alias kafka-1 \
    -import -file kafka-1-cert -noprompt

# 6. 生成信任库(所有 Broker 共用)
keytool -keystore kafka.truststore.jks -alias CARoot \
    -import -file ca-cert -noprompt

# 7. 部署到 Broker
sudo mkdir -p /opt/kafka/ssl
sudo cp kafka-1.keystore.jks kafka.truststore.jks /opt/kafka/ssl/
sudo chown -R kafka:kafka /opt/kafka/ssl
sudo chmod 600 /opt/kafka/ssl/*.jks
```

> 对 Broker-2、Broker-3 重复步骤 2-7,替换对应的 CN/DNS/IP。

### 4.2 SCRAM 用户管理

```bash
# 创建 prom-gw 用户
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=PromGw@2026]' \
  --entity-type users \
  --entity-name prom-gw \
  --command-config /opt/kafka/config/admin-client.properties

# 创建 Flink 用户
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=Flink@2026]' \
  --entity-type users \
  --entity-name flink \
  --command-config /opt/kafka/config/admin-client.properties

# 创建 admin 用户
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=Admin@2026]' \
  --entity-type users \
  --entity-name admin \
  --command-config /opt/kafka/config/admin-client.properties

# 查看用户列表
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --describe \
  --entity-type users \
  --command-config /opt/kafka/config/admin-client.properties
```

### 4.3 JAAS 配置文件

**Broker 侧 — `/opt/kafka/config/kafka_server_jaas.conf`**:

```
KafkaServer {
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="interbroker"
    password="KafkaInterBroker@2026";
};
```

**Admin 客户端 — `/opt/kafka/config/admin-client.properties`**:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="admin" \
    password="Admin@2026";
ssl.truststore.location=/opt/kafka/ssl/kafka.truststore.jks
ssl.truststore.password=<truststore-password>
```

### 4.4 prom-gw 客户端配置

prom-gw 通过环境变量配置 Kafka SASL/SSL:

```bash
# prom-gw systemd 或启动脚本
export KAFKA_BROKERS=kafka-1:9094,kafka-2:9094,kafka-3:9094
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export KAFKA_SASL_USER=prom-gw
export KAFKA_SASL_PASSWORD=PromGw@2026
export KAFKA_SSL_ENABLED=true
export KAFKA_SSL_CA=/opt/prom-gw/ssl/ca-cert
```

### 4.5 Flink 客户端配置

Flink 作业通过 `--kafka-brokers` 等参数配置(见 [Flink 生产部署](flink-production-deployment.md)):

```bash
flink run ... \
  --kafka-brokers kafka-1:9094,kafka-2:9094,kafka-3:9094 \
  --env prod \
  ...
```

`JobConfig.applyEnvPreset()` 自动设置 `SASL_SSL` + `SCRAM-SHA-512`。

### 4.6 ACL 权限控制

```bash
# prom-gw 用户:只能写 prom.*.raw.* 和 prom.*.routed.* topic
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --add \
  --allow-principal User:prom-gw \
  --producer \
  --topic "prom." \
  --resource-pattern-type prefixed

# Flink 用户:只能读 prom.*.routed.* topic,只能写 prom.*.dlq.* topic
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --add \
  --allow-principal User:flink \
  --consumer \
  --topic "prom." \
  --resource-pattern-type prefixed \
  --group "flink-agg5m-"

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --add \
  --allow-principal User:flink \
  --producer \
  --topic "prom." \
  --resource-pattern-type prefixed

# 查看 ACL
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --list
```

---

## 5. Topic 管理与最佳实践

### 5.1 创建 Topic

```bash
# 原始数据 topic(每城每个 tenant 一个)
for city in bj sz hf; do
  for tenant in app_business infra; do
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-1:9094 \
      --command-config /opt/kafka/config/admin-client.properties \
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
    /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-1:9094 \
      --command-config /opt/kafka/config/admin-client.properties \
      --create --topic prom.${city}.routed.${biz} \
      --partitions 64 \
      --replication-factor 3 \
      --config retention.ms=259200000
  done
done

# DLQ topic(Flink 创建)
for city in bj sz hf; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9094 \
    --command-config /opt/kafka/config/admin-client.properties \
    --create --topic prom.${city}.dlq.sr.5m \
    --partitions 12 \
    --replication-factor 3 \
    --config retention.ms=604800000  # 7 天
done
```

### 5.2 Topic 分区数选择

| 场景 | partition 数 | 说明 |
|---|---|---|
| 本地开发 | 4 | 单 Broker,低流量 |
| 小型生产(< 100K samples/s) | 12 | 3 Broker × 4 partition/Broker |
| 中型生产(100K-1M samples/s) | 24-32 | 推荐 |
| 大型生产(> 1M samples/s) | 64 | 生产默认值 |

> **注意**:partition 数只能增加不能减少。建议初始值保守,后续按需扩。

### 5.3 Topic 配置最佳实践

| 配置 | 推荐值 | 说明 |
|---|---|---|
| `retention.ms` | 259200000(72h) | 3 天留存,足以让 Flink 消费 + DLQ 重放 |
| `compression.type` | `producer` | 由 producer 决定(prom-gw 用 zstd) |
| `max.message.bytes` | 10485760(10MB) | prom-gw batch 可能较大 |
| `min.insync.replicas` | 2 | 配合 acks=all,2 副本写入成功才算成功 |
| `unclean.leader.election.enable` | false | 禁止未同步副本成为 leader,防数据丢失 |

### 5.4 Topic 运维命令

```bash
# 列出所有 topic
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --list | grep prom

# 查看 topic 详情
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe --topic prom.sz.raw.app_business

# 增加 partition(只能增,不能减)
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --alter --topic prom.sz.raw.app_business \
  --partitions 128

# 修改 topic 配置
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --alter --topic prom.sz.raw.app_business \
  --add-config retention.ms=172800000  # 改为 2 天

# 删除 topic(谨慎!)
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --delete --topic prom.sz.dlq.sr.5m
```

---

## 6. 监控部署

### 6.1 JMX Exporter

部署 `jmx_prometheus_javaagent` 暴露 Kafka JMX 指标到 Prometheus:

```bash
# 下载 JMX exporter
cd /opt/kafka
sudo wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar

# 创建配置文件
sudo cat > /opt/kafka/jmx-exporter.yml << 'EOF'
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

sudo chown kafka:kafka /opt/kafka/jmx-exporter.yml
```

修改 systemd 服务,添加 JMX exporter agent:

```ini
# /etc/systemd/system/kafka.service 的 [Service] 段追加
Environment="KAFKA_OPTS=-javaagent:/opt/kafka/jmx_prometheus_javaagent-0.20.0.jar=9404:/opt/kafka/jmx-exporter.yml -Djava.security.auth.login.config=/opt/kafka/config/kafka_server_jaas.conf"
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart kafka

# 验证
curl -s http://kafka-1:9404/metrics | head -20
```

### 6.2 Prometheus 抓取配置

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

### 6.3 关键监控指标

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

### 6.4 Consumer Lag 监控

```bash
# 安装 kafka-exporter(独立组件,监控 consumer group lag)
docker run -d --name kafka-exporter \
  -p 9308:9308 \
  -e KAFKA_SERVERS=kafka-1:9094 \
  danielqsj/kafka-exporter:latest \
  --kafka.server=kafka-1:9094 \
  --kafka.server=kafka-2:9094 \
  --kafka.server=kafka-3:9094
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

### 6.5 Grafana Dashboard

导入 Kafka Dashboard:

| Dashboard | ID | 说明 |
|---|---|---|
| Kafka Overview | 721 | Broker 指标总览 |
| Kafka Exporter | 7589 | Consumer lag 监控 |
| JVM (Micrometer) | 4701 | JVM/GC 监控 |

---

## 7. 性能调优

### 7.1 Producer 调优(prom-gw 侧)

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

### 7.2 Consumer 调优(Flink 侧)

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

### 7.3 Broker 调优

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

### 7.4 磁盘 IO 调优

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

### 7.5 网络调优

```bash
# 网卡 ring buffer
ethtool -G eth0 rx 4096 tx 4096

# TCP 参数
echo "net.ipv4.tcp_window_scaling=1" >> /etc/sysctl.d/99-kafka.conf
echo "net.core.netdev_max_backlog=5000" >> /etc/sysctl.d/99-kafka.conf
```

---

## 8. 运维操作

### 8.1 集群状态检查

```bash
# 1. Broker 列表
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  | grep -oP 'id=\K[0-9]+'

# 2. Controller 选举状态
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  describe --status

# 3. Topic 列表
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --list

# 4. Consumer Group 列表
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --list

# 5. Consumer Group lag
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe --group flink-agg5m-sz-app-business
```

### 8.2 消费与生产测试

```bash
# 生产测试
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --topic prom.sz.raw.app_business

# 消费测试
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --topic prom.sz.raw.app_business \
  --from-beginning --max-messages 10 --timeout-ms 10000

# 生产性能测试
/opt/kafka/bin/kafka-producer-perf-test.sh \
  --producer-props bootstrap.servers=kafka-1:9094 \
    security.protocol=SASL_SSL \
    sasl.mechanism=SCRAM-SHA-512 \
    'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="Admin@2026";' \
    ssl.truststore.location=/opt/kafka/ssl/kafka.truststore.jks \
    ssl.truststore.password=<truststore-password> \
  --topic prom.sz.raw.app_business \
  --num-records 100000 \
  --record-size 1024 \
  --throughput 50000

# 消费性能测试
/opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server kafka-1:9094 \
  --consumer.config /opt/kafka/config/admin-client.properties \
  --topic prom.sz.raw.app_business \
  --messages 100000 \
  --threads 4
```

### 8.3 优雅停机

```bash
# 1. 检查该 Broker 是否为 Controller
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  describe --status | grep Leader

# 2. 如果是 Controller,先迁移 Controller(可选)
# KRaft 模式下 Controller 会自动重新选举

# 3. 优雅停机(systemd 会发 SIGTERM,Kafka 会完成写入后退出)
sudo systemctl stop kafka

# 4. 验证副本同步
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe | grep -v "UnderReplicated"
```

### 8.4 日志管理

```bash
# Kafka 日志位置
ls /var/log/kafka/
# server.log    → 主日志
# state-change.log → 状态变更日志
# controller.log   → Controller 日志

# 日志轮转配置(/opt/kafka/config/log4j.properties)
log4j.appender.kafkaAppender.maxFileSize=100MB
log4j.appender.kafkaAppender.maxBackupIndex=10

# 查看错误日志
grep -i "error\|warn\|fatal" /var/log/kafka/server.log | tail -50
```

---

## 9. 扩容与缩容

### 9.1 扩容(增加 Broker)

```bash
# 1. 部署新 Broker(按 §2-§4 步骤)
# 假设新增 kafka-4 (10.0.1.24),node.id=4, rack=az-2

# 2. 更新 controller.quorum.voters(所有 Broker)
#   controller.quorum.voters=1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093,4@kafka-4:9093

# 3. 逐台重启 Broker(滚动重启)
for broker in kafka-1 kafka-2 kafka-3 kafka-4; do
    echo "重启 ${broker}..."
    ssh ${broker} "sudo systemctl restart kafka"
    sleep 10
    # 等待 Broker 恢复
    /opt/kafka/bin/kafka-broker-api-versions.sh \
        --bootstrap-server ${broker}:9094 \
        --command-config /opt/kafka/config/admin-client.properties | head -1
done

# 4. 迁移 partition 副本到新 Broker(使用 kafka-reassign-partitions)
# 生成迁移方案
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --topics-to-move-json-file topics-to-move.json \
  --broker-list "1,2,3,4" \
  --generate

# 执行迁移
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --reassignment-json-file reassignment.json \
  --execute

# 验证迁移完成
/opt/kafka/bin/kafka-reassign-partitions.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --reassignment-json-file reassignment.json \
  --verify
```

### 9.2 缩容(移除 Broker)

```bash
# 1. 先将待移除 Broker 上的 partition 迁移到其他 Broker
#   kafka-reassign-partitions --broker-list "1,2,3"(排除待移除的 4)

# 2. 确认待移除 Broker 上无 partition
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe | grep "Broker: 4"
# 期望: 无输出(该 Broker 上无 partition)

# 3. 停机
ssh kafka-4 "sudo systemctl stop kafka"

# 4. 更新 controller.quorum.voters(移除 4@kafka-4:9093)
# 逐台重启剩余 Broker
```

---

## 10. 备份与恢复

### 10.1 数据备份策略

| 方案 | 工具 | 频率 | 适用场景 |
|---|---|---|---|
| Topic 级别复制 | MirrorMaker2 | 实时 | 跨机房灾备 |
| 快照备份 | kafka-export-snapshot | 按需 | 临时备份 |
| 配置备份 | git + 文件同步 | 每次变更 | 配置版本管理 |

### 10.2 配置备份

```bash
# 定期备份 Kafka 配置(建议纳入 Git)
tar -czf kafka-config-backup-$(date +%Y%m%d).tar.gz \
    /opt/kafka/config/server.properties \
    /opt/kafka/config/kafka_server_jaas.conf \
    /opt/kafka/config/admin-client.properties \
    /opt/kafka/jmx-exporter.yml

# 备份 Topic 列表与配置
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe > topic-backup-$(date +%Y%m%d).txt
```

### 10.3 MirrorMaker2 跨机房复制

```bash
# mm2.properties(在灾备机房执行)
clusters = primary, dr
primary.bootstrap.servers = kafka-1.sz:9094,kafka-2.sz:9094,kafka-3.sz:9094
dr.bootstrap.servers = kafka-1.dr:9094,kafka-2.dr:9094,kafka-3.dr:9094

# 复制 prom-gw 相关 topic
primary->dr.enabled = true
primary->dr.topics = prom\..*
primary->dr.groups = prom-gw.*|flink-agg5m.*

# 安全配置
primary.security.protocol = SASL_SSL
primary.sasl.mechanism = SCRAM-SHA-512
primary.sasl.jaas.config = org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="Admin@2026";
primary.ssl.truststore.location = /opt/kafka/ssl/kafka.truststore.jks
primary.ssl.truststore.password = <password>

# 同步设置
sync.topic.configs.enabled = true
sync.topic.acls.enabled = true
replication.factor = 3
checkpoints.topic.replication.factor = 3
heartbeats.topic.replication.factor = 3

# 启动 MirrorMaker2
/opt/kafka/bin/connect-mirror-maker.sh mm2.properties
```

### 10.4 数据恢复

```bash
# 从 MirrorMaker2 备份恢复(灾备机房 → 主机房)
# 1. 切换 prom-gw / Flink 到灾备机房 Kafka
# 2. 或反向复制:灾备 → 主机房

# 从配置备份恢复
tar -xzf kafka-config-backup-20260812.tar.gz -C /
sudo systemctl restart kafka
```

---

## 11. 灾难恢复

### 11.1 单 Broker 故障

```bash
# 1. 确认 Broker 宕机
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  | grep -c "id="   # 期望: 3(如果只有 2,说明有 Broker 宕机)

# 2. 检查 under-replicated partitions
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe | grep "UnderReplicated"

# 3. 自动恢复:Kafka 会自动在其他 Broker 上重建副本
#    等待 ISR 恢复即可,无需人工干预

# 4. 如果 Broker 无法恢复,需要:
#    a. 部署新 Broker(同 node.id)
#    b. 或使用 kafka-reassign-partitions 将副本迁移到其他 Broker
```

### 11.2 全集群故障

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
/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  describe --status

# 4. 验证所有 partition 恢复
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe | grep "UnderReplicated"
# 期望: 无输出

# 5. prom-gw 会自动从 WAL 降级恢复,无需人工干预
```

### 11.3 数据损坏恢复

```bash
# 1. 定位损坏的 log directory
grep -i "corrupt\|error" /var/log/kafka/server.log

# 2. 停止对应 Broker
sudo systemctl stop kafka

# 3. 删除损坏的 log directory(该 Broker 上的数据会从其他副本同步)
sudo rm -rf /data/kafka/log-5/*
# 注意:只删除损坏的目录,不要删除全部

# 4. 重新启动 Broker,Kafka 会自动从其他副本同步数据
sudo systemctl start kafka

# 5. 监控同步进度
watch -n 5 '/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9094 \
  --command-config /opt/kafka/config/admin-client.properties \
  --describe | grep "UnderReplicated"'
```

---

## 12. 附录

### 12.1 配置文件清单

| 文件 | 位置 | 用途 |
|---|---|---|
| `server.properties` | `/opt/kafka/config/server.properties` | Broker 主配置 |
| `kafka_server_jaas.conf` | `/opt/kafka/config/kafka_server_jaas.conf` | Broker SASL 配置 |
| `admin-client.properties` | `/opt/kafka/config/admin-client.properties` | 管理客户端配置 |
| `jmx-exporter.yml` | `/opt/kafka/jmx-exporter.yml` | JMX exporter 配置 |
| `kafka.service` | `/etc/systemd/system/kafka.service` | systemd 服务 |
| `kafka.keystore.jks` | `/opt/kafka/ssl/kafka.keystore.jks` | Broker SSL 密钥库 |
| `kafka.truststore.jks` | `/opt/kafka/ssl/kafka.truststore.jks` | SSL 信任库 |
| `99-kafka.conf` | `/etc/sysctl.d/99-kafka.conf` | 内核参数 |
| `kafka.conf` | `/etc/security/limits.d/kafka.conf` | 文件句柄限制 |

### 12.2 常用命令速查

```bash
# 集群管理
/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties describe --status
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties

# Topic 管理
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --list
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --describe --topic <topic>
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --alter --topic <topic> --partitions 128

# Consumer Group
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --list
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --describe --group <group>

# ACL 管理
/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --list
/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --add --allow-principal User:prom-gw --producer --topic "prom." --resource-pattern-type prefixed

# SCRAM 用户
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --alter --add-config 'SCRAM-SHA-512=[password=xxx]' --entity-type users --entity-name <user>
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --describe --entity-type users

# 性能测试
/opt/kafka/bin/kafka-producer-perf-test.sh --topic <topic> --num-records 100000 --record-size 1024 --throughput 50000 --producer-props bootstrap.servers=kafka-1:9094 ...
/opt/kafka/bin/kafka-consumer-perf-test.sh --bootstrap-server kafka-1:9094 --topic <topic> --messages 100000

# 副本迁移
/opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --topics-to-move-json-file topics.json --broker-list "1,2,3" --generate
/opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --reassignment-json-file plan.json --execute
/opt/kafka/bin/kafka-reassign-partitions.sh --bootstrap-server kafka-1:9094 --command-config /opt/kafka/config/admin-client.properties --reassignment-json-file plan.json --verify
```

### 12.3 故障排查速查

| 现象 | 排查 | 解决 |
|---|---|---|
| Broker 无法启动 | 检查 `/data/kafka/log-0/meta.properties` 的 cluster.id 是否匹配 | 重新格式化或恢复 cluster.id |
| Client 连接超时 | 检查 `advertised.listeners` 是否可达 | 修正 `advertised.listeners` |
| SASL 认证失败 | 检查 JAAS 配置和 SCRAM 用户 | 重新创建用户 |
| SSL 握手失败 | 检查 truststore 和证书有效期 | 更新证书 |
| Under-replicated partitions | 检查 Broker 状态和磁盘 IO | 恢复 Broker 或扩容 |
| Consumer lag 持续增大 | 检查 Flink 消费速率 | 扩 partition / 扩 TM |
| 磁盘满 | 检查 retention 配置 | 调整 retention 或扩容 |
| Controller 选举失败 | 检查 Quorum 是否有 2/3 存活 | 恢复 Broker |

### 12.4 相关文档

- [生产部署指南](production-guide.md) — Kafka 基础安装(§3)、prom-gw 部署(§5)、LVS(§6)
- [Flink 生产部署](flink-production-deployment.md) — Flink 集群部署与作业管理
- [高可用与负载均衡](ha-lb-deployment.md) — Nginx/Keepalived 部署
- [压力测试指南](stress-test-guide.md) — Kafka 端到端压测(§7.1)
- [故障剧本](runbook.md) — Kafka 故障处理
- [本地部署指南](local-dev-guide.md) — 单节点 Kafka 本地部署
- [Flink 消费指南](flink-consumer-guide.md) — Flink 消费 Kafka 开发指南
