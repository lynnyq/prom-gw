# 本地开发与测试指南
> 本文档面向开发者本机调试,采用 **单节点 Prometheus + 单节点 Kafka + 单实例 prom-gw** 全本地原生部署,**不依赖 Docker**。
> 生产部署请参考 **production-guide.md**(见 §1)。


---

## 1. 适用场景与拓扑

### 1.1 适用场景

- 本地开发调试 prom-gw 代码
- 验证 ruleset 规则逻辑(relabel/route/sample/downsample)
- 复现 WAL 故障切换与 drain 行为
- 端到端联调(Prometheus → prom-gw → Kafka)

### 1.2 最小化拓扑

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

### 1.3 资源需求

| 资源 | 最低 | 建议 |
|---|---|---|
| CPU | 2 核 | 4 核 |
| 内存 | 4 GB | 8 GB |
| 磁盘 | 10 GB | 20 GB |

---

## 2. 环境准备

### 2.1 操作系统

- macOS(Intel / Apple Silicon)或 Linux
- Windows 建议使用 WSL2

### 2.2 安装 Go 1.22+

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

### 2.3 安装 JDK 17(Kafka 依赖)

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

### 2.4 安装辅助工具

```bash
# macOS
brew install jq curl wget

# Linux
sudo apt install -y jq curl wget   # Debian/Ubuntu
sudo yum install -y jq curl wget   # CentOS/RHEL
```

### 2.5 克隆代码

```bash
git clone https://github.com/lynnyq/bigdata.git
cd bigdata
```

---

## 3. Kafka 单节点本地部署

> 本地开发使用 KRaft 模式单节点,无需 ZooKeeper。

### 3.1 下载解压

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
ln -s kafka_2.13-3.4.0 kafka
cd kafka
```

### 3.2 配置文件

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

### 3.3 JVM 配置

创建 `~/kafka/bin/set-local-opts.sh`:

```bash
#!/bin/bash
export KAFKA_HEAP_OPTS="-Xmx1g -Xms1g -XX:+UseG1GC"
export KAFKA_JVM_PERFORMANCE_OPTS="-XX:+AlwaysPreTouch -Djava.awt.headless=true"
```

```bash
chmod +x ~/kafka/bin/set-local-opts.sh
```

### 3.4 格式化存储(仅首次)

```bash
cd ~/kafka
CLUSTER_UUID=$(bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_UUID"

bin/kafka-storage.sh format \
  --config config/local.properties \
  --cluster-id $CLUSTER_UUID
```

### 3.5 启动 Kafka

```bash
cd ~/kafka
source bin/set-local-opts.sh
bin/kafka-server-start.sh config/local.properties
```

启动后保持终端运行,或加 `-daemon` 后台运行:

```bash
bin/kafka-server-start.sh -daemon config/local.properties
```

### 3.6 验证 Kafka

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

### 3.7 创建 prom-gw 所需 Topic

```bash
cd ~/kafka

# 数据 topic(按 team 分桶)
for biz in core infra data app_business; do
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.local.routed.${biz} \
    --partitions 4 --replication-factor 1 \
    --config retention.ms=86400000
done

# 验证
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep prom
```

### 3.8 停止 Kafka

```bash
cd ~/kafka
bin/kafka-server-stop.sh
```

---

## 4. Prometheus 单节点本地部署

### 4.1 下载解压

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

### 4.2 配置文件

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

### 4.3 启动 Prometheus

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

### 4.4 验证 Prometheus

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

### 4.5 热重载配置

修改 `prometheus-local.yml` 后,无需重启:

```bash
curl -X POST http://localhost:9090/-/reload
```

### 4.6 停止 Prometheus

```bash
# 前台运行:Ctrl+C
# 后台运行:
pkill -f "prometheus --config.file=prometheus-local.yml"
```

---

## 5. prom-gw 本地编译与配置

### 5.1 编译

```bash
cd ~/bigdata   # 或实际代码目录

# 依赖
make build     # 产物:bin/prom-gw

# 验证
./bin/prom-gw --version  # prom-gw <version>
```

### 5.2 本地 Token 配置

仓库自带开发用 token,直接使用 `configs/tokens/local.yaml`:

```yaml
tokens:
  "tk_app_business_dev":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.local.routed.app_business
    rate_limit: 80000

  "tk_infra_dev":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.local.routed.infra
    rate_limit: 50000
```

> **注意**:本地开发的 `default_topic` 改为 `prom.local.routed.*`(与 §3.7 创建的 topic 匹配)。

### 5.3 本地 Ruleset 配置

创建 `configs/rules/local-dev.yaml`:

```yaml
rulesets:
  - name: app-business
    tenant: app-business
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

### 5.4 启动参数速查

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

## 6. 全链路启动与验证

### 6.1 启动顺序

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

### 6.2 验证清单

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
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 5 --timeout-ms 10000 | xxd | head -20

# 9. Admin API
curl -s http://127.0.0.1:8082/v1/rulesets | jq
curl -s http://127.0.0.1:8082/v1/stats | jq
curl -s http://127.0.0.1:8082/v1/tenants | jq
```

### 6.3 手动写入单条 sample

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
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head
```

---

## 7. 调试技巧

### 7.1 日志

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

### 7.2 pprof 性能分析

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

### 7.3 Admin API 调试

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

### 7.4 关键指标速查

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

### 7.5 Kafka 消费调试

```bash
K=~/kafka/bin

# 实时消费(持续)
$K/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic prom.local.routed.app_business \
  --from-beginning

# 查看消费组
$K/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 查看 Topic 分区详情
$K/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic prom.local.routed.app_business

# 查看 Topic 最新 offset
$K/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic prom.local.routed.app_business
```

### 7.6 热重载 Token

```bash
# 修改 token 文件
vim configs/tokens/local.yaml

# 发送 SIGHUP
kill -HUP $(pgrep -f "prom-gw")

# 验证日志
grep "tokens reloaded" /tmp/prom-gw.log
```

### 7.7 热更新 Ruleset

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

## 8. 常用测试场景

### 8.1 场景 1:WAL-only 模式(无 Kafka)

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

### 8.2 场景 2:WAL 故障切换

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
  --topic prom.local.routed.app_business \
  --from-beginning --max-messages 10 --timeout-ms 10000 | xxd | head

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-failover-wal /tmp/failover-*.bin /tmp/prom-gw-failover.log /tmp/prom-gw-drain.log
```

### 8.3 场景 3:规则引擎验证

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

### 8.4 场景 4:单元测试 + 集成测试

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

### 8.5 场景 5:批量压测

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
  --broker-list localhost:9092 --topic prom.local.routed.app_business

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-perf-wal
```

---

## 9. 清理与重置

### 9.1 停止所有服务

```bash
# 停止 prom-gw
pkill -f "prom-gw" 2>/dev/null

# 停止 Prometheus
pkill -f "prometheus --config.file=prometheus-local.yml" 2>/dev/null

# 停止 Kafka
~/kafka/bin/kafka-server-stop.sh 2>/dev/null
```

### 9.2 清理数据

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

### 9.3 完全重置(回到初始状态)

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

## 附录

### A. 端口速查

| 端口 | 服务 | 用途 |
|---|---|---|
| `9090` | Prometheus | Web UI / API |
| `9092` | Kafka | 客户端访问 |
| `9093` | Kafka | Controller(KRaft) |
| `19201` | prom-gw | RemoteWrite 接入 |
| `8080` | prom-gw | `/metrics` + pprof |
| `8081` | prom-gw | healthz / readyz |
| `8082` | prom-gw | Admin API |

### B. 目录速查

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

### C. 常见问题

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

