# DLQ 兜底重放工具部署与配置详解

> 消费 `prom.<city>.dlq.sr.5m` 死信队列,重新 Stream Load 写入 StarRocks,保证数据不丢。

## 1. 方案概述

### 1.1 背景与定位

Flink 作业在写入 StarRocks 时,若 Stream Load 连续失败 3 次(1s/2s/4s 退避),会将失败的聚合结果写入 DLQ topic `prom.<city>.dlq.sr.5m`。DLQ 重放工具负责:

1. 消费 DLQ topic 中的失败消息
2. 重新调用 StarRocks Stream Load 写入(复用原始 label,保证幂等去重)
3. 成功则提交 offset,失败则按退避策略重试,超过最大重试次数则告警

**与 Flink 作业的关系**:

```
Flink 作业(主流向):
  Kafka(原始) → 5min 聚合 → Stream Load → StarRocks
                                 │
                          失败3次 ↓
                          Kafka DLQ topic
                                 │
DLQ 重放工具(兜底):             ↓
  Kafka DLQ topic → 消费 → Stream Load → StarRocks
                                    │
                             失败max_retry次 → 告警
```

### 1.2 工作机制

| 维度 | 说明 |
|------|------|
| 消费模式 | Kafka 消费者组,手动 offset 提交(at-least-once) |
| 幂等保证 | 复用原始 label,StarRocks 自动按 label 去重 |
| 重试策略 | 指数退避(10s/30s/60s/120s/300s),最大 5 次 |
| 并发度 | 单线程消费(避免 label 竞争),批量攒批写入 |
| 部署形态 | systemd 管理的常驻进程,每城独立部署 |
| 运行依赖 | Kafka 集群可达 + StarRocks FE 可达 |

### 1.3 DLQ 消息格式

DLQ topic 中的消息为 JSON 格式的 `DlqMessage`:

```json
{
  "original": "{\"ts\":\"2026-08-25 10:50:00\",\"metric\":\"app_cpu_usage\",\"business\":\"order-service\",\"ingest_city\":\"bj\",\"source_dc\":\"beijing-dongba\",\"labels_hash\":\"a1b2c3d4\",\"labels\":{\"host\":\"host-1\"},\"sample_count\":300,\"value_sum\":15432.5,\"value_max\":98.7,\"value_min\":12.3,\"value_avg\":51.44,\"ingest_time\":\"2026-08-25 10:55:02\"}",
  "label": "bj_5m_20260825_1050_app-business_0dcdef41",
  "error": "Connection refused: getsockopt",
  "retryCount": 0,
  "timestamp": 1724352000000
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `original` | string | 原始 AggResult 的 JSON 字符串(可直接作为 Stream Load body) |
| `label` | string | 原始 Stream Load label(重放时复用,保证幂等去重) |
| `error` | string | 首次失败原因 |
| `retryCount` | int | 已重试次数(重放工具递增) |
| `timestamp` | long | 写入 DLQ 的时间(Unix millis) |

## 2. 部署准备

### 2.1 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Kylin V10 SP2 |
| JDK | OpenJDK 17(与 Flink 保持一致) |
| 部署用户 | bdops (uid 6000) |
| 程序目录 | `/appdata/dlq-replayer/` |
| 日志目录 | `/applog/dlq-replayer/` |
| 配置文件 | `/appdata/dlq-replayer/conf/` |
| 服务管理 | systemd |

### 2.2 前置条件检查

```bash
# 1. 确认 JDK 17 已安装
java -version  # 期望: openjdk version "17.x.x"

# 2. 确认 Kafka 集群可达
kafka-broker-api-versions.sh --bootstrap-server <city>-kafka-1:9092 | head -5

# 3. 确认 DLQ topic 存在
kafka-topics.sh --bootstrap-server <city>-kafka-1:9092 \
  --describe --topic prom.<city>.dlq.sr.5m
# 期望: Topic: prom.bj.dlq.sr.5m, PartitionCount: 8, ReplicationFactor: 3

# 4. 确认 StarRocks FE 可达
curl -s http://<starrocks-fe-vip>:8030/api/health

# 5. 确认目标表存在
mysql -h <starrocks-fe-vip> -P 9030 -u root -e \
  "SHOW CREATE TABLE prom.metrics_5m"
```

### 2.3 创建目录

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/dlq-replayer/{bin,conf,lib}
sudo mkdir -p /applog/dlq-replayer
sudo chown -R bdops:bdops /appdata/dlq-replayer /applog/dlq-replayer
```

## 3. 编译与部署

### 3.1 编译 JAR

DLQ 重放工具与 Flink 作业共用同一 Maven 工程,复用 `StarRocksStreamLoadClient` 和 `DlqMessage` 类。

```bash
# 在开发机上编译
cd /Users/yangqian/go/src/github.com/lynnyq/prom-gw/examples/flink-agg5m-starrocks
mvn clean package -DskipTests

# 产物
ls -la target/flink-agg5m-starrocks-1.0.0.jar
```

### 3.2 部署 JAR

```bash
# 拷贝到每城部署节点
scp target/flink-agg5m-starrocks-1.0.0.jar \
  bdops@<dlq-replayer-host>:/appdata/dlq-replayer/lib/

# 部署依赖 JAR(若使用独立 main 类,需打 fat jar;此处复用 Flink 工程 fat jar)
```

### 3.3 部署配置文件

**`/appdata/dlq-replayer/conf/dlq-replayer.properties`**:

```properties
# ====== 城市标识 ======
city=bj

# ====== Kafka 消费配置 ======
kafka.bootstrap.servers=bj-kafka-1:9092,bj-kafka-2:9092,bj-kafka-3:9092
kafka.dlq.topic=prom.bj.dlq.sr.5m
kafka.consumer.group.id=dlq-replayer-bj
kafka.auto.offset.reset=earliest
kafka.enable.auto.commit=false
kafka.max.poll.records=500
kafka.session.timeout.ms=30000

# ====== StarRocks 配置 ======
starrocks.fe.host=<beijing-fe-vip>
starrocks.fe.port=8030
starrocks.db=prom
starrocks.table=metrics_5m
starrocks.user=root
starrocks.password=
starrocks.gzip=true

# ====== 重试配置 ======
# 最大重试次数(不含首次)
replay.max.retry=5
# 退避初始间隔(毫秒)
replay.backoff.base.ms=10000
# 退避最大间隔(毫秒)
replay.backoff.max.ms=300000
# 重试间隔退避系数
replay.backoff.multiplier=2.0

# ====== 攒批配置 ======
# 攒批最大条数(达到即触发 Stream Load)
batch.size=100
# 攒批最大等待时间(毫秒,达到即触发 Stream Load)
batch.wait.ms=5000

# ====== HTTP 配置 ======
http.connect.timeout.ms=60000
http.socket.timeout.ms=60000

# ====== 告警配置 ======
# 超过最大重试次数后是否告警
alert.enabled=true
# 告警 Webhook(Prometheus AlertManager 或企业微信/钉钉)
alert.webhook=http://<alertmanager>:9093/api/v2/alerts
# 告警间隔(毫秒,避免刷屏)
alert.interval.ms=300000

# ====== 日志配置 ======
log.level=INFO
log.dir=/applog/dlq-replayer
log.max.size.mb=200
log.max.history=30
```

## 4. systemd 服务配置

> 配置文件源码位置:`deploy/systemd/dlq-replayer@.service`、`deploy/systemd/dlq-replayer.<city>.env`

### 4.1 服务模板文件

**`/etc/systemd/system/dlq-replayer@.service`**:

```ini
[Unit]
Description=prom-gw DLQ Replayer (city=%i)
Documentation=file:///appdata/dlq-replayer/README.md
After=network-online.target kafka.service starrocks-fe.service
Wants=network-online.target

[Service]
Type=simple
User=bdops
Group=bdops

# 城市标识通过 %i 传入(bj/sz/hf)
Environment=DLQ_CITY=%i
EnvironmentFile=-/appdata/dlq-replayer/conf/dlq-replayer.%i.env

WorkingDirectory=/appdata/dlq-replayer
ExecStart=/usr/lib/jvm/java-17-openjdk/bin/java \
    -Xms512m -Xmx2g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=100 \
    -XX:+AlwaysPreTouch \
    -XX:+HeapDumpOnOutOfMemoryError \
    -XX:HeapDumpPath=/applog/dlq-replayer/ \
    -Dlog.level=${LOG_LEVEL} \
    -Dlog.dir=/applog/dlq-replayer \
    -Dcity=%i \
    -jar /appdata/dlq-replayer/lib/dlq-replayer.jar \
    --city %i \
    --config /appdata/dlq-replayer/conf/dlq-replayer.properties

# 优雅停机:SIGTERM 触发消费者优雅关闭(提交已处理 offset,停止 poll)
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=30
FinalKillSignal=SIGKILL

Restart=always
RestartSec=10
StartLimitIntervalSec=60
StartLimitBurst=5

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096
MemoryMax=2G
TasksMax=1024

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/appdata/dlq-replayer /applog/dlq-replayer
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
RestrictRealtime=true
RestrictNamespaces=true

# 日志走 journald
StandardOutput=journal
StandardError=journal
SyslogIdentifier=dlq-replayer

[Install]
WantedBy=multi-user.target
```

**关键配置说明**:

| 配置项 | 值 | 说明 |
|--------|---|------|
| `User/Group` | bdops | 统一部署用户(uid 6000) |
| `%i` | bj/sz/hf | systemd 实例标识,通过 `dlq-replayer@bj.service` 指定城市 |
| `EnvironmentFile` | `-/appdata/dlq-replayer/conf/dlq-replayer.%i.env` | 前缀 `-` 表示文件不存在不报错(开发环境可省略) |
| `KillMode` | mixed | 主进程 SIGTERM 优雅关闭,子进程一起终止 |
| `MemoryMax` | 2G | OOM 时自动重启 |
| `ReadWritePaths` | `/appdata/dlq-replayer /applog/dlq-replayer` | 安全加固,仅允许写这两个目录 |
| `StandardOutput` | journal | 日志走 journald,通过 `journalctl` 查看 |

### 4.2 环境变量文件

每城一份 `.env` 文件,部署到 `/appdata/dlq-replayer/conf/dlq-replayer.<city>.env`,权限 `600`。

#### 4.2.1 北京 (bj)

**`/appdata/dlq-replayer/conf/dlq-replayer.bj.env`**:

```bash
# ============================================================
# DLQ Replayer 环境变量 - 北京 (bj)
# 部署路径: /appdata/dlq-replayer/conf/dlq-replayer.bj.env
# 权限要求: chmod 600, owner bdops:bdops
# ============================================================

# 日志级别 (DEBUG/INFO/WARN/ERROR)
LOG_LEVEL=INFO

# Kafka 消费配置
DLQ_KAFKA_BROKERS=bj-kafka-1:9092,bj-kafka-2:9092,bj-kafka-3:9092
DLQ_KAFKA_TOPIC=prom.bj.dlq.sr.5m
DLQ_KAFKA_GROUP_ID=dlq-replayer-bj

# StarRocks Stream Load 目标
DLQ_SR_HOST=<beijing-fe-vip>
DLQ_SR_PORT=8030
DLQ_SR_DB=prom
DLQ_SR_TABLE=metrics_5m
DLQ_SR_USER=root
DLQ_SR_PASSWORD=

# gzip 压缩(同城可关闭,跨城建议开启)
DLQ_SR_GZIP=false

# 重试配置
DLQ_MAX_RETRY=5
DLQ_BACKOFF_BASE_MS=10000
DLQ_BACKOFF_MAX_MS=300000

# 攒批配置
DLQ_BATCH_SIZE=100
DLQ_BATCH_WAIT_MS=5000

# 告警
DLQ_ALERT_ENABLED=true
DLQ_ALERT_WEBHOOK=http://<alertmanager>:9093/api/v2/alerts
```

#### 4.2.2 深圳 (sz)

**`/appdata/dlq-replayer/conf/dlq-replayer.sz.env`**:

```bash
# ============================================================
# DLQ Replayer 环境变量 - 深圳 (sz)
# 部署路径: /appdata/dlq-replayer/conf/dlq-replayer.sz.env
# 权限要求: chmod 600, owner bdops:bdops
# 注意: StarRocks 在北京,走跨城专线写入
# ============================================================

# 日志级别 (DEBUG/INFO/WARN/ERROR)
LOG_LEVEL=INFO

# Kafka 消费配置(深圳同城 Kafka)
DLQ_KAFKA_BROKERS=sz-kafka-1:9092,sz-kafka-2:9092,sz-kafka-3:9092
DLQ_KAFKA_TOPIC=prom.sz.dlq.sr.5m
DLQ_KAFKA_GROUP_ID=dlq-replayer-sz

# StarRocks Stream Load 目标(北京 FE VIP,走跨城专线)
DLQ_SR_HOST=<beijing-fe-vip>
DLQ_SR_PORT=8030
DLQ_SR_DB=prom
DLQ_SR_TABLE=metrics_5m
DLQ_SR_USER=root
DLQ_SR_PASSWORD=

# gzip 压缩(跨城专线必须开启,减小带宽占用)
DLQ_SR_GZIP=true

# 重试配置(跨城网络抖动概率高,退避时间更长)
DLQ_MAX_RETRY=5
DLQ_BACKOFF_BASE_MS=15000
DLQ_BACKOFF_MAX_MS=300000

# 攒批配置(跨城延迟较高,攒更大批次减少请求次数)
DLQ_BATCH_SIZE=200
DLQ_BATCH_WAIT_MS=10000

# 告警
DLQ_ALERT_ENABLED=true
DLQ_ALERT_WEBHOOK=http://<alertmanager>:9093/api/v2/alerts
```

#### 4.2.3 合肥 (hf)

**`/appdata/dlq-replayer/conf/dlq-replayer.hf.env`**:

```bash
# ============================================================
# DLQ Replayer 环境变量 - 合肥 (hf)
# 部署路径: /appdata/dlq-replayer/conf/dlq-replayer.hf.env
# 权限要求: chmod 600, owner bdops:bdops
# 注意: StarRocks 在北京,走跨城专线写入
# ============================================================

# 日志级别 (DEBUG/INFO/WARN/ERROR)
LOG_LEVEL=INFO

# Kafka 消费配置(合肥同城 Kafka)
DLQ_KAFKA_BROKERS=hf-kafka-1:9092,hf-kafka-2:9092,hf-kafka-3:9092
DLQ_KAFKA_TOPIC=prom.hf.dlq.sr.5m
DLQ_KAFKA_GROUP_ID=dlq-replayer-hf

# StarRocks Stream Load 目标(北京 FE VIP,走跨城专线)
DLQ_SR_HOST=<beijing-fe-vip>
DLQ_SR_PORT=8030
DLQ_SR_DB=prom
DLQ_SR_TABLE=metrics_5m
DLQ_SR_USER=root
DLQ_SR_PASSWORD=

# gzip 压缩(跨城专线必须开启,减小带宽占用)
DLQ_SR_GZIP=true

# 重试配置(跨城网络抖动概率高,退避时间更长)
DLQ_MAX_RETRY=5
DLQ_BACKOFF_BASE_MS=15000
DLQ_BACKOFF_MAX_MS=300000

# 攒批配置(跨城延迟较高,攒更大批次减少请求次数)
DLQ_BATCH_SIZE=200
DLQ_BATCH_WAIT_MS=10000

# 告警
DLQ_ALERT_ENABLED=true
DLQ_ALERT_WEBHOOK=http://<alertmanager>:9093/api/v2/alerts
```

#### 4.2.4 城市间配置差异

| 参数 | 北京(同城) | 深圳/合肥(跨城) | 原因 |
|------|-----------|----------------|------|
| `DLQ_SR_GZIP` | false | true | 跨城带宽珍贵,gzip 减半流量 |
| `DLQ_BATCH_SIZE` | 100 | 200 | 跨城延迟高,攒大批次减少 HTTP 请求 |
| `DLQ_BATCH_WAIT_MS` | 5000 | 10000 | 跨城等待更长,攒更多消息 |
| `DLQ_BACKOFF_BASE_MS` | 10000 | 15000 | 跨城抖动概率高,退避时间更长 |

> **注意**:深圳、合肥的 DLQ 重放工具消费本城 Kafka,但写入北京 StarRocks(跨城专线),`DLQ_SR_HOST` 指向北京 FE VIP。

### 4.3 部署配置文件

```bash
# 源码位置: deploy/systemd/
# deploy/systemd/dlq-replayer@.service
# deploy/systemd/dlq-replayer.bj.env
# deploy/systemd/dlq-replayer.sz.env
# deploy/systemd/dlq-replayer.hf.env

# 1. 部署 systemd 服务模板
sudo cp deploy/systemd/dlq-replayer@.service /etc/systemd/system/

# 2. 部署城市 .env 文件到配置目录
sudo mkdir -p /appdata/dlq-replayer/conf
sudo cp deploy/systemd/dlq-replayer.bj.env /appdata/dlq-replayer/conf/
sudo cp deploy/systemd/dlq-replayer.sz.env /appdata/dlq-replayer/conf/
sudo cp deploy/systemd/dlq-replayer.hf.env /appdata/dlq-replayer/conf/

# 3. 设置权限(.env 含密码等敏感信息,必须 600)
sudo chmod 600 /appdata/dlq-replayer/conf/dlq-replayer.*.env
sudo chown bdops:bdops /appdata/dlq-replayer/conf/dlq-replayer.*.env

# 4. 重载 systemd
sudo systemctl daemon-reload
```

### 4.4 启动服务

```bash
# 启动北京 DLQ 重放工具
sudo systemctl enable dlq-replayer@bj
sudo systemctl start dlq-replayer@bj

# 查看状态
sudo systemctl status dlq-replayer@bj

# 查看日志(journald)
journalctl -u dlq-replayer@bj -f
```

## 5. 核心实现说明

### 5.1 消费与重放流程

```
┌──────────────────────────────────────────────────────────────────┐
│ 主循环 (单线程)                                                  │
│                                                                  │
│  while (running):                                                │
│    1. Consumer.poll(5000)  → 拉取最多 500 条 DLQ 消息            │
│    2. 遍历消息,反序列化为 DlqMessage                             │
│    3. 检查 retryCount,若超过 maxRetry → 发送告警,跳过           │
│    4. 将 original(JSON)加入当前批次 buffer                       │
│    5. 若 buffer 满(100 条)或超时(5s)→ 触发 Stream Load       │
│    6. Stream Load:                                               │
│       - 构建 batch JSON: [original1, original2, ...]            │
│       - HTTP PUT → FE:8030/api/prom/metrics_5m/_stream_load│
│       - Label: dlq_replay_<原label>  (复用原 label 保证幂等)    │
│       - 失败 → 指数退避重试(10s/30s/60s/120s/300s)              │
│    7. 成功 → consumer.commitSync()  (提交 offset)               │
│    8. 失败(超过 maxRetry)→ 记录 error 日志 + 告警,提交 offset  │
│       (避免毒丸消息阻塞整个队列)                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 5.2 幂等去重机制

```java
// 重放时复用原始 label,StarRocks 自动按 label 去重
String replayLabel = msg.getLabel();  // 如 bj_5m_20260825_1050_app-business_0dcdef41

// Stream Load 请求
HttpPut put = new HttpPut(loadUrl);
put.setHeader("Label", replayLabel);  // 关键:复用原 label
put.setHeader("Format", "json");
put.setHeader("strip_outer_array", "true");
put.setEntity(new ByteArrayEntity(batchJsonBytes));
```

- **label 全局唯一**:格式 `<city>_5m_<windowStart>_<business>_<labelsHashShort>`
- **同 label 重试**:StarRocks 识别已存在的 label,直接返回成功(幂等)
- **at-least-once 语义**:消费者 commit 在 Stream Load 成功之后,可能重复但不会丢

### 5.3 毒丸消息处理

超过 `replay.max.retry`(5 次)仍失败的消息,标记为"毒丸":

1. 记录 ERROR 级别日志(含完整 DlqMessage 内容)
2. 发送告警到 AlertManager
3. **提交 offset 跳过该消息**(避免无限阻塞队列)
4. 毒丸消息保留在 DLQ topic 中(7 天留存),可人工介入排查

### 5.4 攒批优化

单条 Stream Load 的 HTTP 开销较大(连接 + 307 重定向 + BE 处理),因此攒批提交:

```
批次触发条件(任一满足即触发):
  1. 消息数 ≥ batch.size (100 条)
  2. 等待时间 ≥ batch.wait.ms (5 秒)

批次构建:
  [
    {original_msg_1 的 AggResult JSON},
    {original_msg_2 的 AggResult JSON},
    ...
  ]
  → strip_outer_array=true,StarRocks 自动拆分为多行
```

- 批量 label:使用第一条消息的 label 作为 batch label
- 若 batch 中部分消息 label 相同,StarRocks 自动去重

## 6. 监控与告警

### 6.1 关键指标

重放工具通过 JMX 或 HTTP `/metrics` 端点暴露 Prometheus 指标:

| 指标 | 类型 | 说明 |
|------|------|------|
| `dlq_replayer_consumed_total` | Counter | 已消费的 DLQ 消息总数 |
| `dlq_replayer_replayed_success_total` | Counter | 重放成功总数 |
| `dlq_replayer_replayed_failed_total` | Counter | 重放失败总数 |
| `dlq_replayer_retry_total` | Counter | 重试次数(含退避重试) |
| `dlq_replayer_poison_message_total` | Counter | 毒丸消息数(超过 maxRetry) |
| `dlq_replayer_lag` | Gauge | DLQ topic 消费延迟(消息数) |
| `dlq_replayer_stream_load_latency_ms` | Histogram | Stream Load 耗时分布 |
| `dlq_replayer_batch_size` | Histogram | 批次大小分布 |

### 6.2 告警规则

**Prometheus AlertManager 规则**:

```yaml
groups:
  - name: dlq-replayer
    rules:
      # P0: 毒丸消息(数据可能丢失,需人工介入)
      - alert: DlqPoisonMessage
        expr: rate(dlq_replayer_poison_message_total[5m]) > 0
        for: 1m
        labels:
          severity: critical
          city: "{{ $labels.city }}"
        annotations:
          summary: "DLQ 毒丸消息出现(city={{ $labels.city }})"
          description: "DLQ 重放工具遇到超过最大重试次数的消息,数据可能丢失,需人工排查"

      # P1: DLQ 积压
      - alert: DlqLagHigh
        expr: dlq_replayer_lag > 1000
        for: 5m
        labels:
          severity: warning
          city: "{{ $labels.city }}"
        annotations:
          summary: "DLQ 积压过多(city={{ $labels.city }})"
          description: "DLQ 消费延迟 {{ $value }} 条,可能 StarRocks 持续不可用"

      # P1: 重放失败率
      - alert: DlqReplayFailRate
        expr: |
          rate(dlq_replayer_replayed_failed_total[5m])
          / rate(dlq_replayer_consumed_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
          city: "{{ $labels.city }}"
        annotations:
          summary: "DLQ 重放失败率高(city={{ $labels.city }})"
          description: "重放失败率 {{ $value | humanizePercentage }},超过 10%"

      # P2: 重放工具离线
      - alert: DlqReplayerDown
        expr: up{job="dlq-replayer"} == 0
        for: 1m
        labels:
          severity: critical
          city: "{{ $labels.city }}"
        annotations:
          summary: "DLQ 重放工具离线(city={{ $labels.city }})"
          description: "DLQ 重放工具进程不可达,DLQ 消息会持续积压"
```

### 6.3 Grafana 看板

建议创建 Grafana Dashboard,关键面板:

1. **消费速率**:consumed/s、replayed_success/s、replayed_failed/s
2. **积压趋势**:DLQ topic lag 时序图
3. **Stream Load 耗时**:P50/P95/P99 延迟
4. **重试分布**:按 retryCount 分桶统计
5. **毒丸消息**:最近 24 小时毒丸消息列表

## 7. 运维操作

### 7.1 日常检查

```bash
# 1. 服务状态
sudo systemctl status dlq-replayer@bj

# 2. 消费延迟
kafka-consumer-groups.sh --bootstrap-server bj-kafka-1:9092 \
  --describe --group dlq-replayer-bj
# 重点关注 LAG 列

# 3. DLQ topic 积压量
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list bj-kafka-1:9092 \
  --topic prom.bj.dlq.sr.5m

# 4. 最近日志
tail -100 /applog/dlq-replayer/dlq-replayer-bj.log | grep -E "ERROR|WARN"

# 5. 毒丸消息检查
grep "poison message" /applog/dlq-replayer/dlq-replayer-bj.log | tail -20
```

### 7.2 手动重放

当 StarRocks 恢复后,可手动触发全量重放:

```bash
# 1. 停止自动重放工具(避免 offset 冲突)
sudo systemctl stop dlq-replayer@bj

# 2. 重置消费位置到最早(从头消费)
kafka-consumer-groups.sh --bootstrap-server bj-kafka-1:9092 \
  --group dlq-replayer-bj \
  --topic prom.bj.dlq.sr.5m \
  --reset-offsets --to-earliest --execute

# 3. 启动重放工具(从头开始消费)
sudo systemctl start dlq-replayer@bj

# 4. 观察进度
watch -n 5 'kafka-consumer-groups.sh --bootstrap-server bj-kafka-1:9092 \
  --describe --group dlq-replayer-bj'
```

### 7.3 跳过毒丸消息

当确认某条消息无法恢复(如数据格式错误),需要跳过:

```bash
# 1. 查看毒丸消息的 offset
grep "poison message" /applog/dlq-replayer/dlq-replayer-bj.log | \
  grep -oP 'offset=\K[0-9]+'

# 2. 停止重放工具
sudo systemctl stop dlq-replayer@bj

# 3. 跳过指定 offset(将消费位置设置到毒丸消息之后)
kafka-consumer-groups.sh --bootstrap-server bj-kafka-1:9092 \
  --group dlq-replayer-bj \
  --topic prom.bj.dlq.sr.5m:0:<offset+1> \
  --reset-offsets --to-current --execute

# 4. 重启重放工具
sudo systemctl start dlq-replayer@bj
```

### 7.4 暂停与恢复

```bash
# 暂停(不消费,但保留 offset)
sudo systemctl stop dlq-replayer@bj

# 恢复
sudo systemctl start dlq-replayer@bj
```

## 8. 多城部署

### 8.1 部署拓扑

| 城市 | 部署节点 | Kafka 源 | StarRocks 目标 | 备注 |
|------|---------|---------|---------------|------|
| 北京 | 北京 Flink TM 节点之一 | `prom.bj.dlq.sr.5m` | 北京 FE VIP | 同城写入 |
| 深圳 | 深圳 Flink TM 节点之一 | `prom.sz.dlq.sr.5m` | 北京 FE VIP | 跨城专线写入 |
| 合肥 | 合肥 Flink TM 节点之一 | `prom.hf.dlq.sr.5m` | 北京 FE VIP | 跨城专线写入 |

### 8.2 批量部署

```bash
# 在每城部署节点上执行
CITY=bj  # 或 sz/hf
KAFKA_BROKERS=<city>-kafka-1:9092,<city>-kafka-2:9092,<city>-kafka-3:9092
SR_VIP=<beijing-fe-vip>

# 创建目录
sudo mkdir -p /appdata/dlq-replayer/{bin,conf,lib}
sudo mkdir -p /applog/dlq-replayer
sudo chown -R bdops:bdops /appdata/dlq-replayer /applog/dlq-replayer

# 部署 JAR
scp flink-agg5m-starrocks-1.0.0.jar \
  bdops@<host>:/appdata/dlq-replayer/lib/

# 部署配置
cat > /tmp/dlq-replayer.${CITY}.env << EOF
LOG_LEVEL=INFO
DLQ_KAFKA_BROKERS=${KAFKA_BROKERS}
DLQ_KAFKA_TOPIC=prom.${CITY}.dlq.sr.5m
DLQ_SR_HOST=${SR_VIP}
DLQ_SR_PORT=8030
EOF
sudo mv /tmp/dlq-replayer.${CITY}.env /appdata/dlq-replayer/conf/
sudo chown bdops:bdops /appdata/dlq-replayer/conf/dlq-replayer.${CITY}.env

# 部署 systemd 服务
sudo cp dlq-replayer@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable dlq-replayer@${CITY}
sudo systemctl start dlq-replayer@${CITY}
```

## 9. 故障排查

### 9.1 常见问题

| 现象 | 可能原因 | 排查方法 |
|------|---------|---------|
| 服务启动后立即退出 | JDK 版本不对 / 配置文件路径错误 | `journalctl -u dlq-replayer@bj -n 50` |
| 消费但不写入 StarRocks | StarRocks FE 不可达 / 认证失败 | `curl -v http://<fe-vip>:8030/api/health` |
| 消费延迟持续增长 | StarRocks 响应慢 / 重试频繁 | 检查 StarRocks BE 负载,查看 `stream_load_latency_ms` |
| 毒丸消息持续出现 | 数据格式错误 / StarRocks 表结构变更 | 查看日志中的 `original` 字段,验证与 DDL 是否匹配 |
| OOM | 批次过大 / 消息堆积 | 调小 `batch.size`,增大 `-Xmx` |
| offset 提交失败 | 消费者组被其他实例占用 | 确认无重复实例:`kafka-consumer-groups.sh --describe --group dlq-replayer-bj` |

### 9.2 日志关键字

```bash
# 正常运行
grep "replayed success" /applog/dlq-replayer/dlq-replayer-bj.log | tail -10

# 重试
grep "retry" /applog/dlq-replayer/dlq-replayer-bj.log | tail -20

# 毒丸消息(需人工介入)
grep "poison message" /applog/dlq-replayer/dlq-replayer-bj.log

# StarRocks 连接异常
grep "Connection refused\|Stream Load failed" /applog/dlq-replayer/dlq-replayer-bj.log

# 消费者组 rebalance
grep "rebalance" /applog/dlq-replayer/dlq-replayer-bj.log
```

### 9.3 性能调优

| 参数 | 默认值 | 调优建议 |
|------|--------|---------|
| `batch.size` | 100 | StarRocks 负载低时可调到 200~500 |
| `batch.wait.ms` | 5000 | 实时性要求高可调到 1000 |
| `kafka.max.poll.records` | 500 | 消息少时可调小,避免单次拉取过多 |
| `-Xmx` | 2g | OOM 时调到 4g |
| `replay.max.retry` | 5 | StarRocks 频繁抖动时可调到 10 |

## 10. 安全注意事项

1. **Kafka 认证**:若 Kafka 启用 SASL,在 `dlq-replayer.properties` 中补充 `sasl.mechanism` 和 `jaas.config`
2. **StarRocks 认证**:生产环境建议创建专用用户(非 root),仅授予 `prom` 库的 INSERT 权限
3. **配置文件权限**:配置文件含密码等敏感信息,权限设为 `600`:
   ```bash
   sudo chmod 600 /appdata/dlq-replayer/conf/dlq-replayer*.properties
   sudo chmod 600 /appdata/dlq-replayer/conf/dlq-replayer*.env
   sudo chown bdops:bdops /appdata/dlq-replayer/conf/*
   ```
4. **日志脱敏**:日志中不打印 StarRocks 密码、Kafka SASL 凭据
