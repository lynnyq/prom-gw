# prom-gw 生产技术方案说明

> 本文档描述 prom-gw 生产环境的整体技术方案、数据流向与核心实现细节,作为部署文档的技术背景参考。

## 1. 方案概述

### 1.1 设计目标

构建一个多机房 Prometheus RemoteWrite 协议网关,实现:

1. **多机房采集汇聚**:三地(北京、深圳、合肥)Prometheus 实例通过 `remote_write` 协议上报指标数据
2. **同城清洗路由**:每城独立部署 prom-gw,按规则将指标路由到不同 Kafka topic
3. **跨城聚合存储**:三城 Flink 作业做 5min 局部聚合,通过 Stream Load 写入北京 StarRocks
4. **高可用不丢数据**:Kafka 3 副本 + WAL 降级 + LVS 负载均衡,保证端到端零丢失

### 1.2 技术栈选型

| 层次 | 组件 | 选型理由 |
|------|------|---------|
| 采集层 | Prometheus 2.51+ | 已有 5 套生产实例,`remote_write` 原生协议 |
| 接入层 | prom-gw (Go) | 高并发 HTTP 接入 + snappy/protobuf 零拷贝解码 |
| 消息层 | Kafka 3.9 KRaft | 无 ZooKeeper 依赖,JBOD 多盘高吞吐 |
| 计算层 | Flink 1.19 Standalone HA | 窗口聚合 + Checkpoint 精确一次语义 |
| 存储层 | StarRocks 3.3 | 存算一体,Stream Load 高速写入,物化视图级联聚合 |
| 负载均衡 | LVS + Keepalived | 4 层 DR 模式,低延迟高吞吐 |

### 1.3 三级分层架构

```
L1 采集层:  Prometheus (每城 1~2 套) → remote_write (snappy+protobuf)
              │
L2 同城层:  LVS VIP → prom-gw (每城 2~4 台) → Kafka (每城 3 Broker)
                                              │
                                    Flink (每城 Standalone HA)
                                              │
L3 汇聚层:  ──── Stream Load (跨城专线) ────→ StarRocks (北京 3 FE + 3 BE)
```

- **L1 同城采集**:Prometheus 原生 `remote_write`,snappy 压缩 + protobuf 序列化
- **L2 同城清洗**:prom-gw 接收 → 规则路由 → Kafka 暂存 → Flink 5min 聚合
- **L3 跨城汇聚**:仅 5min 聚合数据跨城写入北京 StarRocks,原始 15s 明细严禁跨城

### 1.4 跨城带宽设计

| 数据类型 | 是否跨城 | 单城日量 | 三城合计 | 1G 专线占用 |
|---------|---------|---------|---------|------------|
| 15s 原始 sample | 否(同城 Kafka) | ~5 TB | — | 0% |
| 5min 聚合(gzip) | 是 | 345 GB | 1.0 TB | 9.3% |
| 1h / 1d 聚合 | 否(StarRocks 级联) | — | — | 0% |

## 2. 数据流向

### 2.1 端到端数据流

```
┌─────────────┐     snappy+protobuf      ┌──────────┐     原始 body      ┌─────────┐
│ Prometheus  │ ──── remote_write ──────→ │ prom-gw  │ ──── Produce ────→ │ Kafka   │
│ (每城 1~2套) │     POST /api/v1/write   │ (LVS VIP) │   (zstd 批压缩)   │ (3 Broker)│
└─────────────┘                          └──────────┘                    └────┬────┘
                                               ↑                              │
                                          Bearer Token                    消费 │
                                          X-Source-DC                         ↓
                                         ┌──────────┐  JSON(聚合后)  ┌──────────────┐
                                         │  Flink   │ ── Stream ──→ │  StarRocks   │
                                         │  5min 窗口 │   Load        │  3 FE + 3 BE │
                                         └──────────┘               └──────────────┘
```

### 2.2 prom-gw 内部数据流

```
HTTP Body (snappy+protobuf)
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│ receiver.handleWrite                                        │
│   1. 校验 Content-Type=application/x-protobuf               │
│   2. 校验 Content-Encoding=snappy                           │
│   3. 读取 body (限长 MaxBodyBytes)                           │
│   4. decoder.Decode(body): snappy解压 → protobuf反序列化      │
│      → prompb.WriteRequest{ Timeseries[] }                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ router.Process                                              │
│   1. 加载路由表 (entries,从配置热加载)                        │
│   2. 按 Match 规则分桶 (每条 sample 命中第一个匹配的 ruleset) │
│   3. 逐桶调用 ruleset.Process:                              │
│      - 提取 __name__ 做 prefix/regex 匹配                    │
│      - 命中 → 附加 topic + headers,投递到 sink               │
│      - 全部未命中 → default 兜底                             │
│   4. 原始 body 透传(不重新序列化,字节级相等)                   │
└─────────────────────┬───────────────────────────────────────┘
                      │ sink.Message{Topic, Key, Payload, Headers}
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ pipeline.Submit → channel (buffer=65535)                    │
│   channel 满 → 返回 ErrBackpressure → receiver 返回 503     │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ sink.AdapterSink.Send                                       │
│   正常模式:                                                 │
│     sendToKafka → 成功 → return nil                          │
│     sendToKafka → 失败 → recordFailure → 降级到 WAL          │
│   降级模式(Kafka 连续失败超阈值):                          │
│     sendToWAL → 成功 → return nil                            │
│     sendToWAL → WAL 满 → return ErrBackpressure → 503        │
└─────────────────────┬───────────────────────────────────────┘
                      │
            ┌─────────┴─────────┐
            ▼                   ▼
     ┌──────────────┐   ┌──────────────┐
     │ KafkaSink    │   │ WALSink      │
     │ (franz-go    │   │ (磁盘段文件)  │
     │  异步批量)    │   │              │
     └──────────────┘   └──────┬───────┘
                               │ Kafka 恢复后
                               ▼
                        ┌──────────────┐
                        │ monitor      │
                        │ goroutine    │
                        │ drainWAL()   │
                        └──────────────┘
```

### 2.3 Kafka Topic 规划

#### 2.3.1 命名规则

每城独立 Kafka 集群,topic 命名统一规则:

```
prom.<city>.<category>
       │       │
       │       └── 业务分类:routed.core / routed.infra / routed.data / routed.app_business / dlq.sr.5m
       └────────── 城市:bj(北京)/ sz(深圳)/ hf(合肥)/ local(开发环境)
```

**设计原则**:

1. **城市维度隔离**:每城独立 Kafka 集群,topic 名带城市前缀,避免跨城误消费
2. **按团队分桶**:按 `team` 标签将数据路由到 `core`/`infra`/`data`/`app_business` 等独立 topic,便于按业务域并行消费
3. **DLQ 独立**:死信队列单独 topic,分区数少(8),留存时间长(7 天),便于运维重放

> **架构说明**:prom-gw 采用**单进程同步处理**,收到 Prometheus remote_write 后在进程内完成 relabel + route,直接写入目标 topic。**不存在 raw → routed 两阶段异步消费**,无需 raw topic 中转。

#### 2.3.2 Topic 分类详解

**数据 topic(prom-gw 同步写入)**

prom-gw 收到 Prometheus remote_write 后,在进程内执行 relabel(标签清洗)+ route(按 team 分桶),直接将原始 snappy+protobuf body 写入对应的 topic。payload 未经修改,附加了路由 headers(`business`、`source_dc`、`ingest_city` 等)。

| Topic | 写入方 | 路由规则 | 消费方 | 用途 |
|-------|--------|---------|--------|------|
| `prom.bj.routed.core` | prom-gw | `team=core` | Flink (core 聚合作业) | 核心业务指标(订单/支付/账户) |
| `prom.bj.routed.infra` | prom-gw | `team=infra` | Flink (infra 聚合作业) | 基础设施指标(主机/网络) |
| `prom.bj.routed.data` | prom-gw | `team=data` | Flink (data 聚合作业) | 数据平台指标(离线/实时计算) |
| `prom.bj.routed.app_business` | prom-gw | `team` 未命中规则兜底 | Flink (app_business 聚合作业) | 通用业务指标兜底桶 |

> **配置位置**:[configs/rules/app-business.yaml](file:///Users/yangqian/go/src/github.com/lynnyq/prom-gw/configs/rules/app-business.yaml) 的 `route.rules` 和 `default_topic` 字段。

**路由规则示例**(来自 [app-business.yaml:29-36](file:///Users/yangqian/go/src/github.com/lynnyq/prom-gw/configs/rules/app-business.yaml#L29-L36)):

```yaml
- type: route
  rules:
    - match: { team: "core" }   # 核心业务团队 → prom.bj.routed.core
      topic: prom.bj.routed.core
    - match: { team: "infra" }  # 基础设施团队 → prom.bj.routed.infra
      topic: prom.bj.routed.infra
    - match: { team: "data" }   # 数据团队 → prom.bj.routed.data
      topic: prom.bj.routed.data
  # default_topic: prom.bj.routed.app_business  (兜底桶)
```

**死信队列 topic(Flink 写入)**

| Topic | 写入方 | 消费方 | 触发条件 | 留存 |
|-------|--------|--------|---------|------|
| `prom.bj.dlq.sr.5m` | Flink StarRocksSink | DLQ 重放工具 | Stream Load 3 次重试失败 | 7 天 |

DLQ topic 中消息格式为 JSON,包含原始 AggResult、label(用于幂等去重)、失败原因、重试次数。详见 [14-dlq-replayer-deployment.md](14-dlq-replayer-deployment.md) 第 1.3 节。

#### 2.3.3 Topic 总览表

| Topic | 分区 | 副本 | 留存 | 写入方 | 消费方 | 说明 |
|-------|------|------|------|--------|--------|------|
| `prom.bj.routed.core` | 64 | 3 | 3 天 | prom-gw | Flink (core 作业) | 核心业务数据(team=core) |
| `prom.bj.routed.infra` | 64 | 3 | 3 天 | prom-gw | Flink (infra 作业) | 基础设施数据(team=infra) |
| `prom.bj.routed.data` | 64 | 3 | 3 天 | prom-gw | Flink (data 作业) | 数据平台数据(team=data) |
| `prom.bj.routed.app_business` | 64 | 3 | 3 天 | prom-gw | Flink (app_business 作业) | 通用业务兜底桶 |
| `prom.bj.dlq.sr.5m` | 8 | 3 | 7 天 | Flink StarRocksSink | DLQ 重放工具 | Stream Load 失败死信队列 |

深圳(`sz`)、合肥(`hf`)同理,分别使用 `prom.sz.*` 和 `prom.hf.*` 前缀。

**三城 topic 总数**:每城 5 个 × 3 城 = **15 个 topic**。

#### 2.3.4 数据流向

```
Prometheus
  │ remote_write (snappy+protobuf)
  ▼
┌─────────────────────────────────────────────────┐
│ prom-gw (单进程同步处理)                        │
│   1. receiver: 验证 token                      │
│   2. rule engine: relabel + route + sample     │
│   3. sink: 直接写入 Kafka (routed topic)        │
│   失败时:写入 WAL 降级,后续自动回灌            │
└─────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────┐
│ prom.bj.routed.core         │  ← prom-gw 直接写入
│ prom.bj.routed.infra        │     payload = 原始 snappy+protobuf body
│ prom.bj.routed.data         │     headers = {business, source_dc, ingest_city}
│ prom.bj.routed.app_business │     (兜底桶)
└─────────────────────────────┘
  │ Flink KafkaSource 消费
  │   1. 解压 snappy
  │   2. 解析 protobuf
  │   3. 5min 窗口聚合
  │   4. Stream Load → StarRocks
  ▼
StarRocks (metrics_5m 表)
  │
  │ 若 Stream Load 失败 3 次
  ▼
┌─────────────────────────────┐
│ prom.bj.dlq.sr.5m           │  ← dlq(死信队列)
└─────────────────────────────┘
  │ DLQ 重放工具消费
  │   1. 攒批(100 条或 5s)
  │   2. 重新 Stream Load(复用原 label 幂等去重)
  ▼
StarRocks (重放成功)
```

#### 2.3.5 topic 创建命令

```bash
# 数据 topics(按 team 分桶)
for cat in core infra data app_business; do
  kafka-topics.sh --bootstrap-server bj-kafka-1:9092 --create \
    --topic prom.bj.routed.$cat --partitions 64 --replication-factor 3 \
    --config retention.ms=259200000  # 3 天
done

# dlq topic(分区少、留存长)
kafka-topics.sh --bootstrap-server bj-kafka-1:9092 --create \
  --topic prom.bj.dlq.sr.5m --partitions 8 --replication-factor 3 \
  --config retention.ms=604800000  # 7 天
```

#### 2.3.6 为什么规划多个 Topic(目的与用途)

本方案在三城共规划 15 个 topic(每城 5 个 × 3 城),基于以下三个维度的权衡:

**1. 按 team 分桶:业务域隔离,独立扩容**

拆出 `core`/`infra`/`data`/`app_business` 四个桶,而非全部写入一个 topic:

| 维度 | 单 topic 方案 | 多 topic 分桶方案 |
|------|-------------|------------------|
| **消费隔离** | 所有 Flink 作业共用一个 consumer group,一个作业 OOM 影响其他 | 各团队 Flink 作业消费独立 topic,互不干扰 |
| **背压隔离** | 一个团队流量激增(如大促)→ 整个 topic 积压 → 所有团队延迟 | 仅 `routed.core` 积压,`infra`/`data` 不受影响 |
| **独立扩容** | 只能整体加分区,浪费资源 | 按 topic 实际流量独立调整分区数(核心业务 64,小业务可 16) |
| **差异化采样** | 全局统一采样率 | core 100% 保留,infra 50% 采样,data 30% 采样(在 ruleset 中按桶配置) |
| **独立监控** | 无法区分哪个团队的数据异常 | 每个 topic 独立 lag/吞吐监控,故障定位到团队 |

**2. DLQ 独立:异常数据不污染主流**

| 维度 | 若 DLQ 混入数据 topic | 独立 DLQ topic |
|------|------------------|---------------|
| 消费逻辑 | Flink 主作业需要同时处理正常+异常数据,逻辑复杂 | 主作业只管正常数据,DLQ 由独立工具消费 |
| 留存时间 | 正常数据 3 天清理,异常数据可能还没来得及重放就被清理 | DLQ 独立留存 7 天,给运维足够时间处理 |
| 分区数 | 正常数据 64 分区,DLQ 不需要这么多 | DLQ 独立设 8 分区,节省资源 |
| 消费组 | DLQ 重放工具与 Flink 主作业共用 consumer group,offset 互相干扰 | 独立 consumer group,互不影响 |

**3. 城市前缀:就近写入 + 故障隔离**

| 维度 | 全国单集群 | 每城独立集群(当前方案) |
|------|-----------|----------------------|
| 写入延迟 | 跨城 RTT 20~30ms | 同城 <1ms |
| 带宽成本 | 所有数据跨城传输,专线带宽压力大 | 数据同城消化,仅聚合结果跨城写 StarRocks |
| 故障爆炸半径 | Kafka 故障影响全国所有数据 | 单城 Kafka 故障仅影响该城,其他城正常 |
| 运维独立 | 无法按城市独立升级/重启 | 各城可独立滚动升级 |
| 合规要求 | 数据跨城流转可能不符合数据驻留要求 | 数据同城闭环 |

topic 名带 `bj`/`sz`/`hf` 前缀,从根本上避免跨城误消费(消费 `prom.sz.*` 的作业绝不会拉到 `prom.bj.*` 的数据)。

**总结:多 topic 规划的核心价值**

```
数据层 (routed)   → 按业务域分桶,隔离背压,独立扩容
   ↓
兜底层 (dlq)      → 异常数据独立留存,不污染主流,可重放
   × 3 城         → 就近写入,故障隔离,合规驻留
```

| 设计目标 | 对应 topic 规划 |
|---------|---------------|
| 数据不丢 | prom-gw WAL 降级 + DLQ 兜底双重保障 |
| 故障隔离 | 按城市分集群,按 team 分桶 |
| 独立扩容 | 每个 routed topic 独立分区数,可按需调整 |
| 差异化策略 | 各桶可独立配置采样率、留存时间、消费组 |
| 可运维性 | DLQ 独立 topic + 独立重放工具,不影响主链路 |

### 2.4 Flink 消费与聚合流

> **注意**:Flink 聚合后**直接** Stream Load 写入 StarRocks,不经过 Kafka 中转。
> `prom.<city>.dlq.sr.5m` topic 仅在 Stream Load 失败时作为死信队列使用,不是主流向。

```
Kafka topic (prom.<city>.routed.app_business)   ← prom-gw 写入的原始数据
    │
    ▼ KafkaSource (Exactly-Once, checkpoint)
    │
    ▼ PromWriteRequestDecoder
    │   1. Kafka client 自动解 zstd (batch 级)
    │   2. Snappy.uncompress(value) → 解 snappy
    │   3. protobuf parse → PromProtos.WriteRequest
    │   4. 遍历 Timeseries → 提取 (labels, value, timestamp)
    │
    ▼ keyBy(labels_hash)  →  保证同 series 到同一 subtask
    │
    ▼ TumblingEventTimeWindow(5min)
    │   - trigger: EventTimeTrigger (watermark 推进触发)
    │   - allowedLateness: 1min
    │
    ▼ AggWindowFunction.process()
    │   对每个 key(指标 series):
    │     - count, sum, max, min, avg
    │     - histogram buckets (if type=histogram)
    │     - 生成 5min 聚合行
    │
    ▼ StarRocksSink.invoke()   ← 直连 StarRocks,不写 Kafka
    │   1. JSON 序列化 (strip_outer_array=true)
    │   2. gzip 压缩 (跨城场景)
    │   3. HTTP PUT → FE:8030/api/<db>/<table>/_stream_load
    │   4. FE 307 redirect → BE:8040
    │   5. 成功 → 完成(数据已落 StarRocks)
    │   6. 失败 → 重试 3 次(1s/2s/4s 退避)
    │   7. 3 次仍失败 → 写入 DLQ topic(兜底,不丢数据)
    │
    ├──→ 正常: StarRocks metrics_5m (3副本, 动态分区, 7天TTL)
    │
    └──→ 异常: Kafka prom.<city>.dlq.sr.5m (死信队列,7天留存)
                ↑ 由运维重放工具消费,重新 Stream Load 到 StarRocks
```

### 2.5 StarRocks 多级聚合

```
metrics_5m  (TTL=7天)   ←── Flink 直接写入(三城跨城)
        │
        │ StarRocks 周期任务(每小时执行)
        ▼
metrics_1h  (TTL=90天)  ←── INSERT INTO ... SELECT ... GROUP BY 1h
        │
        │ StarRocks 周期任务(每天执行)
        ▼
metrics_1d  (TTL=3年)   ←── INSERT INTO ... SELECT ... GROUP BY 1d
```

**为什么不用 ROLLUP 物化视图**:ROLLUP 与基础表共享分区生命周期,基础表分区被 drop 时 ROLLUP 数据一起被删除,无法实现 "5m 存 7 天、1d 存 3 年" 的多 TTL 需求。因此采用三张独立物理表,各自管理 `dynamic_partition` 生命周期。

## 3. 技术实现

### 3.1 prom-gw 接收层 (receiver)

**文件**: `internal/receiver/server.go`

#### HTTP 接入

```
POST /api/v1/write
Content-Type: application/x-protobuf
Content-Encoding: snappy
X-Source-DC: beijing-dongba
X-Prometheus-Remote-Write-Version: 1.0
Authorization: Bearer <token>
```

#### 中间件链

```
请求 → recoverer → requestID → realIP → rateLimit → auth → businessRateLimit → handleWrite
```

- **recoverer**: panic 恢复,记录堆栈,返回 500
- **requestID**: 注入 `X-Request-ID`(若上游未传则生成 UUID)
- **realIP**: 从 `X-Forwarded-For` 提取真实 IP
- **rateLimit**: 全局限流(QPS / 字节)
- **auth**: Bearer Token 校验(支持多 Token,从配置热加载)
- **businessRateLimit**: 按business限流

#### Body 处理

```go
// 1. 限长读取
r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
body, err := readAll(r.Body)

// 2. snappy 解压 + protobuf 反序列化
req, err := decoder.Decode(body)
// → prompb.WriteRequest{ Timeseries: []{{Labels, Samples}} }

// 3. 提取 source_dc(Header 优先于启动参数)
sourceDC := r.Header.Get("X-Source-DC")
```

**零拷贝优化**: decoder 只解压+解析 labels 用于路由,原始 HTTP body 字节直接透传到 Kafka(不重新序列化),保证字节级相等。

### 3.2 prom-gw 路由层 (router)

**文件**: `internal/router/router.go`

#### 路由模型

采用 **ruleset 级 fan-out**(非单 sample 级):

```yaml
rulesets:
  - name: app_business
    match:
      - metric_prefix: "app_"
      - metric_regex: "^business_"
    topic: prom.bj.routed.app_business
    default: false

  - name: infra
    match:
      - metric_prefix: "node_"
      - metric_prefix: "disk_"
    topic: prom.bj.routed.infra

  - name: default  # 兜底,无 match
    topic: prom.bj.routed.core
```

#### 分桶算法

```go
func (r *Router) Process(ctx context.Context, samples []parser.Sample, raw []byte, msg sink.Message) error {
    // 1. 预分配桶:每个 ruleset 一个桶
    buckets := make([][]parser.Sample, nEntries)

    // 2. 遍历 samples,命中第一条规则即停止
    for _, s := range samples {
        for i, entry := range entries {
            if entry.Match == nil { // default
                defaultIdx = i; continue
            }
            if entry.Match(s) {
                buckets[i] = append(buckets[i], s)
                break  // 单 sample 只进 1 个 ruleset
            }
        }
    }

    // 3. 逐桶调用 ruleset.Process
    for i, bucket := range buckets {
        if len(bucket) == 0 { continue }
        entries[i].Process(ctx, bucket, raw, msg)  // 原始 body 透传
    }
}
```

- 时间复杂度:O(n_samples + n_entries)
- 无锁设计:`entries` 用 `atomic.Pointer` 加载,配置热更新不阻塞读路径
- 桶分配用 `sync.Pool` 复用,降低 GC 压力

### 3.3 prom-gw Sink 层 (sink)

**文件**: `internal/sink/sink.go`, `internal/sink/pipeline.go`

#### Pipeline (有界 channel)

```go
type Pipeline struct {
    ch     chan Message    // 有界 channel,默认 65535
    sink   Sink            // AdapterSink 或 WALSink
}

// Submit: channel 满 → ErrBackpressure → receiver 返回 503
func (p *Pipeline) Submit(ctx context.Context, msg Message) error {
    select {
    case p.ch <- msg:
        p.submitted.Add(1)
        return nil
    case <-ctx.Done():
        return ctx.Err()
    default:
        return ErrBackpressure  // 503 背压
    }
}
```

- **单 worker goroutine 串行消费**:保证消息顺序,避免并发写竞争
- **Stop 优雅关闭**:先 `close(ch)`,worker 排空剩余消息(30s 超时),再 `cancel(ctx)`

#### AdapterSink (Kafka + WAL 降级)

```go
func (a *AdapterSink) Send(ctx context.Context, msg Message) error {
    if a.state.Load() == StateNormal {
        err := a.sendToKafka(ctx, msg)
        if err == nil { return nil }
        if errors.Is(err, ErrClosed) { return ErrClosed }
        a.recordFailure()  // 累计失败计数
        return a.sendToWAL(ctx, msg)  // 降级到 WAL
    }
    return a.sendToWAL(ctx, msg)  // 降级模式:全部走 WAL
}
```

**状态机**:

```
StateNormal ──(连续失败 ≥ Threshold)──→ StateDegraded
     ↑                                        │
     └──(monitor 探测 Kafka 恢复 + drain 成功)──┘
```

- **正常模式**:消息直接投递到 Kafka(franz-go 异步批量)
- **降级模式**:Kafka 连续失败超阈值,全部消息写 WAL,monitor goroutine 周期探测 Kafka 恢复后自动 drain

#### KafkaSink (franz-go 异步批量)

```go
// Produce 是异步的:消息入 producer channel,broker ack 通过 callback 回调
func (k *KafkaSink) Produce(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error {
    return k.client.Produce(ctx, kgo.Record{
        Topic:   topic,
        Key:      []byte(key),
        Value:    payload,
        Headers:  toHeaders(headers),
    })
}
```

- franz-go 内部维护批量 buffer + 异步发送 goroutine
- `RecordCompression: zstd` — Kafka batch 级别 zstd 压缩
- channel 满时返回 `ErrProduceBackpressure`

### 3.4 WAL 降级机制 (wal)

**文件**: `internal/wal/wal.go`

#### 存储布局

```
/appdata/prom-gw/wal/
├── seg-1724352000-001.log          # active 段(正在写)
├── seg-1724352000-001.log.sealed   # 已关闭段(fsync'd,可 replay)
├── seg-1724351000-000.log.done     # replay 成功,等待清理
└── seg-1724350000-002.log.done
```

#### Record 格式 (大端 binary)

```
[4B total_len]      # 后续所有字节长度
[8B ts]             # record 写入时间(Unix nano)
[1B flags]          # 0=ok
[2B topic_len][topic_bytes]
[4B key_len][key_bytes]
[4B payload_len][payload_bytes]
[4B headers_len][headers_serialized]
```

#### Segment Footer

```
[4B magic = "PWAL"]
[4B record_count]
[8B CRC32 over all record bytes]
```

#### 容量控制 (双阈值)

```go
func (w *fileWAL) checkCapacity() error {
    // 阈值1: WAL 总字节 ≥ MaxBytes (默认 50GB)
    if w.bytes.Load() >= w.cfg.MaxBytes {
        return ErrWALFull  // → receiver 返回 503
    }
    // 阈值2: 磁盘使用率 ≥ DiskUsedRatio (默认 0.80)
    if used, ok := diskUsedRatio(w.cfg.Dir); ok && used >= w.cfg.DiskUsedRatio {
        return ErrWALFull
    }
    return nil
}
```

- `syscall.Statfs` 调用耗时 < 1ms,每次 Write 调用一次,不缓存
- 任意阈值触发 → 返回 `ErrWALFull` → AdapterSink 转为 `ErrBackpressure` → receiver 返回 503

#### 重放语义

```
1. Kafka 恢复后,monitor goroutine 调用 drainWAL()
2. 按 mtime 顺序逐段读取(过滤 .tmp)
3. 逐 record 回放到 Kafka
4. handler 返回 nil → 段标记 .done
5. handler 失败 → 重试 + 退避,累计超 MaxReplayFailures 则告警
6. cleanup goroutine 周期删除 .done 段
```

### 3.5 Flink 聚合作业

**目录**: `examples/flink-agg5m-starrocks/`

#### 作业拓扑

```java
env.addSource(kafkaSource)           // KafkaSource, Exactly-Once
    .flatMap(new PromWriteRequestDecoder())  // 解码 snappy+protobuf
    .keyBy(s -> s.labelsHash)       // 按 series 分组
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowedLateness(Time.minutes(1))
    .process(new AggWindowFunction())  // 5min 聚合
    .addSink(new StarRocksSink())      // Stream Load 写入
    .name("starrocks-sink");
```

#### PromWriteRequestDecoder

```java
public void flatMap(ConsumerRecord<byte[], byte[]> record, Collector<PromSample> out) {
    byte[] value = record.value();
    // 1. Kafka client 已自动解 zstd (batch 级压缩)
    // 2. 解 snappy (Prometheus remote_write body 级压缩)
    byte[] decompressed = Snappy.uncompress(value);
    // 3. protobuf 反序列化
    PromProtos.WriteRequest req = PromProtos.WriteRequest.parseFrom(decompressed);
    // 4. 遍历 Timeseries → 输出 PromSample
    for (Timeseries ts : req.getTimeseriesList()) {
        for (Sample s : ts.getSamplesList()) {
            out.collect(new PromSample(ts.getLabelsList(), s));
        }
    }
}
```

#### AggWindowFunction

```java
public void process(String key, Context ctx, Iterable<PromSample> samples, Collector<AggResult> out) {
    long count = 0; double sum = 0, max = MIN, min = MAX;
    Map<String, Double> buckets = new HashMap<>();  // histogram buckets

    for (PromSample s : samples) {
        count++; sum += s.value;
        if (s.value > max) max = s.value;
        if (s.value < min) min = s.value;
        if (s.isHistogram) buckets.put(s.le, s.value);
    }
    out.collect(new AggResult(key, count, sum, max, min, sum/count, buckets, windowEnd));
}
```

#### StarRocksSink (Stream Load)

```java
public void invoke(AggResult value, Context context) {
    buffer.add(value);
    if (buffer.size() >= batchSize || timeSinceFlush >= flushInterval) {
        String json = JSON.toJSONString(buffer);
        // gzip 压缩(跨城场景减小带宽)
        byte[] body = gzip ? gzipCompress(json) : json.getBytes();
        // HTTP PUT → FE:8030, FE 307 redirect → BE:8040
        client.load(label, body, gzip);
        // 失败重试 3 次 → DLQ
        buffer.clear();
    }
}
```

- **label 幂等**:每批用全局唯一 label,StarRocks 保证同 label 不重复写入
- **DLQ 机制**:重试 3 次仍失败 → 写入 `prom.<city>.dlq.sr.5m` topic,不阻塞窗口推进

### 3.6 Kafka 集群设计

#### KRaft 模式(无 ZooKeeper)

```
# config/local.properties (每个 broker)
process.roles=broker,controller
controller.quorum.voters=1@broker-1:9093,2@broker-2:9093,3@broker-3:9093
advertised.listeners=PLAINTEXT://broker-1:9092
log.dirs=/data01/kafka,/data02/kafka,...,/data11/kafka  # 11盘 JBOD
```

- 3 Broker 同时担任 broker + controller 角色
- Quorum 多数派(2/3)存活即可工作,容 1 节点故障

#### JBOD 多盘存储

- 每节点 11~12 块 16T HDD,独立挂载(`/data01` ~ `/data11`)
- Kafka 自动在多盘间分配 partition replica,IO 并行度高
- 单盘故障只丢该盘上的 partition replica,其余 2 副本仍可用

#### Topic 配置

```
分区数: 64        # 高并行度,支持 64 个 Flink subtask
副本数: 3          # 3 副本跨 AZ(broker.rack 配置)
min.insync.replicas: 2   # 2 副本 ack 即可
retention: 3 天    # 72 小时
compression: zstd  # batch 级压缩
```

### 3.7 StarRocks 存储设计

#### 存算一体混合部署

```
节点 1 (AZ-1): 1 FE (Follower) + 1 BE
节点 2 (AZ-1): 1 FE (Follower) + 1 BE
节点 3 (AZ-2): 1 FE (Follower) + 1 BE
```

- 3 FE 均为 Follower,多数派(2/3)选举 Leader,容 1 故障
- 3 BE 跨 AZ 部署,3 副本数据自动均衡

#### 表设计

```sql
CREATE TABLE metrics_5m (
    ts DATETIME NOT NULL,
    city VARCHAR(16),
    source_dc VARCHAR(32),
    metric_name VARCHAR(256),
    labels JSON,
    count BIGINT,
    sum DOUBLE,
    max DOUBLE,
    min DOUBLE,
    avg DOUBLE
)
ENGINE=OLAP
DUPLICATE KEY(ts, city, metric_name)
PARTITION BY RANGE(ts)()  -- 动态分区
DISTRIBUTED BY HASH(metric_name) BUCKETS 64
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",   -- 保留 7 天
    "dynamic_partition.end" = "3",
    "replication_num" = "3"
);
```

#### 动态分区生命周期

| 表 | TTL | 数据来源 | 清理方式 |
|----|-----|---------|---------|
| `metrics_5m` | 7 天 | Flink Stream Load | dynamic_partition 自动 drop |
| `metrics_1h` | 90 天 | StarRocks 周期任务(从 5m 聚合) | dynamic_partition |
| `metrics_1d` | 3 年 | StarRocks 周期任务(从 1h 聚合) | dynamic_partition |

### 3.8 高可用与负载均衡

#### LVS + Keepalived (4 层)

```
Prometheus → LVS VIP (DR 模式) → prom-gw × 4
```

- DR 模式:报文直接转发到 RealServer,不改目标 IP,延迟极低
- Keepalived VIP 双机主备,VRRP 秒级切换

#### Nginx 反向代理 (7 层)

```
Flink → Nginx (HTTPS 443) → StarRocks FE:8030 (Stream Load)
```

- Nginx 做 TLS 终止 + 负载均衡到 3 个 FE
- 健康检查:HTTP GET `/api/health`,自动摘除故障 FE

#### Flink Standalone HA

```
3 节点 ZooKeeper ensemble → JM Leader 选举
2 JobManager (1 Active + 1 Standby)
TM 节点 → ZK 注册 → Active JM 分配 task
```

- ZK 容 1 节点故障(3 节点 ensemble)
- JM 故障 → ZK 自动切换到 Standby JM
- Checkpoint 存储到 HDFS,JM 切换后从最近 checkpoint 恢复

## 4. 容错与可靠性

### 4.1 数据不丢保证链

```
Prometheus remote_write
    ↓ 失败自动重试(Prometheus 内置)
LVS VIP
    ↓ Keepalived 秒级切换
prom-gw
    ↓ pipeline channel 满 → 503 → Prometheus 退避重试
    ↓ Kafka 写失败 → 降级 WAL 落盘(不丢)
Kafka
    ↓ 3 副本 + min.insync.replicas=2
Flink
    ↓ Checkpoint Exactly-Once(未 ack 的消息不 commit offset)
    ↓ Stream Load 失败 → 重试 3 次 → DLQ(不阻塞窗口)
StarRocks
    ↓ label 幂等(同 label 不重复写入)
```

### 4.2 降级矩阵

| 故障场景 | prom-gw 行为 | 数据影响 |
|---------|-------------|---------|
| 单台 prom-gw 宕机 | LVS 摘除,其余实例接管 | 无丢失(Prometheus 重试) |
| Kafka 单 Broker 宕机 | 其余 2 副本继续服务 | 无丢失 |
| Kafka 双 Broker 宕机 | min.insync.replicas 不满足 → 写失败 → 降级 WAL | WAL 落盘,Kafka 恢复后 drain |
| Kafka 全不可用 | 降级 WAL-only 模式 | WAL 满(50GB/80%磁盘)→ 503 |
| Flink JM 宕机 | Standby JM 接管 | 从最近 checkpoint 恢复,秒级中断 |
| StarRocks 单 BE 宕机 | 其余 2 副本继续服务 | 无丢失 |
| StarRocks FE 全宕机 | Flink Stream Load 失败 → DLQ | 数据在 DLQ topic,FE 恢复后重放 |
| 跨城专线中断 | 深圳/合肥 Flink 写入失败 → DLQ | 本地 DLQ 暂存,专线恢复后重放 |

### 4.3 监控告警

| 指标 | 阈值 | 告警 |
|------|------|------|
| `gateway_http_503_total` | > 0/min | P0: 有请求被拒绝 |
| `gateway_wal_bytes` | > 40GB(80% MaxBytes) | P1: WAL 接近满 |
| `gateway_wal_hard_reject_total` | > 0/min | P0: WAL 满导致拒绝 |
| `gateway_sink_error_total` | > 10/min | P1: Kafka 写入错误 |
| `gateway_pipeline_depth` | > 60000(92% capacity) | P1: 积压严重 |
| `gateway_wal_oldest_segment_age_seconds` | > 3600 | P1: WAL 积压超 1 小时 |
| Kafka under replicated partitions | > 0 | P1: 副本不同步 |
| Flink checkpoint failure rate | > 5% | P1: Checkpoint 异常 |

## 5. 部署约定

### 5.1 统一约定

| 项 | 值 |
|---|---|
| 部署用户 | bdops (uid 6000) |
| 程序目录 | `/appdata/<component>/` |
| 日志目录 | `/applog/<component>/` |
| 配置文件 | `/appdata/<component>/conf/` |
| 服务管理 | systemd |
| 操作系统 | Kylin V10 SP2 |

### 5.2 JDK 版本

| 组件 | JDK | 原因 |
|------|-----|------|
| Kafka | OpenJDK 25 | KRaft 性能优化 |
| StarRocks | OpenJDK 25 | JVM GC 优化 |
| Flink | OpenJDK 17 | Flink 1.19 官方仅支持 Java 11/17 |
| prom-gw | 无 JDK 依赖 | Go 编译为静态二进制 |

### 5.3 服务启动顺序

```
OpenJDK25 → Kafka → Prometheus → prom-gw → StarRocks → Flink
```

- Kafka 必须先于 prom-gw 启动(prom-gw 启动时探测 Kafka 连通性)
- StarRocks 必须先于 Flink 启动(Flink Stream Load 目标)
- Prometheus 可在 prom-gw 前或后启动(remote_write 会自动重试)

## 6. 容量规划

### 6.1 数据量估算

| 维度 | 北京 | 深圳 | 合肥 | 三城合计 |
|------|------|------|------|---------|
| Prometheus 实例 | 2 | 2 | 1 | 5 |
| 原始数据量(15s) | ~5 TB/天 | ~5 TB/天 | ~2.5 TB/天 | ~12.5 TB/天 |
| Kafka 留存(3天) | 15 TB | 15 TB | 7.5 TB | 37.5 TB |
| 5min 聚合(gzip) | 345 GB/天 | 345 GB/天 | 345 GB/天 | 1.0 TB/天 |
| StarRocks 5m 表(7天,3副本) | 7.2 TB | — | — | 7.2 TB |
| StarRocks 1h 表(90天,3副本) | 4.3 TB | — | — | 4.3 TB |
| StarRocks 1d 表(3年,3副本) | 3.5 TB | — | — | 3.5 TB |

### 6.2 资源清单

| 角色 | 规格 | 数量(京/深/肥) | 小计 |
|------|------|----------------|------|
| LVS | VM 8C/16G | 2/2/2 | 6 |
| prom-gw | VM 16C/32G/500G SSD | 4/4/2 | 10 |
| Kafka Broker | 物理机 64C/512G/12×16T | 3/3/3 | 9 |
| Flink JM | VM 32C/64G | 2/2/2 | 6 |
| Flink ZK | VM 8C/16G | 3/3/3 | 9 |
| Flink TM | VM 16C/32G/500G SSD | 6/4/2 | 12 |
| StarRocks FE+BE | 物理机 64C/512G/22×1.92T SSD | 3 (全在北京) | 3 |
