# 多机房 Prometheus 采集到数据中台方案设计

- **Status**: Draft
- **Date**: 2026-07-28
- **Author**: Brainstorming
- **Repo**: `github.com/lynnyq/bigdata`

## 1. 背景与目标

业务在多机房(华东/华北/华南)部署大量 Prometheus,通过 `remote_write` 上报指标。当前痛点:

1. 多机房直连后端存储,网络抖动易丢数据。
2. 缺乏统一的过滤/清洗层,下游存储(Flink/Spark/CK/SR)各自重复处理。
3. 没有按租户/业务路由能力,数据耦合在一个 topic。
4. 清洗规则管理分散,变更需要重启服务。

**目标**:自研 RemoteWrite 协议网关(`prom-gw`),作为多机房 Prometheus 与数据中台之间的统一接入层,提供:

- 高吞吐(单机 ≥ 1.5M samples/s 持续)
- 多租户路由、按业务分 topic
- 标签/指标/采样/下采样/死值等多维清洗
- 配置热更新
- 端到端可观测

## 2. 整体架构

```
┌────────────────┐
│ Prometheus×N   │ (各机房)
│ remote_write   │
└──────┬─────────┘
       │ HTTP (Snappy+Protobuf)
       ▼
┌──────────────────────────────────────────────┐
│         prom-gw 集群 (每机房一个)            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐      │
│  │  GW-1   │  │  GW-2   │  │  GW-3   │      │
│  └────┬────┘  └────┬────┘  └────┬────┘      │
│       └────────────┴────────────┘            │
│              固定 VIP (LVS/Nginx)            │
└──────────────────┼───────────────────────────┘
                   │
                   ▼
       ┌────────────────────────────┐
       │  Center Kafka (单集群)     │
       │  - prom.raw.<tenant>       │
       │  - prom.cleaned.<tenant>   │
       │  - prom.routed.<business>  │
       └────┬───────────┬───────────┘
            │           │
            ▼           ▼
       ┌────────┐  ┌────────┐
       │ Flink  │  │ Spark  │
       └────┬───┘  └────┬───┘
            └─────┬─────┘
                  ▼
        ┌──────────────────┐
        │ ClickHouse /     │
        │ StarRocks        │
        └──────────────────┘

控制面:Nacos (Config Center) ←─→ Admin API
可观测:prom-gw self-exporter → 业务监控体系
```

## 3. 组件与职责

| 组件 | 职责 | 形态 |
|------|------|------|
| Receiver | HTTP 接入、认证、限流 | Stateless |
| Decoder | Snappy + Protobuf 解码 | Stateless |
| RuleEngine | 标签增删改、路由、采样、下采样、死值 | Pipeline 多 stage |
| Router | 根据 rule 决策路由到目标 Kafka topic | Stateless |
| KafkaProducer | 异步批量写入,幂等,压缩 | 共享客户端 |
| ConfigManager | 加载/热更新规则 | Watcher |
| AdminAPI | 配置 CRUD、Reload、健康检查 | gRPC |
| Observability | `/metrics`、`/healthz`、TraceID | 内置 |

## 4. 数据流

### 4.1 接入协议

复用 Prometheus RemoteWrite 标准 v1:

| 项 | 值 |
|---|---|
| HTTP Path | `/api/v1/write` |
| Method | `POST` |
| Content-Encoding | `snappy` |
| Content-Type | `application/x-protobuf` |
| Body | `prometheus.WriteRequest` |
| Auth | `Authorization: Bearer <tenant_token>` |
| Headers | `X-Prometheus-Remote-Write-Version`, `X-Tenant`, `TraceID` |

### 4.2 内部流水线

```
[HTTP Request]
    │
    ▼
[Stage 1: Auth & RateLimit]   ← 100K samples/s/instance
    │ WriteRequest
    ▼
[Stage 2: Decode]             ← Snappy → Protobuf
    │ WriteRequest struct
    ▼
[Stage 3: Parse]              ← TimeSeries → Internal Sample
    │ []Sample
    ▼
[Stage 4: RuleEngine Pipeline]
    │ 4.1 Relabel (label drop/keep/map)
    │ 4.2 Enrich   (region, env, dc, project)
    │ 4.3 Route    (decide target topic)
    │ 4.4 Sample   (random drop)
    │ 4.5 Downsample (1m→5m aggregation)
    │ 4.6 DeadValue (drop unchanged series)
    ▼
[Stage 5: BatchBuffer per Topic]
    ▼
[Stage 6: Async Kafka Producer]
    ▼
[200 OK → Prometheus]
```

每个 stage 是一个 goroutine pool,阶段间用**有界 channel**(默认 65535)解耦。

### 4.3 Kafka 数据格式

支持两种模式(规则配置选择):

- **模式 1:RemoteWrite 透传** - 透传 `WriteRequest` Protobuf,加 envelope (`tenant`, `source_dc`, `ingest_time_ms`)
- **模式 2:JSON Lines** - `{metric, labels, value, timestamp_ms, tenant, dc}[]`

默认模式 1。

### 4.4 Topic 命名

```
prom.raw.<tenant>          # 原始
prom.cleaned.<tenant>      # 清洗后
prom.routed.<business>     # 按业务路由
```

每个 topic 默认 64 个分区(可在 config 中按 topic 覆盖),分区 key = `hash(tenant + metric_name + sorted_labels_hash)`,保证同 series 顺序写。

## 5. 规则引擎

### 5.1 规则结构 (YAML)

```yaml
rulesets:
  - name: app-business-clean
    tenant: app-business
    input_topic: prom.raw.app_business
    stages:
      - type: relabel
        drop_labels: [pod_template_hash, container_hash]
        keep_labels: [app, env, instance]
        label_map:
          instance: server_id

      - type: enrich
        add_labels:
          region: cn-east-1
          env: ${labels.env}
          ingest_cluster: gw-shanghai-1

      - type: route
        match: { app: "payment" }
        to_topic: prom.routed.payment
        default_topic: prom.routed.default

      - type: sample
        rate: 0.1
        scope: { metric_regex: "go_.*" }

      - type: downsample
        interval: 5m
        aggregations: [avg, max, p99]
        scope: { metric_regex: "http_request_duration_.*" }

      - type: deadvalue
        window: 5m
        scope: { metric_regex: "kube_pod_info" }
```

### 5.2 执行模型

- 每条 ruleset = 一条独立 pipeline(独立 goroutine、配置版本)
- Stage 顺序固定(因有数据依赖)
- 每个 stage 是无状态函数 `func([]Sample, RuleConfig) ([]Sample, error)`
- 跨 ruleset 故障隔离

### 5.3 性能

- 编译后的正则、聚合函数缓存在 `atomic.Value`
- Stage 接收批量 `[]Sample`,非单条
- sync.Pool 复用 encoder/buffer
- 每个 stage 输出 metric:`processed_count / dropped_count / duration_ms`

### 5.4 热更新

```
Admin API (gRPC):
  PutRuleset   /v1/rulesets/<name>
  ReloadRuleset /v1/rulesets/<name>:reload
  GetRuleset   /v1/rulesets/<name>
  ListRulesets /v1/rulesets

本地文件:
  configs/rules/*.yaml → SIGHUP 重载

Config Center:
  Nacos dataId=prom-gw-rules, group=GATEWAY
  长轮询监听 → 校验 → 编译 → 原子切换
```

校验失败的规则不替换线上版本,只告警。回滚保留最近 N 份历史版本(双 buffer 原子切换)。

### 5.5 Admin API 响应规范

为与全栈统一,Admin API(JSON 形态)使用统一响应体:

```json
{
  "code": 0,
  "message": "ok",
  "data": { ... },
  "trace_id": "abc123"
}
```

- `code`: 业务码,`0`=成功;公共错误码 1000-1999,网关服务码 4000-4999
- `trace_id`: 与入口请求一致,便于排查
- `data`: 业务负载,类型随接口而定

## 6. 可靠性与背压

### 6.1 错误响应

| 错误 | HTTP | 重试 | 行为 |
|---|---|---|---|
| Auth 失败 | 401 | ❌ | 拒绝 + 告警 |
| 限流命中 | 429 | ✅ | Prometheus 退避 |
| Channel 满 | 503 | ✅ | 触发退避 |
| 解码失败 | 400 | ❌ | 丢弃 + 告警 |
| 单条规则失败 | 200 | ❌ | 跳过 + 记录 metric |
| Kafka 暂时不可用 | 200 | (内部) | 内存 + WAL 重试 |
| Kafka 长期不可用 | 503 | ✅ | WAL 满则拒绝 |

**关键原则**:5xx 必须可重试(协议层语义);单条异常不影响整批。

### 6.2 背压三道防线

1. **应用层令牌桶限流** (默认 100K samples/s/instance,可通过 flag `--rate-limit` 调整;同时支持按租户维度动态下发限流配置)
2. **有界 channel** (每个 stage 间 `chan []Sample`,容量 65535,满则 503)
3. **磁盘 WAL** (`/data/wal/`,Kafka 长期不可用时落盘,后台重放;磁盘使用达 80% 后转硬拒绝)

### 6.3 Kafka 写入参数

| 参数 | 值 | 理由 |
|---|---|---|
| `acks` | `all` | 跨副本确认 |
| `enable.idempotence` | `true` | 防 batch 内重复 |
| `compression.type` | `zstd` | 节省 60-70% 带宽 |
| `linger.ms` | 50 | 凑齐 batch |
| `batch.size` | 1MB | 单 batch 上限 |
| `retries` | 10 | 客户端重试 |
| `delivery.timeout.ms` | 120000 | 2 分钟硬上限 |

**投递语义**:**At-least-once**。下游 (Flink/Spark) 需做幂等去重(主键 `tenant + metric + labels + ts`)。

### 6.4 故障切换

- 实例宕机:机房 LVS 自动剔除
- Kafka 故障:WAL 缓冲 + 告警,恢复后追平
- 机房链路故障:机房内 WAL 缓冲
- Nacos 故障:使用最后成功版本,降级静态配置

### 6.5 优雅启停

**启动**:`Load Local Config → Watch Config Center → Start Receivers → Start Pipelines → Start Producers → Health Ready`

**停机 (SIGTERM)**:
1. 健康检查 fail,LB 摘除流量
2. 等待 in-flight 请求处理完(超时 30s)
3. Flush 所有 batch buffer
4. 关闭 Kafka producer
5. 退出

### 6.6 故障隔离

- 每个 goroutine(包括 stage workers、Kafka producer flush、config watcher、admin server handler)统一通过 `pkg/safego` 包裹,`panic` 转换为 `gateway_panic_recovered_total` 指标并记录堆栈,**不允许 panic 逃逸导致进程崩溃**
- HTTP 中间件增加 panic recovery,捕获 handler 内的 panic 返回 500

## 7. 可观测性

### 7.1 指标 (self-export `/metrics`)

```
# 吞吐
gateway_samples_total{stage, tenant, status}
gateway_bytes_in_total{tenant}
gateway_bytes_out_total{topic}

# 延迟
gateway_stage_duration_seconds{stage, op}     # Histogram
gateway_request_duration_seconds              # Histogram

# 错误 / 背压
gateway_errors_total{stage, type}
gateway_backpressure_rejected_total{stage}
gateway_wal_bytes
gateway_wal_oldest_age_seconds

# 资源
gateway_goroutines
gateway_mem_bytes
gateway_cpu_ratio

# 规则
gateway_ruleset_processed_total{ruleset, stage}
gateway_ruleset_version{ruleset}
```

### 7.2 链路追踪

- OpenTelemetry SDK,每请求 `TraceID`
- Header 透传到 Kafka message headers(Flink/Spark 接力)
- Span:`receive → decode → parse → rule_engine → produce_kafka`

### 7.3 日志

- 必带 `trace_id, tenant, dc, stage`
- 不打印原始 metric/label value
- 错误打印完整堆栈
- JSON 格式(Loki/ELK 友好)

## 8. 测试策略

| 层级 | 范围 | 工具 | 目标 |
|---|---|---|---|
| 单元 | 每个 stage、relabel、router | `testing` + `testify` | 覆盖率 ≥ 60% |
| 集成 | 端到端 + 嵌入式 Kafka | `testcontainers-kafka` | 关键路径 |
| 性能 | 1.5M samples/s × 1h | `vegeta` + 自研 client | 吞吐 + p99 |
| 混沌 | 杀实例、杀 Kafka、网络分区 | `chaos-mesh` | 恢复路径 |
| 兼容 | 多版本 Prometheus | matrix test | v2.40+ |

**性能基线**(16 核 32G 单机):
- 1.5M samples/s 持续
- p99 < 500ms
- CPU < 70%,Mem < 8G,GC pause < 50ms

## 9. 部署

### 9.1 形态(非 K8s,VM/bare-metal + systemd)

```ini
# /etc/systemd/system/prom-gw.service
[Unit]
Description=Prometheus RemoteWrite Gateway
After=network.target

[Service]
ExecStart=/opt/prom-gw/bin/gw --config=/etc/prom-gw/config.yaml
Restart=always
RestartSec=5
LimitNOFILE=65535
User=prom-gw

[Install]
WantedBy=multi-user.target
```

### 9.2 拓扑

- 每机房 N 台 VM/物理机(默认每机房 4 台),每台运行一个 `prom-gw` systemd 服务
- 前置 LVS/Nginx SLB,固定 VIP(地址固定)
- 配置中心 Nacos 独立集群
- 升级:`ansible-playbook deploy.yml` 滚动升级(逐台 stop → 升级 → start,等 healthz 通过再下一台)

### 9.3 仓库结构

```
github.com/lynnyq/bigdata/
├── cmd/prom-gw/                # main 入口
├── api/proto/                  # 协议定义 (prometheus remote_write, admin)
├── internal/
│   ├── receiver/               # HTTP 接入
│   ├── decoder/                # Snappy+Protobuf 解码
│   ├── parser/                 # TimeSeries → Sample
│   ├── ruleengine/             # 规则引擎
│   │   ├── stages/             # relabel/enrich/route/sample/downsample/deadvalue
│   │   └── pipeline.go
│   ├── router/                 # topic 路由
│   ├── kafkasink/              # Kafka producer
│   ├── config/                 # 配置加载 + 监听
│   ├── admin/                  # Admin API
│   └── obs/                    # metrics/tracing/log
├── pkg/                        # 公共工具
├── configs/
│   └── rules/                  # 默认规则
├── deploy/
│   ├── ansible/                # 部署脚本
│   └── systemd/                # service 文件
├── test/
│   ├── integration/
│   └── perf/
└── docs/
```

## 10. 里程碑

| 阶段 | 时长 | 目标 |
|---|---|---|
| M1 | 2w | 骨架:Prometheus → 透传 Kafka,无规则 |
| M2 | 2w | 规则引擎 v1:relabel + route + sample |
| M3 | 1w | downsample + deadvalue + enrich |
| M4 | 1w | 配置中心 + 热更新 + Admin API |
| M5 | 1w | 性能压测 + 混沌 + 文档 + Dashboard |

总计 ~7 周(单人);2-3 人团队可压缩到 4 周。

## 11. 风险与权衡

| 风险 | 影响 | 缓解 |
|---|---|---|
| 中心 Kafka 单点 | 全 GW 写入失败 | 监控 + 告警 + WAL;长期可演进 MirrorMaker 跨机房 |
| At-least-once 重复 | 下游数据重复 | 下游主键去重(Flink/Spark 已具备) |
| 规则热更新误操作 | 大量数据被丢弃/路由错 | 校验 + 不替换失败版本 + 告警 + 回滚 |
| 单机瓶颈 | 大促写入堆积 | 多实例水平扩展;阶段独立可调优 |
| Prometheus 版本兼容 | 解码失败 | 严格按官方 proto,矩阵测试 |

## 12. 待办

- [ ] M1 启动前确定 Nacos 集群位置
- [ ] M2 前确认下采样/死值精度需求(p99 vs median)
- [ ] M4 前与运维确定 Ansible 仓库位置
- [ ] 选型 Kafka 客户端(franz-go vs sarama vs confluent-kafka-go)
