# prom-gw 生产部署与配置详解

---

## 目录

| # | 章节 | 关键内容 |
|---|------|---------|
| 1 | [编译](#1-编译) | Go 1.22+，3 种构建模式 |
| 2 | [数据流模型](#2-数据流模型单阶段同步处理) | 单阶段同步处理 + 配置关系图 |
| 3 | [生产配置总览](#3-生产配置总览) | 五要素总览表 |
| 4 | [ENV 环境变量配置](#4-env-环境变量配置--必须配置) | 三城 env 文件 + 全量表 |
| 5 | [Token 配置](#5-token-配置) | tokens.yaml + 热重载 |
| 6 | [Ruleset 配置](#6-ruleset-配置) | relabel + route + Stage 完整功能表 |
| 7 | [Kafka Producer 内部行为](#7-kafka-producer-内部行为已固化不可配置) | Backpressure 链 + AdapterSink 状态机 |
| 8 | [Auth 接口与 Token 语义](#8-auth-接口与-token-语义) | sentinel 错误 + 两层限流 |
| 9 | [WAL 磁盘配置](#9-wal-磁盘配置--生产关键) | 双阈值 + 崩溃恢复 + Replay + drain |
| 10 | [systemd template 部署](#10-systemd-template-部署) | 6 步标准部署流程 |
| 11 | [启动参数速查](#11-启动参数速查flag--env--默认值) | flag / env / 默认值 三列表 |
| 12 | [配置变更操作速查](#12-配置变更操作速查) | 热加载 / 回滚 / 备份 |
| 13 | [Receiver HTTP 接口规范](#13-receiver-http-接口规范) | remote_write + 中间件链 |
| 14 | [Admin API 完整端点](#14-admin-api-完整端点) | ruleset / state / WAL segments |
| 15 | [升级与回滚 SOP](#15-升级与回滚-sop) | 二进制 / 配置 回滚策略 |
| 16 | [监控指标全集](#16-监控指标全集) | 26 个 gateway_* 指标 + PromQL + 告警 |
| 17 | [日志说明](#17-日志说明) | JSON 结构 + 22 字段 + 样例 + 查询 |
| 18 | [网络与防火墙规则](#18-网络与防火墙规则) | firewalld 规则示例 |
| 19 | [Nacos 配置中心](#19-nacos-配置中心可选) | 三级降级优先级 |
| 20 | [Kafka Topic 预创建清单](#20-kafka-topic-预创建清单) | 三城 topic + KRaft 创建命令 |
| 21 | [安全加固说明](#21-安全加固说明) | systemd + 文件权限 + 暴露面 |
| A | [附录 A:端口与防火墙](#附录-a端口与防火墙) | |
| B | [附录 B:信号处理](#附录-b信号处理) | |
| C | [附录 C:退出码](#附录-c退出码) | |
| D | [附录 D:Prometheus 对接配置](#附录-dprometheus-对接配置完整) | |
| E | [附录 E:快速故障速查手册](#附录-e快速故障速查手册) | |

---

## 1 编译

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

## 2 数据流模型(单阶段同步处理)

prom-gw 采用 **单进程同步处理模型**:收到 Prometheus remote_write 后,在进程内完成 relabel + route,直接写入目标 Kafka topic。**不存在 raw → routed 两阶段异步消费**。

#### 2.1 数据流

```
Prometheus
  │ remote_write (HTTP POST, body=snappy(protobuf))
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ prom-gw (单进程,6 层中间件 + 多 ruleset 并行 fan-out)          │
│                                                                 │
│  [Middleware 链]                                                 │
│   requestID → realIP → recoverer → rateLimit →                  │
│   businessRateLimit → auth → handler                            │
│                                                                 │
│  [Receiver Handler]                                             │
│   1. 提取 meta: business(来自 token) / source_dc /              │
│      ingest_city / ingest_time_ms                               │
│   2. Manager 按 ruleset.match.metric_prefix fan-out             │
│      ├─ Ruleset A: relabel → enrich → route → sample            │
│      └─ Ruleset B: relabel → deadvalue (并行,各自有独立状态)    │
│                                                                 │
│  [每 Ruleset Pipeline] 同步串行执行                              │
│   buf1 ↔ buf2 双 buffer 原地复用,单批内 rules 原子快照          │
│   末尾按 sample.TargetTopic 逐 sample 出队                      │
│                                                                 │
│  [Sink Adapter] Kafka ↔ WAL 自动切换                           │
│   FailThreshold=3 → 降级 WAL                                    │
│   RecoverCheck=1s + _gw_probe ping → RecoverSuccessThreshold=3  │
│   → 切回 Kafka + drainWAL (30s 同步重放)                       │
│                                                                 │
│  [Kafka Producer] franz-go,单 flusher goroutine                 │
│   zstd 压缩,幂等写,acks=all,linger=50ms,batch=1MB               │
│   retries=10,delivery.timeout=120s                              │
│   ❌ 禁止 auto-create topic (topic 必须预创建)                   │
└─────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ Kafka: prom.<city>.routed.<category>                           │
│                                                                 │
│ key     = series hash (同 series 落同 partition,保序)          │
│ payload = 原始 snappy+protobuf WriteRequest body                │
│                                                                 │
│ headers (6 个,全部是 string → string):                          │
│   business       token 对应的 business 名                      │
│   source_dc      Prometheus remote_write.header 里的 source_dc  │
│   ingest_city    prom-gw 实例的 --ingest-city (bj/sz/hf)       │
│   ingest_dc      prom-gw 实例的 --source-dc (dc-bj-dongba)      │
│   ingest_time_ms receiver 端毫秒时间戳                         │
│   traceparent    W3C trace context OTel 注入                   │
│                                                                 │
│ ⚠️ source_dc ≠ ingest_dc:                                       │
│   source_dc = 数据产生的机房(Prometheus 侧标记)                 │
│   ingest_dc = 处理本条数据的 prom-gw 实例所在机房               │
└─────────────────────────────────────────────────────────────────┘
```

**关键点**:
- prom-gw **不消费 Kafka**,只写入
- token 的 `default_topic` 仅作为 `msg.Topic` 初始值,被 ruleset 的 `default_topic` 覆盖
- ruleset 的 `input_topic` 字段**仅作文档标识,运行期不参与逻辑**(代码注释明确)
- 实际写入的 topic 由 ruleset 的 `default_topic` + route rules 决定
- **Fan-out**:一个 Prometheus 批次会被 Manager 按 `match.metric_prefix` 分发给多个 ruleset 并行处理;ruleset 不按 business 过滤,而是按 metric 名前缀匹配
- **多 topic 输出**:同一 ruleset 内,route stage 可以按不同 label 把 samples 分到不同 topic(如 core/infra/data/app_business),一次 Process 产生多次 `out()` 调用
- **背压链**: receiver → ruleengine → sink.Pipeline(有界 channel 65535) → AdapterSink → Kafka/WAL。任何一层 ErrBackpressure 向上传播 → receiver 返回 503 + Retry-After
- **Kafka topic 必须预创建**:禁止 auto-create,因为需要 64 分区 3 副本的硬约束

#### 2.2 配置关系

```
tokens.yaml                    rules.yaml (ruleset)
─────────────                  ─────────────────────────────
tokens:                        rulesets:
  "tk_app_business_prod":         - name: app-business
    default_topic:                  business: app-business
      prom.bj.routed.app_business   default_topic: prom.bj.routed.app_business
    rate_limit: 80000               stages:
                                      - relabel ...
  "tk_infra_prod":                    - route:
    default_topic:                      rules:
      prom.bj.routed.infra               - { team: core,  topic: prom.bj.routed.core }
    rate_limit: 50000                   - { team: infra, topic: prom.bj.routed.infra }
                                       - { team: data,  topic: prom.bj.routed.data }

           │                              │
           │   token.default_topic         │   ruleset.default_topic
           │   = 兜底 topic               │   + route.rules[].topic
           │   (ruleset 未配置时使用)      │   = 实际写入的 topic
           ▼                              ▼
    ┌──────────────────────────────────────────┐
    │  prom.bj.routed.core                     │
    │  prom.bj.routed.infra                    │
    │  prom.bj.routed.data                     │
    │  prom.bj.routed.app_business (兜底)      │
    └──────────────────────────────────────────┘
```

**topic 选取优先级**(代码确认,见 [pipeline.go:225-230](file:///Users/yangqian/go/src/github.com/lynnyq/prom-gw/internal/ruleengine/pipeline.go#L225-L230)):

1. route stage 设置的 `sample.TargetTopic`(最高优先级)
2. ruleset 的 `default_topic`(route 未命中时)
3. token 的 `default_topic`(ruleset 未配置时兜底)

---

## 3 生产配置总览

prom-gw 生产部署涉及 **三层配置**,按优先级从低到高:

| 层级 | 文件 | 路径 | 加载方式 | 热重载 |
|------|------|------|----------|--------|
| **ENV 环境变量** | `prom-gw.<city>.env` | `/etc/prom-gw/prom-gw.<city>.env` | systemd `EnvironmentFile` | ❌ 需重启 |
| **Config Ruleset** | `rules.yaml` | `/appdata/prom-gw/conf/rules.yaml` | `--config` flag / `PROM_GW_CONFIG` | ✅ fsnotify 自动 |
| **Tokens 鉴权** | `tokens.yaml` | `/appdata/prom-gw/conf/tokens.yaml` | `--tokens` flag / `PROM_GW_TOKENS` | ✅ `kill -HUP` |

**三层职责**:

| 层级 | 职责 | 典型内容 |
|------|------|----------|
| ENV | 进程级运行参数 | Kafka 地址、Go runtime、Tracing endpoint、SourceDC |
| Config | 数据处理规则 | relabel/route/sample/downsample 等 ruleset 定义 |
| Tokens | 鉴权 + 限流 | token → business 映射、默认 topic、per-business rate limit |

---

## 4 ENV 环境变量配置 ⚠️ **必须配置**

> 本节列出 prom-gw 进程读取的所有环境变量。ENV 是连接 Kafka、注入 runtime 参数的关键入口,**缺失将导致进程降级为 WAL-only 模式或指标缺失**。

#### 4.1 文件位置

systemd template unit 通过 `EnvironmentFile=-/etc/prom-gw/prom-gw.%i.env` 加载,`%i` 为城市标识(bj/sz/hf)。

```bash
# 北京机房
/etc/prom-gw/prom-gw.bj.env

# 深圳机房
/etc/prom-gw/prom-gw.sz.env

# 合肥机房
/etc/prom-gw/prom-gw.hf.env
```

`-` 前缀表示文件不存在时不报错(首次部署需创建)。

#### 4.2 环境变量全量表

| 变量 | 类型 | 默认值 | 必填 | 说明 |
|------|------|--------|------|------|
| **`KAFKA_BROKERS`** | string(逗号分隔 `host:port`) | **空** | ✅ 生产必填 | Kafka broker 列表。**空 = 进入 WAL-only 模式**(数据只落本地 WAL,不投递 Kafka)。必须逗号分隔多 broker,不可传带逗号的单个字符串 |
| **`INGEST_CITY`** | string | `dc-unknown` | ✅ 生产必填 | 城市标识(`bj`/`sz`/`hf`)。由 systemd `Environment=INGEST_CITY=%i` 注入,可被 `--ingest-city` flag 覆盖。写入 Kafka header `ingest_city`,用于 StarRocks 城市切片 |
| `PROM_GW_CONFIG` | string(文件路径) | `configs/rules/default.yaml` | 否 | ruleset 配置路径。systemd 已通过 `Environment=PROM_GW_CONFIG=/appdata/prom-gw/conf/rules.yaml` 注入,一般无需在 env 文件重复 |
| `PROM_GW_TOKENS` | string(文件路径) | `configs/tokens/local.yaml` | 否 | token 配置路径。systemd 已注入,一般无需在 env 文件重复 |
| **`OTEL_EXPORTER_OTLP_ENDPOINT`** | string(URL) | 空 | 否 | OpenTelemetry OTLP/gRPC 接收端(如 `http://otel-collector:4317`)。空时 tracing 降级为 noop,不发送 span |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | string(URL) | 空 | 否 | Traces 专用 OTLP endpoint。**优先级高于** `OTEL_EXPORTER_OTLP_ENDPOINT` |
| **`GOMAXPROCS`** | int | CPU 核数 | ✅ 生产必填 | Go runtime 线程数上限。prom-gw 是 CPU 密集型(relabel/sample),建议设为物理核数 |
| **`GOMEMLIMIT`** | string(如 `6GiB`) | 无限制 | ✅ 生产必填 | Go runtime 软内存上限。防止 OOM,建议设为物理内存的 70-80% |

#### 4.3 详细说明

##### `KAFKA_BROKERS`(最关键)

- **行为**:
  - 空 → 进入 **WAL-only 模式**(数据只落本地 WAL,不投递 Kafka)
  - 非空但连不上 → 自动降级到 WAL-only,日志输出 `kafka connect failed`
  - 非空且连上 → 正常模式,故障时降级到 WAL
- **格式**:必须是逗号分隔的多 broker 列表,如 `kafka-1:9092,kafka-2:9092,kafka-3:9092`
- **踩坑**:不能把整个字符串当单个 broker 地址传入,否则 DNS 解析失败 → Ping 超时 → 静默降级
- **生产建议**:使用内部域名 + 集群内通信端口(9092),跨安全组时用 SSL 端口(9094)
- **代码位置**:见 [main.go:147-172](file:///Users/yangqian/go/src/github.com/lynnyq/prom-gw/cmd/prom-gw/main.go#L147-L172)

##### `INGEST_CITY`

- systemd unit 已通过 `Environment=INGEST_CITY=%i` 自动注入,env 文件中**无需重复声明**
- 但如果不走 systemd 直接启动进程,需手动设置

##### `GOMAXPROCS` / `GOMEMLIMIT`

- systemd unit 已通过 `Environment=GOMAXPROCS=8` / `Environment=GOMEMLIMIT=6GiB` 注入
- env 文件中**无需重复声明**(除非需要覆盖默认值)

#### 4.4 生产环境 ENV 示例

##### 北京 (bj)

```bash
# ============================================================
# prom-gw 环境变量 - 北京机房 (bj)
# 部署路径: /etc/prom-gw/prom-gw.bj.env
# 权限要求: chmod 600, owner root:root
# 加载方式: systemd EnvironmentFile
#
# 变更需重启: sudo systemctl restart prom-gw@bj
# ============================================================

# ── Kafka 集群(3 broker,KRaft 模式) ──
# 必须逗号分隔多 broker 地址,不可传单个长字符串
# 内网通信使用 9092(PLAINTEXT),跨安全组使用 9094(SSL)
KAFKA_BROKERS=bj-kafka-1:9092,bj-kafka-2:9092,bj-kafka-3:9092

# ── OpenTelemetry Tracing(可选但推荐) ──
# OTLP/gRPC endpoint,空时 tracing 降级为 noop
# tracer 会在 Kafka header 中注入 traceparent,下游 Flink/StarRocks 可追踪
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.bj:4317
# 如果 traces 和 metrics 分开接收,用专用变量覆盖
# OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://trace-collector:4317
```

##### 深圳 (sz)

```bash
# ============================================================
# prom-gw 环境变量 - 深圳机房 (sz)
# 部署路径: /etc/prom-gw/prom-gw.sz.env
# ============================================================

# 深圳 Kafka 集群地址
KAFKA_BROKERS=sz-kafka-1:9092,sz-kafka-2:9092,sz-kafka-3:9092

# OTel collector 就近接入(避免跨城写)
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.sz:4317
```

##### 合肥 (hf)

```bash
# ============================================================
# prom-gw 环境变量 - 合肥机房 (hf)
# 部署路径: /etc/prom-gw/prom-gw.hf.env
# ============================================================

KAFKA_BROKERS=hf-kafka-1:9092,hf-kafka-2:9092,hf-kafka-3:9092

OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.hf:4317
```

#### 4.5 systemd unit 中已硬编码的环境变量

以下变量**已在 systemd unit 中通过 `Environment=` 注入**,**不需要**在 env 文件重复:

| 变量 | systemd unit 中的值 | 说明 |
|------|---------------------|------|
| `INGEST_CITY` | `%i`(模板实例名) | 自动按城市注入 |
| `PROM_GW_CONFIG` | `/appdata/prom-gw/conf/rules.yaml` | ruleset 配置路径 |
| `PROM_GW_TOKENS` | `/appdata/prom-gw/conf/tokens.yaml` | token 配置路径 |
| `GOMAXPROCS` | `8` | Go 线程数(物理核数) |
| `GOMEMLIMIT` | `6GiB` | Go 软内存上限(8G 物理内存的 75%) |

---

## 5 Token 配置

路径:`/appdata/prom-gw/conf/tokens.yaml`

```yaml
# ============================================================
# prom-gw Token 配置 - 生产环境 - 北京 (bj)
# 部署路径: /appdata/prom-gw/conf/tokens.yaml
# 权限要求: chmod 600, owner bdops:bdops
# 热重载: kill -HUP <pid>(无需重启进程)
#
# token 与 topic 的对应关系:
#   tk_app_business_prod → prom.bj.routed.app_business
#   tk_infra_prod       → prom.bj.routed.infra
# ============================================================
tokens:
  # ── app-business 业务 ──
  # 业务应用指标:CPU/内存/QPS/RT/错误率 等
  "tk_app_business_prod":
    business: app-business
    business_id: "1001"
    default_topic: prom.bj.routed.app_business    # 写入的 topic(ruleset 配置后被覆盖)
    rate_limit: 80000                             # samples/s 上限,超过返回 429

  # ── infra 业务 ──
  # 基础设施指标:主机/网络/存储/容器 等
  "tk_infra_prod":
    business: infra
    business_id: "1002"
    default_topic: prom.bj.routed.infra           # 写入的 topic
    rate_limit: 50000
```

**字段说明**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `business` | string | ✅ | business 名,写入 Kafka header `business` 和 metric 标签 |
| `business_id` | string | 否 | IAM 主键(预留字段,v1 可不填) |
| `default_topic` | string | ✅ | 初始 topic(ruleset 配置后被 ruleset.default_topic 覆盖) |
| `rate_limit` | int | 否 | 该 business 的 samples/s 上限。0 或不填 = 不限 |

**多城 token 配置**:每城 prom-gw 实例独立部署,token 配置中 `default_topic` 带城市前缀:

```bash
# 北京: /appdata/prom-gw/conf/tokens.yaml
#   default_topic: prom.bj.routed.app_business
#   default_topic: prom.bj.routed.infra

# 深圳: /appdata/prom-gw/conf/tokens.yaml
#   default_topic: prom.sz.routed.app_business
#   default_topic: prom.sz.routed.infra

# 合肥: /appdata/prom-gw/conf/tokens.yaml
#   default_topic: prom.hf.routed.app_business
#   default_topic: prom.hf.routed.infra
```

修改后通过 `kill -HUP <pid>` 热重载,**不重启进程**。

---

## 6 Ruleset 配置

路径:`/appdata/prom-gw/conf/rules.yaml`(每城独立部署,文件名统一)

```yaml
# ============================================================
# prom-gw Ruleset 配置 - 生产环境 - 北京 (bj)
# 源文件: configs/rules/bj/default.yaml
# 部署路径: /appdata/prom-gw/conf/rules.yaml
# 权限要求: chmod 644, owner bdops:bdops
# 热加载: fsnotify 监听文件变化(5s 检测),自动编译+原子切换
#
# 路由规则:
#   app-business business → relabel → route → prom.bj.routed.{core,infra,data,app_business}
#   infra business        → relabel          → prom.bj.routed.infra
# ============================================================
rulesets:
  # ── app-business 租户规则集 ──
  # 收到 remote_write 后同步处理:relabel + route,直接写入 routed topic
  - name: app-business
    business: app-business
    default_topic: prom.bj.routed.app_business   # 路由未命中时的兜底 topic
    version: 1
    match:
      metric_prefix: ""                         # 空 = 全量接收
    stages:
      # Stage 1: relabel - 标签清洗
      - type: relabel
        drop_labels:
          - env                                 # 环境标签(噪音)
          - instance                            # 实例标签(高基数,用 host 替代)
          - pod                                 # Pod 名(高基数)
        keep_labels: []                          # 空 = 关闭白名单
        label_map:
          kubernetes_io_cluster: cluster         # 重命名: kubernetes_io_cluster → cluster

      # Stage 2: route - 按 team 标签分桶到不同 topic
      - type: route
        rules:
          - match: { team: "core" }              # 核心业务团队(订单/支付/账户)
            topic: prom.bj.routed.core
          - match: { team: "infra" }              # 基础设施团队(主机/网络/监控)
            topic: prom.bj.routed.infra
          - match: { team: "data" }              # 数据平台团队(离线/实时计算)
            topic: prom.bj.routed.data
          # 未命中以上规则 → 写入 default_topic (prom.bj.routed.app_business)

      # Stage 3: sample - 采样(降低下游存储压力)
      - type: sample
        rate: 0.1                                # 保留 10%,生产可按需调整

  # ── infra 租户规则集 ──
  # 基础设施指标通常不按 team 分桶,直接写入 routed.infra
  - name: infra
    business: infra
    default_topic: prom.bj.routed.infra           # 写入的 topic
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels:
          - env
          - instance
      # 无 route stage → 全部写入 default_topic (prom.bj.routed.infra)

global:
  rate_limit_per_instance: 100000                # 单实例全局 samples/s 上限
  channel_buffer: 65535                           # 内部 channel 缓冲区大小
```

**stage 执行顺序**(固定,6 个已实现):

```
relabel → enrich → route → sample → downsample → deadvalue
 无状态   无状态   无状态   无状态   状态型(LRU)  状态型(LRU)
```

#### 6.1 已实现 Stage 完整功能表

##### ① relabel(无状态)

| 配置项 | 类型 | 说明 |
|--------|------|------|
| `drop_labels` | `[]string` | 显式删除的 label 名列表 |
| `keep_labels` | `[]string` | 白名单模式(与 drop 互斥,编译时 detect 冲突打 metric) |
| `label_map` | `map<string,string>` | 重命名:`{源label: 目标label}` |

**规则**:keep 与 drop 同时配 → 优先 keep(更严格语义);空配置 → 透传(等价 noop)。

##### ② enrich(无状态)

给 sample **增加/覆盖** label。值支持两种形式:

```yaml
- type: enrich
  add_labels:
    cluster: "prod"                  # 静态值,直接作为 label value
    env_from: "${labels.env}"        # 模板引用:从 sample 现有 labels 中取 env 的值
    region:   "${labels.region}"
```

- 模板引用 `${labels.<name>}`:目标 label 不存在 → 跳过该条 enrich,记 `enrich_template_missing` metric
- 新 label key 与 sample 已有 key 重名 → **直接覆盖**
- 向后兼容:配置字段名 `labels` 也支持(旧版写法)

##### ③ route(无状态)

按 sample labels **条件匹配**,命中时给 sample 设置 `TargetTopic`。

```yaml
- type: route
  default_topic: prom.bj.routed.app_business   # route stage 级兜底(可省略,编译时自动用 ruleset.default_topic)
  rules:
    - match: { team: "core", app: "order" }    # 多条件 AND
      topic: prom.bj.routed.core
    - match: { team: "infra" }
      topic: prom.bj.routed.infra
    # 未命中任何规则 → sample.TargetTopic 仍为空 → Pipeline 用 ruleset.default_topic
```

**编译期兜底**:如果 route stage 没配 `default_topic`,编译器自动注入 ruleset 的 `default_topic` 作为 route 级兜底。

##### ④ sample(无状态)

按 rate **丢弃**部分 samples。

```yaml
- type: sample
  rate: 0.1              # 保留 10%,丢弃 90%
  scope:
    metric_regex: ".*_latency.*"    # 可选:仅对匹配正则的 metric 做采样,其他原样透传
```

- `rate=0.0` → 全丢;`rate=1.0` → 全保留
- 有状态与无状态都支持 `scope.metric_regex`(对 metric 名做正则过滤)

##### ⑤ downsample(状态型 ⚠️ 跨 batch)

按固定 interval 对 **per-series 聚合**,产出聚合后 sample。**跨 batch 有状态**,用 LRU 维护每个 series 的桶。

```yaml
- type: downsample
  interval: "1m"                        # 桶大小,Go duration 格式
  aggregations: [avg, sum, max, min, count, p50, p99]
  scope:
    metric_regex: "node_.*|container_.*"    # 可选:只对 node/container 指标做聚合
  max_series: 1000000                   # 可选:LRU 容量,超过后按 LRU 淘汰
```

**运行机制**:
- 每 series 一个 bucket(interval 时间窗内的 float64 数组)
- 每次新 sample 进入对应 bucket;bucket 关闭(interval 结束)时 emit 多个聚合 sample(avg/max/p99...),然后重置 bucket
- LRU 默认容量:代码未显式设 max,用 `len(in)` 估算;可通过 `max_series` 覆盖
- `gateway_state_series{name="downsample"}` 实时上报 LRU 当前 series 数

##### ⑥ deadvalue(状态型 ⚠️ 跨 batch)

对连续 **N 个 interval 都不变**(或近似不变)的 series 标记为 dead,后续 samples 丢弃,直到值变化才重新放行。

```yaml
- type: deadvalue
  window: "5m"                          # 观察窗口,Go duration 格式(必须 > 0)
  scope:
    metric_regex: "cpu_.*|memory_.*"    # 可选:只对 CPU/内存类指标做死值过滤
  max_series: 1000000
```

**运行机制**:
- LRU 维护 per-series 的 dead 状态 + 最近 seen time
- sample 进入时:该 series 上次 seen 时间在 window 内且值相同 → 标记 dead 丢弃;值变化 → 放行 + 重置 timer
- `gateway_state_series{name="deadvalue"}` 实时上报 LRU 当前 series 数
- **注意**:deadvalue 只过滤,**不 emit 新 sample**(与 downsample 不同)

#### 6.2 编译期校验规则

| 规则 | 行为 |
|------|------|
| stage 顺序必须 6 个类型按固定位 | 编译器按 registry 序号(relabel=0,enrich=1,route=2,sample=3,downsample=4,deadvalue=5)强制对齐 |
| relabel/enrich 可重复(多次调用) | 其他 type(route/sample/downsample/deadvalue)编译期 detect 重复 → **reject + version 不切换** |
| downsample.interval 必须 > 0 | Go duration 格式:`30s` / `1m` / `5m` / `1h` |
| deadvalue.window 必须 > 0 | 同上 |
| 编译失败 | 整条 ruleset 拒绝切换,继续使用上一个生效版本 + 打 error log |

#### 6.3 关键行为澄清

| 文档原意 | 实际代码行为 |
|----------|-------------|
| 一个 ruleset 处理一个 business | ❌ **错误**。Manager 按 `ruleset.match.metric_prefix` fan-out,所有 ruleset **并行处理同一批 samples**,每个 ruleset 独立跑自己的 stage 链。business 只用于 Kafka header 注入和 metric label,不做路由过滤 |
| 单 ruleset 一个 topic | ❌ **错误**。route stage 可以按 label 匹配,**同一 ruleset 的同一 batch 内 sample 可以被分发到多个 topic**(逐 sample 单独 `out()`,不是整 batch 按 topic 聚合后 out) |
| 修改后 fsnotify 检测 | ✅ 正确,但 **state 型 stage(downsample/deadvalue)热重载时 LRU 状态丢失**(新 ruleset 用新的 state 实例),历史 series 需要重新累计 |

---

## 7 Kafka Producer 内部行为(已固化,不可配置)

prom-gw 使用 **franz-go** 异步批量 Kafka producer,关键参数固化在 `kafkasink/producer.go`,**不暴露成启动 flag**。部署/运维需了解其行为:

| 参数 | 固化值 | 含义 |
|------|--------|------|
| 压缩算法 | **zstd** | Kafka broker 需支持 zstd(KRaft 默认支持) |
| 幂等写 | **true** | `enable.idempotence=true`,broker 端按 producer_epoch 去重,防止重复消息 |
| acks | **all**(=all ISR ack) | 必须所有 ISR broker 都 ack 才算成功 |
| linger | **50ms** | ProducerBatch 最久等待时间,满 50ms 就 flush,不等满 batch |
| batch_max_bytes | **1MB** | 单 batch 最大字节数 |
| retries | **10** | 单条消息 broker 拒绝的最大重试次数 |
| delivery.timeout.ms | **120000**(2min) | 单条消息从入队到 broker ack 的总时间硬上限(含重试) |
| auto-create topic | **❌ 禁止** | 必须预先用 `kafka-topics.sh` 创建 routed topics(64 分区 3 副本) |
| flusher goroutine | **单** | 串行从 channel 读 envelope → 调 `kgo.Produce`,避免 franz-go 内部锁竞争 |
| clientID | **prom-gw** | Kafka broker 日志/指标中可识别来源 |
| Kafka key | **series hash**(`Sample.SeriesKey()`) | 同一 series 的 samples 落同 partition,保证顺序 |

#### 7.1 Backpressure 行为链

```
Receiver handler
  │
  ▼ (SinkFunc)
ruleengine.Pipeline.Process()
  │  按 TargetTopic 逐 sample out()
  ▼
sink.Pipeline.Submit()      ← 有界 channel 65535,满 → ErrBackpressure → HTTP 503 + Retry-After
  │
  ▼ (sink.Pipeline.workerLoop 单 goroutine)
AdapterSink.Send()
  │  state=Normal → sendToKafka
  │  state=Degraded → sendToWAL
  ▼
kafkasink.Produce()          ← franz-go channel 满 → ErrProduceBackpressure
  │                           channel 满累计 FailThreshold=3 次
  ▼                        → AdapterSink 自动切 Degraded
franz-go flusher goroutine
  │
  ▼ broker
Kafka ISR ack (同步等待 onAck 仅 drain 场景)
```

#### 7.2 AdapterSink 故障切换状态机

```
              生产 Kafka Send 累计失败 ≥ FailThreshold(3)
StateNormal ─────────────────────────────────────────────> StateDegraded
(投递 Kafka)                                                  (投递 WAL)
                                                                   │
                                                                   │ 每秒 probeKafka()
                                                                   │ 向 _gw_probe topic 发 1B ping
                                                                   │ 连续 RecoverSuccessThreshold(3) 次成功
                                                                   ▼
StateNormal + drainWAL() ←────────────────────────────────── 切回 StateNormal
(同步重放 WAL 段到 Kafka,30s DrainTimeout 硬上限)
```

**关键细节**:
- `probeKafka` 往 **`_gw_probe`** topic 发 1B 消息(ping),这个 topic 也需要预创建(生产环境可以复用 app_business 的 topic,或者单独创建一个小 topic)
- drainWAL 用 **同步投递**(`ProduceWithCallback` + wait ack),只有 broker 确认接收后才 mark 段 `.done`,保证 **at-least-once**
- drain **超时 30s 硬上限**(`DrainTimeout`),超时后 Close 不再阻塞,未 drain 的数据保留在 WAL 供下次启动重放
- **关闭顺序**(`AdapterSink.Close`):Stop monitor goroutine → drain WAL(同步) → Close WAL → Flush Kafka producer(等所有 in-flight ack,默认 30s) → Close Kafka client

---

## 8 Auth 接口与 Token 语义

prom-gw 的鉴权通过 **Authenticator 接口**抽象,当前 v1 实现是 **LocalTokenAuthenticator**(读本地 tokens.yaml)。接口预留了接入 IAM/OIDC 的扩展点。

```go
// 鉴权接口(v2 可新增 OIDCAuthenticator,IAMAuthenticator)
type Authenticator interface {
    Verify(ctx context.Context, token string) (Business, error)
}
```

#### 8.1 预定义的 sentinel 错误与 HTTP 状态码映射

| sentinel error | 触发条件 | HTTP 状态码 | errorBody.code | reason 标签 |
|----------------|----------|------------|----------------|-------------|
| `ErrTokenMissing` | Authorization header 缺失或格式不对 | 401 | 4001 | missing |
| `ErrTokenInvalid` | token 不存在于 tokens.yaml | 401 | 4001 | invalid |
| `ErrTokenExpired` | **(预留)** token 已过期,LocalToken 暂不感知 | 401 | 4001 | expired |
| `ErrTokenRevoked` | **(预留)** token 已吊销,LocalToken 暂同 invalid | 403 | 4001 | revoked |

**注意**:LocalTokenAuthenticator 当前只校验 token 是否存在于 yaml,不做过期/吊销检查。如果未来接入 IAM/OIDC,只需新增实现,**receiver 中间件和状态码映射完全不用改**。

#### 8.2 限流层次(两层)

```
请求到达
  │
  ├── rateLimitMW          全局 IP 限流(按 Prometheus/Promgw 段白名单豁免)
  │                         超限 → 429 + Retry-After
  │
  ├── authMW               Bearer Token 校验
  │
  └── businessRateLimitMW  token 对应的 business 级限流(来自 tokens.yaml 的 rate_limit)
                            超限 → 429 + Retry-After + gateway_rate_limit_rejected_total{business}
```

---

## 9 WAL 磁盘配置 ⚠️ **生产关键**

prom-gw 内置 **WAL(Write-Ahead Log)** 作为 Kafka 不可用时的兜底防线。Kafka 故障期间数据落盘 WAL,Kafka 恢复后自动重放回 Kafka。理解 WAL 的行为和磁盘需求是生产部署的核心。

#### 9.1 WAL 工作原理

```
正常模式(Kafka 可用):
  Receiver → Pipeline → KafkaSink → Kafka ✅
                        └─ 连续 FailThreshold=3 次失败 → 切 StateDegraded,后续直接走 WAL

降级模式(Kafka 故障):
  Receiver → Pipeline → WALSink → WAL 文件(sync 落盘)→ 返回 200 ✅
                        └─ WAL 容量满 → ErrWALFull → 返回 503 ❌

恢复流程(Kafka 恢复后):
  monitorLoop 每 RecoverCheck=1s 发送 probeKafka ping(1B → _gw_probe topic)
  → Produce 立即返回 nil 才 recordSuccess;累计 RecoverSuccessThreshold=3 次
  → StateDegraded → StateNormal,触发 drainCh 信号
  → drainWAL() 同步把所有 .sealed 段重放回 Kafka(sendToKafkaSync + broker ack)
  → 超时 DrainTimeout=30s 未 drain 完 → 未处理的段保留在 WAL 下次启动继续
  → drain 过程中 WAL 持续记录新数据(drain 不阻塞写入)
```

**写入语义(代码审计)**:
- **每条 record 同步 fsync**(`active.f.Sync()`),保证进程崩溃不丢已落盘数据
- 段轮转条件:`active.written + len(encoded_record) + 16B footer > SegmentBytes(64MB)`
- 活跃段的尾部会在封段时才追加 **16B footer**(4B magic=PWAL + 4B record_count + 8B CRC32 IEEE)
- 关闭 WAL 时 `SealActiveSegment` 会把当前活跃段封段 + 写 footer + fsync + rename `.sealed`

**双阈值容量控制(Third Defense)**:
1. WAL 总字节数 `w.bytes.Load() >= MaxBytes`(默认 50GB)
2. WAL 所在磁盘 `syscall.Statfs(dir)` 的 `(Blocks - Bavail) / Blocks >= DiskUsedRatio`(默认 80%)

> **⚠️ 磁盘使用率检测细节**:代码用 `Bavail`(非 root 用户可用块数)而非 `Bfree`(全部空闲块数),所以 **非 root 运行时磁盘阈值会更保守**(可能实际还有空间但已触发拒绝)。`syscall.Statfs` 失败时跳过此阈值,只靠 MaxBytes 限制。

**崩溃恢复(启动时)**:
1. `scanExisting()` 遍历 `--wal-dir` 所有文件,按后缀识别状态:
   - `.log` → 调 `sealRecoveredSegment`:逐条读 record,遇到截断/损坏 → `Truncate(lastValidOffset)`,然后计算 CRC32 + 写 16B footer + fsync + rename `.sealed`
   - 空段(无有效 record) → 直接删除(无法封段)
   - `.log.sealed` / `.log.sealed.done` / `.log.done` → 加入 segments 索引,累加 bytes
2. 打开新活跃段:`seg-{unixNano}-{seq}.log`(seq 基于已有段 map 最大值 +1)
3. 启动 cleanup goroutine(每 60s 扫一次)

**Replay 重试机制(代码审计)**:
- `Replay` 按 `CreatedAt` 升序排序所有未 `.done` 的 `.sealed` 段
- 逐段 `replaySegment`:最多重试 `MaxReplayFailures=10` 次
- 退避策略:**指数退避** 100ms → 200ms → 400ms → ... → **上限 5s**
- handler 返回 nil 才 rename `.done`(此时 cleanup 才能清理);返回 error → 段保留,下次 drain 再试

> **⚠️ 运维风险**:`.sealed` 但未 `.done` 的段 **永远不会被 cleanup 删除**。如果 Kafka 长时间不可用导致 drain 反复失败,`.sealed` 段会无限累积,最终可能触发 WAL 双阈值硬拒绝。此时需要人工介入修复 Kafka 或手动 `.done`/删除段(手动删除会丢失这段数据!)。

**自动清理范围**:只删除 `.sealed.done`(已 replay 成功且 Retention=24h 超时)。`.sealed` 段 **不在清理范围内**。

**probeKafka 异步语义**(代码注释):
- probeKafka 复用 `sendToKafka`(franz-go 异步 Produce),**无法同步确认 broker ack**
- 当前实现:Produce 立即返回 nil → recordSuccess()。这是"乐观"探测 —— 实际 broker 可能还没恢复(producer channel 有 buffer),但连续 3 次 Produce 返回 nil 说明 broker 至少**可达且 channel 不阻塞**
- v2 计划:改用 franz-go 的 `Ping` 做 active probing(需要额外 API)

#### 9.2 WAL 参数配置

**WAL 自身参数(通过 flag 或 ENV 配置)**:

| 参数 | flag | 默认值 | 说明 |
|------|------|--------|------|
| WAL 目录 | `--wal-dir` | `/data/wal` | WAL 数据存放目录,需独立挂载;**文件权限 644,目录权限 755** |
| WAL 总字节上限 | `--wal-max-bytes` | `50GB` | WAL 文件总大小达到此值 → 硬拒绝(HTTP 503);原子计数,每次 Write +encoded_len |
| WAL 磁盘使用率阈值 | `--wal-disk-used-ratio` | `0.80` | WAL 所在磁盘使用率达到此比例 → 硬拒绝;**用 syscall.Statfs + Bavail 计算**(见 5.7.1 双阈值) |

**WAL 内部常量(硬编码,需编译才能改)**:

| 常量 | 值 | 说明 |
|------|----|------|
| `DefaultSegmentBytes` | `64MB` | 段轮转阈值;**段总大小 = N×record + 16B footer**,非严格 64MB |
| `DefaultRetention` | `24h` | `.done` 段保留时长;cleanup goroutine 每 `DefaultCleanupInterval=60s` 扫一次 |
| `DefaultMaxReplayFailures` | `10` | 单段重放失败上限,超过后整 Replay 返回 error |
| `segmentFooterSize` | `16B` | 段尾追加一次:4B magic(PWAL) + 4B record_count + 8B CRC32 |

**AdapterSink 状态机参数(控制降级/恢复行为,内部常量)**:

| 常量 | 值 | 说明 |
|------|----|------|
| `FailThreshold` | `3` | Kafka 连续 3 次失败才切 StateDegraded,后续直接走 WAL |
| `RecoverCheck` | `1s` | Degraded 状态下每秒发 1B ping 到 `_gw_probe` topic(异步 Produce,无法 broker ack) |
| `RecoverSuccessThreshold` | `3` | 连续 3 次 Produce 立即返回 nil 才切 StateNormal + 触发 drain |
| `DrainTimeout` | `30s` | drainWAL 最大等待时间;超时后未 drain 的段保留 WAL,下次启动继续 |

> **所有内部常量**在 `internal/wal/wal.go` 和 `internal/sink/sink.go` 的 `const` 组中定义。如需调整(比如 Kafka 故障可能持续很久 → 提高 FailThreshold 避免频繁降级),必须重新编译。

#### 9.3 磁盘需求估算

**核心公式**:`磁盘需求 = max(WAL 总容量 / 磁盘使用率阈值, WAL 总容量 + 系统预留) × 1.3 safety margin`

> **⚠️ WAL 双阈值的交互**:实际拒绝先到哪个取决于配置。举例:50GB WAL + 80% 磁盘阈值 = 磁盘总需 62.5GB(50/0.80)。如果你给的磁盘小于 62.5GB,会先触发磁盘阈值拒绝;如果给的磁盘大于 62.5GB,会先触发 MaxBytes 拒绝。**实际拒绝阈值 = min(MaxBytes, 磁盘总空间 × DiskUsedRatio)**。

**典型场景估算**:

| 场景 | 单实例吞吐 | WAL MaxBytes | 磁盘预留(OS) | 磁盘使用率阈值 | 先触发阈值 | 理论最小值 | **推荐磁盘** |
|------|-----------|--------------|--------------|----------------|------------|------------|--------------|
| 测试 | 10K samples/s | 5GB | 2GB | 80% | MaxBytes(7GB) | 5GB + 5GB/0.8×0.3 ≈ 7.1 | **10GB SSD** |
| 生产常态 | 100K samples/s | 50GB | 5GB | 80% | MaxBytes(55GB) | 55GB × 1.3 ≈ 71.5 | **100GB SSD** |
| 高吞吐峰值 | 500K samples/s | 100GB | 10GB | 80% | MaxBytes(110GB) | 110GB × 1.3 ≈ 143 | **200GB SSD** |
| Kafka 跨城容灾 | 500K samples/s | 200GB | 10GB | 70% | Disk(200/0.7=285) | max(295,285)×1.3 ≈ 384 | **500GB SSD** |

**按 Kafka 故障持续时间估算**(snappy 压缩后 payload):

```
单 samples 平均 payload ≈ 1-2KB(snappy 压缩后)
→ 100K samples/s × 1.5KB = 150MB/s 写入 WAL(每条都 fsync)

Kafka 故障 1 分钟   → 150MB/s × 60s  = 9GB
Kafka 故障 10 分钟  → 150MB/s × 600s = 90GB
Kafka 故障 30 分钟  → 150MB/s × 1800s = 270GB
Kafka 故障 1 小时   → 150MB/s × 3600s = 540GB
```

> **注意**:以上估算没算 record 头开销(~27B + topic + headers ≈ 50-100B)和段 footer(16B/段)。如果 topic + headers 平均 100B,实际磁盘写入是 `150MB/s × 1.6 ≈ 240MB/s`。**保守估算建议 ×2 系数**。

**生产建议**:
- WAL 数据目录 **必须挂载独立 SSD**,`noatime` + `nodiratime` 挂载(减少 fsync 干扰 + attr 写)
- WAL 总容量设为 **Kafka 故障 10-15 分钟可承接的数据量**(50GB 默认值适用于 100K samples/s 场景,100K×1.5KB×600s=90GB > 50GB 默认 → 默认其实只能承接 ~5.5 分钟)
- 磁盘使用率阈值设 **80%**(默认),留 20% 余量给系统和 WAL 封段开销
- 如果 Kafka 集群可能长时间不可用,应相应调大 `--wal-max-bytes`

**systemd mount 建议**:

```ini
# /etc/systemd/system/data-wal.mount
[Unit]
Description=WAL data mount
Requires=local-fs.target
After=local-fs.target

[Mount]
What=/dev/disk/by-uuid/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
Where=/data/wal
Type=ext4
Options=defaults,noatime,nodiratime,data=ordered
TimeoutSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# XFS 推荐(SSD):
# What=/dev/disk/by-uuid/...
# Type=xfs
# Options=defaults,nobarrier,noatime,inode64

sudo systemctl daemon-reload
sudo systemctl enable data-wal.mount
sudo systemctl start data-wal.mount
```

#### 9.4 WAL 文件布局

**段命名规则**:`seg-{unixNano}-{seq}.log`
- `unixNano` = 段创建时间戳(纳秒精度)
- `seq` = 基于已有段 map 最大值 +1(0 起始,6 位补零)
- 段后缀状态:`.sealed`(已封可重放)→ `.done`(已确认,等待清理)

```
/data/wal/                                   ← 独立 SSD 挂载点
├── seg-1727424000000000000-000001.log      ← 活跃段(正在写,无 .sealed/.done 后缀)
│                                             总大小 = N×record + 16B footer
├── seg-1727424000000000000-000002.log.sealed  ← 已封段(Replay 未完成,永不清理!)
├── seg-1727424000000000000-000003.log.sealed  ← drainWAL 正在处理中
├── seg-1727424000000000000-000004.log.sealed.done  ← 已重放成功,等待 Retention=24h 清理
├── seg-1727424000000000000-000005.log.done     ← 罕见:recover 时直接 .done(中间态)
└── 权限:文件 644,目录 755
```

**段状态流转(精确)**

```
          openNewActive()                    sealActiveSegment()
          新段起一个 active                   段满 / Close
.log ─────────────────────> .log(write中) ──────────────────────────────> .log.sealed
  │                                                                       │
  │                                                                       │ scanExisting()
  │                                                                       │ (启动恢复)
  │                                                                       ▼
  │  进程崩溃重启                                                        sealRecoveredSegment()
  │  scanExisting 发现遗留 .log                                          截断损坏 record +
  │  → sealRecoveredSegment()                                           计算 CRC + footer +
  │  → 空段直接删除,有 record 的 →                                       fsync + rename → .sealed
  │    Truncate(lastValidOffset) + CRC + footer + fsync → .sealed        │
  │                                                                       │ drainWAL Replay
  │                                                                       │ handler(broker ack) nil
  │                                                                       ▼
  │                                                                .log.sealed.done
  │                                                                       │
  │                                                                       │ cleanup goroutine
  │                                                                       │ 条件: info.Done=true
  │                                                                       │       AND CreatedAt < now-24h
  │                                                                       ▼
  │                                                                    DELETE ✓
  │
  └──── (空段,无有效 record) → os.Remove(segment) → 彻底丢失!
         说明:崩溃可能发生在 record header 中间,sealRecoveredSegment
         遍历 record 时发现 total_len 无效 → break 跳出 → recordCount=0 →
         返回 error → 调用方删除文件
```

> **⚠️ .sealed 段永不清理**:cleanup 只遍历 `info.Done == true` 的段。`info.Sealed == true && info.Done == false` 的段 **不在 cleanup 范围内**,永远保留在磁盘上。如果 Kafka 长时间不可用导致 drain 反复失败,`.sealed` 段会无限累积 → 最终触发 WAL 双阈值硬拒绝。
>
> **⚠️ 手动删除 .sealed 段 = 数据丢失**:下次启动 `scanExisting` 只扫描存在的文件。如果一定要手动清理,正确做法是先确认 broker 可用,让 drainWAL 正常重放成功后自动变成 `.done` 再清理。

**Record 二进制格式(大端序,每条 record 完整结构)**

```
┌──────────────────────────────────────────────────────────────────────────┐
│  record                                                                 │
│  ┌───────────┬────────────┬───────┬──────────┬──────────┬───────────┬────────────┐ │
│  │ total_len │   ts(nano) │ flags │ topic    │  key     │  payload  │  headers    │ │
│  │ 4B BE     │ 8B BE      │ 1B    │2B+topic  │4B+key    │4B+payload │4B+headers  │ │
│  │ uint32    │ int64      │ = 0   │ UTF-8     │ raw []    │ snappy+pb │ 见下       │ │
│  └───────────┴────────────┴───────┴──────────┴──────────┴───────────┴────────────┘ │
│                                                                        │
│  segment footer(16B,**段尾追加一次,封段时写**,非每条 record)            │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ PWAL (4B magic) │ record_count (4B BE) │ CRC32 IEEE (8B BE)    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────┘
```

| 字段 | 大小 | 类型 | 说明 |
|------|------|------|------|
| `total_len` | 4B BE | uint32 | 后续 body 总长度(不含自身),= 23 + len(topic) + len(key) + len(payload) + len(headersB) |
| `ts` | 8B BE | int64 | Unix nanoseconds(`time.Now().UnixNano()`) |
| `flags` | 1B | uint8 | **当前固定为 0**,预留位(未来可能用于压缩/版本/加密等) |
| `topic` | 2B+N | uint16 + string | Kafka topic 名,UTF-8 编码 |
| `key` | 4B+N | uint32 + []byte | Kafka message key(prom-gw 未使用,写入空字节) |
| `payload` | 4B+N | uint32 + []byte | Kafka message value = **snappy 压缩 + protobuf remote_write 序列化** |
| `headers` | 4B+N | uint32 + encoded | 6 个 Kafka Headers,按 name **字典序**编码(保证 CRC 稳定) |
| **segment footer** | **16B** | 4B + 4B + 8B | magic=`PWAL`,record_count=段内总 record 数,CRC32 IEEE=整段 CRC |

**Headers 编码格式**:`[2B name_len][name][4B value_len][value]...` 按 name 字典序。包含 6 个 Kafka Headers:
`business`, `source_dc`, `ingest_city`, `ingest_dc`(本机固定), `ingest_time_ms`, `traceparent`(W3C)

#### 9.5 WAL 运维命令

```bash
# ====== WAL 实时状态(优先) ======
# 从 metrics 拿 —— 最可靠,不依赖文件系统解析
curl -s http://127.0.0.1:8080/metrics | grep -E "^gateway_wal_"
# 输出示例:
#   gateway_wal_bytes{ingest_city="bj"} 137438953472      ← WAL 当前总字节(w.bytes atomic)
#   gateway_wal_oldest_age_seconds{ingest_city="bj"} 216000  ← 最老未确认段存活时长(秒)
#   gateway_wal_hard_reject_total 0                       ← 硬拒绝次数(>0 说明曾触发容量阈值)

# ====== 磁盘使用率(第二阈值) ======
df -h /data/wal                         # 看总容量和使用率
df -B1 /data/wal | tail -1 | awk '{print $3/$2}'  # 精确比例(与 DiskUsedRatio 对比)
sudo python3 -c "import os,statvfs; s=os.statvfs('/data/wal'); print(f'Block used: {(s.f_blocks-s.f_bavail)/s.f_blocks:.2%}')"  # 与 syscall.Statfs 一致

# ====== 段文件统计 ======
# 各状态段数量
ls /data/wal/*.log 2>/dev/null | wc -l           # active(正在写,无后缀)
ls /data/wal/*.log.sealed 2>/dev/null | wc -l    # sealed 未 done(待 Replay)
ls /data/wal/*.log.sealed.done 2>/dev/null | wc -l  # sealed + done(已重放,待 24h 清理)
ls /data/wal/*.log.done 2>/dev/null | wc -l       # 罕见:recover 中间态

# 最老 sealed 段(= Kafka 故障持续时长)
ls -lht --time-style=long-iso /data/wal/*.log.sealed 2>/dev/null | tail -1

# 总占用
sudo du -sh /data/wal/

# ====== Admin API 查询 WAL 段 ======
# Admin API 需要 --admin-allow-cidr 放行本机
curl -s http://127.0.0.1:8081/api/v1/admin/wal/segments | jq .
# 输出示例:
# {
#   "segments": [
#     {"path":"/data/wal/seg-...-000002.log.sealed","size":67108816,"record_count":33500,"created_at":"2025-08-31T10:00:00Z","sealed":true,"done":false},
#     ...
#   ],
#   "total_bytes": 137438953472,
#   "oldest_unacked_age_seconds": 216000
# }

# ====== 强制清理已确认段(谨慎!) ======
# 只删 Retention > 24h 的 .done 段(安全,cleanup goroutine 本来也会做)
sudo find /data/wal/ -name "*.done" -mtime +1 -delete

# ====== 紧急 drain 触发 ======
# 目前没有 Admin API 手动触发 drain —— 触发条件只有:
#   1. AdapterSink.Close() 时自动 drain
#   2. probeKafka 连续 3 次 Produce 立即返回 nil → recordSuccess → drainCh
# 所以要手动触发的话:等 Kafka 恢复后 AdapterSink 自动切回 Normal 并触发 drain
# 或者重启实例,启动时 Replay 会自动把 WAL 里 .sealed 段重放到 Kafka

# ====== 进程关闭行为 ======
# systemctl stop prom-gw@bj → AdapterSink.Close() 按以下顺序:
#   1. close(doneCh) → monitor goroutine 退出(wg.Wait)
#   2. drainWAL() → Replay → sendToKafkaSync(broker ack) 带 DrainTimeout=30s
#   3. walSink.Close() → sealActiveSegment → fsync footer
#   4. kafka.Close() → franz-go Close()
# 验证关闭是否完整: journalctl -u prom-gw@bj | grep -E "draining|drained|wal initialized"
```

#### 9.6 WAL 故障排查

| 症状 | 可能原因 | 排查 | 处理方式 |
|------|----------|------|----------|
| **`gateway_wal_hard_reject_total > 0`** | WAL 双阈值被触发(最严重级) | `curl -s http://127.0.0.1:8080/metrics \| grep wal_hard_reject`;`df -h /data/wal`;`sudo du -sh /data/wal` | 扩容磁盘 → 先看哪个阈值触发的(df vs wal_bytes)→ 扩容后调大 `--wal-max-bytes` 或降低 `--wal-disk-used-ratio`(但降低后磁盘快满时才触发 503 更危险) |
| **`gateway_wal_bytes` 持续增长不减少** | Kafka 故障,AdapterSink 在 Degraded 模式持续写 WAL | `journalctl -u prom-gw@bj \| grep -E "switched to WAL|KAFKA_BROKERS"`;检查 Kafka broker 健康 | 修复 Kafka 或等待恢复;恢复后 AdapterSink 自动切 Normal + drain |
| **`gateway_wal_oldest_age_seconds` 持续增长** | drainWAL 反复失败(Kafka broker ack 超时/拒绝) | `journalctl -u prom-gw@bj \| grep "drain" \| grep -i error`;`journalctl -u prom-gw@bj \| grep "handler panic"` | 检查 Kafka broker 网络 + topic ACL;必要时检查 WAL 段 CRC(启动时 `sealRecoveredSegment` 会做) |
| **HTTP 503 持续** | WAL 双阈值被触发 + Kafka 也不可用(双重故障) | `curl -s http://127.0.0.1:8080/metrics \| grep -E "wal_hard_reject\|backpressure"`;`df -h /data/wal`;检查 Kafka | **先恢复 Kafka**,让 drainWAL 开始工作;同时临时扩容磁盘;如果 503 持续 30 分钟以上,数据已开始丢 |
| **启动时卡在 `wal initialized` 之后** | KAFKA_BROKERS 不可用 → 启动后持续 WAL-only 模式 | `journalctl -u prom-gw@bj \| grep "kafka connect failed"`;检查 env 文件 broker 地址 | 修正 env 文件中 broker 地址 → `systemctl daemon-reload && systemctl restart prom-gw@bj` |
| **日志 `"wal: replay ... failed after 10 attempts"`** | drainWAL 的 Replay handler 失败,重试 10 次耗尽 | `journalctl -u prom-gw@bj \| grep -i "replay.*failed"`;检查 broker ack 日志 | 检查 Kafka broker 状态 + topic ACL + 磁盘 CRC 完整性;临时手动触发 drain:重启实例(启动时 Replay 会跑) |
| **日志 `"wal: segment corrupt"` 或 CRC mismatch** | WAL 段 CRC 校验失败(磁盘损坏 / 手动篡改文件) | `journalctl -u prom-gw@bj \| grep -i "corrupt\|truncated\|crc"` | 手动检查坏段 `ls -lh /data/wal/`;**手动删除坏段 = 数据丢失**,但如果 CRC mismatch 这个段里的数据肯定没正确落到 Kafka,删了等于少丢一点 |
| **日志 `"wal: no valid records in ..."` 被封段删除** | 崩溃发生在 record 中间,整条 record 损坏 → sealRecoveredSegment 遍历后 recordCount=0 → 删文件 | `journalctl -u prom-gw@bj \| grep -i "no valid records"` | 这是设计行为,空段会被删除;丢失的 record 只能从上游 Prometheus 重推 |
| **`.sealed` 段无限累积** | Kafka 长时间不可用,drain 反复失败,cleanup 不清理 sealed 段 | `ls /data/wal/*.sealed \| wc -l`;`curl -s http://127.0.0.1:8080/metrics \| grep wal_bytes` | **这是最危险的情况**:如果 Kafka 长期不可用,`.sealed` 会一直写一直封,最终触发 WAL 双阈值硬拒绝 → HTTP 503 → 上游 Prometheus remote_write 重试后超时 → **数据开始丢**。必须尽快恢复 Kafka 或调大 `--wal-max-bytes` |

#### 9.7 WAL 参数调优建议

**WAL 自身(通过 flag)**:

| 场景 | `--wal-max-bytes` | `--wal-disk-used-ratio` | 说明 |
|------|-------------------|-------------------------|------|
| 测试(降级验证) | `5GB` | `0.80` | 快速填满触发 503,便于测试双阈值行为 |
| 生产常态(100K samples/s) | **`100GB`** | `0.80` | 承接 Kafka 故障 ~12 分钟(之前 50GB 默认只能承接 ~5.5 分钟,太保守) |
| 高吞吐峰值(500K samples/s) | **`200GB`** | `0.75` | 承接 Kafka 故障 ~10 分钟,磁盘阈值更紧(避免磁盘完全写满导致 OS panic) |
| Kafka 跨城容灾(可能 > 30min) | **`500GB`** | `0.70` | 极大 WAL + 更紧磁盘阈值;磁盘给 1TB 独立 SSD |

> **⚠️ 50GB 默认值的实际承接能力**:50GB / (100K × 1.5KB/s) ≈ 5.5 分钟。对于生产环境(可能 100K+ samples/s),**建议至少 100GB**。
>
> **`--wal-disk-used-ratio` 的权衡**:值越大 → 越晚触发磁盘阈值硬拒绝 → 但留给 OS 的空间越少,极端情况下可能磁盘完全写满导致进程 panic / kernel OOM。值越小 → 越早拒绝 → 但保护 OS 正常运行。**生产推荐 0.70-0.80**。

**AdapterSink 状态机(需编译才能改)**:

| 参数 | 当前值 | 建议调优 | 说明 |
|------|--------|----------|------|
| `FailThreshold` | 3 | 生产保持 3;测试可改 1 | Kafka 故障后连续 N 次 send 失败才降级;设太小会频繁降级,设太大 Kafka 不可用时才反应太慢 |
| `RecoverCheck` | 1s | 生产保持 1s;测试可改 500ms | Degraded 状态下探测间隔;越小越快切回 Normal,但 Kafka 仍在故障时探测本身是浪费 |
| `RecoverSuccessThreshold` | 3 | 生产保持 3;Kafka 不稳定可改 5 | 连续 N 次 Produce 立即返回 nil 才切 Normal;越大越稳健但越慢 |
| `DrainTimeout` | 30s | 可改 60-120s | 关闭时 drain 等待时间;太小可能 drain 不完;太大 `systemctl stop` 会慢。取决于 WAL 积压量和 drain 速率 |
| `MaxReplayFailures` | 10 | 保持 10 | Replay 单段重试次数;指数退避下 10 次最大等待 ~10s(累加所有退避) |
| `DefaultSegmentBytes` | 64MB | 可改 128MB | 段越大,封段次数越少,footer 开销越少;但崩溃恢复时遍历段更慢 |

---

## 10 systemd template 部署

prom-gw 使用 **template unit**(`prom-gw@.service`,`%i` 为城市标识):

#### 10.1 步骤 1:WAL 磁盘准备 ⚠️ 独立 SSD 强制要求

**必须**先挂载独立 SSD 到 `/data/wal`(WAL 写入走同步 fsync,与业务 IO 隔离):

```bash
# 假设新 SSD 设备为 /dev/nvme1n1
sudo mkfs.xfs -f /dev/nvme1n1

# 挂载(noatime 减少 fsync 干扰)
sudo mkdir -p /data/wal
sudo mount -o defaults,noatime /dev/nvme1n1 /data/wal

# 持久化挂载
echo 'UUID=$(sudo blkid -s UUID -o value /dev/nvme1n1) /data/wal xfs defaults,noatime 0 2' | sudo tee -a /etc/fstab

# 验证
df -h /data/wal
# 输出应看到独立设备,大小 ≥ 100GB(生产)
```

#### 10.2 步骤 2:创建目录

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/prom-gw/bin /appdata/prom-gw/conf /var/log/prom-gw
sudo chown -R bdops:bdops /appdata/prom-gw /data/wal /var/log/prom-gw
```

#### 10.3 步骤 3:放置二进制和配置文件

```bash
# 二进制
sudo cp bin/prom-gw /appdata/prom-gw/bin/prom-gw
sudo chmod 755 /appdata/prom-gw/bin/prom-gw

# ruleset 配置(按城市选择源文件)
sudo cp configs/rules/bj/default.yaml /appdata/prom-gw/conf/rules.yaml
sudo chmod 644 /appdata/prom-gw/conf/rules.yaml

# token 配置(按城市选择源文件)
sudo cp configs/tokens/bj.yaml /appdata/prom-gw/conf/tokens.yaml
sudo chmod 600 /appdata/prom-gw/conf/tokens.yaml   # ⚠️ 必须 600,含 token 密钥

# env 环境变量(按城市命名)
sudo mkdir -p /etc/prom-gw
sudo cp deploy/systemd/prom-gw.bj.env /etc/prom-gw/prom-gw.bj.env
sudo chmod 600 /etc/prom-gw/prom-gw.bj.env        # ⚠️ 含 Kafka broker/OTel 地址
```

#### 10.4 步骤 4:安装 systemd unit

拷贝仓库 `deploy/systemd/prom-gw@.service` 到 systemd 目录:

```bash
sudo cp deploy/systemd/prom-gw@.service /etc/systemd/system/
sudo systemctl daemon-reload
```

#### 10.5 步骤 5:启动服务

```bash
# 北京机房
sudo systemctl enable --now prom-gw@bj

# 深圳机房(部署到 sz 机器时)
sudo systemctl enable --now prom-gw@sz

# 合肥机房
sudo systemctl enable --now prom-gw@hf
```

#### 10.6 步骤 6:验证(含 WAL 磁盘)

```bash
# 查看服务状态
sudo systemctl status prom-gw@bj

# 看日志(验证 Kafka 连接 + WAL 初始化)
sudo journalctl -u prom-gw@bj -f | grep -E "kafkasink connected|wal initialized|tokens loaded|receiver listening"

# 健康检查
curl -s http://127.0.0.1:8081/healthz
curl -s http://127.0.0.1:8081/readyz

# Admin API 查询 ruleset
curl -s http://127.0.0.1:8082/admin/rulesets

# WAL 磁盘健康检查 ⚠️ 部署后必须确认
df -h /data/wal           # 确认是独立 SSD 挂载
df -h /data/wal | awk 'NR==2 {print $5}'  # 使用率 < 80%
ls -lh /data/wal/         # 检查段文件是否正常

# WAL 实时指标(确认 drain 正常)
curl -s http://127.0.0.1:8080/metrics | grep wal
# 预期输出:
#   gateway_wal_bytes{ingest_city="bj"} 0        ← 正常模式应为 0(Kafka 可用无 WAL 数据)
#   gateway_wal_oldest_age_seconds{ingest_city="bj"} 0  ← 无积压
```

**启动成功标志**:日志依次出现
```
starting prom-gw version=...
tokens loaded count=2
kafkasink connected brokers=[bj-kafka-1:9092 bj-kafka-2:9092 bj-kafka-3:9092]
wal initialized dir=/data/wal max_bytes=53687091200
receiver listening addr=:19201
```

---

## 11 启动参数速查(flag / env / 默认值)

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--config` | `PROM_GW_CONFIG` | `configs/rules/default.yaml` | ruleset 配置文件 |
| `--tokens` | `PROM_GW_TOKENS` | `configs/tokens/local.yaml` | token 配置文件 |
| `--write-addr` | - | `:19201` | RemoteWrite 接入地址 |
| `--metrics-addr` | - | `:8080` | Prometheus 指标 + pprof |
| `--health-addr` | - | `:8081` | healthz / readyz |
| `--admin-addr` | - | `:8082` | Admin API |
| `--admin-allow-cidr` | - | `127.0.0.1/32,10.0.0.0/8` | Admin API IP 白名单 |
| `--source-dc` | - | `dc-unknown` | **本实例所属机房标识**,写入 Kafka header `source_dc`。**生产必须显式指定**,否则 Flink 下游 `source_dc` 字段为 `dc-unknown` |
| `--ingest-city` | `INGEST_CITY` | `dc-unknown` | 城市标识(bj/sz/hf),由 systemd template 注入 |
| `--wal-dir` | - | `/data/wal` | WAL 数据目录 |
| `--wal-max-bytes` | - | `50GB` | WAL 总字节上限 |
| `--wal-disk-used-ratio` | - | `0.80` | WAL 磁盘使用率硬阈值 |
| `--nacos-addr` | - | (空) | Nacos 地址,逗号分隔 |

**systemd 已硬编码、无需手动传的参数**:
- `--config` / `--tokens` / `--ingest-city` 通过 `Environment=` 注入
- `--source-dc` **必须**在 ExecStart 中显式指定(如 `--source-dc=dc-bj-dongba`)

---

## 12 配置变更操作速查

| 变更类型 | 操作 | 影响范围 | 热重载 |
|----------|------|----------|--------|
| 修改 Kafka broker 地址 | 编辑 `/etc/prom-gw/prom-gw.<city>.env` → `systemctl restart` | 进程级 | ❌ |
| 修改 Go runtime 参数 | 编辑 env 文件 → `systemctl restart` | 进程级 | ❌ |
| 修改 OTel endpoint | 编辑 env 文件 → `systemctl restart` | Tracing | ❌ |
| 修改 Nacos 地址/账号 | 编辑 env 文件 → `systemctl restart` | 配置中心 | ❌ |
| 新增/修改 token | 编辑 `tokens.yaml` → `kill -HUP $(pgrep prom-gw)` | 鉴权/限流 | ✅ |
| 修改 ruleset(relabel/route) | 编辑 `rules.yaml` → 等待 5s | 数据处理 | ✅ fsnotify |
| 回滚 ruleset 到历史版本 | Admin API `POST /v1/rulesets/{name}:rollback?to_version=N` | 数据处理 | ✅ |
| 手动触发 ruleset 重载 | Admin API `POST /v1/rulesets/{name}:reload` | 数据处理 | ✅ |
| 修改 `source-dc` | 改 systemd unit ExecStart → `daemon-reload` + `restart` | Kafka header | ❌ |

---

## 13 Receiver HTTP 接口规范

prom-gw 的 Prometheus 接入端口是 **`:19201`**,路由为 **`POST /api/v1/write`**。

#### 13.1 请求规范

```
POST /api/v1/write
Content-Type: application/x-protobuf
Authorization: Bearer <token>         # 必须,PROM_GW_TOKENS 中配置的 token
Content-Encoding: snappy              # Prometheus remote_write 压缩格式

# Body: snappy(protobuf(RemoteWrite))
#   RemoteWrite.timeseries[] = 原始 Prometheus 时序数据
#   RemoteWrite.metadata[]   = metric 类型定义
```

#### 13.2 中间件链

请求按此顺序经过 6 层中间件,任何一层失败直接返回错误:

```
requestID → realIP → recoverer → rateLimit → businessRateLimit → auth → handler
```

| 中间件 | 检查内容 | 失败时状态码 |
|--------|----------|--------------|
| requestID | 注入 TraceID(从 `X-Request-ID` 或生成) | - |
| realIP | 从 `X-Forwarded-For` 取真实 IP | - |
| recoverer | panic 捕获(防止进程崩) | 500 + `{"code":1500}` |
| rateLimit | 全局 IP 限流(systemd `adminCIDR` 内不受限) | 429 + `Retry-After` |
| businessRateLimit | token 对应的 business 级限流 | 429 + `Retry-After` |
| auth | Bearer Token 校验(过期/吊销/缺失) | 401 / 403 |

#### 13.3 状态码与错误码对照表

| HTTP | errorBody.code | 场景 | 排查方向 |
|------|----------------|------|----------|
| **200** | - | ✅ 接收成功(已写入 Kafka 或 WAL) | - |
| **400** | `4003` | 方法不对(非 POST) | Prometheus `remote_write` 配置 |
| **401** | `4001` + `reason=missing\|expired\|invalid` | Token 缺失/过期/无效 | 检查 `Authorization` header 和 `tokens.yaml` |
| **403** | `4001` + `reason=revoked` | Token 已吊销 | 重新颁发 token + `kill -HUP` |
| **413** | `4101` | body 过大(> 64MB) | 缩小 Prometheus 端 `remote_write.max_samples_per_send` |
| **422** | `4201` | protobuf 解析失败 | 检查 snappy 压缩格式和 protobuf 版本 |
| **429** | `4291` | 限流(全局或 business 级) | 看 metrics `gateway_rate_limit_rejected_total`;调大 `tokens.yaml` 中的 `rate_limit` |
| **500** | `1500` | 内部错误(panic recover) | journald 搜 panic stacktrace |
| **503** | `5301` | WAL 容量满/磁盘阈值触发 | 看 metrics `gateway_wal_bytes` 和 `df -h /data/wal` |

**示例错误响应**:

```json
{"code":4001,"message":"auth failed: token expired","trace_id":"abc123"}
```

#### 13.4 Prometheus remote_write 对接示例

Prometheus `prometheus.yml` 中:

```yaml
remote_write:
  - url: http://prom-gw-vip:19201/api/v1/write
    headers:
      Authorization: Bearer tk_app_business_prod    # 对应 tokens.yaml 中的 token
      source_dc: dc-bj-dongba                        # 机房标识,写入 Kafka header
    send_exemplars: true
    max_samples_per_send: 5000                      # 单请求 size 控制
    batch_send_deadline: 30s                        # 批量发送等待时间
```

---

## 14 Admin API 完整端点

Admin API 监听 **`:8082`**,受 **CIDR 白名单**保护(`--admin-allow-cidr=127.0.0.1/32,10.0.0.0/8`)。非白名单 IP 直接返回 **403**。

#### 14.1 端点一览

| Method | 路径 | 功能 | 热操作 |
|--------|------|------|--------|
| GET | `/v1/healthz` | Admin API 自身健康 | - |
| **GET** | **`/v1/rulesets`** | 列出全部 ruleset 及版本 | - |
| **GET** | **`/v1/rulesets/{name}`** | 查看单个 ruleset 详情 | - |
| **PUT** | **`/v1/rulesets/{name}`** | 以 YAML body 更新 ruleset | ✅ 原子切换 |
| **POST** | **`/v1/rulesets/{name}:reload`** | 手动触发 ruleset 重载 | ✅ 强制 reload |
| **POST** | **`/v1/rulesets/{name}:rollback?to_version=N`** | 回滚到指定历史版本 | ✅ 原子切换 |
| **GET** | **`/v1/rulesets/{name}/history`** | 查看 ruleset 历史版本列表 | - |
| **GET** | **`/v1/businesses`** | 列出当前所有已配置 business | - |
| **GET** | **`/v1/stats`** | 实时运行状态(吞吐/depth/队列) | - |

#### 14.2 响应格式

所有 Admin API 返回标准 JSON:

```json
// 成功
{"code":0,"message":"ok","data":{...},"trace_id":"abc123"}

// 错误
{"code":4001,"message":"ruleset not found","trace_id":"abc123"}
```

#### 14.3 常用运维示例

```bash
# 查看所有 ruleset + 当前版本
curl -s http://127.0.0.1:8082/v1/rulesets

# 查看 app-business ruleset 详情(含 stages/version)
curl -s http://127.0.0.1:8082/v1/rulesets/app-business

# 查看 ruleset 变更历史(用于回滚)
curl -s http://127.0.0.1:8082/v1/rulesets/app-business/history
# 返回:[{"version":1,"ts":"...","md5":"..."},{"version":2,"ts":"...","md5":"..."}]

# 回滚到 version 1(零停机!)
curl -X POST 'http://127.0.0.1:8082/v1/rulesets/app-business:rollback?to_version=1'

# 手动强制 reload ruleset
curl -X POST http://127.0.0.1:8082/v1/rulesets/app-business:reload

# 实时运行状态(吞吐/队列深度/WAL 状态)
curl -s http://127.0.0.1:8082/v1/stats

# 列出所有 business
curl -s http://127.0.0.1:8082/v1/businesses
```

#### 14.4 PUT 更新 ruleset(高级用法)

```bash
# 直接通过 Admin API 更新 ruleset,无需改文件 + 重启
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
  -H "Content-Type: application/yaml" \
  --data-binary @new-rules.yaml

# 成功返回:{"code":0,"message":"ok","data":{"version":3},...}
```

---

## 15 升级与回滚 SOP

#### 15.1 版本升级(二进制)

prom-gw 的版本升级是**滚动二进制替换** + systemd 重启:

```bash
# Step 1: 下载新版本
VERSION=v1.2.4
curl -L -o /tmp/prom-gw.tar.gz \
  "https://github.com/lynnyq/prom-gw/releases/download/${VERSION}/prom-gw-${VERSION}-linux-amd64.tar.gz"
sudo tar -xzf /tmp/prom-gw.tar.gz -C /tmp/

# Step 2: 校验 + 备份旧版本
sudo cp /appdata/prom-gw/bin/prom-gw /appdata/prom-gw/bin/prom-gw.bak.$(date +%Y%m%d-%H%M%S)

# Step 3: 替换二进制 + 启动新进程
# systemd 配置 Restart=always,会自动拉起
sudo install -m 755 /tmp/prom-gw /appdata/prom-gw/bin/prom-gw
sudo systemctl restart prom-gw@bj

# Step 4: 验证(等 10s,观察日志无 panic)
sudo journalctl -u prom-gw@bj --since "20s ago" --no-pager | tail -30
sudo systemctl status prom-gw@bj

# Step 5: metrics 确认版本已切换
curl -s http://127.0.0.1:8080/metrics | grep gateway_ruleset_version
```

#### 15.2 版本回滚(紧急)

```bash
# 秒级回滚:直接用备份二进制替换 + 重启
sudo cp /appdata/prom-gw/bin/prom-gw.bak.20260831-100000 /appdata/prom-gw/bin/prom-gw
sudo systemctl restart prom-gw@bj
```

#### 15.3 配置回滚(无需重启)

```bash
# Ruleset 版本回滚 → Admin API,零停机
curl -s http://127.0.0.1:8082/v1/rulesets/app-business/history
# 选择 to_version=N 执行 rollback
curl -X POST "http://127.0.0.1:8082/v1/rulesets/app-business:rollback?to_version=1"

# Token 配置回滚 → 文件回退 + SIGHUP
sudo cp /appdata/prom-gw/conf/tokens.yaml.bak /appdata/prom-gw/conf/tokens.yaml
kill -HUP $(pidof prom-gw)
```

#### 15.4 配置文件备份策略

```bash
# 建议每日 cron 自动备份
# crontab -e:
0 2 * * * bdops tar czf /backup/prom-gw/prom-gw-conf-$(date +\%Y\%m\%d).tar.gz \
  -C /appdata/prom-gw conf/ tokens/ 2>/dev/null
0 3 * * * bdops find /backup/prom-gw -name "*.tar.gz" -mtime +7 -delete
```

---

## 16 监控指标全集

prom-gw 所有指标暴露在 **`:8080/metrics`**(由 Prometheus 自身 `promauto` 注册到 `prometheus.DefaultRegisterer`)。指标命名空间 `gateway_*`。

> **⚠️ 标签规范(code spec 7.1)**:所有指标**必带 `ingest_city` 和 `source_dc` 两个标签**,用于北京 Grafana 跨城聚合/切片。这两个值由 `cmd/prom-gw` 启动时通过 `obs.SetMetaLabels(ingestCity, sourceDC)` 注入,所有 `obs.*WithLabelValues` 间接通过 `obs.MetaLabels()` 获取。

#### 16.1 核心业务流水线

| 指标名 | 类型 | Labels | 含义 | 关键值 |
|--------|------|--------|------|--------|
| `gateway_samples_total` | Counter | **stage**, **business**, **status**, ingest_city, source_dc | 按阶段计 samples 处理量 | stage ∈ {receive, decode, parse, rule, pipeline, kafka, wal}; status ∈ {ok, error, drop, in, out} |
| `gateway_bytes_in_total` | Counter | business, ingest_city, source_dc | 入口接收字节数(HTTP body 原始大小,snappy 压缩体) | 与 `samples_total{stage="receive"}` 正相关 |
| `gateway_bytes_out_total` | Counter | **topic**, ingest_city | 输出到 Kafka 的消息体字节数(snappy+protobuf,带 6 个 Headers) | topic 实际落入的 topic 名,**与 bytes_in 之差可估算 protobuf 膨胀率** |
| `gateway_ruleset_processed_total` | Counter | **ruleset**, **stage**, ingest_city, source_dc | 各 ruleset + 各 stage 处理的 sample 数 | stage ∈ {relabel, enrich, route, sample, downsample, deadvalue}; 每个 ruleset 单独计数 |
| `gateway_ruleset_routed_total` | Counter | **ruleset**, ingest_city, source_dc | fan-out 路由到各 ruleset 的 sample 数(非 default) | **核心路由验证**:加和 = `samples_total{stage="parse"}` |
| `gateway_ruleset_errors_total` | Counter | **ruleset**, ingest_city, source_dc | 各 ruleset 内部处理错误计数(router 层聚合) | 持续增长 → 对应 ruleset 有 bug |

#### 16.2 质量与拒绝链路

| 指标名 | 类型 | Labels | 含义 | 触发条件 |
|--------|------|--------|------|----------|
| `gateway_errors_total` | Counter | **stage**, **type**, ingest_city, source_dc | 各类错误总量 | stage ∈ {decode, parse, auth, rule, kafka, wal, sink, pipeline, router, config}; type 细分见下方 |
| `gateway_auth_fail_total` | Counter | **reason**, ingest_city, source_dc | 鉴权失败次数 | reason ∈ {missing, invalid, expired, revoked, iam_unavailable}; expired/revoked 暂预留 |
| `gateway_rate_limit_rejected_total` | Counter | business, ingest_city, source_dc | business 级限流拒绝(HTTP 429) | 两层限流链中的 business 级触发时 +1 |
| `gateway_backpressure_rejected_total` | Counter | **stage**, ingest_city, source_dc | 背压拒绝(HTTP 503)次数 | stage ∈ {global_rl, business_rl, pipeline, kafka, wal, kafkasink} |
| `gateway_produce_errors_total` | Counter | **reason**, ingest_city, source_dc | Kafka produce 独立错误计数 | reason ∈ {kafka_produce, kafka_timeout, kafka_backpressure, flusher_panic} |
| `gateway_wal_hard_reject_total` | Counter | *(无)* | WAL 硬拒绝(HTTP 503)次数 | WAL 磁盘使用率超过 `--wal-disk-used-ratio` 阈值时触发 |
| `gateway_admin_auth_fail_total` | Counter | **reason**, ingest_city, source_dc | Admin API CIDR 白名单外访问次数 | reason ∈ {ip_not_allowed, other} |

**`gateway_errors_total` 的 type 细分值(从代码审计)**:

| stage | 可能的 type 值 | 来源 |
|-------|----------------|------|
| decode | content_type, content_encoding, body_read, snappy, protobuf, decode_xxx | receiver/server.go |
| parse | meta_missing, parse_series | receiver/server.go |
| rule | stage_type(relabel/enrich/route/sample), enrich_template_missing, relabel_drop_keep_conflict, downsample_series_full, send, panic_xxx | ruleengine/stage.go, pipeline.go |
| kafka | produce, flusher_panic | kafkasink/producer.go |
| sink | kafka_failover, drain_incomplete | sink/sink.go |
| pipeline | worker_panic, send | sink/pipeline.go |
| router | no_entries, no_match_drop, process_error | router/router.go |
| config | watcher_load, watcher_compile, watcher_apply, watcher_fsnotify | ruleengine/watcher.go, config/source.go |

#### 16.3 延迟与资源

| 指标名 | 类型 | Labels | 含义 | 直方图桶 |
|--------|------|--------|------|----------|
| `gateway_request_duration_seconds` | Histogram | **endpoint**, **status**, ingest_city | 端到端 `/api/v1/write` 请求处理延迟 | 0.5ms → 8s(指数 14 桶) |
| `gateway_stage_duration_seconds` | Histogram | **stage**, **op**, ingest_city | 各 pipeline 阶段耗时 | 0.1ms → 1.6s(指数 14 桶); stage ∈ {decode, parse, rule_xxx}; op ∈ {ok, error} |
| `gateway_goroutines` | Gauge | *(无)* | 当前进程 goroutine 数(泄漏告警用) | `runtime.NumGoroutine()` |
| `gateway_mem_bytes` | Gauge | *(无)* | 进程驻留内存(HeapAlloc + HeapInuse) | runtime 采样 |
| `gateway_cpu_ratio` | Gauge | *(无)* | 进程 CPU busy ratio(0–1,最近 1s 窗口) | 由 runtime cpu collector 提供 |
| `gateway_panic_recovered_total` | Gauge | *(无)* | 累计 panic 恢复次数(safego 跨 goroutine 聚合) | 所有 stage worker / kafka flusher / config watcher / admin handler 的 panic 都 +1 |

> **注意**:prom-gw **没有**注册 `process_resident_memory_bytes` / `process_cpu_seconds_total` / `go_goroutines`(Go runtime 默认 collector),使用的是**自定义指标** `gateway_mem_bytes` / `gateway_cpu_ratio` / `gateway_goroutines`。

#### 16.4 WAL 与 Ruleset 状态

| 指标名 | 类型 | Labels | 含义 | 正常值 |
|--------|------|--------|------|--------|
| `gateway_wal_bytes` | Gauge | ingest_city | WAL 当前磁盘占用字节 | Kafka 正常时应为 **0**;非零说明 AdapterSink 在 Degraded 模式 |
| `gateway_wal_oldest_age_seconds` | Gauge | ingest_city | 最老未确认 WAL segment 存活秒数 | 持续增长 → drainWAL 卡死,需人工介入 |
| `gateway_state_series` | Gauge | **ruleset**, ingest_city | downsample / deadvalue 状态型 stage 当前跟踪的 series 数 | 反映 LRU 状态桶大小;过高提示下游内存压力 |
| `gateway_ruleset_version` | Gauge | **ruleset**, ingest_city | 当前生效 ruleset 版本号 | ruleset 版本号(从 1 开始递增);**Nacos/File 热重载后应立即变化** |
| `gateway_ruleset_history_size` | Gauge | **ruleset**, ingest_city | Config History ring buffer 中各 ruleset 保存的历史版本数 | 用于 `Admin GET /api/v1/admin/ruleset/:name/history` 查询 |
| `gateway_ruleset_switch_total` | Counter | **ruleset**, **from_version**, **to_version**, ingest_city, source_dc | ruleset 原子切换次数 | `from_version="v0"` 表示首次加载 |
| `gateway_config_reload_total` | Counter | **source**, **status**, ingest_city, source_dc | 配置热重载次数 | source ∈ {file, nacos, api}; status ∈ {ok, error} |

#### 16.5 关键 PromQL 查询速查

```promql
# 1. 三城各 business 吞吐(Grafana 面板核心图)
sum by (ingest_city, business) (rate(gateway_samples_total{stage="rule",status="ok"}[5m]))

# 2. 路由验证:各 ruleset 实际接到多少 samples
sum by (ruleset) (rate(gateway_ruleset_routed_total[5m]))

# 3. 各 topic 真实入 Kafka 流量(Mbps,需乘 8)
sum by (ingest_city, topic) (rate(gateway_bytes_out_total[5m])) * 8

# 4. WAL 退化检测 —— 如果 gateway_wal_bytes > 0 持续 3 分钟
gateway_wal_bytes / 53687091200 > 0.05
  and
  delta(gateway_wal_oldest_age_seconds[5m]) > 30

# 5. 背压链诊断 —— 看拒绝发生在哪一层
sum by (stage) (increase(gateway_backpressure_rejected_total[10m]))

# 6. ruleset 某 stage 是否频繁报错
increase(gateway_ruleset_errors_total{ruleset="order_service"}[5m]) > 10

# 7. WAL 硬拒绝(最严重级)
increase(gateway_wal_hard_reject_total[1m]) > 0

# 8. panic 恢复频率(持续增长 = 有 bug)
rate(gateway_panic_recovered_total[10m]) > 0

# 9. 内存泄漏观察(持续上升 30 分钟)
gateway_mem_bytes - gateway_mem_bytes offset 30m > 100000000

# 10. ruleset 热重载是否卡住(版本长时间不变)
time() - gateway_ruleset_version{ruleset="default"} * 0 > 3600
```

#### 16.6 Prometheus 抓取配置 + 推荐告警

```yaml
# ========== prometheus.yml ==========
scrape_configs:
  - job_name: prom-gw
    scrape_interval: 15s
    scrape_timeout: 10s
    metrics_path: /metrics
    static_configs:
      - targets:
          - 'prom-gw-bj-1:8080'
          - 'prom-gw-bj-2:8080'
          - 'prom-gw-sz-1:8080'
          - 'prom-gw-hf-1:8080'
        labels:
          component: prom-gw

# ========== alertmanager.yml 推荐告警规则 ==========
groups:
  - name: prom-gw-critical
    rules:
      # C1: WAL 硬拒绝发生(最严重 — 数据开始丢了)
      - alert: PromGWWALHardReject
        expr: increase(gateway_wal_hard_reject_total[1m]) > 0
        for: 1m
        labels: { severity: critical, team: sre }
        annotations:
          summary: "prom-gw {{ $labels.ingest_city }} WAL 硬拒绝"
          description: "gateway_wal_hard_reject_total 增长,磁盘超过 --wal-disk-used-ratio 阈值,开始丢数据"

      # C2: AdapterSink 降级 WAL 持续 10 分钟(Kafka 有严重问题)
      - alert: PromGWWALDegraded
        expr: gateway_wal_bytes > 1073741824 and delta(gateway_wal_oldest_age_seconds[5m]) > 60
        for: 10m
        labels: { severity: critical, team: kafka }
        annotations:
          summary: "prom-gw {{ $labels.ingest_city }} WAL Degraded 持续 10 分钟"
          description: "Kafka 不可用,已降级 WAL。WAL 已 {{ $value | humanize }}"

      # C3: Backpressure 503 持续发生
      - alert: PromGWBackpressureChain
        expr: increase(gateway_backpressure_rejected_total[5m]) > 1000
        for: 3m
        labels: { severity: critical }
        annotations:
          summary: "prom-gw {{ $labels.ingest_city }} 背压拒绝激增"

  - name: prom-gw-warning
    rules:
      # W1: Auth fail 激增(可能有攻击)
      - alert: PromGWAuthFailSpike
        expr: increase(gateway_auth_fail_total[5m]) > 500
        for: 5m
        labels: { severity: warning, team: security }

      # W2: Rate limit 429 激增
      - alert: PromGWRateLimitSpike
        expr: increase(gateway_rate_limit_rejected_total[10m]) > 2000
        for: 5m
        labels: { severity: warning }

      # W3: Goroutine 泄漏
      - alert: PromGWGoroutineLeak
        expr: gateway_goroutines > 5000
        for: 10m
        labels: { severity: warning }

      # W4: Panic 恢复频繁
      - alert: PromGWPanicRecovered
        expr: rate(gateway_panic_recovered_total[10m]) > 0
        for: 10m
        labels: { severity: warning, team: platform }

      # W5: ruleset errors 持续发生
      - alert: PromGWRulesetErrors
        expr: increase(gateway_ruleset_errors_total[10m]) > 50
        for: 5m
        labels: { severity: warning }

      # W6: P99 latency > 500ms
      - alert: PromGWP99Latency
        expr: histogram_quantile(0.99, rate(gateway_request_duration_seconds_bucket[5m])) > 0.5
        for: 5m
        labels: { severity: warning }

      # W7: Config reload error
      - alert: PromGWConfigReloadError
        expr: increase(gateway_config_reload_total{status="error"}[5m]) > 0
        for: 3m
        labels: { severity: warning, team: platform }
```

---

## 17 日志说明

prom-gw 使用 **`go.uber.org/zap` + JSON 编码**(Production Config 基础,仅自定义三个 key: `ts`/`msg`/`level`),通过 systemd `StandardOutput=journal` 写入 journald。**没有 OTel zap bridge**(避免高吞吐下 span→log 关联的额外 CPU 开销),trace_id 通过 `zap.String("trace_id", ...)` 显式注入到错误日志中。

#### 17.1 JSON 日志结构

**基础字段(所有日志行必带)**:

| 字段 | zap encoder key | 类型 | 含义 | 示例 |
|------|-----------------|------|------|------|
| `level` | LevelKey(自定义) | string | 日志级别 | `"info"` / `"warn"` / `"error"` |
| `ts` | TimeKey(自定义) | ISO8601 string | 日志产生时间(zapcore.ISO8601TimeEncoder) | `"2025-08-31T10:30:45.123+08:00"` |
| `msg` | MessageKey(自定义) | string | 日志消息内容 | `"kafkasink connected"` |

**context 字段(代码中显式注入,按场景出现)**:

| 字段 | zap 方法 | 出现场景 | 类型 | 示例 |
|------|----------|----------|------|------|
| `trace_id` | `zap.String` | receiver/admin 出错时从 ctx 提取 | string | `"4bf92f3577b34da6a3ce929d0e0e4736"` |
| `request_id` | *(未使用)* | — | — | prom-gw **不记录 request_id 到日志**(request_id 只在 HTTP Header 中传递,不写入日志) |
| `remote_ip` | `zap.String` | auth fail / handler panic | string | `"10.1.2.3"` |
| `ingest_city` | `zap.String` | 所有 admin 日志 / 部分启动日志 | string | `"bj"` |
| `source_dc` | `zap.String` | 所有 admin 日志 / 部分启动日志 | string | `"dc-bj-dc01"` |
| `business` | `zap.String` | auth fail / pipeline debug | string | `"app-business"` |
| `ruleset` | `zap.String` | ruleset 热切换 / ruleset error | string | `"order_service"` |
| `stage` | `zap.String` | stage error / admin handler panic | string | `"relabel"` / `"admin"` |
| `topic` | `zap.String` | kafka produce error / sink backpressure | string | `"prom.bj.routed.core"` |
| `type` | `zap.String` | 错误 type 细分 | string | `"snappy"` / `"downsample_series_full"` |
| `reason` | `zap.String` | auth fail reason / produce error reason | string | `"missing"` / `"kafka_produce"` |
| `source` | `zap.String` | config source name / config reload source | string | `"nacos"` / `"filesource"` |
| `path` | `zap.String` | fsnotify path / http request path | string | `"/appdata/prom-gw/conf/rules.yaml"` / `"/api/v1/write"` |
| `err` | `zap.Error` | 所有 error/warn 级别日志 | string(error text) | `"context deadline exceeded"` |
| `panic` | `zap.Any` | panic recover | any | `"*errors.errorString"` |
| `stack` | `zap.ByteString` | panic recover | string(goroutine 栈) | `"goroutine 123 [running]:..."` |
| `version` | `zap.Int64` | ruleset version | number | `3` |
| `from_version` / `to_version` | `zap.Int64` | ruleset 原子切换 | number | `2` → `3` |
| `fail_count` / `success_count` | `zap.Int32` | AdapterSink 状态机 | number | `3` / `3` |
| `payload_size` / `bytes` | `zap.Int` | sink send / config push | number | `1048576` |
| `brokers` | `zap.Strings` | kafka init / connect fail | string array | `["bj-kafka-1:9092","bj-kafka-2:9092","bj-kafka-3:9092"]` |
| `linger` / `close_timeout` / `block_timeout` | `zap.Duration` | producer init / close | duration string | `"50ms"` |
| `drain_timeout` | `zap.Duration` | drainWAL 开始 | duration string | `"30s"` |
| `n_samples` / `n_stages` | `zap.Int` | pipeline debug | number | `5000` |
| `stage_index` | `zap.Int` | stage apply error | number | `2` |

#### 17.2 日志级别语义

prom-gw 的日志级别**非常克制**(高频路径如 handler 成功、rule pipeline 正常处理**不产生任何日志**),只在以下事件打日志:

| Level | 触发场景 | 频率 | 典型 msg |
|-------|----------|------|----------|
| **Info** | 启动阶段 / Config Init 成功 / Kafka 连接成功 / Ruleset 热切换成功 | 低频 | `"starting prom-gw"`, `"tokens loaded"`, `"kafkasink connected"`, `"wal initialized"`, `"router: entries updated"`, `"watcher: ruleset reloaded"`, `"rule engine: rules swapped"`, `"sink adapter: draining WAL to Kafka"` |
| **Warn** | 非致命退化(有 fallback) | 中低频 | `"tracing degraded to noop"`, `"kafka connect failed, will run in WAL-only mode"`, `"watcher: reload LoadFile failed, keep old ruleset"`, `"stage apply error, continue"`, `"sink backpressure, will retry on next message"`, `"decode failed"`, `"admin: source ip not allowed"`, `"config: source push with error, keep current"` |
| **Error** | 致命故障 / handler 失败 / panic recover | 极低频 | `"parse failed (internal)"`, `"handler failed"`, `"sink send failed"`, `"sink pipeline worker panic"`, `"handler panic"`, `"drain incomplete"`, `"admin: handler panic"`, `"admin: service error"` |
| **Debug** | pipeline 调试(当前代码里有但**高吞吐下不会启用**,默认 Production 配置不打 Debug) | — | `"rule pipeline: processing"`, `"rule pipeline: dispatching"` |

#### 17.3 实际 JSON 日志样例

```json
// 启动成功(Info 级别)
{"level":"info","ts":"2025-08-31T10:00:00.123+08:00","msg":"starting prom-gw","version":"v1.2.3","config":"/appdata/prom-gw/conf/rules.yaml","tokens":"/appdata/prom-gw/conf/tokens.yaml","source_dc":"dc-bj-dc01","ingest_city":"bj","wal_dir":"/data/wal"}

// Kafka 连接成功(Info)
{"level":"info","ts":"2025-08-31T10:00:00.456+08:00","msg":"kafkasink connected","brokers":["bj-kafka-1:9092","bj-kafka-2:9092","bj-kafka-3:9092"]}

// Kafka 故障 → WAL-only 退化(Warn)
{"level":"warn","ts":"2025-08-31T10:01:00.789+08:00","msg":"kafka connect failed, will run in WAL-only mode","brokers":["bj-kafka-1:9092","bj-kafka-2:9092","bj-kafka-3:9092"],"err":"dial tcp: connection refused"}

// AdapterSink 降级 WAL(Info)
{"level":"info","ts":"2025-08-31T10:01:05.001+08:00","msg":"sink adapter: draining WAL to Kafka","timeout":"30s"}

// sink send 失败 → 记 WAL(Error)
{"level":"error","ts":"2025-08-31T10:01:05.123+08:00","msg":"sink send failed","topic":"prom.bj.routed.core","payload_size":1048576,"err":"kafka produce: leader not available"}

// ruleset 热切换成功(Info)
{"level":"info","ts":"2025-08-31T10:02:00.456+08:00","msg":"rule engine: rules swapped","from_name":"default","from_version":1,"to_name":"default","to_version":2,"stage_count":3}

// ruleset 热切换失败(Warn)
{"level":"warn","ts":"2025-08-31T10:02:00.789+08:00","msg":"watcher: reload CompileConfig failed, keep old ruleset","path":"/appdata/prom-gw/conf/rules.yaml","ruleset":"default","err":"stage[2]: unknown type 'downsample'"}

// downsample LRU series 满 → drop oldest(Warn)
{"level":"warn","ts":"2025-08-31T10:03:00.123+08:00","msg":"stage apply error, continue","stage":"downsample","stage_index":2,"err":"downsample: series full, dropping oldest"}

// handler panic recover(Error,带 trace_id)
{"level":"error","ts":"2025-08-31T10:04:00.456+08:00","msg":"handler panic","panic":"*runtime.errorString","path":"/api/v1/write","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","ingest_city":"bj","source_dc":"dc-bj-dc01","stack":"goroutine 42 [running]:\nfmt.Panicf(...)\n..."}

// Admin API CIDR 拒绝(Warn)
{"level":"warn","ts":"2025-08-31T10:05:00.789+08:00","msg":"admin: source ip not allowed","ip":"8.8.8.8","path":"/api/v1/admin/rulesets","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","ingest_city":"bj","source_dc":"dc-bj-dc01","stage":"admin"}
```

#### 17.4 运维常用日志查询

```bash
# ====== 启动成功验证 ======
journalctl -u prom-gw@bj -f --since "10 min ago" | grep -E "starting prom-gw|tokens loaded|kafkasink connected|wal initialized|receiver listening"

# ====== Kafka 故障 → 自动降级 WAL ======
journalctl -u prom-gw@bj | grep -i "WAL-only\|kafka connect failed\|switched to wal"

# ====== AdapterSink 状态机关键事件 ======
journalctl -u prom-gw@bj | grep -E "adapter.*draining|failover|recovered|drain incomplete"

# ====== Ruleset 热重载(成功/失败分开看) ======
journalctl -u prom-gw@bj | grep "rules swapped"           # 成功
journalctl -u prom-gw@bj | grep -E "CompileConfig|LoadFile|watcher.*failed"  # 失败

# ====== 背压链定位 ======
journalctl -u prom-gw@bj | grep -E "backpressure|WAL-only|send failed"

# ====== Panic 恢复(最关键的异常) ======
journalctl -u prom-gw@bj -p err | grep "panic"

# ====== Admin API CIDR 拒绝(安全告警) ======
journalctl -u prom-gw@bj | grep "admin: source ip not allowed"

# ====== WAL drain 完整流程(Kafka 恢复后) ======
journalctl -u prom-gw@bj | grep -E "draining|drain|sealed"

# ====== Auth fail(可能有攻击) ======
journalctl -u prom-gw@bj | grep -i "auth fail" | jq -r '.reason, .remote_ip, .business'

# ====== 结构化字段过滤(journalctl -o json → jq) ======
# 按 trace_id 关联整条链路日志
journalctl -u prom-gw@bj -o json | jq 'select(.trace_id == "4bf92f3577b34da6a3ce929d0e0e4736")'

# 只看 error 级别,按 ruleset 聚合
journalctl -u prom-gw@bj -p err -o json | jq -r '.ruleset, .stage, .msg, .err'

# ====== pprof/profile 风险 ======
journalctl -u prom-gw@bj | grep "debug/pprof"
```

#### 17.5 journald 持久化配置

prom-gw 通过 systemd `StandardOutput=journal` 写 journald。Kylin V10 / CentOS 默认可能走内存,建议持久化:

```bash
# 1. 创建持久化目录
sudo mkdir -p /var/log/journal
sudo systemd-tmpfiles --create --prefix /var/log/journal

# 2. 编辑 /etc/systemd/journald.conf:
#    SystemMaxUse=500M        # journal 总上限 500M
#    SystemKeepFree=1G       # 磁盘至少留 1G 空间
#    SystemMaxFiles=10       # 最多 10 个轮转文件
#    MaxRetentionSec=7day    # 保留 7 天
#    RateLimitIntervalSec=30s
#    RateLimitBurst=10000    # 允许短时间内 10000 条(避免 prom-gw 高吞吐日志被限流)

sudo systemctl restart systemd-journald

# 3. 验证
journalctl --disk-usage    # 看 journald 当前占用
```

---

## 18 网络与防火墙规则

prom-gw 涉及 4 个端口,在生产防火墙中需按来源/去向精确控制:

| 端口 | 协议 | 来源 | 去向 | 说明 |
|------|------|------|------|------|
| `19201` (write) | TCP | **Prometheus 节点 / LVS VIP** | prom-gw | 唯一对外数据接收口,生产只允许 Prometheus 集群段 |
| `8080` (metrics+pprof) | TCP | **Prometheus 抓取节点** | prom-gw | 仅 Prometheus 服务器可访问,**pprof 暴露,禁止全网** |
| `8081` (health) | TCP | **LVS / K8s LB** | prom-gw | 健康检查,允许 LB 段 |
| `8082` (admin) | TCP | **运维网段**(CIDR) | prom-gw | Admin API 已内置 CIDR 白名单,防火墙额外限制更安全 |
| `9092` (Kafka) | TCP | prom-gw | **Kafka broker** | prom-gw → Kafka,出站流量 |

#### 18.1 防火墙规则示例(ip_tables / firewalld)

```bash
# firewalld 添加可信网段(示例 10.10.0.0/16 为 Prometheus 段,10.20.0.0/16 为运维段)
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" \
  source address="10.10.0.0/16" port port="19201" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" \
  source address="10.10.0.0/16" port port="8080" protocol="tcp" accept'
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" \
  source address="10.20.0.0/16" port port="8082" protocol="tcp" accept'
# prom-gw → Kafka 出站:firewalld 默认允许出站,如需限制:
sudo firewall-cmd --permanent --add-rich-rule='rule family="ipv4" \
  destination address="10.30.0.0/16" port port="9092" protocol="tcp" accept'
sudo firewall-cmd --reload
```

---

## 19 Nacos 配置中心(可选)

prom-gw 支持从 Nacos 拉取 ruleset 配置,并维护本地 last-good snapshot。当前 systemd unit **未启用**,如需配置中心管理 ruleset 可开启。

#### 19.1 Nacos flag 一览

| flag | 默认值 | 说明 |
|------|--------|------|
| `--nacos-addr` | 空(不启用) | Nacos 服务器列表,逗号分隔 `ip:port` |
| `--nacos-namespace` | 空 | Nacos namespace |
| `--nacos-username` | 空 | Nacos 用户名(**⚠️ 敏感,建议 env 注入**) |
| `--nacos-password` | 空 | Nacos 密码(**⚠️ 敏感,建议 env 注入**) |
| `--nacos-data-id` | `prom-gw-rules` | Nacos 中 ruleset 的 dataId |
| `--nacos-group` | `GATEWAY` | Nacos group |
| `--nacos-snapshot-path` | `/data/nacos_snapshot.json` | 本地快照持久化路径(空 = 不持久化) |

#### 19.2 启用 Nacos 的 systemd 示例

```ini
# 加到 prom-gw@.service 的 [Service] section:
EnvironmentFile=-/etc/prom-gw/prom-gw.%i.env
# env 文件中追加:
#   NACOS_USERNAME=nacos_reader
#   NACOS_PASSWORD=<secret>

ExecStart=/appdata/prom-gw/bin/prom-gw \
  ...(原有参数)... \
  --nacos-addr=nacos-cluster:8848 \
  --nacos-namespace=prod \
  --nacos-data-id=prom-gw-rules \
  --nacos-group=GATEWAY \
  --nacos-snapshot-path=/data/nacos_snapshot.json
```

#### 19.3 Nacos 中的 ruleset 内容

```yaml
# Nacos dataId: prom-gw-rules,group: GATEWAY
rulesets:
  - name: app-business
    business: app-business
    default_topic: prom.bj.routed.app_business
    version: 3
    stages:
      - type: relabel
        drop_labels: [env, instance, pod]
      - type: route
        rules:
          - { team: core,  topic: prom.bj.routed.core }
          - { team: infra, topic: prom.bj.routed.infra }
          - { team: data,  topic: prom.bj.routed.data }
```

#### 19.4 Config Manager 多源优先级(三级降级)

prom-gw 内部有 **Config Manager** 编排三个 Source,**优先级从高到低**:

```
NacosSource (--nacos-addr 非空才启用)
  │ 长轮询 watch + 主动 Get
  │ 失败 → 该 source 标记 Err,降级到下一级
  ▼
FileSource (--config 路径,fsnotify 监听)
  │ 文件不存在 / 语法错 → 该 source Err
  │ FileSource **永远启用**(即使配了 Nacos 也会 load 一次本地文件做兜底)
  ▼
DefaultSource (内置空 ruleset,全量透传)
  │ 只有 Nacos + File 都失败才启用
  │ 行为:所有 samples 按 ruleset 的 default_topic(来自 token)透传到 Kafka
```

**关键行为**:
- 任何 source 产出新的有效快照(`Snapshot.Valid()` 为 true) → 自动触发 ruleset 编译 + 原子切换
- Nacos 拉取成功 → 覆盖 FileSource 的快照(因为 Nacos 优先级高)
- Nacos 挂了但 FileSource 还能读到有效 yaml → **继续用 FileSource 版本**(不会回退到 Default)
- 全部 source 都 Err → 启动 fatal + 不启动 receiver(避免数据不一致)

#### 19.5 Nacos 启用后的文件权限注意

启用 Nacos 后,`--nacos-snapshot-path` 指向的本地快照文件也需要加到 systemd `ReadWritePaths`:

```ini
# prom-gw@.service [Service] section:
ReadWritePaths=/data/wal /var/log/prom-gw /data/nacos_snapshot.json
```

---

## 20 Kafka Topic 预创建清单

prom-gw **禁止 auto-create topic**,所有 routed topics 必须预先用 `kafka-topics.sh` 创建(64 分区 3 副本硬约束)。

#### 20.1 三城 Topic 列表

| Topic | 分区 | 副本 | 用途 | 写入来源 |
|-------|------|------|------|----------|
| `prom.bj.routed.app_business` | 64 | 3 | 北京 app-business 兜底 topic | route 未命中的 samples |
| `prom.bj.routed.core` | 64 | 3 | 北京核心业务(订单/支付/账户) | route: `team=core` |
| `prom.bj.routed.infra` | 64 | 3 | 北京基础设施 | route: `team=infra` |
| `prom.bj.routed.data` | 64 | 3 | 北京数据平台 | route: `team=data` |
| `prom.sz.routed.*`(同上 4 个) | 64 | 3 | 深圳机房 | Prometheus remote_write → prom-gw@sz → sz Kafka |
| `prom.hf.routed.*`(同上 4 个) | 64 | 3 | 合肥机房 | 同上 |
| **`_gw_probe`** | 1 | 3 | AdapterSink 健康探测 | probeKafka 往此 topic 发 1B ping 消息 |

#### 20.2 创建命令(KRaft 模式)

```bash
# 北京机房 broker 清单
BROKERS="bj-kafka-1:9092,bj-kafka-2:9092,bj-kafka-3:9092"

# 创建 routed topics
for topic in prom.bj.routed.app_business prom.bj.routed.core \
             prom.bj.routed.infra prom.bj.routed.data; do
  kafka-topics.sh --bootstrap-server $BROKERS \
    --create --if-not-exists \
    --topic $topic \
    --partitions 64 --replication-factor 3
done

# 创建 probe topic(单分区足够,因为只发 1B ping)
kafka-topics.sh --bootstrap-server $BROKERS \
  --create --if-not-exists \
  --topic _gw_probe --partitions 1 --replication-factor 3

# 验证
kafka-topics.sh --bootstrap-server $BROKERS --list | grep prom.bj
```

**⚠️ 生产硬约束**:所有 topic 必须是 **64 分区 3 副本**。broker 默认配置通常是 1 分区 1 副本,auto-create 会导致数据倾斜和单点故障。

---

## 21 安全加固说明

#### 21.1 systemd 加固项(已在 unit 中启用)

prom-gw@.service 已启用以下 systemd 安全机制,部署前确认:

| 指令 | 作用 |
|------|------|
| `NoNewPrivileges=true` | 禁止进程通过 setuid/getcap 提升权限 |
| `ProtectSystem=strict` | 根目录只读,只有 `/data/wal` 和 `/var/log/prom-gw` 可写 |
| `ProtectHome=true` | /home 目录不可访问 |
| `PrivateTmp=true` | 独立 /tmp 命名空间,进程间 tmp 不可见 |
| `PrivateDevices=true` | 无直接设备访问 `/dev/*` |
| `ProtectKernel*` | 禁止修改 kernel tunables/modules/cgroups |
| `RestrictNamespaces=true` | 禁止创建新 user namespace(防容器逃逸) |
| `RestrictSUIDSGID=true` | 禁止创建 setuid 文件 |
| `MemoryMax=8G` | cgroup 级内存硬上限,防止物理机 OOM |
| `TasksMax=8192` | cgroup 级进程数上限,防止 fork bomb |
| `ReadWritePaths=/data/wal /var/log/prom-gw` | 严格限定可写目录 |

#### 21.2 文件权限要求(部署时必须验证)

| 文件 | 权限 | Owner | 原因 |
|------|------|-------|------|
| `/etc/prom-gw/prom-gw.<city>.env` | `600` | root:root | 含 Kafka broker / OTel / Nacos 密码 |
| `/appdata/prom-gw/conf/tokens.yaml` | `600` | bdops:bdops | 含 Bearer Token(鉴权凭据) |
| `/appdata/prom-gw/conf/rules.yaml` | `644` | bdops:bdops | ruleset 定义,无敏感信息 |
| `/data/wal/` | `700` | bdops:bdops | WAL 目录,含原始 Prometheus 数据(可能含 PII label) |
| `/data/wal/seg-*.log` | `600` | bdops:bdops | WAL 段文件,禁止其他用户读 |

```bash
# 快速验证权限
find /etc/prom-gw -name "*.env" -exec stat -c '%a %U:%G %n' {} \;
stat -c '%a %U:%G %n' /appdata/prom-gw/conf/tokens.yaml
ls -la /data/wal/ | head -5
```

#### 21.3 暴露面风险提示

| 端口 | 风险 | 建议 |
|------|------|------|
| `:8080/debug/pprof/*` | heap/profile 可触发,泄露内存布局和 goroutine 栈 | 防火墙限制到 Prometheus 段 + 运维段 |
| `:8080/metrics` | 暴露 business 列表和失败率 | 同 pprof,不要全网暴露 |
| `:8082` Admin API | CIDR 白名单已内置,防火墙再包一层 | 运维网段 only |
| `:19201` remote_write | 无 TLS,Token 明文传输 | **内网安全域部署**,跨安全组时考虑反向代理 TLS 终止 |

---

## 附录 A:端口与防火墙

| 端口 | 协议 | 用途 | 来源 | 默认暴露 |
|------|------|------|------|----------|
| `19201` | TCP | Prometheus RemoteWrite **写入入口** | Prometheus / LVS | ✅ 对内 |
| `8080` | TCP | `/metrics` + `/debug/pprof/*` | Prometheus 抓取 / 运维 | ✅ 对内 |
| `8081` | TCP | `/healthz` + `/readyz`(LB 健康检查) | LB / K8s | ✅ 对内 |
| `8082` | TCP | Admin API(已内置 CIDR 白名单) | **运维网段 only** | ❌ 防火墙限制 |
| `9092` | TCP | Kafka broker 出站 | prom-gw → Kafka | ✅ 出站 |

## 附录 B:信号处理

| 信号 | 行为 | 热效果 |
|------|------|--------|
| `SIGINT` / `SIGTERM` | 优雅停机(30s 超时:先停 receiver → drain pipeline → flush WAL → close Kafka) | 停机 |
| `SIGHUP` | 热重载 token + business rate limit 配置 | ✅ 零停机 |

## 附录 C:退出码

| 退出码 | 含义 | 常见原因 |
|--------|------|----------|
| `0` | 正常退出 | `--version` 执行完 / 优雅 shutdown |
| `1` | fatal 错误 | 配置加载失败 / WAL init 失败 / port already in use |
| `2` | SIGHUP 触发(预留) | - |

## 附录 D:Prometheus 对接配置(完整)

Prometheus 端 remote_write 完整配置,对接三城 prom-gw:

```yaml
# prometheus.yml
remote_write:
  # 北京机房 prom-gw 集群
  - url: http://prom-gw-bj-vip:19201/api/v1/write
    headers:
      Authorization: Bearer tk_app_business_prod
      source_dc: dc-bj-dongba
    send_exemplars: true
    max_samples_per_send: 5000
    batch_send_deadline: 30s
    relabel_configs:
      - source_labels: [__name__]
        regex: 'up|go_.*|container_.*'
        target_label: source_dc
        replacement: dc-bj-dongba

  # 深圳机房
  - url: http://prom-gw-sz-vip:19201/api/v1/write
    headers:
      Authorization: Bearer tk_infra_prod
      source_dc: dc-sz-wulian

# prom-gw 自身 metrics 抓取
scrape_configs:
  - job_name: prom-gw
    scrape_interval: 15s
    static_configs:
      - targets: ['prom-gw-bj-vip:8080']
        labels: { ingest_city: bj }
      - targets: ['prom-gw-sz-vip:8080']
        labels: { ingest_city: sz }
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: 'gateway_.*'
        action: keep
```

## 附录 E:快速故障速查手册

| 症状 | 查哪里 | 命令 |
|------|--------|------|
| Prometheus 报 remote_write 401 | token 过期/缺失 | `journalctl -u prom-gw@bj | grep "auth failed"` |
| Prometheus 报 remote_write 429 | 限流触发 | `curl -s localhost:8080/metrics | grep rate_limit_rejected` |
| Prometheus 报 remote_write 503 | WAL 容量满 | `df -h /data/wal` + `curl -s localhost:8080/metrics | grep wal` |
| Kafka 无数据 | prom-gw 降级 WAL 了 | `journalctl -u prom-gw@bj | grep -i "wal\|kafka connect"` |
| prom-gw 进程 OOM | 内存不足 | `journalctl -k | grep -i oom` + 调大 `GOMEMLIMIT` |
| Ruleset 变更没生效 | fsnotify 没检测到 | `curl -s localhost:8082/v1/rulesets` 看 version 是否切换 |
| Admin API 访问 403 | CIDR 白名单 | 检查 `--admin-allow-cidr` 和请求来源 IP |
| WAL drain 卡住 | Kafka 不通 | `journalctl -u prom-gw@bj | grep drain` + 检查 Kafka broker |
| goroutine 持续增长 | 可能泄漏 | `curl -s localhost:8080/metrics | grep go_goroutines` + pprof 抓快照 |
