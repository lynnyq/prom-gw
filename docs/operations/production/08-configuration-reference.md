# prom-gw 配置参数参考
> 本文档覆盖 prom-gw 的**全部启动参数、环境变量、配置文件(ruleset / token)、内部模块参数**,提供字段类型、默认值、取值范围、配置示例和注意事项。
>
> 配套文档:**local-dev-guide.md**(见 §10)(本地部署)、**production-guide.md**(见 §1)(生产部署)、**ruleset-reference.md**(ruleset 字段)


---

## 1. 启动命令行参数

### 1.1 参数总览

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

### 1.2 详细说明

#### `--config`

- **类型**:string(文件路径)
- **默认**:`configs/rules/default.yaml`
- **环境变量覆盖**:`PROM_GW_CONFIG`
- **说明**:ruleset 配置文件路径,定义 prom-gw 如何清洗、路由、采样、下采样指标。支持热更新(fsnotify 5s 检测)。
- **生产建议**:按城市分目录 `configs/rules/<city>/default.yaml`
- **示例**:
  ```bash
  --config=configs/rules/bj/default.yaml
  ```

#### `--tokens`

- **类型**:string(文件路径)
- **默认**:`configs/tokens/local.yaml`
- **环境变量覆盖**:`PROM_GW_TOKENS`
- **说明**:token 鉴权配置,定义 token → tenant 映射、默认 topic、限流。支持 SIGHUP 热重载。
- **示例**:
  ```bash
  --tokens=configs/tokens/production.yaml
  ```

#### `--metrics-addr`

- **类型**:string(`host:port`)
- **默认**:`:8080`
- **说明**:暴露 `/metrics`(Prometheus 抓取)和 `/debug/pprof/*`(性能分析)。
- **生产建议**:仅对 Prometheus 和运维网段开放
- **示例**:
  ```bash
  --metrics-addr=:8080
  ```

#### `--health-addr`

- **类型**:string(`host:port`)
- **默认**:`:8081`
- **说明**:暴露 `/healthz`(200)和 `/readyz`(204)。LVS / Keepalived 探测此端口。
- **示例**:
  ```bash
  --health-addr=:8081
  ```

#### `--write-addr`

- **类型**:string(`host:port`)
- **默认**:`:19201`
- **说明**:Prometheus remote_write 的接入点,完整路径为 `http://<addr>/api/v1/write`。
- **生产建议**:通过 LVS VIP 暴露给 Prometheus
- **示例**:
  ```bash
  --write-addr=:19201
  ```

#### `--admin-addr`

- **类型**:string(`host:port`)
- **默认**:`:8082`
- **说明**:Admin API 监听地址,提供 ruleset 热更新、stats、tenants 等查询。
- **安全**:**必须**通过 `--admin-allow-cidr` 限制来源 IP,默认仅本机
- **示例**:
  ```bash
  --admin-addr=:8082 --admin-allow-cidr=127.0.0.1/32,10.0.0.0/8
  ```

#### `--admin-allow-cidr`

- **类型**:string(逗号分隔 CIDR)
- **默认**:`127.0.0.1/32,10.0.0.0/8`
- **说明**:Admin API 白名单。仅匹配 CIDR 的来源 IP 可访问。
- **生产建议**:收紧为运维网段,如 `10.10.0.0/16`
- **示例**:
  ```bash
  --admin-allow-cidr=127.0.0.1/32,10.10.0.0/16
  ```

#### `--source-dc`

- **类型**:string
- **默认**:`dc-unknown`
- **说明**:本实例所属机房标识,写入每条消息的 Kafka header `source_dc` 和 `ingest_dc`。也用于指标 `gateway_*{source_dc=...}`。
- **生产建议**:按实际机房命名,如 `dc-bj-dongba`、`dc-sz-wulian`
- **示例**:
  ```bash
  --source-dc=dc-bj-dongba
  ```

#### `--ingest-city`

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

#### `--wal-dir`

- **类型**:string(目录路径)
- **默认**:`/data/wal`
- **说明**:Kafka 故障时数据落盘目录。建议挂载独立 SSD,`noatime` 挂载。
- **本地开发**:`/tmp/prom-gw-local-wal`
- **生产建议**:`/data/wal`(独立盘)
- **示例**:
  ```bash
  --wal-dir=/data/wal
  ```

#### `--wal-max-bytes`

- **类型**:int64(字节数)
- **默认**:`53687091200`(50GB)
- **说明**:WAL 总字节上限。达到上限后,**新请求返回 HTTP 503**(背压)。
- **本地开发**:`1073741824`(1GB)
- **生产建议**:50GB(默认),Kafka 恢复后自动 drain
- **示例**:
  ```bash
  --wal-max-bytes=53687091200
  ```

#### `--wal-disk-used-ratio`

- **类型**:float64(0.0-1.0)
- **默认**:`0.80`(80%)
- **说明**:WAL 所在磁盘使用率硬阈值。达到后切硬拒绝(503)。与 `--wal-max-bytes` 为**双阈值机制**,任一触发即拒绝。
- **生产建议**:0.80(默认),磁盘紧张可调到 0.70
- **示例**:
  ```bash
  --wal-disk-used-ratio=0.80
  ```

#### `--nacos-addr`

- **类型**:string(逗号分隔 `ip:port`)
- **默认**:`""`(空,不启用)
- **说明**:Nacos 配置中心地址。空则仅用本地文件源。配置后 ruleset 可从 Nacos 远程拉取并热更新。
- **示例**:
  ```bash
  --nacos-addr=10.0.0.1:8848,10.0.0.2:8848,10.0.0.3:8848
  ```

#### `--nacos-namespace` / `--nacos-username` / `--nacos-password`

- **类型**:string
- **默认**:空
- **说明**:Nacos 鉴权信息。生产环境必须配置账号密码。
- **示例**:
  ```bash
  --nacos-namespace=production --nacos-username=prom-gw --nacos-password=<secret>
  ```

#### `--nacos-data-id` / `--nacos-group`

- **类型**:string
- **默认**:`prom-gw-rules` / `GATEWAY`
- **说明**:Nacos 中 ruleset 配置的 dataId 和 group。
- **示例**:
  ```bash
  --nacos-data-id=prom-gw-rules-bj --nacos-group=GATEWAY
  ```

#### `--nacos-snapshot-path`

- **类型**:string(文件路径)
- **默认**:`/data/nacos_snapshot.json`
- **说明**:last-good snapshot 持久化路径。Nacos 拉取成功后写本地快照,Nacos 不可用时从快照恢复。空则不持久化。
- **示例**:
  ```bash
  --nacos-snapshot-path=/data/nacos_snapshot.json
  ```

#### `--version`

- **类型**:bool
- **默认**:`false`
- **说明**:打印版本后退出。版本号由 Makefile 通过 `-ldflags` 注入。
- **示例**:
  ```bash
  ./prom-gw --version
  # 输出: prom-gw v1.2.3
  ```

---

## 2. 环境变量

### 2.1 环境变量总览

| 环境变量 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `KAFKA_BROKERS` | string | 空 | Kafka broker 列表(逗号分隔)。**空 = 进入 WAL-only 模式** |
| `INGEST_CITY` | string | `dc-unknown` | 城市标识(bj/sz/hf),可被 `--ingest-city` 覆盖 |
| `PROM_GW_CONFIG` | string | `configs/rules/default.yaml` | ruleset 配置路径,可被 `--config` 覆盖 |
| `PROM_GW_TOKENS` | string | `configs/tokens/local.yaml` | token 配置路径,可被 `--tokens` 覆盖 |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | 空 | OpenTelemetry OTLP 接收端。空 = tracing 降级为 noop |

### 2.2 详细说明

#### `KAFKA_BROKERS`

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

#### `INGEST_CITY`

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

#### `PROM_GW_CONFIG` / `PROM_GW_TOKENS`

- **类型**:string(文件路径)
- **说明**:与 `--config` / `--tokens` flag 等价。flag 优先级 > env。
- **用途**:容器化部署时避免改启动命令
- **示例**:
  ```bash
  PROM_GW_CONFIG=/etc/prom-gw/rules.yaml \
  PROM_GW_TOKENS=/etc/prom-gw/tokens.yaml \
  ./prom-gw
  ```

#### `OTEL_EXPORTER_OTLP_ENDPOINT`

- **类型**:string(URL)
- **默认**:空
- **说明**:OpenTelemetry OTLP/gRPC 接收端,如 `http://otel-collector:4317`。空时 tracing 降级为 noop,不发送 span。
- **示例**:
  ```bash
  OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability:4317 ./prom-gw
  ```

---

## 3. Ruleset 配置文件

### 3.1 顶层结构

```yaml
rulesets:        # 规则集列表(支持多 ruleset 并行)
  - name: ...
    ...
global:          # 全局参数
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

### 3.2 顶层字段

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `rulesets` | array | 否 | `[]` | 规则集列表。空数组 = 透传模式(只接收不清洗) |
| `global` | object | 否 | 见下 | 全局参数 |

### 3.3 `global` 字段

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

### 3.4 `rulesets[]` 字段

| 字段 | 类型 | 必填 | 默认 | 说明 |
|---|---|---|---|---|
| `name` | string | ✓ | — | ruleset 唯一名,用于 Admin API 路径 |
| `tenant` | string | 否 | `""` | 适用租户(多租户预留,v1 全局生效) |
| `default_topic` | string | ✓ | — | 没路由命中时的兜底 topic |
| `match` | object | 否 | `{}`(全量) | metric 命中条件 |
| `stages` | array | 否 | `[]`(透传) | 处理阶段列表 |
| `version` | int | ✓ | — | 单调递增版本号 |

### 3.5 `match` 字段

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

### 3.6 `stages[]` 字段

每个 stage 有:
- `type`:阶段类型(见下表)
- 其他字段:按 type 不同而不同,**支持 inline 写法**(推荐)

#### 支持的 stage 类型

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

### 3.7 `relabel` stage

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

### 3.8 `enrich` stage

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

### 3.9 `route` stage

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

### 3.10 `sample` stage

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

### 3.11 `downsample` stage(状态型)

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

### 3.12 `deadvalue` stage(状态型)

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

## 4. Token 配置文件

### 4.1 顶层结构

```yaml
tokens:
  "<token-string>":
    tenant: ...
    tenant_id: ...
    default_topic: ...
    rate_limit: ...
```

### 4.2 字段说明

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `tokens` | map | ✓ | token → 配置的映射。key 是 token 字符串 |
| `tokens[].tenant` | string | ✓ | 租户名,写入 Kafka header `tenant` |
| `tokens[].tenant_id` | string | 否 | IAM 主键(预留,本地可空) |
| `tokens[].default_topic` | string | ✓ | 默认 topic(route 未命中时兜底) |
| `tokens[].rate_limit` | int | 否 | 该 tenant 的 samples/s 上限。0 = 不限 |

### 4.3 完整示例

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

  "tk_prod_bj_payment":
    tenant: payment
    tenant_id: "2001"
    default_topic: prom.bj.routed.payment
    rate_limit: 200000
```

### 4.4 热重载

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

## 5. 内部模块参数

> 以下参数在代码中定义,部分通过 flag/env 注入,部分为常量不可配置。

### 5.1 KafkaSink(producer)

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

### 5.2 WAL

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

### 5.3 Sink Adapter(kafka + wal 切换)

定义在 [internal/sink/sink.go](../../internal/sink/sink.go)。

| 参数 | 默认值 | 说明 |
|---|---|---|
| `FailThreshold` | `3` | 连续失败次数,达到后切到 WAL |
| `RecoverCheck` | `1s` | 恢复探测间隔 |
| `RecoverSuccessThreshold` | `3` | 连续成功次数,达到后切回 Kafka |

### 5.4 Pipeline

| 参数 | 默认值 | 说明 |
|---|---|---|
| `BufferSize` | `65535`(来自 ruleset `global.channel_buffer`) | channel 容量 |

### 5.5 Receiver

| 参数 | 默认值 | 说明 |
|---|---|---|
| `Addr` | `:19201`(flag 注入) | 监听地址 |
| `ReadHeaderTimeout` | `5s` | HTTP 读 header 超时 |
| `ShutdownTimeout` | `30s` | 停机等待 in-flight 完成超时 |

### 5.6 Tracing(OpenTelemetry)

| 参数 | 默认值 | 说明 |
|---|---|---|
| `ServiceName` | `prom-gw` | OTel service.name |
| `OTLPEndpoint` | env `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP/gRPC 接收端 |
| `SampleRatio` | `1.0` | 采样率(1.0 = 全采样) |
| `Insecure` | `true` | 是否禁用 TLS |

---

## 6. 完整配置示例

### 6.1 本地开发最小配置

`configs/rules/local-dev.yaml`:

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
    default_topic: prom.local.routed.app_business
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

### 6.2 生产完整配置(北京)

`configs/rules/bj/default.yaml`:

```yaml
rulesets:
  # 1. app-business 业务指标
  - name: app-business-bj
    tenant: app-business
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
    default_topic: prom.bj.routed.app_business
    rate_limit: 200000

  "tk_prod_bj_infra_<secret>":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.bj.routed.infra
    rate_limit: 150000

  "tk_prod_bj_payment_<secret>":
    tenant: payment
    tenant_id: "2001"
    default_topic: prom.bj.routed.payment
    rate_limit: 100000
```

systemd 启动(`prom-gw@bj.service`):

```ini
[Service]
Environment=KAFKA_BROKERS=kafka-1.bj:9094,kafka-2.bj:9094,kafka-3.bj:9094
Environment=INGEST_CITY=bj
Environment=OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector.observability.bj:4317
ExecStart=/appdata/prom-gw/bin/prom-gw \
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

### 6.3 Nacos 配置中心 ruleset(Nacos 中的 dataId 内容)

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

## 7. 参数调优速查表

### 7.1 按场景调优

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

### 7.2 端口速查

| 端口 | 用途 | 暴露范围 |
|---|---|---|
| `19201` | RemoteWrite 接入 | Prometheus / LVS |
| `8080` | metrics + pprof | Prometheus 抓取 |
| `8081` | healthz / readyz | LB health check |
| `8082` | Admin API | 运维网段(白名单) |

### 7.3 信号处理

| 信号 | 行为 |
|---|---|
| `SIGINT` / `SIGTERM` | 优雅停机(30s 超时) |
| `SIGHUP` | 热重载 token + tenant 限流配置 |

### 7.4 退出码

| 退出码 | 含义 |
|---|---|
| `0` | 正常退出 |
| `1` | fatal 错误 |
| `2` | SIGHUP 触发(预留) |

---

## 附录

### A. 配置文件路径速查

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

