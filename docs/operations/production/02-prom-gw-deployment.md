# prom-gw 生产部署与配置详解

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

### 5.2 数据流模型(单阶段同步处理)

prom-gw 采用 **单进程同步处理模型**:收到 Prometheus remote_write 后,在进程内完成 relabel + route,直接写入目标 Kafka topic。**不存在 raw → routed 两阶段异步消费**。

#### 5.2.1 数据流

```
Prometheus
  │ remote_write (HTTP POST, body=snappy(protobuf))
  ▼
┌─────────────────────────────────────────────────┐
│ prom-gw (单进程)                                │
│                                                 │
│  1. receiver: 验证 token                        │
│  2. 查 tokens.yaml → 获取 default_topic(初始)  │
│  3. rule engine 同步处理:                      │
│     a. relabel: drop env/instance/pod          │
│     b. route: 按 team 标签设置 TargetTopic      │
│     c. sample: 按 rate 采样                     │
│  4. pipeline 用 TargetTopic 覆盖 msg.Topic      │
│  5. sink 写入 Kafka (routed topic)              │
│                                                 │
│  失败时:写入 WAL 降级,后续自动回灌             │
└─────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────┐
│ Kafka: prom.<city>.routed.<category>           │
│   payload = 原始 snappy+protobuf body          │
│   key = series hash (同 series 落同分区)       │
│   headers = {tenant, source_dc, ingest_city}   │
└─────────────────────────────────────────────────┘
  │
  │ Flink KafkaSource 消费
  ▼
Flink 聚合作业 → StarRocks
```

**关键点**:
- prom-gw **不消费 Kafka**,只写入
- token 的 `default_topic` 仅作为 `msg.Topic` 初始值,被 ruleset 的 `default_topic` 覆盖
- ruleset 的 `input_topic` 字段**仅作文档标识,运行期不参与逻辑**(代码注释明确)
- 实际写入的 topic 由 ruleset 的 `default_topic` + route rules 决定

#### 5.2.2 配置关系

```
tokens.yaml                    rules.yaml (ruleset)
─────────────                  ─────────────────────────────
tokens:                        rulesets:
  "tk_app_business_prod":         - name: app-business
    default_topic:                  tenant: app-business
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

### 5.3 Token 配置

路径:`/appdata/prom-gw/conf/tokens.yaml`

```yaml
# ============================================================
# prom-gw Token 配置 - 生产环境
# 部署路径: /appdata/prom-gw/conf/tokens.yaml
# 权限要求: chmod 600, owner bdops:bdops
# 热重载: kill -HUP <pid>(无需重启进程)
#
# token 与 topic 的对应关系:
#   tk_app_business_prod → prom.bj.routed.app_business
#   tk_infra_prod       → prom.bj.routed.infra
# ============================================================
tokens:
  # app-business 租户:业务应用指标(CPU/内存/QPS/RT 等)
  "tk_app_business_prod":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.bj.routed.app_business    # 写入的 topic
    rate_limit: 80000                             # samples/s 上限

  # infra 租户:基础设施指标(主机/网络/存储 等)
  "tk_infra_prod":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.bj.routed.infra           # 写入的 topic
    rate_limit: 50000
```

**字段说明**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `tenant` | string | 租户名,用于 Kafka key 和日志标识 |
| `tenant_id` | string | 租户 ID(预留 IAM 主键,v1 可不填) |
| `default_topic` | string | 写入的 topic(ruleset 配置后被覆盖) |
| `rate_limit` | int | 该 tenant 的 samples/s 上限,超过则 429 |

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

### 5.4 Ruleset 配置

路径:`/appdata/prom-gw/conf/rules.yaml`(每城独立部署,文件名统一)

```yaml
# ============================================================
# prom-gw Ruleset 配置 - 北京 (bj)
# 源文件: configs/rules/bj/default.yaml
# 部署路径: /appdata/prom-gw/conf/rules.yaml
# 热加载: fsnotify 监听文件变化,自动编译+原子切换
#
# 路由规则:
#   app-business 租户 → relabel → route → prom.bj.routed.{core,infra,data,app_business}
#   infra 租户        → relabel          → prom.bj.routed.infra
# ============================================================
rulesets:
  # ── app-business 租户规则集 ──
  # 收到 remote_write 后同步处理:relabel + route,直接写入 routed topic
  - name: app-business
    tenant: app-business
    default_topic: prom.bj.routed.app_business   # 路由未命中时的兜底 topic
    version: 1
    match:
      metric_prefix: ""                         # 空 = 全量接收
    stages:
      # Stage 1: relabel - 标签清洗
      - type: relabel
        drop_labels: [env, instance, pod]        # 删除噪音标签
        keep_labels: []                          # 空 = 关闭白名单
        label_map:
          kubernetes_io_cluster: cluster         # 重命名标签

      # Stage 2: route - 按 team 标签分桶到不同 topic
      - type: route
        rules:
          - match: { team: "core" }              # 核心业务团队
            topic: prom.bj.routed.core
          - match: { team: "infra" }              # 基础设施团队
            topic: prom.bj.routed.infra
          - match: { team: "data" }              # 数据平台团队
            topic: prom.bj.routed.data
          # 未命中以上规则 → 写入 default_topic (prom.bj.routed.app_business)

      # Stage 3: sample - 采样(降低下游存储压力)
      - type: sample
        rate: 0.1                                # 保留 10%

  # ── infra 租户规则集 ──
  # 基础设施指标通常不按 team 分桶,直接写入 routed.infra
  - name: infra
    tenant: infra
    default_topic: prom.bj.routed.infra           # 写入的 topic
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels: [env, instance]
      # 无 route stage → 全部写入 default_topic

global:
  rate_limit_per_instance: 100000                # 单实例全局 samples/s 上限
  channel_buffer: 65535                           # 内部 channel 缓冲区大小
```

**stage 执行顺序**(固定):`relabel → enrich → route → sample → downsample → deadvalue`

**ruleset 字段说明**:

| ruleset 字段 | 作用 |
|-------------|------|
| `default_topic` | 路由未命中时的兜底 topic(实际写入) |
| `route.rules[].topic` | 按 team 匹配后写入的 topic(实际写入) |

**关键约束**:
- `default_topic` 和 `route.rules[].topic` 决定数据实际写入哪些 topic
- 一个 ruleset 处理一个 tenant 的数据
- `input_topic` 字段已废弃,仅作文档标识,运行期不参与逻辑

### 5.5 systemd template 部署

prom-gw 使用 **template unit**(`prom-gw@.service`,`%i` 为城市标识):

**步骤 1:创建目录**

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/prom-gw/bin /appdata/prom-gw/conf /appdata/prom-gw/wal /applog/prom-gw
sudo chown -R bdops:bdops /appdata/prom-gw /applog/prom-gw
```

**步骤 2:放置二进制和配置**

```bash
sudo cp bin/prom-gw /appdata/prom-gw/bin/
sudo cp configs/tokens/local.yaml /appdata/prom-gw/conf/tokens.yaml
sudo cp configs/rules/bj/default.yaml /appdata/prom-gw/conf/rules.yaml
sudo chmod 600 /appdata/prom-gw/conf/tokens.yaml
```

**步骤 3:配置 Kafka broker 地址**

```bash
# /appdata/prom-gw/conf/prom-gw.env
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
User=bdops
Group=bdops
Environment=INGEST_CITY=%i
Environment=PROM_GW_CONFIG=/appdata/prom-gw/conf/config-%i.yaml
Environment=PROM_GW_TOKENS=/appdata/prom-gw/conf/tokens.yaml
ExecStart=/appdata/prom-gw/bin/prom-gw \
  --config=/appdata/prom-gw/conf/config-%i.yaml \
  --tokens=/appdata/prom-gw/conf/tokens.yaml \
  --ingest-city=%i
Restart=always
RestartSec=5
LimitNOFILE=65535
MemoryMax=8G
KillSignal=SIGTERM
TimeoutStopSec=30
EnvironmentFile=-/appdata/prom-gw/conf/prom-gw.env

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/appdata/prom-gw/wal /applog/prom-gw
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

### 5.6 启动参数速查

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
| `--wal-dir` | - | `/appdata/prom-gw/wal` | WAL 数据目录 |
| `--wal-max-bytes` | - | `50GB` | WAL 总字节上限 |
| `--nacos-addr` | - | (空) | Nacos 地址,逗号分隔 |

Kafka broker 列表通过 `KAFKA_BROKERS` 环境变量注入,未设置时进入 **WAL-only 模式**。

---

