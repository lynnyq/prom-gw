# Implementation Plan: 多机房 Prometheus RemoteWrite 网关 (prom-gw)

- **Status**: Draft
- **Spec**: [2026-07-28-prometheus-multidc-remotewrite-gateway-design.md](../specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md)
- **Date**: 2026-07-28
- **Author**: writing-plans

## Goals

完成自研 RemoteWrite 协议网关 `prom-gw`,实现:

1. 多机房 Prometheus 统一接入,直写中心 Kafka
2. 标签/指标/路由/采样/下采样/死值等多维清洗
3. 配置热更新(本地 + Nacos)
4. 端到端可观测(指标 + Trace + 日志)
5. 单机 ≥ 1.5M samples/s 持续,p99 < 500ms

## Out of Scope

- Admin Web UI(后端 API 已交付,前端另立项目)
- 跨机房 Kafka 复制(MirrorMaker 留待后续)
- Prometheus 端到端可观测(已有,本服务只暴露自己的指标)
- Flink/Spark/CK/SR 侧适配(spec 假设它们按现有协议消费)
- 鉴权体系(本次仅做本地 Token 校验,后续接公司 IAM)

## Tech Stack (resolved)

| 组件 | 选型 | 理由 |
|---|---|---|
| 语言/版本 | Go 1.22+ | 用户规范 |
| HTTP 框架 | `github.com/go-chi/chi/v5` | 轻量、stdlib 风格、与 OpenTelemetry 兼容好 |
| Kafka 客户端 | `github.com/twmb/franz-go` | 纯 Go、幂等、压缩、低内存、性能匹配 franz-go |
| YAML 配置 | `gopkg.in/yaml.v3` | 主流选择 |
| 配置中心 | `github.com/nacos-group/nacos-sdk-go/v2` | 用户规范 |
| 链路追踪 | `go.opentelemetry.io/otel` | CNCF 标准,跨语言 |
| 日志 | `go.uber.org/zap` | 高性能 JSON 日志 |
| 指标 | `github.com/prometheus/client_golang` | 自暴露 `/metrics` |
| 限流 | `golang.org/x/time/rate` | 官方维护 |
| Protobuf | `google.golang.org/protobuf` | Prometheus 官方依赖 |
| Snappy | `github.com/klauspost/compress/snappy` | 主流 |
| 集成测试 | `github.com/testcontainers/testcontainers-go/modules/kafka` | 嵌入式真实 Kafka |
| 性能测试 | `github.com/tsenart/vegeta` + 自研 client | 标准压测工具 |
| Lint | `golangci-lint` | 社区标准 |
| 部署 | Ansible + systemd(VM/bare-metal,非 K8s) | spec 9 |

## Phases

### Phase 0: 项目骨架(2-3 天)

**Goal**:可编译运行的 Go 工程,带 CI、lint、单测框架,空 `main` 启动后打印 "ready"。

**Tasks**:

- [ ] **T0.1** 初始化 Git 仓库并建立分支
  - `cd /Users/yangqian/go/src/github.com/lynnyq/bigdata && git init && git checkout -b feat/prom-gw`
  - 创建 `.gitignore` (Go 模板)
  - 创建 `README.md`(项目说明 + 快速启动)
  - 初始 commit

- [ ] **T0.2** 初始化 Go module 与目录结构
  - `go mod init github.com/lynnyq/bigdata`
  - 按 spec 9.3 创建目录:
    - `cmd/prom-gw/main.go`(占位,只打 "ready")
    - `internal/{receiver,decoder,parser,ruleengine,router,kafkasink,config,admin,obs}/`(每目录空 `doc.go`)
    - `pkg/{safego,httpx,metrichelper,tracex}/`(占位)
    - `api/proto/`(占位)
    - `configs/rules/`(放示例 ruleset)
    - `deploy/{ansible,systemd}/`(占位)
    - `test/{integration,perf,loadgen,chaos,compat}/`
  - 引入核心依赖(用 `go get`):
    - 框架与协议:`chi`, `franz-go`, `klauspost/compress`, `google.golang.org/protobuf`
    - 配置:`yaml.v3`, `nacos-sdk-go/v2`, `fsnotify`(热更新)
    - 可观测:`go.opentelemetry.io/otel` + `otelexporters/otlp/otlptrace/otlptracegrpc`, `zap`, `prometheus/client_golang`
    - 工具:`golang.org/x/time/rate`, `stretchr/testify`
  - 引入工具:`golangci-lint`, `mockgen`, `oapi-codegen`(T4.7), `redocly/cli`(T4.7), `chaos-mesh` 客户端(K8s 外部,走 toxiproxy 时可省), `toxiproxy-cli`(T5.5)

- [ ] **T0.3** 配置 `Makefile`
  - targets: `build`, `test`, `test-integration`, `lint`, `fmt`, `run`, `perf`
  - 编译输出 `bin/prom-gw`

- [ ] **T0.4** 配置 `golangci-lint` (`.golangci.yml`)
  - 启用: `govet`, `errcheck`, `staticcheck`, `gofmt`, `goimports`, `gosec`, `gocognit`(限制 30)
  - 用户规范:行宽、import 分组、错误处理必须显式

- [ ] **T0.5** 配置 GitHub Actions CI(`.github/workflows/ci.yml`)
  - jobs: `lint`, `test`, `build` (matrix: Go 1.22, 1.23)
  - 上传覆盖率到 codecov(可选)

- [ ] **T0.6** 实现 `pkg/safego`(用户规范强制)
  - `pkg/safego/safego.go`:
    ```go
    func Go(name string, fn func())
    func GoWithRecover(name string, fn func(), onPanic func(any, []byte))
    ```
  - `pkg/safego/safego_test.go`:验证 panic 被捕获且回调被调用

- [ ] **T0.7** 实现 `cmd/prom-gw/main.go` 骨架
  - **启动顺序**(依赖关系由先到后,任何一步失败都 fast-fail):
    1. 解析 CLI flag(`--config`, `--kafka-brokers`, `--nacos-addr` ...)
    2. 初始化 zap logger(预 JSON 格式,带 `trace_id` 字段)
    3. 初始化 `pkg/safego` 全局 panic 钩子(将 panic 转为 `gateway_panic_recovered_total` 计数)
    4. 启动 `internal/obs` 子模块:prom exporter 注册到 `8080`、tracing exporter(OTLP,异步)
    5. 加载 `configs/rules/default.yaml`(空 ruleset,失败致命退出)
    6. 启动 `:8081/healthz` + `:8081/readyz`(readyz 检查 Kafka/WAL/Config 三个依赖)
    7. 监听 SIGINT/SIGTERM,优雅退出(收信号 → 摘流量 → flush → 关闭 Kafka → 退出)
  - 退出码符合 systemd 期望:`EXIT_OK=0`、`EXIT_FATAL=1`、`EXIT_RELOAD=2`
  - **Phase 0 阶段尚未实现 receiver 与 kafkasink**,仅暴露 healthz/metrics 即可验证骨架

- [ ] **T0.8** 添加 `configs/rules/default.yaml`(空 ruleset,作为后续基础)
  ```yaml
  rulesets: []
  global:
    rate_limit_per_instance: 100000
    channel_buffer: 65535
  ```

- [ ] **T0.9** 添加 systemd unit 文件
  - `deploy/systemd/prom-gw.service` 完整 spec(见 spec 9.1)

- [ ] **T0.10** 添加基础 README + DEVELOPING.md
  - README: 简介、快速启动、配置说明
  - DEVELOPING: 开发流程、测试、调试

**Verification**:

```bash
make build              # 产出 bin/prom-gw,exit 0
make lint               # 无 error
make test               # 全部通过(此时只有 safego 测试)
bin/prom-gw --config=configs/rules/default.yaml &
curl localhost:8081/healthz   # 200
curl localhost:8080/metrics | head -5
kill %1                       # 收到 SIGTERM 干净退出
```

**Done when**:
- CI 全绿
- `make build` 产出二进制
- `make run` 启动后 `curl /healthz` 返回 200
- `pkg/safego` 覆盖率 100%

---

### Phase 1 (M1): 端到端透传 + WAL(2.5 周)

**Goal**:Prometheus 通过 `remote_write` 把数据写入 `prom-gw`,`prom-gw` 透传到中心 Kafka。**无规则引擎**,仅做最简路径。

**Tasks**:

- [ ] **T1.1** 复制 Prometheus 官方 proto
  - `api/proto/remote.proto`(从 prometheus/prometheus 仓库 `storage/remote/remote.proto` 复制)
  - `api/proto/types.proto`(同上)
  - `make proto` 生成 `*.pb.go`(用 `buf` 或 `protoc-gen-go`)

- [ ] **T1.2** 定义内部 `Sample` 数据模型
  - `internal/parser/sample.go`:
    ```go
    type Sample struct {
        Business    string            // 来源business(intern 池复用,见 pkg/stringpool)
        SourceDC  string            // 来自哪个机房(intern 池复用)
        Metric    string            // metric name
        Labels    []Label           // 排序后的 label 集合
        Value     float64
        Timestamp int64             // ms
        IngestTs  int64             // 进入 GW 时间
        // TraceID 字段被刻意移除:
        //   TraceID 走 request-scoped context(由 receiver 在 ctx 注入,
        //   各 stage 通过 ctx 透传),不存进 Sample 结构体,
        //   避免 1.5M samples/s 下的 string 分配压力。
        //   写入 Kafka 时再从 ctx 取出,放进 message header `traceparent`。
    }
    type Label struct { Name, Value string }
    func (s Sample) SeriesKey() string // hash(business+metric+sortedLabels) 用于分区
    ```
  - 性能约束:`Sample` 整体大小 ≤ 256 字节;`Labels` 容量 4 起,超过时复用底层数组

- [ ] **T1.3** 实现 `internal/config` 的 Token 加载(必须在 receiver 之前)
  - **设计原则**:为未来 IAM 接入预留 `Authenticator` 接口,本计划只实现 `LocalTokenAuthenticator`(见 F.3)
  - `internal/auth/authenticator.go`(本计划**只占位**,放接口 + doc,不写实现):
    ```go
    package auth
    type Authenticator interface {
        Verify(ctx context.Context, token string) (Business, error)
    }
    type Business struct {
        Name, DefaultTopic, BusinessID string
        RateLimit int
    }
    ```
  - `internal/config/token.go`(实现 `LocalTokenAuthenticator`):
    ```go
    type LocalTokenAuthenticator struct { tokens map[string]auth.Business }
    func (a *LocalTokenAuthenticator) Verify(ctx context.Context, token string) (auth.Business, error)
    func NewLocalTokenAuthenticator(path string) (*LocalTokenAuthenticator, error)
    func (a *LocalTokenAuthenticator) Reload(path string) error  // SIGHUP 调用
    ```
  - 启动时加载 `configs/tokens.yaml`,运行期 SIGHUP 重载
  - 失败原因细分:`ErrTokenMissing` / `ErrTokenInvalid` / `ErrTokenExpired` / `ErrTokenRevoked`,对应 metric `gateway_auth_fail_total{reason}`(v1 只 emit `invalid`)
  - `configs/tokens.yaml`:
    ```yaml
    tokens:
      "tk_app_business_xxx":
        business: app-business
        business_id: 1001            # 未来 IAM 主键,v1 可空
        default_topic: prom.raw.app_business
        rate_limit: 80000
      "tk_infra_xxx":
        business: infra
        business_id: 1002
        default_topic: prom.raw.infra
        rate_limit: 50000
    ```

- [ ] **T1.4** 实现 `internal/receiver`(HTTP 接入)
  - `internal/receiver/server.go`:
    - `func New(cfg Config) *Server`
    - 监听端口:**`:19201`**(`/api/v1/write`,固定端口,Prometheus 端 `remote_write` URL 直接配)
    - chi 路由,`POST /api/v1/write`
    - **端口分配**(避免与 metrics/healthz/admin 冲突,统一约定):
      | 端口 | 用途 | 监听端 |
      |---|---|---|
      | `19201` | RemoteWrite 接入(主流量) | 全部实例 |
      | `8080` | Prometheus self-export `/metrics` | 全部实例 |
      | `8081` | `/healthz` + `/readyz` | 全部实例 |
      | `8082` | Admin API(本机/内网,可选加白名单) | 全部实例 |
      | `9090` | pprof(仅 debug build) | 全部实例 |
    - 中间件链: `RequestID` → `RealIP` → `Tracing`(在 Logger 前以保证 trace_id 注入)→ `Logger` → `Recoverer` → `RateLimit`(全局 100K/s) → `Auth`
    - `Auth`: 从 header `Authorization: Bearer <token>` 解析 token,调 `auth.Authenticator.Verify`(由 T1.3 注入 `LocalTokenAuthenticator`);成功得 `auth.Business` 注入 ctx
    - 失败 reason 分类:401(missing/invalid) / 403(revoked);计数 `gateway_auth_fail_total{reason}`
  - `internal/receiver/middleware.go`: rate limit, recover, trace 注入
  - `internal/receiver/server_test.go`:httptest 验证路由/中间件/限流/认证(覆盖 401/200/429/503)

- [ ] **T1.5** 实现 `internal/decoder`(snappy + protobuf)
  - `internal/decoder/decoder.go`:
    ```go
    func Decode(body []byte) (*prompb.WriteRequest, error)
    ```
  - 校验 `Content-Encoding: snappy`、`Content-Type: application/x-protobuf`,缺失/错误 → 400
  - 错误 → 400 + 日志 + metric `gateway_errors_total{type="decode"}`
  - `internal/decoder/decoder_test.go`: happy path / 错误 snappy / 错误 protobuf / 错误 content-type

- [ ] **T1.6** 实现 `internal/parser`(WriteRequest → []Sample)
  - `internal/parser/parser.go`:
    ```go
    // Meta 携带请求级元数据,从 receiver 注入到 ctx 中,
    // parser 通过 ctx 取出,不再依赖外部参数透传。
    type Meta struct {
        Business    string // 来自 token.Name
        BusinessID  string // 来自 token.BusinessID(未来 IAM 主键,v1 可空)
        SourceDC  string // 来自 instance tag(--source-dc 启动参数)
        RemoteIP  string // 来自 http.Request.RemoteAddr
        IngestTs  int64  // 进入 GW 时刻(纳秒时间戳,由 receiver 注入)
        TraceID   string // 由 tracing 中间件注入,后续写 Kafka header
    }
    func MetaFromContext(ctx context.Context) (Meta, bool)
    func ContextWithMeta(ctx context.Context, m Meta) context.Context

    func Parse(ctx context.Context, req *prompb.WriteRequest) ([]Sample, error)
    ```
  - 实现细节:
    - `ctx` 必带 `Meta`,缺失则视为内部 bug 返回 `error`(panic 而非静默)
    - 填 `Business/SourceDC/IngestTs` 等元数据;**TraceID 不进 Sample**,由 ctx 透传(见 T1.2)
    - `Labels` 排序(保证 hash 一致),`Business/SourceDC` 走 `pkg/stringpool` 复用
    - 单条 series 失败不阻断整批,跳过并 `gateway_errors_total{type="parse_series"}` 计数
  - 单测:fixture 文件 + 黄金结果;ctx 缺失 Meta 的负向 case

- [ ] **T1.7** 实现 `internal/kafkasink` v1(仅透传模式)
  - `internal/kafkasink/producer.go`:
    - 基于 `franz-go`,共享 client
    - 启动时建连(配置来自 `--kafka-brokers`)
    - 异步批量:`linger=50ms, batch=1MB, acks=all, enable.idempotence=true, compression=zstd`
    - **Produce 接口签名与语义**:
      ```go
      // Produce 语义: 入内部有界 channel 即返回 nil error,异步批量由后台 goroutine
      // 真正投递到 Kafka;真正的错误通过 `gateway_produce_errors_total{reason}` 指标
      // 和可选 callback 反馈。ctx 只用于 channel 写入(非阻塞时 abort)和优雅停机信号,
      // 不会同步等待 broker ack。
      //
      // 同步等待必须用 Flush(timeout),仅在停机 + WAL 落盘场景使用。
      func Produce(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error
      func Flush(timeout time.Duration) error  // 阻塞等待所有 in-flight 消息 ack 完成
      func Close() error                      // 关闭 client,等所有消息 ack
      ```
    - **Headers**:`map[string]string`,常规放 `traceparent`、`business`、`source_dc`、`ingest_ts` 等元数据;**T1.12 接入 OTel 时填 traceparent**
    - **启动行为**(对应 T1.8 WAL 实现状态):
      - **WAL 未启用(Phase 1 早期)**:探测失败 → 写错误日志并退出(`EXIT_FATAL`),由 systemd `Restart=always` 拉起,失败间隔 5s 内 3 次后触发告警
      - **WAL 已实现(T1.8 完成后)**:探测失败 → 写 warn 日志,以 **WAL 启动模式**运行(写入本地磁盘,后台 goroutine 持续重连),**进程不退出**
    - **Channel 满 → 503**:Produce 在 channel 满时阻塞(等待空闲槽),如果超过 `produce_block_timeout`(默认 100ms)则返回 `ErrProduceBackpressure`,receiver 将其映射为 503(spec 6.1)
  - `internal/kafkasink/producer_test.go`:用 testcontainers 启动 Kafka,验证消息送达、headers 透传、503 行为、Flush 语义

- [ ] **T1.8** 实现磁盘 WAL(spec 6.2 第三道防线)
  - **配置**(默认,可在 `/etc/prom-gw/config.yaml` 覆盖):
    | 配置项 | 默认值 | 说明 |
    |---|---|---|
    | `wal.dir` | `/data/wal/` | WAL 数据目录(独立挂载,避免与系统盘共抢 IO) |
    | `wal.segment_bytes` | `64MB` | 单个 segment 文件大小 |
    | `wal.max_bytes` | `50GB` | WAL 总字节上限(到达后切硬拒绝) |
    | `wal.disk_used_ratio` | `0.80` | 磁盘总使用率硬阈值(到达后切硬拒绝) |
    | `wal.sync_mode` | `every_batch` | 写盘策略:`every_batch`=`fsync` per batch(默认,安全),`interval`=每 N ms `fsync`(吞吐) |
    | `wal.replay_concurrency` | `4` | 重放并发数 |
  - `internal/wal/wal.go`:
    - 接口:
      ```go
      type WAL interface {
          Write(ctx context.Context, topic, key string, payload, headers []byte) error
          Replay(ctx context.Context, handler func(topic, key string, payload, headers []byte) error) error
          Bytes() int64        // 当前占用字节
          OldestAge() time.Duration  // 最老未确认 segment 的存活时长
          Close() error
      }
      func New(cfg Config) (WAL, error)
      ```
    - 存储:分段文件 `<dir>/seg-<ts>-<seq>.log`,每段 64MB,段内顺序写;写满后 `fsync` + 关闭 + `atomic rename` 到 `seg-<ts>-<seq>.log.sealed`
    - **写盘格式**:`[4B length][8B ts][1B flags][topic_len(topic)][key_len(key)][payload][headers_len(headers)][headers]`,**追加 CRC32 校验** 段尾防撕裂
    - **重放**:启动时按 mtime 顺序逐段读出(过滤 `.tmp`),调 `handler` 投递到 Kafka;**handler 返回 nil 才 truncate 该段**;失败重试 + 退避,失败累计超 `max_replay_failures`(默认 10)则告警
    - **容量控制**:**双阈值硬拒绝** — `WAL 字节 ≥ max_bytes` **或** `磁盘使用率 ≥ disk_used_ratio` → 返回 `ErrWALFull`,receiver 映射 503(spec 6.1)
    - **回环清理**:已确认段(`.sealed` 且 `replay_done=true`)由后台 goroutine 每 60s 清理一次
  - `internal/wal/wal_test.go`:正常写读、磁盘满拒绝、crash 后重放一致性、CRC 校验失败、segment 轮转、并发写安全
  - **main 启动顺序**:
    1. `kafkasink.New()` 探测连接
    2. 探测成功 → **直连模式**(kafkasink 接收)
    3. 探测失败 → `wal.New()` + **WAL 启动模式**(直接落盘,后台 goroutine 持续 `kafkasink.New()` 重连,成功后 drain WAL)
    4. 直连模式运行中,kafkasink 连续失败 N 次(默认 3,间隔 1s)→ 切换为 WAL 模式,直到恢复
  - **systemd 配套**:`/data/wal` 必须独立挂载或 `Type=tmpfs` 不可,部署文档强制要求

- [ ] **T1.9** 串联 6 阶段(空规则版)
  - `cmd/prom-gw/main.go`:
    - 启动 receiver → 阶段 channel(`chan []Sample`,容量 65535)→ parser → (空 pipeline)→ **sink adapter**(包装 kafkasink + WAL)
    - **sink adapter 逻辑**(由 T1.7 + T1.8 共同实现):
      ```go
      type Sink interface {
          Send(ctx context.Context, batch []Sample) error  // 返回 ErrBackpressure → 503
      }
      // 内部: 先尝试 kafkasink.Produce,失败 N 次(默认 3) 切 WAL.Write
      // kafkasink 恢复后,drain WAL → kafkasink 接管
      ```
    - 出口前加 WAL 包装(Kafka 失败时转 WAL,Kafka 恢复后 drain WAL)
    - 满 channel → 503(spec 6.1)

- [ ] **T1.10** 集成测试
  - `test/integration/passthrough_test.go`:
    - testcontainers 启 Kafka
    - 启动 `prom-gw` 子进程
    - 模拟 `prometheus.WriteRequest` 推入
    - 消费 Kafka 验证消息字节级相等 + headers 中 traceparent 存在
  - 覆盖 happy path / 401 / 400 / 503(模拟 channel 满)/ WAL(模拟 Kafka 宕机,验证落盘与重放)
  - `test/integration/wal_test.go`:Kafka kill → 写 5s → Kafka 恢复 → 验证落盘数据被消费

- [ ] **T1.11** 端到端手动验证脚本
  - `test/manual/e2e.sh`:
    - 启动 Kafka + GW
    - 启动一个测试用 Prometheus
    - `prometheus.yml` 配 `remote_write: [http://localhost:19201/api/v1/write]`
    - 跑 `promtool query instant` 验证指标采集
    - 消费 Kafka 验证

- [ ] **T1.12** 接入 OpenTelemetry 基础
  - `internal/obs/tracing.go`:OTLP exporter 初始化
  - 在 receiver middleware 注入 span,decoder / parser / kafka / wal 内部也开 span
  - **TraceID 写入 Kafka message header `traceparent`**(通过 T1.7 的 headers 参数)
  - request-scoped TraceID 通过 `context.Context` 在 6 阶段间传递,不存进 Sample

- [ ] **T1.13** 暴露第一批指标
  - `internal/obs/metrics.go`:定义 spec 7.1 中列出的指标
  - 阶段级指标:`gateway_samples_total{stage, business, status}`,`gateway_stage_duration_seconds`
  - 错误指标:`gateway_errors_total{stage, type}`
  - 资源指标:`gateway_goroutines`(用 `runtime.NumGoroutine`)
  - **WAL 指标**:`gateway_wal_bytes`, `gateway_wal_oldest_age_seconds`, `gateway_wal_hard_reject_total`

**Verification**:

```bash
make test                                # 全部单测 + 集成测试通过
make test-integration                    # testcontainers 集成测试,含 WAL
bash test/manual/e2e.sh                  # 端到端真 Kafka 验证
# WAL 场景:停掉 Kafka,继续写入 30s,启动 Kafka,验证落盘数据被消费
# 简易 sample load gen(用 test/loadgen/client.go):
go run ./test/loadgen --rate=50000 --duration=30s
```

**Done when**:
- M1 验收:`prometheus remote_write → GW → Kafka` 端到端跑通
- 1 个真实 Prometheus 实例持续写入 30 分钟,无丢失
- 指标/日志/trace 完整
- 单测覆盖率 ≥ 60%

---

### Phase 2 (M2): 规则引擎 v1(2 周)

**Goal**:支持 `relabel`、`route`、`sample` 三类规则,完整 pipeline 跑通。

**Tasks**:

- [ ] **T2.1** 规则模型定义
  - `internal/ruleengine/types.go`:
    ```go
    type RuleSet struct {
        Name         string
        Business       string
        DefaultTopic string   // 没路由命中时的兜底 topic
        // InputTopic 字段: GW 不消费 Kafka,故不持有
        // 仅在 spec 5.1 文档中描述"该 ruleset 处理哪类入站数据",运行期不参与逻辑
        Stages       []Stage
        Version      int64
    }
    type Stage struct {
        Type   string
        Config yaml.Node
    }
    type CompiledRuleSet struct {
        RuleSet
        Stages []CompiledStage
    }
    ```

- [ ] **T2.2** 规则加载与编译
  - `internal/ruleengine/compiler.go`:
    ```go
    func Compile(rs *RuleSet) (*CompiledRuleSet, error)
    ```
  - 验证规则合法性
  - 预编译正则、计算 scope 索引
  - 编译失败 → 返回详细错误(行号)

- [ ] **T2.3** Stage 接口
  - `internal/ruleengine/stage.go`:
    ```go
    type Stage interface {
        Name() string
        Apply(ctx context.Context, in []Sample, cfg CompiledStage) ([]Sample, StageStats, error)
    }
    type StageStats struct { In, Out, Dropped int; DurationMS int64 }
    ```
  - 统一的 panic 捕获(在 Apply 入口 defer recover)

- [ ] **T2.4** 实现 Relabel stage
  - `internal/ruleengine/stages/relabel.go`:
    - 支持 `drop_labels`, `keep_labels`, `label_map`
    - 删除/重写 label 集合
    - 大 label 集合(>32)不抛错,只 metric
  - 单测:fixtures(每种操作一个 case)

- [ ] **T2.5** 实现 Route stage
  - `internal/ruleengine/stages/route.go`:
    - `match` 字典匹配 label
    - 命中 → 设置 sample.TargetTopic
    - 不命中 → `default_topic`
    - 同一 sample 只能路由到一个 topic
  - 单测:多 match case、default、metric

- [ ] **T2.6** 实现 Sample stage
  - `internal/ruleengine/stages/sample.go`:
    - `rate` 0.0-1.0,基于 `math/rand` 随机丢弃
    - 每个 pipeline goroutine 持自己的 `*rand.Rand`(避免锁)
  - 单测:rate=0.1 在 10000 sample 上分布 ±1%

- [ ] **T2.7** Pipeline 调度器
  - `internal/ruleengine/pipeline.go`:
    ```go
    type Pipeline struct {
        // rules 字段必须是 atomic.Pointer,Run 方法每批处理前 Load()
        // 才能支持热切换(普通指针无法并发安全切换,见 T2.10)
        rules    atomic.Pointer[CompiledRuleSet]
        in       <-chan []Sample
        out      func(ctx context.Context, batch []Sample, targetTopic string) error
        outTopic func(s Sample) string  // 按 sample.TargetTopic 选 topic
    }
    func NewPipeline(in <-chan []Sample, out func(...) error) *Pipeline
    func (p *Pipeline) SetRules(rs *CompiledRuleSet)  // atomic.Store
    func (p *Pipeline) Rules() *CompiledRuleSet       // atomic.Load
    func (p *Pipeline) Run(ctx context.Context)
    ```
  - 串行执行 stages(无锁)
  - `Run` 每批处理前 `p.rules.Load()` 拿到当前版本;`SetRules` 立即生效,正在跑的批次用旧版本完成
  - 指标:`gateway_ruleset_processed_total{ruleset, stage, version}`(带 version 便于回滚后比对)

- [ ] **T2.8** Pipeline 路由分发
  - `cmd/prom-gw/main.go`:
    - 入口 channel → fanout 到 N 条 pipeline(per ruleset)
    - 出口 → 按 sample.TargetTopic 投递到 `kafkasink.Produce`

- [ ] **T2.9** 规则配置文件 + 示例
  - `configs/rules/app-business.yaml`(对应 spec 5.1)
  - 启动时加载,失败致命退出

- [ ] **T2.10** 基础热更新(本地文件)
  - `internal/config/watcher.go`:
    - 监听 `configs/rules/*.yaml` 文件变化(fsnotify)
    - 重新编译 + 校验
    - 通过 `atomic.Pointer` 切换
    - 校验失败 → 保留旧版本,告警
    - 保留最近 10 个版本用于回滚(与 T4.6 统一)
  - `internal/config/watcher_test.go`:写入新规则 → 验证切换;写入非法规则 → 验证不切换

- [ ] **T2.11** 集成测试
  - `test/integration/rule_test.go`:
    - 启动 GW + Kafka
    - 写入带噪声 label 的指标
    - 验证 relabel 删除了目标 label
    - 验证 route 把特定 metric 路由到对应 topic
    - 验证 sample 丢弃比例

**Verification**:

```bash
make test                                     # 全部通过
bash test/manual/rule_e2e.sh                  # 真实数据走完三种规则
# 用自研 loadgen 压 200K samples/s(vegeta 是 HTTP 压测,不能控制 sample 数)
go run ./test/loadgen --rate=200000 --duration=60s
```

**Done when**:
- 三种 stage 全部工作
- 配置文件热更新生效
- 单条规则失败不影响其他
- 单元测试覆盖率 ≥ 65%

---

### Phase 3 (M3): 高级 stage + 状态(1 周)

**Goal**:补全 `downsample`、`deadvalue`、`enrich`,完善规则引擎的"状态型"能力。

**Tasks**:

- [ ] **T3.1** 实现 Enrich stage
  - `internal/ruleengine/stages/enrich.go`:
    - 支持静态 label、模板字符串 `${labels.env}`、常量
  - 单测

- [ ] **T3.2** 实现 Downsample stage(状态型)
  - `internal/ruleengine/stages/downsample.go`:
    - 按 `interval`(1m, 5m, ...)桶聚合
    - `aggregations`: `avg`, `max`, `min`, `sum`, `p50`, `p99`
    - p99 **先用 P² 算法自实现**(`pkg/p2`),Phase 3 末做 benchmark;如果误差 > 1% 或内存 > 500MB/百万 series,再升级到 `github.com/influxdata/tdigest`(评估结果写进 `docs/decisions/`)
    - 内存索引:`map[seriesKey]*Bucket` + 定时 flush
    - 状态通过 `atomic.Pointer` 切换(老状态排空)
  - 单测:多桶 flush、P² 精度、series 消失处理

- [ ] **T3.3** 实现 DeadValue stage
  - `internal/ruleengine/stages/deadvalue.go`:
    - 跟踪 series 最后值与最后时间
    - `window` 内值未变 → 丢弃
    - 用 LRU 控制内存(spec 默认 1M series)
  - 单测

- [ ] **T3.4** 状态持久化(可选,M3 内)
  - 状态全内存,重启丢失(由 Prometheus 短期重传补)
  - 记录 metric `gateway_state_series{ruleset}` 监控规模

- [ ] **T3.5** enrich + downsample + deadvalue 集成测试
  - 端到端跑 30 分钟,验证清洗效果

**Verification**:

```bash
make test                                  # 全部通过
bash test/manual/stages_e2e.sh             # 状态型 stage 行为验证
# metric 校验
curl localhost:8080/metrics | grep gateway_state_series
```

**Done when**:
- 6 个 stage 全部可用
- 状态型 stage 内存可控(10 分钟压测下 OOM 不发生)
- 整体单测覆盖率 ≥ 65%

---

### Phase 4 (M4): 配置中心 + Admin API + 热更新(1 周)

**Goal**:对接 Nacos,补全 Admin API,完成端到端的配置管理闭环。

**Tasks**:

- [ ] **T4.1** Nacos client 封装
  - `internal/config/nacos.go`:
    - 启动时拉取 `dataId=prom-gw-rules, group=GATEWAY`(同步,失败致命退出)
    - 解析为 `[]RuleSet` 列表
    - **长轮询**:使用 nacos-sdk-go 默认 10s 长轮询(`LongPollTimeout = 10 * 1000ms`,由 SDK 在 `ConfigService.Listen` 内部封装 TCP 长连接,服务端 hold 30s 推送变更),**勿手动用 2s 短轮询**;启动时主动 query 一次作为 warm-up
    - 失败保留本地最后一次成功的版本(spec 6.4)
    - 持久化 `last_good_snapshot` 到 `/data/nacos_snapshot.json`,下次启动时优先恢复
  - 单测:用 mock 替代 nacos 验证拉取/切换/降级;长轮询 mock 推送;冷启动从 snapshot 恢复

- [ ] **T4.2** 配置源优先级
  - 启动顺序:Nacos → 本地文件 → 内置默认
  - Nacos 不可用时降级到本地(且告警)
  - 实现 `internal/config/manager.go` 抽象

- [ ] **T4.3** Admin API 路由
  - `internal/admin/server.go`:chi 路由,监听 `:8082`
  - 中间件:recover + tracing + 统一响应包装 + **来源 IP 白名单**(`--admin-allow-cidr`,默认 `127.0.0.1/32,10.0.0.0/8`,见 Risks 表)
  - 不命中白名单 → 403 + 记录 `gateway_admin_auth_fail_total{source_ip}`

- [ ] **T4.4** 统一响应 + 错误码
  - `pkg/httpx/response.go`:
    ```go
    type Response struct {
        Code int `json:"code"`
        Message string `json:"message"`
        Data any `json:"data,omitempty"`
        TraceID string `json:"trace_id"`
    }
    func OK(w, data); func Err(w, code, msg, httpStatus)
    ```
  - 业务码:公共 1000-1999,GW 4000-4999
  - 错误码定义在 `internal/admin/codes.go`

- [ ] **T4.5** Admin API 实现
  - `PUT    /v1/rulesets/{name}` — 创建/替换
  - `GET    /v1/rulesets/{name}` — 读
  - `GET    /v1/rulesets`        — 列表(含每条 ruleset 的 version)
  - `POST   /v1/rulesets/{name}:reload` — 强制重载
  - `POST   /v1/rulesets/{name}:rollback?to_version=N` — 回滚
  - `GET    /v1/rulesets/{name}/history` — 历史版本
  - `GET    /v1/healthz` — 复用
  - `GET    /v1/businesss` — 列出当前生效的 token→business 映射
  - `GET    /v1/stats` — 运行时统计(per ruleset QPS/drop rate)

- [ ] **T4.6** 历史版本存储
  - `internal/config/history.go`:
    - 内存 ring buffer(最近 10 版)
    - 每个版本:RuleSet 字节 + 编译产物 + 切换时间
  - Rollback 时按 version 取回,走校验 + 原子切换

- [ ] **T4.7** API 文档(OpenAPI spec + chi adapter)
  - 不使用 `swaggo/swag`(主要为 gin/echo 设计,chi 适配差)
  - 流程:`api/openapi/admin.yaml` 手工写 OpenAPI 3.0 spec → `make codegen` 用 `oapi-codegen` 生成 chi router/types → 业务 handler 只填函数体
  - 文档站:`make docs` 用 `redocly/cli` 把同一份 spec 渲染为 `docs/api/index.html`
  - 每个 endpoint 必带: 完整 request/response schema、错误码枚举、TraceID 说明、Auth 要求

- [ ] **T4.8** Nacos 集成测试
  - `test/integration/nacos_test.go`:
    - testcontainers 启 Nacos(或用 nacos-standalone 镜像)
    - push ruleset → 验证切换
    - 修改 ruleset → 验证切换
    - Nacos 宕机 → 验证降级到本地

- [ ] **T4.9** Admin API 集成测试
  - `test/integration/admin_test.go`:
    - httptest 覆盖各 endpoint
    - 验证错误码、TraceID、回滚行为

**Verification**:

```bash
make test
# 手动:推一份 ruleset 到 Nacos
curl -X POST http://nacos:8848/nacos/v1/cs/configs \
  -d "dataId=prom-gw-rules&group=GATEWAY&content=..."
# GW 自动 reload
curl http://gw:8082/v1/rulesets | jq   # 看到新 ruleset
curl -X POST http://gw:8082/v1/rulesets/foo:rollback?to_version=1
```

**Done when**:
- Nacos push 5 秒内生效
- Admin API 全功能 + 文档
- 降级行为正确
- 单元测试覆盖率 ≥ 65%

---

### Phase 5 (M5): 性能、混沌、文档(1 周)

**Goal**:达到 spec 性能基线,完成可观测大盘,产出上线文档。

**Tasks**:

- [ ] **T5.1** 限流细化
  - 增加 per-business 限流器(动态下发)
  - 监控:`gateway_rate_limit_rejected_total{business}`

- [ ] **T5.2** Kafka 写入优化
  - Producer 调优(linger, batch)基准测试
  - franz-go 异步 Produce 调成 batch 模型
  - 验证 zstd 压缩比

- [ ] **T5.3** 内存优化
  - sync.Pool 复用 buffer
  - `runtime.SetGCPercent` 调到合适值(默认 100,先看 profile 再调)
  - 大 label map 用 `slices.SortFunc` + `string(b)` pool

- [ ] **T5.4** 性能压测脚本
  - `test/perf/load.go`:
    - 自研 Prometheus-like client
    - 模拟 1.5M samples/s 持续 1 小时
    - 输出 p50/p95/p99,错误率,CPU/Mem profile
  - `test/perf/profile.sh` 一键执行 + 采集
  - 目标:**1.5M samples/s, p99 < 500ms, CPU < 70%, Mem < 8G, 错误率 < 0.01%**

- [ ] **T5.5** 混沌测试
  - `test/chaos/`:用 `chaos-mesh` 或自写脚本
  - 场景:杀 GW 实例、杀 Kafka、网络分区(用 toxiproxy)、CPU 打满、磁盘满
  - 验证恢复时间 + 数据不丢

- [ ] **T5.6** 监控大盘
  - `deploy/grafana/dashboards/prom-gw.json`
  - Panel:吞吐、延迟(p50/p99)、错误率、背压拒绝率、WAL 大小、规则状态、版本号、Kafka lag
  - 告警规则:`deploy/grafana/alerts/prom-gw.yaml`
    - 错误率 > 1% 持续 5 分钟
    - p99 > 1s 持续 5 分钟
    - WAL oldest > 60s(由 T1.8 引入)
    - WAL 硬拒绝率 > 0(`gateway_wal_hard_reject_total` 增量)
    - ~~任意 ruleset version 没递增 > 1h~~(删除:稳定 ruleset 不递增是正常状态,告警会一直触发)

- [ ] **T5.7** 运维文档
  - `docs/operations/`
    - `deploy.md`:Ansible 用法、滚动升级、配置变更
    - `troubleshooting.md`:常见问题(WAL 卡住、版本不切换、QPS 不达标)
    - `runbook.md`:故障响应剧本
    - `slo.md`:可用性/性能/数据完整性 SLO

- [ ] **T5.8** 用户文档
  - `docs/user/`
    - `quickstart.md`:5 分钟接入
    - `ruleset-reference.md`:规则字段详细说明
    - `examples/`:常见用例(按团队路由、压测、敏感数据脱敏)

- [ ] **T5.9** 兼容性测试
  - `test/compat/`:
    - 跑 Prometheus v2.40 / v2.45 / v2.50 / latest
    - 跑 Cortex remote_write 客户端
    - 跑 VM remote_write 客户端
  - 记录在 `docs/compatibility.md`

- [ ] **T5.10** 流水线发布
  - `make release`:build + lint + test + 打包 tar.gz
  - `Makefile` 加 `version` 参数(用 git tag)
  - Ansible 通过 `prom_gw_version` 变量支持版本回退

**Verification**:

```bash
make test && make lint && make build
make perf                    # 1.5M samples/s × 1h 全过
make chaos                   # 混沌测试全过
# 加载 grafana 大盘,验证指标可见
```

**Done when**:
- 性能基线达标(spec 第 8 节)
- 混沌测试全过
- 监控大盘 + 告警上线
- 文档齐备
- 整体单测覆盖率 ≥ 60%(用户规范硬性要求)

---

## Cross-Phase Standards

贯穿所有 phase 的硬性要求(用户规范):

1. **错误处理**:`_ =` 一律禁止,每个 error 必须处理或向上抛
2. **链路追踪**:每次外部调用/IO 必带 `context.Context`,TraceID 透传
3. **Panic recovery**:所有 goroutine + HTTP middleware 必带 recover
4. **配置管理**:`config` 包集中管理,flag 启动时只覆盖开发态
5. **敏感信息**:token、key 禁止出现在日志
6. **API 文档**:每个新接口必带 OpenAPI 注释
7. **单测**:每个新函数必带单测,新功能覆盖率不低于 phase 总目标
8. **Lint**:`make lint` 0 error 才能 commit

## Risks & Open Questions

| 风险/未决 | 影响 | 建议 |
|---|---|---|
| P² vs t-digest 算法选型 | 选型影响 downsample 输出 | Phase 3 末 benchmark(误差 < 1% 且内存 < 500MB/百万 series 才升级 t-digest) |
| Kafka client 性能基线 | 决定单机最大吞吐 | Phase 1 末压测 franz-go,如果不达标考虑 confluent-kafka-go (CGO) |
| Nacos 降级行为 | 中心 Nacos 故障影响 | Phase 4 加 `last_good_snapshot` 持久化到 `/data/nacos_snapshot.json`,启动时优先 |
| 跨机房专线带宽 | 中心 Kafka 写入带宽上限 | M5 末压测验证后,出"按机房专线规划"文档 |
| 旧 Prometheus 版本(<=2.30)兼容性 | 解码字段可能不同 | Phase 5 才覆盖,优先 v2.40+ |
| 状态型 stage 重启数据丢失 | Downsample/DeadValue 状态全内存 | M3 留 TODO,M5 视情况上 BoltDB 持久化 |
| 单实例 WAL 与 LB 重启协调 | 重启时 in-flight 数据的归属 | 已用 LB 摘流 + Flush + Close 顺序;需要混沌验证 |
| 多语言客户端兼容 | Thanos / VM / Mimir agent 接入 | Phase 5 兼容性测试覆盖 |
| **并行化策略缺失** | 2-3 人 4 周压缩依赖并行 | 依赖图:Phase 1 串行完成后,Phase 2 (rule engine) ‖ Phase 4 (admin API + Nacos)可全并行;Phase 3 必须 Phase 2 后;Phase 5 文档/压测可与 Phase 4 部分并行 |
| **依赖与工具链不完整** | 实施中可能漏装 | 完整列表见 T0.2 + 各 phase 增量:`fsnotify` (T2.10)、`p2` 自实现 (T3.2)、`oapi-codegen` + `redocly/cli` (T4.7)、`chaos-mesh` + `toxiproxy` (T5.5)、`prometheus/client_golang/api/promv1` (T5.9) |
| **sample load gen 工具不匹配** | vegeta 是 HTTP 压测,不能精确控制每请求 sample 数 | Phase 1 起在 `test/loadgen/client.go` 自研 client(可控 `--rate` `--samples-per-batch`),Phase 5 性能压测复用 |
| **Sample 内存模型** | 1.5M samples/s 持续下 string 分配会爆 GC | TraceID 走 request-scoped context(T1.12);Business/SourceDC 字符串 intern 池;T1.2 类型定义已加注释,实施时 `pkg/stringpool` 实现 |
| **Prometheus server 假设** | T5.6 大盘需要外部 Prometheus 抓 GW /metrics | 假设运维已部署 Prometheus(由 `docs/operations/deploy.md` 列出),不在本项目范围 |
| **GitHub Actions CI 假设** | 仓库可能在 GitLab/自建 Gitea | T0.5 写 GitHub Actions 作默认,如不是 GitHub 再补 GitLab CI |
| **WAL 目录 IO 隔离** | 系统盘与 WAL 盘共抢 IO 导致 fsync 延迟 | 部署文档(`/docs/operations/deploy.md`)强制要求 `/data/wal` 独立挂载 SSD,IOPS ≥ 5K;ansible role 加挂载断言 |
| **限流维度选择** | 全局 vs per-business 限流位置选错,导致被攻击business拖垮所有 | T1.4 全局 100K/s(防单实例) + T5.1 per-business 动态下发(防单business);两者并存,per-business 默认 80K/s |
| **规则 history 内存增长** | 长生命周期下 in-memory history 膨胀 | T4.6 用 ring buffer 限 10 版 + 单版 ≤ 1MB YAML,超限 LRU 驱逐;`gateway_ruleset_history_size` 指标 |
| **OTel SDK 自身开销** | 高吞吐下 SDK 自身 CPU/内存占大头 | T1.12 选用 BatchSpanProcessor(默认),不启用 Console exporter;Phase 5 末做 profile,如 SDK > 5% CPU 改用 lightweight tracer 抽象 |
| **Pipeline 切换时的批次丢失** | 切换瞬间 in-flight 批次被丢弃 | T2.7 旧规则跑完当前批次才生效新规则(per-batch load);切换过程中 `gateway_ruleset_switch_total{from_version, to_version, reason}` 计数,reason ∈ {nacos, file, api} |
| **Admin API 鉴权** | 8082 暴露在内网可被误调 | T4.3 启动参数加 `--admin-allow-cidr`(默认 `127.0.0.1/32,10.0.0.0/8`),由 chi middleware 校验来源 IP,记录 `gateway_admin_auth_fail_total` |
| **配置中心 snapshot 一致性** | 冷启动时 snapshot 与 Nacos 不一致产生回滚幻觉 | T4.1 snapshot 持久化时同时记录 Nacos `md5`,加载时优先用 snapshot + 启动后再 fetch 一次校正,版本号对齐后才提交 |

## Definition of Done

- [ ] 所有 Phase 0-5 完成,各 phase 验收命令全过
- [ ] 整体单测覆盖率 ≥ 60%
- [ ] 性能基线达标(1.5M samples/s × 1h, p99 < 500ms)
- [ ] 混沌测试 100% 通过
- [ ] 监控大盘 + 告警上线(已部署到 Grafana)
- [ ] 所有用户/运维文档齐备
- [ ] Spec 全部验收项落地
- [ ] 一次灰度上线(单机房切 10% 流量观察 24 小时)无异常
- [ ] Code review 通过 2 人 approval
- [ ] 主干合并,tag 标记 `v1.0.0`
- [ ] `CHANGELOG.md` 按 conventional commits 整理,列出所有 breaking change
- [ ] 文档 review 一次(README + DEVELOPING + operations + user + runbook 全套),issue 全部关闭
- [ ] 安全自评:SQL 注入 / XSS / 命令注入 / 敏感信息泄漏 / 接口越权 五项 checklist 全过
- [ ] 资源回收:无 goroutine 泄漏(`goleak` 检测)、无 fd 泄漏(启动 24h 后 `lsof -p $PID` 增量 < 100)

## Suggested Schedule

| 阶段 | 工时(单人) | 累计 |
|---|---|---|
| Phase 0 | 2-3 天 | 3d |
| Phase 1 (M1,含 WAL) | 2.5 周 | 3w 1d |
| Phase 2 (M2) | 2 周 | 5w 1d |
| Phase 3 (M3) | 1 周 | 6w 1d |
| Phase 4 (M4) | 1 周 | 7w 1d |
| Phase 5 (M5) | 1 周 | 8w 1d |

2-3 人团队可压到 5 周。优先砍 Phase 5 的兼容性测试广度(只覆盖主要 Prometheus 版本),保核心 0-4 全过。Phase 2 (rule engine) ‖ Phase 4 (admin API + Nacos) 是主要并行加速点。

---

## Future: IAM 接入扩展点(本计划不实施,留作后续项目)

> 范围声明:`prom-gw` v1.0 仅做本地 Token 校验;接入公司 IAM 体系是另一项目,这里只留**扩展契约**,确保未来切换不破坏现有接口。

### F.1 当前实现(T1.3 + T1.4)

- `internal/config.TokenStore` 持有 `token → BusinessInfo` 内存映射
- `receiver.Auth` 中间件从 `Authorization: Bearer <token>` 取 token,查表得 `business/default_topic/rate_limit`
- 启动时加载 `configs/tokens.yaml`,SIGHUP 重载
- **未做**:签名验证、过期、吊销、审计

### F.2 未来 IAM 接入要替换的边界(本计划已刻意留口)

| 当前实现 | 未来 IAM 实现 | 替换点 |
|---|---|---|
| `internal/config.TokenStore`(内存 map) | `internal/auth.Authenticator` 接口(本地实现 + IAM 实现并存) | receiver.Auth 中间件改为调 `Authenticator.Verify(ctx, token)` |
| `Bearer <token>` 头 | 可能加 `Cookie`/`mTLS`/`OIDC JWT` | 仅改 Auth 中间件,下游 `Meta.Business` 来源不变 |
| 启动加载 YAML | 启动时 JWKS 拉取 + 周期刷新 | 新增 `internal/auth/jwks.go`,鉴权失败有缓存回退 |
| SIGHUP 重载 tokens.yaml | IAM 端点 / Nacos 推 token 列表 | 删除 `configs/tokens.yaml`,走 Nacos 配置中心(与 T4.1 复用) |

### F.3 提前预留的接口(本计划实施时必须遵守)

1. **Authenticator 接口草案**(`internal/auth/authenticator.go`,本计划**只占位不实现**):
   ```go
   package auth

   type Authenticator interface {
       // Verify 返回 business 与默认 topic;err != nil → 401
       Verify(ctx context.Context, token string) (Business, error)
   }
   type Business struct {
       Name         string
       DefaultTopic string
       RateLimit    int
   }
   ```
   当前 v1 实现 `LocalTokenAuthenticator` 内嵌在 `internal/config` 即可,但**函数签名应与 F.3 一致**,未来不需改 receiver。

2. **Business 元数据扩展**:在 `internal/parser.Meta`(T1.6)中预留 `BusinessID string`(用于审计、上报到 IAM),当前空字符串即可。

3. **审计日志**:在 `Auth` 中间件失败分支预留 `auth_fail_total{reason}` 指标点,reason ∈ {missing, invalid, expired, revoked, iam_unavailable};v1 只打 `invalid`。

4. **Admin API 调用者身份**:T4.3 的 IP 白名单是过渡方案,未来 IAM 接入时改为 mTLS + IAM 颁发的 service account token;接口契约(`PUT/GET /v1/rulesets`)不变。

### F.4 切换清单(后续 IAM 项目启动时直接照搬)

- [ ] 引入 `github.com/coreos/go-oidc/v3`,实现 `OIDCAuthenticator`(JWKS 自动刷新)
- [ ] 删除 `internal/config/token.go` 的硬编码 tokens 加载逻辑
- [ ] receiver.Auth 切到 `Authenticator.Verify`,本地模式保留为 `--auth-mode=local` 兜底
- [ ] Admin API 增 `Authorization: Bearer <iam_token>`,校验 audience = `prom-gw-admin`
- [ ] 引入 `gateway_auth_latency_seconds` Histogram,跟踪 IAM 端点延迟
- [ ] 文档新增 `docs/user/auth.md`,说明 token 申请 / 轮换 / 吊销流程

### F.5 不在本计划的可观测/安全收益

- 集中审计(token 用量、调用方 IP、限流命中可上送 IAM)
- 细粒度 RBAC(目前只到 business 粒度)
- token 轮换自动化(目前只支持 SIGHUP 全量重载)
- 跨服务统一登录态(目前各服务独立 token)
