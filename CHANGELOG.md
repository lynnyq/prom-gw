# Changelog

所有对 `prom-gw` 的显著变更都记录在这里。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/),
版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。本项目尚未发版,所有变更仍属 `Unreleased`。

## [Unreleased]

### Added
- **核心网关(`prom-gw`)**:`cmd/prom-gw/main.go` 端到端串联,实现 RemoteWrite → decoder → parser → rule engine → sink → Kafka/WAL。
- **接收层**:`internal/receiver` 暴露 `:19201/api/v1/write`,含鉴权、限流、Recoverer、TraceID 注入、租户限流。
- **解码/解析**:`internal/decoder`(snappy+protobuf)、`internal/parser`(WriteRequest → Sample,带 `stringpool` 复用)。
- **WAL**:`internal/wal` 段式写入、CRC32 校验、`max_bytes` 硬拒绝、Reopen 重放一致性。
- **Kafka 写入**:`internal/kafkasink` 基于 `franz-go`,启停/批量/压缩/幂等。
- **Sink 适配器**:`internal/sink` 包装 Kafka + WAL,失败自动切换、恢复后 drain。
- **6 阶段 Pipeline**:`internal/sink.Pipeline` 有界 channel,背压即 503。
- **规则引擎**:`internal/ruleengine` 支持 `relabel / route / sample / enrich / downsample / deadvalue` 六类 stage。
- **多 ruleset 编排**:`internal/ruleengine.Manager` + `internal/router`,按 Metric 特征 fan-out 到独立 Pipeline。
- **配置中心**:`internal/config` 支持 Nacos / 本地文件 / 默认源三层优先级,long-poll 监听 + last-good snapshot 持久化。
- **历史版本**:`internal/config.History` ring buffer,支持回滚到 N 版本。
- **Admin API**:`internal/admin` `:8082`,提供 `rulesets / tenants / stats / healthz`,带 IP 白名单。
- **可观测**:`internal/obs` 提供 `/metrics`(Prometheus 格式)、OpenTelemetry OTLP、zap JSON 日志。
- **可观测辅助包**:`pkg/safego`(panic recover)、`pkg/tracex`(traceparent 注入/提取)、`pkg/stringpool`(高频字符串 intern)、`pkg/httpx`(统一响应包装)、`pkg/metrichelper`。
- **文档**:
  - 设计 `docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md`
  - 实施计划 `docs/superpowers/plans/2026-07-28-prometheus-multidc-remotewrite-gateway-plan.md`
  - 运维 `docs/operations/{deploy,runbook,troubleshooting,slo}.md`
  - 用户 `docs/user/{quickstart,ruleset-reference,auth}.md` + `docs/user/examples/{deadvalue,downsample,redact-sensitive,route-by-team}.md`
  - API `docs/api/openapi.yaml` + 渲染 `docs/api/index.html`
  - 兼容 `docs/compatibility.md`
  - Grafana 仪表盘 `deploy/grafana/dashboards/prom-gw.json`、告警 `deploy/grafana/alerts/prom-gw.yaml`
- **部署**:
  - systemd 单元 `deploy/systemd/prom-gw.service`
  - Ansible 骨架 `deploy/ansible/`(inventory、playbook、role、template)
- **混沌测试**:`test/chaos/chaos_test.go` + `test/chaos/chaos_runbook.md` + `test/chaos/run.sh`
- **压测**:`test/loadgen/main.go`(自研 Prometheus-like 客户端,精确控制 rate)
- **集成测试**:`test/integration/`(含 passthrough、rule、stages、wal、admin 套件,testcontainers Kafka)
- **兼容性测试**:`test/compat/`(Prometheus v2.40+ RemoteWrite 协议 + 矩阵冒烟)

### Changed

- `receiver` 引入 `requestIDMW + realIPMW + recovererMW + rateLimitMW + authMW + tenantRateLimitMW` 标准化中间件链,trace 注入位置提前以保证 trace_id 写日志。
- `kafkasink` 启动失败不再 fatal,降级到 WAL-only 模式运行(spec T1.7 + T1.8 行为闭环)。
- `ruleengine.Pipeline.rules` 升级为 `atomic.Pointer[CompiledRuleSet]`,运行期 `SetRules` 立即生效,正在跑批次用旧版本完成。
- 规则配置改用 `rulesets[]` 多 ruleset 形态,每 ruleset 独立 `default_topic / match / stages`,启动时注册到 `Manager`。
- **`docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md` §4.5 / §4.6 / §2.2.6**:StarRocks 存储模型从"三层 ROLLUP 方案"纠正为**三独立物理表 + 周期任务级联聚合方案**:
  - **纠正原因**:ROLLUP 物化视图与基础表共享分区生命周期,基础表分区 drop 时 ROLLUP 一并删除,无法实现"5m 存 7 天、1d 存 1 年"的多 TTL 需求
  - `sr_bj_metrics_5m`(5 min 聚合,Flink Stream Load 唯一跨城写入点,7 天 `dynamic_partition`)
  - `sr_bj_metrics_1h`(1 h 聚合,StarRocks 周期任务从 5m 表级联聚合,90 天)
  - `sr_bj_metrics_1d`(1 d 聚合,StarRocks 周期任务从 1h 表级联聚合,3 年)
  - 跨城流量:1 TB/天(占 1G 专线 9.3%,Stream Load gzip 压缩后)
  - StarRocks 3 副本物理:34.5 TB → **46.35 TB**(1h 表从 30 天延长到 90 天 + 三表独立开销)
  - 查询路由:从"CBO 自动透明改写"改为"应用层按时间范围选表"(CBO 不跨独立表路由)
  - 实际收益:**5 min 精度的跨城告警 + 事故复盘能力 + 多 TTL 独立管理**

- **`docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md` 全面审查修正(P0/P1/P2)**:
  - **P0-1 表模型矛盾**:三表 DDL 从 `DUPLICATE KEY` 改为 `PRIMARY KEY(ts, metric, tenant, business, ingest_city, source_dc, labels_hash)`,新增 `labels_hash`(XXH3 标量列)替代 MAP 作 PK 键列;周期任务从 `INSERT INTO` 改为 `INSERT OVERWRITE` 保证幂等;§4.6.3 / §6.3 / §11 同步修正去重描述
  - **P0-2 FE 角色容错**:2 Follower + 1 Observer → **3 Follower**(容忍 1 故障,多数派 2/3);§2.2.2 / §2.2.6 / §9.2 同步修正
  - **P0-3 跨城带宽口径**:明确 Stream Load 启用 HTTP gzip 压缩(660 GB 未压缩 → 345 GB gzip 后),跨城占比统一为 9.3%(原 10%);降级 2 数据量从 8.7 GB 改为 2.1 GB(与新方案 1d 表一致)
  - **P1 Flink state**:修正 t-digest 开销估算(原 5.8 GB → 32 GB,含 t-digest compression=100),给出 compression=50 调优建议
  - **P1 ClickHouse 兜底**:补充资源规划(32C/128G/2T SSD/城,schema 与 5m 表一致,双 sink 切换机制,回灌流程)
  - **P1 DLQ 留存**:从 3 天延长至 7 天,覆盖长故障
  - **P1 systemd template**:从 `@<city>` 改为 `@<city>-<instance>` 区分同城多实例
  - **P1 §2.4 描述**:修正"Flnk"拼写 + 删除"1m/少量明细"错误描述
  - **P2 小问题**:利用率 45%→44.7% 统一;"1.0 × 7"公式修正;"平均分布在 3 BE"改为"每 BE 持有一份完整副本";`gateway_cross_dc_bytes_total` 改为 `flink_cross_dc_bytes_total`(归 Flink 采集);Kafka producer 补 `max.in.flight.requests.per.connection=5`
- **`docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md` 架构图与拓扑图一致性审查**:
  - **§2.2.4 网络描述**:"Prometheus HA 双节点走主备 Keepalived" 修正为 "Prometheus remote_write 到 LVS VIP(LVS 双节点主备走 Keepalived)"——Keepalived 属于 LVS 而非 Prometheus
  - **§2.2.4 流量整形**:"L2a / L2b 明细严禁走跨城" 修正为 "原始 sample 明细(15s)严禁走跨城"——L2b 现为 StarRocks 周期任务,不再由 Flink 输出,不存在跨城一说
  - **§7.1 指标归属**:跨城写入 4 个指标(`gateway_stream_load_total` / `gateway_stream_load_duration_seconds` / `gateway_sr_dlq_messages` / `gateway_cross_dc_latency_seconds`)统一改为 `flink_*` 前缀并标注"由 Flink exporter 暴露"——prom-gw 不参与跨城链路;§9.2 专线监控同步修正
  - **§9.2 Flink JM 数量**:三城 Flink 从 "1 JM" 修正为 "2 JM"(1 Active + 1 Standby),与 §2.2.2 资源清单和 §2.2.1 拓扑图一致
  - **§6.3 / §11 Flink 去重键**:从 `tenant + metric + labels_hash + ts + ingest_city` 修正为 `ts + metric + tenant + business + ingest_city + source_dc + labels_hash`,与 StarRocks PK 完全对齐(原缺 `business` / `source_dc` 可能导致跨业务/跨机房同指标误去重)
  - **§4.6.2 行字节估算**:5m 表单行从 "≈ 125 字节" 修正为 "≈ 120 字节",与 §2.2.6 (4) 及日增计算公式(1000w × 288 × 120 B = 345 GB)一致
- **功能代码与设计文档一致性审核修复(P0×2 / P1×14 / P2×7)**:
  - **P0-1 WAL 故障转移失效**:`internal/sink/sink.go` `AdapterSink.Send` 在 `ErrProduceBackpressure`(Kafka broker 宕机 → channel 满)时直接返回 503,WAL 未触发;修正为 backpressure 时 fall through 到 WAL 写入,WAL 满才返回 503(§6.1)
  - **P0-2 成功状态码 204→200**:`internal/receiver/server.go` 从 `StatusNoContent` 改为 `StatusOK`,与 §4.2 流程图 `[200 OK → Prometheus]` 一致
  - **P1-1 Kafka delivery timeout/retries**:`internal/kafkasink/producer.go` 补 `RecordDeliveryTimeout(120s)` + `RecordRetries(10)`,对应 §6.3 `delivery.timeout.ms=120000` / `retries=10`(原 franz-go 默认 0=无限重试)
  - **P1-2 WAL active segment 恢复**:`internal/wal/wal.go` `scanExisting` 检测到未 seal 的 `.log` 文件时,读取有效记录(容错截断)→ 写 footer → 重命名为 `.sealed`,纳入 `Replay` 路径(§6.2 Reopen replay consistency)
  - **P1-3 Pipeline Stop drain**:`internal/sink/pipeline.go` `Stop()` 改为 `close(ch) → wg.Wait()(drain) → cancel(ctx)`,避免先 cancel ctx 导致 worker 丢弃 channel 内消息(§6.5)
  - **P1-4 drainWAL 同步 ack**:`internal/sink/sink.go` 新增 `sendToKafkaSync()`(用 `ProduceWithCallback` + channel 等 ack),`drainWAL` 改用同步路径,仅 broker ack 成功才标记 `.done`(§6.3 at-least-once)
  - **P1-5~P1-8 规则 YAML schema 对齐**:`internal/ruleengine/types.go` + `stage.go` — `source_topic` → `input_topic`;Stage 实现 `UnmarshalYAML` 读 inline 字段(非 `config:` 嵌套);`labels` → `add_labels`;Route 同时支持 `match`/`to_topic`(设计)与 `rules` 数组(兼容)
  - **P1-9~P1-11 scope/metric_regex**:`stage.go` / `downsample.go` / `deadvalue.go` 补 `scope: { metric_regex: "..." }`,仅匹配 metric 的 sample 被采样/下采样/死值检测,不匹配的 pass through
  - **P1-12 Stage 顺序校验**:`types.go` `Validate()` 新增顺序检查(relabel→enrich→route→sample→downsample→deadvalue);relabel 允许多条,其余 type 拒绝重复
  - **P1-13 Tracer resource 属性**:`internal/obs/tracing.go` `TracingConfig` 新增 `IngestCity`/`SourceDC`,resource attributes 补 `ingest_city`/`source_dc`(§7.2 所有 span 必带)
  - **P1-14 指标 label + admin trace_id**:`obs/metrics.go` `gateway_ruleset_version` label 从 `name` 改为 `ruleset`(与 §7.1 + `gateway_ruleset_processed_total` 一致);`admin/server.go` `tracingMW` 改用 OTel span(从 `otel.GetTextMapPropagator().Extract` + `obs.Tracer.Start`),响应体 `trace_id` 不再恒空
  - **P2-1 Admin safego**:`admin/server.go` `recoverMW` 新增 `safego.ReportPanic` 调用(新增 `pkg/safego.ReportPanic` 公开函数),panic 计入 `gateway_panic_recovered_total`(§6.6)
  - **P2-2 Admin 日志字段**:所有 admin 日志补 `ingest_city`/`source_dc`/`stage` 字段(§7.3)
  - **P2-3 Kafka Close 超时**:`kafkasink/producer.go` `Close()` 加 30s 超时(`DefaultCloseTimeout`),避免 RecordTimeout=0 时永久阻塞(§6.5)
  - **P2-4 Close 顺序**:`sink/sink.go` `AdapterSink.Close()` 改为先关 WAL 再关 Kafka(§6.5 Flush WAL → Close producer)
  - **P2-5 Close 等 monitor**:`sink/sink.go` 新增 `sync.WaitGroup` 跟踪 monitor goroutine,`Close()` 等 `wg.Wait()` 后返回
  - **P2-6/P2-7 设计文档签名对齐**:§5.2 stage 函数签名更新为 `func(ctx, in, prev) (out, dropped, err)`;§5.3 `atomic.Value` 更新为 `atomic.Pointer[T]`(等价泛型版)

### Fixed

- `obs/tracing.go`:Noop tracing 模式显式设置 W3C TraceContext + Baggage propagator,避免全局 propagator 退化导致 traceparent 注入失败。
- `sink/pipeline.go`:Worker span 改用 `msg.Headers["traceparent"]` 还原的 context,避免使用 `p.ctx` 导致的链路断裂。
- `receiver/server.go`:请求 ID 生成由 `reqCounter++` 改为 `atomic.Uint64.Add(1)`,消除 `TestChaos_Concurrent_NoLeak` 中的 race。
- `config/nacos.go`:Nacos 客户端在并发关闭时引入 `sync.WaitGroup` 跟踪清理 goroutine,关闭时先等待再退出,消除 `TestNacosClientAdapter_Close` race。
- `router/router_test.go`:`TestSetEntries_ConcurrentSafe` 修复 reader 永远不退出的死锁问题,改为统一响应 `stop` 信号。
- `ruleengine/manager.go`:rebuildRouterLocked 显式分离有 Match 与 default entries,避免 default 顺序错乱与多 default 错误。
- `cmd/prom-gw/main.go` + `config/token.go` + `receiver/tenant_rl.go`:**SIGHUP 重新加载 token 后同步刷新 receiver 的 per-tenant 限流**(此前只重载了 `auth.Tokens`,新 rate_limit 不生效;新增 `LocalTokenAuthenticator.TenantLimits()` 与 `receiver.UpdateTenantLimits` 在启动 + SIGHUP 路径都被调用)。

### Security

- Admin API 强制 `AllowCIDR` 来源 IP 白名单(默认 `127.0.0.1/32,10.0.0.0/8`),不命中即 403 + `gateway_admin_auth_fail_total` 计数。
- 鉴权抽象 `auth.Authenticator` 预留 IAM 接入扩展点(plan F.1-F.5),当前 v1 仅本地 Token 校验。
- Token 错误四象限分类(`missing / invalid / expired / revoked`),预留 reason 标签便于未来 IAM 接入直接复用。

### Deprecated

- 无。

### Removed

- 无。

### Breaking Changes

- 配置文件 schema 由 `ruleset: <单条>` 升级为 `rulesets: []<多条>`,迁移时需要在原有 `ruleset:` 字段外加 `- name: <name>` 并把字段平移为 list item。
- `--config` 指向的 YAML 现在必须包含 `rulesets` 顶层数组;空文件不再被识别为"启用 default 空规则"。

---

## 版本约定(尚未发版,以下为预案)

- **MAJOR**:breaking schema / CLI / 协议变化(预计 v1.0.0 之后,v2.x 引入新 protocol 版本)。
- **MINOR**:新增功能、向后兼容(配置 schema 追加可选字段、新 stage 类型、新 CLI flag)。
- **PATCH**:bug fix、性能优化、文档修正。

## Definition of Done(每发版前核对)

- [ ] 全部 Phase 0-5 任务完成并标注
- [ ] 单测覆盖率 ≥ 60%(用户规范硬性要求)
- [ ] `make test && make lint && make build` 全过
- [ ] `make chaos` 全过
- [ ] 监控大盘 + 告警规则已部署到 Grafana
- [ ] CHANGELOG.md 已更新(本文件)
- [ ] `git tag v<MAJOR>.<MINOR>.<PATCH>` 已打
- [ ] `dist/prom-gw-<VERSION>.tar.gz` 已发布
