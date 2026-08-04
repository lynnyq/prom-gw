# 兼容性矩阵

> 本文档定义 `prom-gw` 与各类 RemoteWrite 客户端的兼容性边界。
> 配套代码:`test/compat/prompb_test.go`(协议级单元测试)、`test/compat/matrix_docker_smoke.sh`(真实镜像冒烟)。

## 1. 已验证兼容

| 客户端 | 版本范围 | wire format | 验证方式 | 状态 |
|---|---|---|---|---|
| **Prometheus server** | v2.40 / v2.45 / v2.50 / latest | prompb v1 | 单元 + Docker | ✅ |
| **Cortex distributor** | 任意(走 prompb) | prompb v1 | 单元 | ✅ |
| **Thanos receiver** | v0.30+ | prompb v1 | 单元 | ✅ |
| **VictoriaMetrics vmagent** | latest | prompb v1 | 单元 | ✅ |
| **OpenMetrics exporter**(桥接到 prom remote_write) | latest | prompb v1 | 单元(UTF-8 label) | ✅ |
| **Grafana Mimir** | 任意(走 prompb) | prompb v1 | 单元(等同 Cortex) | ✅ |
| **agent_exporter / node_exporter**(走桥接) | 任意 | prompb v1 | 单元(大 label) | ✅ |
| **OTel collector prometheus receiver** | 任意 | prompb v1 | 单元 | ✅ |

### 1.1 验证细节

每个客户端的具体差异点:

| 客户端 | 已知差异 | 我们的处理 |
|---|---|---|
| Prometheus v2.40 | 原生 histogram 实验性引入(默认关闭) | v1 parser 忽略 histogram 字段,只取 sample |
| Prometheus v2.45+ | exemplars 默认开启 | v1 不读 exemplar 字段,无影响 |
| Prometheus v2.50+ | remote_write 默认开启 retry | retry 在客户端完成,GW 无感 |
| Cortex | labels 可能乱序 | parser 按 name 排序,不影响 routing |
| Cortex | 可能带 cluster/region 标签 | labels 透传,rule 阶段可重写 |
| VM agent | 偶发缺 `__name__` | 跳过该 series,记 `gateway_errors_total{type="parse_series"}` |
| Thanos receiver | 带外部 label(`external_labels`) | 走 prompb,GW 正常处理 |
| OTel collector | 默认带 `job=otelcol`,`instance=...` | rule 阶段用 `relabel` 改写 |

## 2. 已知不兼容 / 限制

| 客户端 / 场景 | 不兼容原因 | 建议 |
|---|---|---|
| Prometheus **<= v2.30** | proto 字段定义不同(Timestamp 编码) | 升级 Prometheus 到 v2.40+ |
| **InfluxDB v2** line protocol | 协议不兼容 | 走 InfluxDB → Prometheus exporter |
| **Graphite** plaintext | 协议不兼容 | 走 graphite_exporter → prom |
| **StatsD** | 协议不兼容 | 走 statsd_exporter → prom |
| **OpenTelemetry OTLP** | OTLP 协议不兼容 | 用 otel-collector 转 prom remote_write(本表已覆盖) |
| **WAL 硬拒绝 + 客户端无重试** | 数据丢失风险 | 客户端必须配 retry(默认 retry) |
| **Native histogram v2**(Prometheus v2.47+ 实验) | v1 parser 不解析 NHCB 桶 | 客户端关闭 `native_histogram` 或 GW 升级到 v2 |

## 3. 协议级保证

`prom-gw` 严格遵守 Prometheus RemoteWrite v1 spec:

- `Content-Type: application/x-protobuf` ← 客户端发什么 GW 都收,不符合的返回 415
- `Content-Encoding: snappy` ← 同上
- Body: `prompb.WriteRequest` ← gogo/proto 解码
- 必带 label `__name__` ← 缺则跳过
- 单 series 单 sample ← 多 sample 时只取 `sample[0]`,其余忽略(不报错)

## 4. 鉴权兼容性

所有客户端通过 `Authorization: Bearer <token>` 头传 token:

```
remote_write:
  - url: http://gw:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: tk_xxx
```

| 客户端 | 配置语法 | 验证 |
|---|---|---|
| Prometheus | `authorization.credentials` | ✅ |
| Cortex distributor | `authorization.credentials` | ✅ |
| VM agent | `authorization.credentials` | ✅ |
| Thanos receiver | `authorization.credentials` | ✅ |
| OTel collector | `authorization` 块 | ✅ |

## 5. Trace 上下文透传

支持 W3C `traceparent` header 透传,所有列出的客户端都自动支持(走 HTTP 标准 header):

| 客户端 | 自动加 traceparent | GW 识别 |
|---|---|---|
| Prometheus | ❌(HTTP client 不加) | 兜底生成 traceID |
| Cortex distributor | ✅(走 trace 注入) | ✅ |
| Thanos | ✅(走 trace 注入) | ✅ |
| VM agent | ❌ | 兜底生成 traceID |
| OTel collector | ✅(走 OTel propagation) | ✅ |

GW 无客户端时,会自己生成 root trace span,WAL/Kafka header 中也会带 `traceparent`。

## 6. 验证步骤

### 6.1 协议级(无 Docker,毫秒级)

```bash
go test -v -count=1 ./test/compat/...
```

应看到 16 个 `TestCompat_*` 通过。

### 6.2 镜像矩阵(需 Docker,数分钟)

```bash
make build
bash test/compat/matrix_docker_smoke.sh
```

期望:全部 PASS 或 SKIP(网络拉不到镜像),无 FAIL。

### 6.3 已知不兼容场景(回归测试)

```bash
# 旧 Prometheus 客户端(< v2.30)
docker run --rm --network host prom/prometheus:v2.30.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --web.listen-address=:9090
# 预期: GW 返回 400,记 gateway_errors_total{type="protobuf"}
```

## 7. 兼容性变更流程

当 Prometheus / Cortex / VM 发布新版本时:

1. **看 changelog**:是否有 proto 字段变更(罕见,通常都是向后兼容)
2. **跑镜像矩阵**:`bash test/compat/matrix_docker_smoke.sh`
3. **看 `gateway_errors_total` 增长**:若新增 type,可能是新 wire format 字段,评估是否需要 schema 升级
4. **更新本文档**:把新版本加入"已验证兼容"表
5. **CHANGELOG**:记录兼容性边界变化

## 8. 未来扩展

- **Native histogram v2 完整支持**:需要 `prompb.WriteRequest` 升级到 v2 字段(目前用 v1)
- **OTLP 直连**:让 GW 直接收 OTLP,免去 collector 桥接
- **PRAW(Prometheus Remote Write 2.0)**:CNCF 正在制订的新协议,等 spec 稳定后支持
- **Snappy 替代压缩**:支持 zstd 压缩(需客户端支持)
