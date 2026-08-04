# test/compat — 跨客户端兼容性测试

本目录验证 `prom-gw` 与各种 RemoteWrite 客户端(Prometheus、Cortex、VictoriaMetrics 等)的兼容性。

## 文件清单

| 文件 | 用途 | 何时跑 |
|---|---|---|
| `prompb_test.go` | 协议级单元测试,覆盖各种 wire format 变体 | 每次 PR(`go test ./test/compat/...`) |
| `matrix_docker_smoke.sh` | Docker 镜像矩阵冒烟测试,验证真实客户端互通 | 上线前 / 升级 Prometheus 时手动跑 |
| `README.md` | 本文档 | — |

## 单元测试覆盖的协议变体

`prompb_test.go` 用 16 个用例覆盖常见客户端的 wire format 行为差异:

| # | 场景 | 验证点 |
|---|---|---|
| 1 | Prometheus 官方 client | 标准格式解析 |
| 2 | Cortex distributor | labels 乱序、多 DC label |
| 3 | VM agent(缺 `__name__`) | 优雅跳过无效 series,不污染整批 |
| 4 | Thanos receiver | histogram 字段不影响 v1 解析 |
| 5 | OpenMetrics / Mimir | UTF-8 label value 透传 |
| 6 | agent_exporter 桥接 | 8KB 大 label value 不被截断 |
| 7 | noop / heartbeat | 空 WriteRequest 204 |
| 8 | 大批量 | 10K series / request 不 OOM |
| 9 | 多 sample per series | 只取 `sample[0]`,后续忽略 |
| 10 | 极端 timestamp | 0 / 远未来 / 负数不 panic |
| 11 | 重复 label name | 不去重,SeriesKey 含全部副本 |
| 12 | 错误 snappy 流 | 400 + 错误分类 |
| 13 | 错误 protobuf | 400 + 错误分类 |
| 14 | 空 body | 400 |
| 15 | 只有 labels 无 sample | 跳过 |
| 16 | 超大 body(>64MB) | 拒绝 |

## Docker 镜像矩阵冒烟测试

`matrix_docker_smoke.sh` 用真实 Docker 镜像拉起客户端,验证全链路互通。

### 前置

- `docker` 可用(`docker info` OK)
- `curl` 可用
- `prom-gw` 已构建:`make build` 产出 `bin/prom-gw`

### 跑法

```bash
# 默认矩阵(5 个 Prometheus 版本 + 2 个外部客户端)
bash test/compat/matrix_docker_smoke.sh

# 自定义镜像
PROM_IMAGES="prom/prometheus:v2.45.6 prom/prometheus:v2.50.1" \
  bash test/compat/matrix_docker_smoke.sh

# 自定义 prom-gw 二进制
PROM_GW_BIN=/path/to/prom-gw bash test/compat/matrix_docker_smoke.sh
```

### 工作流

每个镜像:

1. `docker pull` 拉镜像
2. 用 `docker run -d --network host` 启动,内嵌 prometheus.yml 指向 `host:port/api/v1/write`
3. 等 30 秒让客户端采集自身指标
4. 拉 `prom-gw` `/metrics`,断言:
   - `gateway_samples_total{stage="parse",status="ok"}` 增长
   - `gateway_errors_total` 计数 = 0

### 输出

```
==> 启动 prom-gw(:19201, WAL-only 模式)
==> 启动 OK,baseline samples_total=0
==> 镜像: prom/prometheus:v2.40.8
  OK: samples 0 -> 12345, errors=0
==> 镜像: prom/prometheus:v2.45.6
  OK: samples 12345 -> 24789, errors=0
...
==> 矩阵结果
  PASS=5  FAIL=0  SKIP=0
```

## 已知行为 / 限制

1. **v1 parser 只取 `sample[0]`**:Prometheus 协议允许一个 series 多 sample(spec 4.2 写的是 1 个),我们严格遵守。客户端发多 sample 不会丢数据,只是只采第一个。
2. **重复 label name 不去重**:Prometheus 协议不允许,客户端会先去重。但万一发过来,我们的 SeriesKey 会包含所有副本。`prompb_test.go §11` 有覆盖。
3. **Cortex/Thanos/VM 通过 prompb 协议**:wire format 与 Prometheus 官方完全一致,差异在 metadata(集群、region label)上,这些在 parser 后会被正常放入 labels,不影响 routing。
4. **`<= 2.30` 的旧 Prometheus 不在覆盖范围**:协议字段可能不同,需要降级 proto 版本支持(目前 v1 不承诺)。
5. **Mimir agent 兼容**:wire format 同 prompb,但 label 名 `cluster/replica` 等需要 rule 阶段专门处理(不是协议问题)。
6. **OTel collector 的 prometheus receiver**:协议同 prompb,但默认会带 `job/instance` label,需要在 rule 阶段用 `relabel` 重写。

## CI 接入

将 `bash test/compat/matrix_docker_smoke.sh` 加到 `release` pipeline 即可:

```yaml
- name: compat smoke
  if: github.ref == 'refs/heads/main'
  run: |
    make build
    bash test/compat/matrix_docker_smoke.sh
```

PR 阶段不跑(太重),只对 `main` 跑一次保证没回归。
