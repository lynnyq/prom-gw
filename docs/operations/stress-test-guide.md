# 压力测试指南与报告

> 本文档定义 prom-gw 的压力测试方法论、数据生成方式、执行步骤和报告模板,用于发版前性能回归、容量规划和性能瓶颈定位。
>
> 配套文档:[SLO 指标](slo.md)、[配置参数](configuration-reference.md)、[本地部署](local-dev-guide.md)、[生产部署](production-guide.md)

## 目录

1. [概述](#1-概述)
2. [测试方法](#2-测试方法)
3. [数据生成方式](#3-数据生成方式)
4. [测试步骤](#4-测试步骤)
5. [压力测试报告](#5-压力测试报告)
6. [性能分析与优化指南](#6-性能分析与优化指南)
7. [更多测试场景](#7-更多测试场景)
8. [CI/CD 性能回归集成](#8-cicd-性能回归集成)
9. [压测报告自动化](#9-压测报告自动化)
10. [附录](#10-附录)

---

## 1. 概述

### 1.1 测试目标

| 目标 | 说明 |
|---|---|
| 性能基线回归 | 每次发版前验证单实例吞吐 ≥ 1.5M samples/s |
| 容量规划 | 确定不同负载下所需实例数,指导扩缩容 |
| 瓶颈定位 | 通过 pprof + metrics 定位 CPU / 内存 / GC 瓶颈 |
| 稳定性验证 | 长时间运行(1h+)无内存泄漏、无 FD 泄漏、无 goroutine 泄漏 |
| 故障行为验证 | Kafka 不可用时降级 WAL、磁盘满时 503 拒绝 |

### 1.2 SLO 基线

压力测试的判定标准依据 [SLO 文档](slo.md):

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 吞吐 | ≥ 1.5M samples/s 单实例 | `rate(gateway_samples_total{stage="parse",status="ok"})[1m]` |
| p99 延迟 | < 500ms | `histogram_quantile(0.99, rate(gateway_request_duration_seconds_bucket[1m]))` |
| p50 延迟 | < 50ms | 同上,quantile=0.50 |
| 错误率 | < 0.01% | `rate(gateway_errors_total) / rate(gateway_samples_total)` |
| 背压拒绝率 | < 0.1% | `rate(gateway_backpressure_rejected_total) / rate(gateway_samples_total)` |
| CPU | < 70% | `gateway_cpu_ratio` |
| 内存 | < 8 GB | `gateway_mem_bytes` |
| Goroutines | < 5000 | `gateway_goroutines` |

### 1.3 测试工具

prom-gw 自带两个压测工具,无需引入第三方依赖:

| 工具 | 路径 | 用途 |
|---|---|---|
| loadgen | [test/loadgen/main.go](../../test/loadgen/main.go) | 自研 Prometheus RemoteWrite 协议压测客户端,精确控制每请求 sample 数 |
| profile.sh | [test/perf/profile.sh](../../test/perf/profile.sh) | 一键执行压测 + CPU/heap profile 采集 + metrics 抓取 |

---

## 2. 测试方法

### 2.1 测试类型

| 类型 | 目的 | 负载 | 时长 | 使用场景 |
|---|---|---|---|---|
| 冒烟测试 | 验证基本功能可用 | 50K samples/s | 30s | CI / 本地开发 |
| 基线回归 | 验证 SLO 达标 | 1.5M samples/s | 5min | 发版前 |
| 容量阶梯 | 寻找性能拐点 | 100K → 500K → 1M → 1.5M → 2M | 每档 3min | 容量规划 |
| 稳定性测试 | 检测内存/FD/goroutine 泄漏 | 1.5M samples/s | 1h+ | 发版前 |
| 故障注入 | 验证降级行为 | 1M samples/s | 5min | 发版前 |
| 多租户测试 | 验证限流与隔离 | 多 token 并发 | 5min | 上线前 |

### 2.2 测试环境要求

#### 2.2.1 硬件要求

| 资源 | 最低 | 建议 | 备注 |
|---|---|---|---|
| CPU | 4 核 | 8 核 | 1.5M 吞吐需要 ≥ 4 核 |
| 内存 | 8 GB | 16 GB | SLO 上限 8G,建议预留余量 |
| 磁盘 | 50 GB SSD | 100 GB SSD | WAL 目录需独立盘,NVMe 最佳 |
| 网络 | 1 Gbps | 10 Gbps | 1.5M samples/s 压缩后约 150-200 Mbps |

#### 2.2.2 软件要求

| 组件 | 版本 | 备注 |
|---|---|---|
| Go | ≥ 1.21 | 编译 prom-gw 和 loadgen |
| Kafka | ≥ 3.4(KRaft) | 可选,WAL-only 模式可跳过 |
| curl | 任意 | 健康检查和 metrics 抓取 |
| Go pprof | 内置 | CPU/heap profile 分析 |

#### 2.2.3 网络拓扑

```
压测机 (loadgen)                被测机 (prom-gw)              下游
┌──────────────┐    HTTP     ┌──────────────────────┐    ┌────────┐
│  loadgen     │ ──────────> │  receiver :19201     │    │ Kafka  │
│  (8 并发)    │             │  metrics  :8080      │ ─> │ :9092  │
│              │             │  health   :8081      │    └────────┘
│              │             │  admin    :8082      │
│              │             │  pprof    :9090      │
└──────────────┘             └──────────────────────┘
```

> 生产环境压测时,loadgen 和 prom-gw 应部署在不同机器,避免争抢 CPU。本机测试可同机部署,但需注意 loadgen 自身 CPU 开销。

### 2.3 采集指标

压测过程中需采集以下指标,分三类:

#### 2.3.1 客户端指标(loadgen 输出)

| 指标 | 来源 | 说明 |
|---|---|---|
| rate | loadgen stdout | 实际发送 samples/s |
| sent_batches | loadgen stdout | 已发送 batch 总数 |
| err_batches | loadgen stdout | 错误 batch 数 |
| latency p50/p95/p99/max | loadgen stdout | HTTP 请求延迟分布 |
| bytes_sent | loadgen stdout | 已发送字节数 |

#### 2.3.2 服务端指标(/metrics)

| 指标 | PromQL | 说明 |
|---|---|---|
| 解析吞吐 | `rate(gateway_samples_total{stage="parse",status="ok"}[1m])` | GW 实际处理 samples/s |
| 请求延迟 | `histogram_quantile(0.99, rate(gateway_request_duration_seconds_bucket[1m]))` | GW 侧 p99 延迟 |
| 错误计数 | `rate(gateway_errors_total[1m])` | 错误速率 |
| 背压拒绝 | `rate(gateway_backpressure_rejected_total[1m])` | 503 拒绝速率 |
| 限流拒绝 | `rate(gateway_rate_limit_rejected_total[1m])` | 429 拒绝速率 |
| WAL 占用 | `gateway_wal_bytes` | WAL 当前字节数 |
| Goroutines | `gateway_goroutines` | goroutine 数 |
| 内存 | `gateway_mem_bytes` | 驻留内存 |
| CPU | `gateway_cpu_ratio` | CPU 使用率(0-1) |

#### 2.3.3 系统指标(pprof + OS)

| 指标 | 采集方法 | 说明 |
|---|---|---|
| CPU profile | `go tool pprof http://localhost:8080/debug/pprof/profile?seconds=60` | 函数级 CPU 热点 |
| Heap profile | `curl http://localhost:8080/debug/pprof/heap -o heap.pprof` | 堆分配热点 |
| Goroutine profile | `curl http://localhost:8080/debug/pprof/goroutine -o goroutine.pprof` | goroutine 堆栈 |
| FD 数 | `lsof -p <pid> \| wc -l` | 文件描述符数 |
| 磁盘 IO | `iostat -x 1` | WAL 写入吞吐 |

### 2.4 判定标准

| 判定项 | 通过标准 | 阻断发版 |
|---|---|---|
| 吞吐 | 持续 ≥ 1.5M samples/s | 是 |
| p99 延迟 | < 500ms | 是 |
| p50 延迟 | < 50ms | 否(告警) |
| 错误率 | < 0.01% | 是 |
| 背压拒绝率 | < 0.1% | 是 |
| 内存 | < 8 GB | 是 |
| CPU | < 70% | 否(告警) |
| Goroutines | < 5000 | 否(告警) |
| 1h 内存增长 | < 5% | 是(泄漏) |
| 1h FD 增长 | < 100 | 是(泄漏) |

---

## 3. 数据生成方式

### 3.1 loadgen 工具

prom-gw 使用自研 loadgen 而非 vegeta/wrk,原因:

- **精确控制 sample 数**:每个 WriteRequest 携带指定数量 sample,模拟真实 Prometheus RemoteWrite 负载
- **协议完整**:构造 protobuf + snappy 压缩的 WriteRequest,与真实 Prometheus 行为一致
- **series 滚动**:预生成 series 池,worker 轮转选取,模拟真实指标滚动

### 3.2 数据结构

loadgen 生成的每条 TimeSeries 包含以下标签:

| 标签 | 取值 | 说明 |
|---|---|---|
| `__name__` | `metric_0` ~ `metric_{series_count-1}` | 指标名 |
| `instance` | `host-{0-99}.{0-999}.example.com` | 实例标识 |
| `job` | `node` / `app` / `db` / `kafka` / `redis` | 作业名(随机 5 选 1) |

每个 sample 包含:
- `value`: 0-1000 的随机浮点数
- `timestamp`: 当前时间戳(毫秒)

### 3.3 负载参数

loadgen 通过以下 flag 控制负载模型:

| 参数 | 默认值 | 说明 | 调优建议 |
|---|---|---|---|
| `--rate` | 100000 | 目标 samples/s | 基线测试设 1500000 |
| `--samples-per-batch` | 500 | 每个 WriteRequest 的 sample 数 | 500(模拟真实 Prometheus),大 batch 提升吞吐但增加延迟 |
| `--concurrency` | 4 | 并发 worker 数 | 4-8,过多会争抢 CPU |
| `--duration` | 30s | 压测时长 | 冒烟 30s,基线 5min,稳定性 1h |
| `--series-count` | 10000 | series 池大小 | 10000(模拟中型集群),高基数测试设 100000 |
| `--token` | `tk_app_business_dev` | Bearer token | 多租户测试切换不同 token |
| `--url` | `http://127.0.0.1:19201/api/v1/write` | RemoteWrite URL | 指向被测实例 |
| `--metrics-url` | (空) | GW metrics URL | 填写后压测结束自动拉取 GW 指标 |

### 3.4 payload 计算

单个 WriteRequest 的 payload 大小计算:

```
series_per_batch = 10 (固定)
samples_per_series = samples_per_batch / series_per_batch

raw_bytes ≈ series_per_batch × (labels_size + samples_per_series × 16)
compressed_bytes ≈ raw_bytes × 0.4  (snappy 压缩比约 40%)
```

典型配置(`--samples-per-batch=500`):

| 项 | 值 |
|---|---|
| series_per_batch | 10 |
| samples_per_series | 50 |
| raw_bytes | ~8 KB |
| compressed_bytes | ~3-4 KB |

1.5M samples/s 时的网络带宽:

```
batches_per_sec = 1500000 / 500 = 3000
bandwidth = 3000 × 4KB = 12 MB/s ≈ 96 Mbps
```

### 3.5 负载场景

#### 场景 1:标准基线负载

模拟真实 Prometheus 默认配置:

```bash
--rate=1500000 --samples-per-batch=500 --concurrency=8 --series-count=10000 --duration=300s
```

#### 场景 2:高基数负载

模拟大量 instance/metric,测试内存和 series 跟踪:

```bash
--rate=1000000 --samples-per-batch=500 --concurrency=8 --series-count=100000 --duration=300s
```

#### 场景 3:大 batch 负载

模拟低频大包(如联邦集群),测试单请求处理延迟:

```bash
--rate=1000000 --samples-per-batch=5000 --concurrency=4 --series-count=10000 --duration=300s
```

#### 场景 4:小 batch 高频负载

模拟边缘节点高频小包,测试 HTTP 连接和调度开销:

```bash
--rate=500000 --samples-per-batch=50 --concurrency=16 --series-count=5000 --duration=300s
```

#### 场景 5:多租户负载

模拟多租户并发,需开多个 loadgen 进程:

```bash
# 终端 1: app-business 租户
go run ./test/loadgen --token=tk_app_business_dev --rate=800000 --duration=300s &

# 终端 2: infra 租户
go run ./test/loadgen --token=tk_infra_dev --rate=500000 --duration=300s &
```

---

## 4. 测试步骤

### 4.1 前置准备

#### 4.1.1 编译 prom-gw

```bash
cd /path/to/prom-gw
make build
# 产物: ./bin/prom-gw
```

#### 4.1.2 准备配置

使用 app-business ruleset(含 relabel + route + sample 三个 stage,覆盖典型处理链路):

```bash
# 配置文件: configs/rules/app-business.yaml
# Token 文件: configs/tokens/local.yaml
```

> 如需测试纯 WAL 模式(无 Kafka),跳过 KAFKA_BROKERS 环境变量即可。

#### 4.1.3 准备 Kafka(可选)

```bash
# 启动单节点 Kafka(KRaft 模式)
# 详见 local-dev-guide.md 第 3 章
export KAFKA_BROKERS=localhost:9092
```

#### 4.1.4 确认端口可用

| 端口 | 用途 | 检查 |
|---|---|---|
| 19201 | RemoteWrite | `lsof -i:19201` 应为空 |
| 8080 | metrics + pprof | `lsof -i:8080` 应为空 |
| 8081 | healthz | `lsof -i:8081` 应为空 |
| 8082 | admin | `lsof -i:8082` 应为空 |

### 4.2 一键压测(profile.sh)

最便捷的方式,自动完成构建、启动、压测、采集、汇总:

```bash
# 冒烟测试(30s)
bash test/perf/profile.sh

# 基线回归(1.5M samples/s × 5min)
RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 带 Kafka 的完整压测
KAFKA_BROKERS=localhost:9092 RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 高基数测试
RATE=1000000 DURATION=300s SERIES=100000 bash test/perf/profile.sh
```

`profile.sh` 支持的环境变量:

| 变量 | 默认值 | 说明 |
|---|---|---|
| `RATE` | 500000 | 目标 samples/s |
| `DURATION` | 60s | 压测时长 |
| `CONCURRENCY` | 4 | loadgen 并发数 |
| `BATCH` | 500 | 每 batch sample 数 |
| `SERIES` | 10000 | series 池大小 |
| `GW_BIN` | `./bin/prom-gw` | prom-gw 二进制路径 |
| `CFG` | `configs/rules/app-business.yaml` | ruleset 配置 |
| `TOKENS` | `configs/tokens/local.yaml` | token 配置 |
| `WAL_DIR` | 随机临时目录 | WAL 数据目录 |
| `OUT_DIR` | `./perf-out/<timestamp>` | 输出目录 |
| `WRITE_PORT` | 19201 | RemoteWrite 端口 |
| `METRICS_PORT` | 8080 | metrics 端口 |
| `HEALTH_PORT` | 8081 | healthz 端口 |
| `ADMIN_PORT` | 8082 | admin 端口 |
| `PPROF_PORT` | 9090 | pprof 端口 |
| `KEEP_WAL` | 0 | 是否保留 WAL 数据(1=保留) |

输出文件结构:

```
perf-out/20260812-143000/
├── prom-gw.log          # prom-gw 运行日志
├── loadgen.log          # loadgen 压测输出
├── cpu.pprof            # CPU profile
├── heap.pprof           # Heap profile
├── metrics.txt          # /metrics 全量快照
└── admin-stats.json     # admin /v1/stats 快照
```

### 4.3 手动分步压测

需要精细控制时,可手动执行各步骤:

#### 步骤 1:启动 prom-gw

```bash
# WAL-only 模式(无 Kafka)
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-wal \
    --write-addr=:19201 \
    --metrics-addr=:8080 \
    --health-addr=:8081 \
    --admin-addr=:8082 \
    --admin-allow-cidr=127.0.0.1/32 \
    --source-dc=dc-perf \
    > /tmp/prom-gw.log 2>&1 &

# 记录 PID
echo $! > /tmp/prom-gw.pid

# 等待启动
for i in $(seq 1 50); do
    curl -fsS http://127.0.0.1:8081/healthz && break
    sleep 0.2
done
```

#### 步骤 2:启动 CPU profile 采集(后台)

```bash
go tool pprof -proto -seconds=300 \
    "http://127.0.0.1:8080/debug/pprof/profile?seconds=300" \
    > /tmp/cpu.pprof &
```

#### 步骤 3:执行压测

```bash
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1500000 \
    --samples-per-batch=500 \
    --duration=300s \
    --concurrency=8 \
    --series-count=10000 \
    --metrics-url=http://127.0.0.1:8080/metrics \
    2>&1 | tee /tmp/loadgen.log
```

#### 步骤 4:采集 profile 和 metrics

```bash
# Heap profile
curl -sS http://127.0.0.1:8080/debug/pprof/heap -o /tmp/heap.pprof

# Goroutine profile
curl -sS http://127.0.0.1:8080/debug/pprof/goroutine -o /tmp/goroutine.pprof

# /metrics 全量
curl -sS http://127.0.0.1:8080/metrics -o /tmp/metrics.txt

# Admin stats
curl -sS http://127.0.0.1:8082/v1/stats -o /tmp/admin-stats.json
```

#### 步骤 5:分析 profile

```bash
# CPU 热点 top 20
go tool pprof -top -cum -nodecount=20 /tmp/cpu.pprof

# Heap 分配热点 top 20
go tool pprof -top -nodecount=20 -sample_index=alloc_space /tmp/heap.pprof

# 交互式火焰图
go tool pprof -http=:9999 /tmp/cpu.pprof
```

#### 步骤 6:清理

```bash
kill $(cat /tmp/prom-gw.pid)
rm -rf /tmp/perf-wal
```

### 4.4 容量阶梯测试

逐步加压,找到性能拐点:

```bash
for rate in 100000 500000 1000000 1500000 2000000; do
    echo "===== RATE=$rate ====="
    RATE=$rate DURATION=180s OUT_DIR=./perf-out/staircase-$rate \
        bash test/perf/profile.sh
    sleep 10  # 冷却
done
```

每档记录以下数据,绘制吞吐-延迟曲线:

- 实际吞吐(samples/s)
- p99 延迟
- CPU 使用率
- 内存占用

拐点判定:p99 延迟开始指数增长或错误率 > 0.01% 时的 rate。

### 4.5 稳定性测试

长时间运行,检测资源泄漏:

```bash
# 1h 稳定性测试
RATE=1500000 DURATION=3600s OUT_DIR=./perf-out/soak-1h \
    bash test/perf/profile.sh
```

压测期间每 5 分钟采集一次资源指标:

```bash
# 后台采集脚本
while true; do
    ts=$(date +%H:%M:%S)
    goroutines=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_goroutines | awk '{print $2}')
    mem=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_mem_bytes | awk '{print $2}')
    fd=$(lsof -p $(cat /tmp/prom-gw.pid) | wc -l)
    echo "$ts goroutines=$goroutines mem=$mem fd=$fd"
    sleep 300
done
```

判定标准:
- 1h 内存增长 < 5%
- 1h FD 增长 < 100
- goroutine 数稳定不持续增长

### 4.6 故障注入测试

#### 4.6.1 Kafka 不可用降级

```bash
# 1. 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 ./bin/prom-gw ... &

# 2. 启动压测
go run ./test/loadgen --rate=1000000 --duration=300s &

# 3. 压测中途停止 Kafka
kill $(pidof kafka)

# 4. 观察:gateway_samples_total{stage="wal"} 应增长,errors 不增
# 5. 重启 Kafka,观察 WAL drain(gateway_wal_bytes 应下降)
```

#### 4.6.2 磁盘满硬拒绝

```bash
# 1. 启动 prom-gw(WAL 目录设为小盘)
./bin/prom-gw --wal-dir=/tmp/small-wal --wal-max-bytes=1073741824 ... &

# 2. 压测并停止 Kafka(迫使数据落 WAL)
# 3. 观察 WAL 填满后返回 503
# 4. metrics: gateway_wal_hard_reject_total 应 > 0
```

### 4.7 多租户限流测试

```bash
# 启动 prom-gw(token 配置:app-business=80K/s, infra=50K/s)
./bin/prom-gw --tokens=configs/tokens/local.yaml ... &

# 同时压两个租户,各超过其限流
go run ./test/loadgen --token=tk_app_business_dev --rate=120000 --duration=60s &
go run ./test/loadgen --token=tk_infra_dev --rate=80000 --duration=60s &

# 观察:gateway_rate_limit_rejected_total{tenant="app-business"} 应增长
# app-business 超过 80K/s 的部分被 429 拒绝
```

---

## 5. 压力测试报告

### 5.1 报告模板

每次压测完成后,按以下模板填写报告:

```markdown
# prom-gw 压力测试报告

## 测试信息
| 项 | 值 |
|---|---|
| 测试日期 | YYYY-MM-DD |
| 测试人员 | |
| prom-gw 版本 | |
| Git commit | |
| 测试类型 | 冒烟 / 基线 / 容量阶梯 / 稳定性 / 故障 |

## 环境信息
| 项 | 值 |
|---|---|
| 机器规格 | |
| 操作系统 | |
| Go 版本 | |
| Kafka 版本 | |
| 网络 | 本机 / 跨机 |

## 压测参数
| 参数 | 值 |
|---|---|
| rate | |
| samples-per-batch | |
| concurrency | |
| series-count | |
| duration | |
| token | |
| ruleset | |

## 测试结果
| 指标 | 目标 | 实测 | 判定 |
|---|---|---|---|
| 吞吐 (samples/s) | ≥ 1,500,000 | | PASS/FAIL |
| p50 延迟 | < 50ms | | PASS/FAIL |
| p99 延迟 | < 500ms | | PASS/FAIL |
| 错误率 | < 0.01% | | PASS/FAIL |
| 背压拒绝率 | < 0.1% | | PASS/FAIL |
| CPU | < 70% | | PASS/FAIL |
| 内存 | < 8 GB | | PASS/FAIL |
| Goroutines | < 5000 | | PASS/FAIL |

## Profile 分析
(CPU/Heap 热点 top 10 截图或文本)

## 结论与建议
(是否通过 / 瓶颈分析 / 改进建议)
```

### 5.2 基线回归报告(示例)

以下为基于设计目标和 SLO 的示例报告,实际数值以压测实测为准:

---

#### prom-gw 压力测试报告 — 基线回归

**测试信息**

| 项 | 值 |
|---|---|
| 测试日期 | 2026-08-12 |
| 测试类型 | 基线回归 |
| prom-gw 版本 | v1.0.0 |
| Git commit | a1b2c3d |
| 测试模式 | WAL-only(无 Kafka) |

**环境信息**

| 项 | 值 |
|---|---|
| 机器规格 | 8C 16G,100GB SSD |
| 操作系统 | macOS 14 / Linux 8 |
| Go 版本 | 1.22 |
| 网络 | 本机回环 |

**压测参数**

| 参数 | 值 |
|---|---|
| rate | 1,500,000 samples/s |
| samples-per-batch | 500 |
| concurrency | 8 |
| series-count | 10,000 |
| duration | 300s |
| token | tk_app_business_dev |
| ruleset | app-business(relabel + route + sample) |

**测试结果**

| 指标 | 目标 | 实测 | 判定 |
|---|---|---|---|
| 吞吐 (samples/s) | ≥ 1,500,000 | 1,502,400 | PASS |
| p50 延迟 | < 50ms | 12ms | PASS |
| p95 延迟 | - | 85ms | - |
| p99 延迟 | < 500ms | 180ms | PASS |
| max 延迟 | - | 420ms | - |
| 错误率 | < 0.01% | 0.000% | PASS |
| 背压拒绝率 | < 0.1% | 0.000% | PASS |
| CPU | < 70% | 58% | PASS |
| 内存 | < 8 GB | 2.1 GB | PASS |
| Goroutines | < 5000 | 320 | PASS |

**loadgen 输出摘要**

```
=== Final ===
duration=5m0s sent_batches=9000 err_batches=0 (0.0000%) bytes=33120000000
rate=1502400 samples/s
latency p50=12ms p95=85ms p99=180ms max=420ms
samples_sent=4500000
```

**GW 侧关键指标**

```
gateway_samples_total{stage="parse",status="ok"}   4507200
gateway_request_duration_seconds{quantile="0.99"}  0.180
gateway_errors_total                               0
gateway_backpressure_rejected_total                0
gateway_goroutines                                 320
gateway_mem_bytes                                  2254857830  (~2.1GB)
gateway_cpu_ratio                                  0.58
gateway_wal_bytes                                  0           (WAL-only 无积压)
```

**Profile 分析**

CPU top 5:

```
Showing nodes accounting for 4.20s, 85.71% of 4.90s total
      flat  flat%   sum%        cum   cum%
     1.20s 24.49% 24.49%      1.80s 36.73%  github.com/lynnyq/bigdata/internal/parser.parseSeries
     0.80s 16.33% 40.82%      1.20s 24.49%  github.com/lynnyq/bigdata/internal/decoder.Decode
     0.60s 12.24% 53.06%      0.60s 12.24%  snappy.Decode
     0.40s  8.16% 61.22%      0.50s 10.20%  github.com/lynnyq/bigdata/internal/ruleengine.(*compiledStage).Apply
     0.30s  6.12% 67.35%      0.30s  6.12%  runtime.mallocgc
```

Heap top 5(alloc_space):

```
Showing nodes accounting for 8.50GB, 78.70% of 10.80GB total
      flat  flat%   sum%        cum   cum%
    3.20GB 29.63% 29.63%     3.20GB 29.63%  github.com/prometheus/prometheus/prompb.(*WriteRequest).Marshal
    2.10GB 19.44% 49.07%     2.10GB 19.44%  github.com/lynnyq/bigdata/internal/parser.parseSeries
    1.50GB 13.89% 62.96%     1.50GB 13.89%  snappy.Encode
    1.00GB  9.26% 72.22%     1.00GB  9.26%  net/http.(*Server).Serve
    0.70GB  6.48% 78.70%     0.70GB  6.48%  runtime.malg
```

**瓶颈分析**

1. **CPU 热点**:parser.parseSeries 占 24.49%,是 protobuf 反序列化的主要开销,属正常水平
2. **内存分配**:WriteRequest.Marshal 占 29.63%,可通过 sync.Pool 优化(当前未启用)
3. **无背压**:pipeline buffer 65535 足够,无 channel 满拒绝

**结论**

- 全部 SLO 指标 PASS,可发版
- 内存有余量(2.1G / 8G),可支持更高 series 基数
- 建议:后续考虑对 WriteRequest 启用 sync.Pool 降低 GC 压力

---

### 5.3 容量阶梯报告(示例)

| rate (samples/s) | 实际吞吐 | p99 延迟 | CPU | 内存 | 错误率 | 判定 |
|---|---|---|---|---|---|---|
| 100,000 | 100,200 | 8ms | 8% | 0.8 GB | 0% | PASS |
| 500,000 | 500,100 | 25ms | 22% | 1.2 GB | 0% | PASS |
| 1,000,000 | 1,001,500 | 95ms | 42% | 1.7 GB | 0% | PASS |
| 1,500,000 | 1,502,400 | 180ms | 58% | 2.1 GB | 0% | PASS |
| 2,000,000 | 1,850,000 | 850ms | 89% | 3.5 GB | 0.12% | FAIL |

**拐点分析**:rate=2M 时 CPU 达到 89%,实际吞吐无法跟上目标(1.85M < 2M),p99 超过 500ms。单实例性能上限约 1.8-1.9M samples/s。

---

### 5.4 稳定性报告(示例)

| 时间点 | 吞吐 (samples/s) | p99 延迟 | 内存 (GB) | Goroutines | FD |
|---|---|---|---|---|---|
| 0min | 1,502,000 | 175ms | 2.1 | 318 | 42 |
| 15min | 1,501,800 | 178ms | 2.2 | 320 | 44 |
| 30min | 1,502,100 | 172ms | 2.2 | 319 | 44 |
| 45min | 1,501,900 | 180ms | 2.3 | 321 | 45 |
| 60min | 1,502,000 | 176ms | 2.3 | 320 | 45 |

**判定**:
- 内存增长:(2.3-2.1)/2.1 = 9.5% > 5% → 需关注
- goroutine 稳定:320 ± 2 → PASS
- FD 稳定:42 → 45(+3)→ PASS

> 注:内存增长 9.5% 可能是 series 状态缓存预热,建议延长到 2h 观察是否稳定。

---

## 6. 性能分析与优化指南

### 6.1 Profile 分析方法

#### 6.1.1 CPU profile 分析

压测结束后,CPU profile 是定位性能热点的首要工具:

```bash
# 文本模式:按累积耗时(cum)排序,展示 top 20
go tool pprof -top -cum -nodecount=20 perf-out/<latest>/cpu.pprof

# 火焰图:浏览器交互式分析
go tool pprof -http=:9999 perf-out/<latest>/cpu.pprof

# 对比两次 profile(检测回归)
go tool pprof -base perf-out/old/cpu.pprof perf-out/new/cpu.pprof
```

**CPU profile 输出解读**:

```
Showing nodes accounting for 4.20s, 85.71% of 4.90s total
      flat  flat%   sum%        cum   cum%
     1.20s 24.49% 24.49%      1.80s 36.73%  parser.parseSeries
```

| 列 | 含义 | 关注点 |
|---|---|---|
| `flat` | 函数自身耗时(不含子调用) | 高 flat = 函数本身是热点 |
| `flat%` | flat 占总 CPU 百分比 | > 20% 需重点关注 |
| `cum` | 函数含子调用的累积耗时 | 高 cum + 低 flat = 子函数是瓶颈 |
| `cum%` | cum 占总 CPU 百分比 | 用于定位调用链 |

**正常分布**(参考基线):

| 函数 | 预期 flat% | 说明 |
|---|---|---|
| `parser.parseSeries` | 20-30% | protobuf 反序列化,主要 CPU 开销 |
| `decoder.Decode` | 10-20% | snappy 解压 + protobuf 解码 |
| `snappy.Decode` | 5-15% | snappy 解压缩 |
| `ruleengine.(*Stage).Apply` | 5-10% | 规则引擎处理 |
| `runtime.mallocgc` | 3-8% | GC 开销,> 15% 需优化 |
| `net/http.(*Server).Serve` | 2-5% | HTTP 框架开销 |

**异常信号**:

| 信号 | 可能原因 | 排查方向 |
|---|---|---|
| `runtime.mallocgc` > 15% | 对象分配过多 | 检查 heap profile,考虑 sync.Pool |
| `runtime.gcBgMarkWorker` > 10% | GC 压力大 | 调整 GOGC 或减少分配 |
| `runtime.lock` / `semacquire` > 5% | 锁竞争 | 检查 mutex profile |
| `syscall.Read` / `Write` > 20% | IO 瓶颈 | 检查磁盘 / 网络 |

#### 6.1.2 Heap profile 分析

```bash
# 按分配空间排序(找出分配最多的函数)
go tool pprof -top -nodecount=20 -sample_index=alloc_space perf-out/<latest>/heap.pprof

# 按当前驻留内存排序(找出内存泄漏)
go tool pprof -top -nodecount=20 -sample_index=inuse_space perf-out/<latest>/heap.pprof

# 火焰图
go tool pprof -http=:9999 -sample_index=alloc_space perf-out/<latest>/heap.pprof
```

**关键指标**:

| sample_index | 用途 | 说明 |
|---|---|---|
| `alloc_space` | 累计分配字节数 | 找出分配热点,优化 GC |
| `inuse_space` | 当前驻留内存 | 找出内存泄漏 |
| `alloc_objects` | 累计分配对象数 | 找出小对象频繁分配 |
| `inuse_objects` | 当前驻留对象数 | 找出未释放的对象 |

**泄漏判定**:对比压测开始和结束的 `inuse_space`,如果持续增长且不回落,可能是泄漏。

#### 6.1.3 Goroutine profile 分析

```bash
# 查看 goroutine 堆栈
go tool pprof -top perf-out/<latest>/goroutine.pprof

# 查看阻塞原因(需开启 mutex/Block profile)
curl -s "http://127.0.0.1:8080/debug/pprof/block?seconds=60" -o block.pprof
go tool pprof -top block.pprof
```

**正常状态**:goroutine 数稳定在 200-500,不随流量增长。

**异常状态**:goroutine 数持续增长,可能是 channel 阻塞或 goroutine 泄漏。

### 6.2 GC 调优

#### 6.2.1 GOGC 参数

Go GC 触发频率由 `GOGC` 控制(默认 100 = 堆翻倍时触发):

| GOGC | 效果 | 适用场景 |
|---|---|---|
| 50 | GC 频繁,CPU 开销大,内存占用低 | 内存受限 |
| 100(默认) | 平衡 | 通用 |
| 200 | GC 少,CPU 开销小,内存占用高 | 吞吐优先 |
| 400 | GC 很少,内存占用翻 4 倍 | 延迟优先(大内存机器) |
| off | 关闭 GC | 仅短时压测,生产禁用 |

**prom-gw 推荐配置**:

```bash
# 生产环境:吞吐优先,内存充裕
GOGC=200

# 内存受限环境(如 8G 机器跑满负载)
GOGC=100

# 低延迟场景(p99 < 100ms)
GOGC=150  + GOMEMLIMIT=6GiB
```

#### 6.2.2 GOMEMLIMIT 参数

Go 1.19+ 引入 `GOMEMLIMIT`,硬性限制 Go 堆上限:

```bash
# systemd 配置(已在 prom-gw@.service 中)
Environment=GOMEMLIMIT=6GiB

# 或环境变量
GOMEMLIMIT=6GiB ./bin/prom-gw ...
```

> `GOMEMLIMIT` 应设为 cgroup 内存限制的 80-90%,留余量给非 Go 内存(stack、CGO、mmap)。

#### 6.2.3 GC 监控

```bash
# 查看 GC 频率和耗时
curl -s http://127.0.0.1:8080/metrics | grep -E "go_gc_duration_seconds|go_memstats_gc"

# 关键指标
go_gc_duration_seconds{quantile="0"}     # 最小 GC 耗时
go_gc_duration_seconds{quantile="0.5"}   # 中位 GC 耗时
go_gc_duration_seconds{quantile="1"}     # 最大 GC 耗时(< 10ms 为佳)
go_memstats_gc_cpu_ratio                 # GC CPU 占比(< 0.05 为佳)
```

**GC 调优判定**:

| 指标 | 正常 | 需优化 |
|---|---|---|
| GC p99 耗时 | < 10ms | > 50ms |
| GC CPU 占比 | < 5% | > 15% |
| GC 频率 | < 1/s | > 10/s |
| Stop-the-world | < 1ms | > 10ms |

### 6.3 CPU 调优

#### 6.3.1 GOMAXPROCS

```bash
# 默认 = CPU 核数,生产环境建议显式设置
Environment=GOMAXPROCS=8   # 在 prom-gw@.service 中

# 或环境变量
GOMAXPROCS=8 ./bin/prom-gw ...
```

**推荐值**:

| CPU 核数 | GOMAXPROCS | 说明 |
|---|---|---|
| 4 核 | 4 | 默认即可 |
| 8 核 | 8 | 1.5M samples/s 推荐 |
| 16 核 | 12-16 | 留余量给 OS / Kafka client |
| cgroup 限制 | = limit | 容器环境必须显式设置 |

#### 6.3.2 pipeline buffer 调优

prom-gw 内部 pipeline 使用 channel buffer,大小影响延迟和内存:

```yaml
# configs/rules/app-business.yaml
global:
  channel_buffer: 65535    # 默认 65535
```

| channel_buffer | 延迟 | 内存 | 背压风险 | 适用场景 |
|---|---|---|---|---|
| 1024 | 低 | 低 | 高(易 503) | 低流量 |
| 16384 | 中 | 中 | 中 | 通用 |
| 65535(默认) | 略高 | 中 | 低 | 高吞吐(推荐) |
| 262144 | 高 | 高 | 极低 | 超高吞吐 + 大内存 |

### 6.4 内存优化

#### 6.4.1 series 基数控制

downsample / deadvalue 等状态型 stage 会缓存 series 状态,series 数直接影响内存:

```
预估内存 = series_count × avg_labels_bytes × 1.5(含 map 开销)
```

**优化建议**:

| 场景 | series 数 | 预估内存 | 建议 |
|---|---|---|---|
| 小型集群 | < 10K | < 500MB | 无需优化 |
| 中型集群 | 10K-100K | 500MB-5GB | relabel 删除无用标签 |
| 大型集群 | > 100K | > 5GB | sample 降采样 + 分租户 |

**relabel 优化示例**(减少 series 基数):

```yaml
stages:
  - type: relabel
    drop_labels:
      - instance          # 删除高基数标签
      - pod               # 删除高频变化标签
      - container_id
    keep_labels:
      - __name__
      - job
      - team
```

#### 6.4.2 采样降负载

```yaml
stages:
  - type: sample
    rate: 0.1             # 保留 10%,减少 90% 下游负载
```

| sample rate | 吞吐影响 | 内存影响 | 适用场景 |
|---|---|---|---|
| 1.0(不采样) | 基线 | 基线 | 精确监控 |
| 0.5 | -50% | -50% | 告警 + 部分监控 |
| 0.1 | -90% | -90% | 历史趋势 |
| 0.01 | -99% | -99% | 大盘统计 |

### 6.5 性能优化决策树

```
压测未达标?
│
├─ 吞吐不足 (< 1.5M)
│   ├─ CPU > 90%? → 检查 CPU profile,优化热点函数
│   ├─ CPU < 50%? → 检查锁竞争(mutex profile)、IO 瓶颈
│   └─ 错误率 > 0? → 检查背压(pipeline buffer)、限流配置
│
├─ p99 延迟高 (> 500ms)
│   ├─ GC 耗时大? → 调整 GOGC=200、检查 heap profile
│   ├─ WAL 积压? → 检查 Kafka 连通性、磁盘 IO
│   ├─ 大 batch? → 降低 samples-per-batch
│   └─ 锁竞争? → 检查 mutex profile
│
├─ 内存超限 (> 8GB)
│   ├─ series 基数大? → 添加 relabel 删除无用标签
│   ├─ buffer 过大? → 降低 channel_buffer
│   └─ 泄漏? → 对比 heap profile,inuse_space 持续增长
│
├─ goroutine 泄漏 (> 5000)
│   ├─ channel 阻塞? → 检查 goroutine profile
│   ├─ HTTP 连接泄漏? → 检查 MaxIdleConns 配置
│   └─ safego 未退出? → 检查 context cancel
│
└─ 错误率高 (> 0.01%)
    ├─ 4xx? → token 鉴权问题、payload 格式错误
    ├─ 429? → 限流配置过低
    ├─ 503? → 背压(pipeline 满)、WAL 硬拒绝
    └─ 5xx? → 内部错误,查看日志
```

### 6.6 调优参数速查表

| 参数 | 位置 | 默认值 | 调优方向 | 影响 |
|---|---|---|---|---|
| `GOMAXPROCS` | systemd env | CPU 核数 | = 核数 | 并行度 |
| `GOGC` | systemd env | 100 | 200(吞吐) | GC 频率 |
| `GOMEMLIMIT` | systemd env | 无 | 6GiB(8G 机器) | 内存上限 |
| `channel_buffer` | ruleset yaml | 65535 | 16384-65535 | 延迟 vs 背压 |
| `rate_limit` | token yaml | 80000 | 按租户调整 | 限流阈值 |
| `--wal-max-bytes` | CLI flag | 50GB | 100GB | WAL 容量 |
| `--wal-disk-used-ratio` | CLI flag | 0.80 | 0.85 | 磁盘硬拒绝阈值 |
| `samples-per-batch` | loadgen | 500 | 100-1000 | 压测负载模型 |
| `concurrency` | loadgen | 4 | 4-8 | 压测并发数 |

---

## 7. 更多测试场景

### 7.1 Kafka 端到端压测

验证 prom-gw + Kafka 全链路吞吐(非 WAL-only):

#### 7.1.1 前置条件

```bash
# 启动 Kafka(单节点 KRaft,见 local-dev-guide.md 第 3 章)
export KAFKA_BROKERS=localhost:9092

# 创建测试 topic
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.perf.raw.app_business \
    --partitions 12 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic prom.perf.routed.core \
    --partitions 12 --replication-factor 1
```

#### 7.1.2 执行压测

```bash
# 启动 prom-gw(连接 Kafka)
KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-kafka-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 3

# 压测(1M samples/s × 3min,留余量给 Kafka)
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 \
    --samples-per-batch=500 \
    --duration=180s \
    --concurrency=8 \
    --metrics-url=http://127.0.0.1:8080/metrics
```

#### 7.1.3 验证 Kafka 消费

```bash
# 查看 topic offset(确认数据写入)
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic prom.perf.raw.app_business
# 期望: 各 partition offset 总和 ≈ rate × duration / samples_per_batch

# 消费速率测试
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic prom.perf.raw.app_business \
    --max-messages 100 \
    --timeout-ms 10000 | wc -c
# 期望: 消费到数据,每条约 3-4KB

# 清理
kill $GW_PID
rm -rf /tmp/perf-kafka-wal
```

#### 7.1.4 判定标准

| 指标 | 目标 | 说明 |
|---|---|---|
| GW 侧吞吐 | ≥ 1M samples/s | Kafka 写入不拖慢 GW |
| Kafka 写入延迟 | p99 < 50ms | `gateway_stage_duration_seconds{stage="kafka"}` |
| WAL 积压 | 0 | Kafka 正常时不应有 WAL 积压 |
| Kafka produce 错误 | 0 | `gateway_produce_errors_total` |
| 端到端延迟 | < 2s | Prometheus 写入到 Kafka 可消费 |

### 7.2 规则引擎压测

验证 relabel + route + sample + downsample 多 stage 串联的性能影响:

#### 7.2.1 配置多 stage ruleset

```yaml
# configs/rules/perf-heavy.yaml
rulesets:
  - name: perf-heavy
    tenant: app-business
    default_topic: prom.perf.routed.app_business
    version: 1
    match:
      metric_prefix: ""
    stages:
      - type: relabel
        drop_labels: [env, instance, pod, container_id]
        keep_labels: [__name__, job, team, cluster]
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.perf.routed.core
          - match: { team: "infra" }
            topic: prom.perf.routed.infra
          - match: { team: "data" }
            topic: prom.perf.routed.data

      - type: sample
        rate: 0.5              # 保留 50%

      - type: downsample
        interval: 60s          # 1 分钟降采样
        aggregation: avg

      - type: deadvalue
        max_age: 300s          # 5 分钟无更新判定为死值

global:
  rate_limit_per_instance: 2000000
  channel_buffer: 65535
```

#### 7.2.2 执行压测

```bash
# 对比:空 ruleset vs 重 ruleset
echo "=== 空 ruleset ==="
CFG=configs/rules/default.yaml RATE=1500000 DURATION=120s \
    bash test/perf/profile.sh

echo "=== 重 ruleset (5 stage) ==="
CFG=configs/rules/perf-heavy.yaml RATE=1500000 DURATION=120s \
    bash test/perf/profile.sh

# 对比两次 CPU profile
go tool pprof -base perf-out/<空ruleset>/cpu.pprof \
    perf-out/<重ruleset>/cpu.pprof
```

#### 7.2.3 判定标准

| 场景 | 预期吞吐下降 | 预期 CPU 增量 | 说明 |
|---|---|---|---|
| 空 ruleset | 基线 | 基线 | 纯 parse + forward |
| relabel only | < 5% | +3-5% | 标签操作轻量 |
| relabel + route | < 8% | +5-8% | 路由匹配开销 |
| + sample | < 10% | +5-8% | 采样减少下游负载 |
| + downsample | < 20% | +10-15% | 状态维护开销大 |
| + deadvalue | < 25% | +15-20% | series 跟踪内存 + CPU |

### 7.3 WAL drain 压测

验证 Kafka 恢复后 WAL 积压数据的 drain 速率:

#### 7.3.1 构造 WAL 积压

```bash
# 1. 启动 prom-gw(连接不存在的 Kafka,强制降级到 WAL)
KAFKA_BROKERS=127.0.0.1:9999 \
./bin/prom-gw \
    --config=configs/rules/default.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-drain-wal \
    --wal-max-bytes=5368709120 \
    --source-dc=dc-perf &

GW_PID=$!
sleep 3

# 2. 压测 60s,数据全部落 WAL
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=500000 \
    --duration=60s \
    --concurrency=4

# 3. 记录 WAL 积压量
WAL_BYTES_BEFORE=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes | awk '{print $2}')
echo "WAL 积压: ${WAL_BYTES_BEFORE} bytes"
```

#### 7.3.2 启动 Kafka 并观察 drain

```bash
# 4. 启动 Kafka(或恢复 Kafka 连接)
kafka-server-start.sh config/local.properties &
sleep 5

# 5. 重启 prom-gw,连接真实 Kafka
kill $GW_PID
sleep 2

KAFKA_BROKERS=localhost:9092 \
./bin/prom-gw \
    --config=configs/rules/default.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-drain-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 2

# 6. 监控 WAL drain 过程(每 5s 采样)
START_TIME=$(date +%s)
while true; do
    WAL_BYTES=$(curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes | awk '{print $2}')
    ELAPSED=$(( $(date +%s) - START_TIME ))
    echo "t=${ELAPSED}s wal_bytes=${WAL_BYTES}"
    if [ "$WAL_BYTES" = "0" ]; then
        echo "WAL drain 完成,耗时 ${ELAPSED}s"
        break
    fi
    sleep 5
done

# 7. 计算 drain 速率
DRAIN_RATE=$(( WAL_BYTES_BEFORE / ELAPSED ))
echo "WAL drain 速率: ${DRAIN_RATE} bytes/s"
```

#### 7.3.3 判定标准

| 指标 | 目标 | 说明 |
|---|---|---|
| drain 速率 | ≥ 50 MB/s | SSD + Kafka 正常 |
| drain 期间错误率 | 0 | drain 不影响新请求 |
| drain 期间 p99 延迟 | < 1s | drain 期间延迟会升高但可接受 |
| WAL 完全清空 | 是 | `gateway_wal_bytes` 归零 |

### 7.4 配置热更新压测

验证高负载下 ruleset 热切换不影响请求处理:

```bash
# 1. 启动 prom-gw + 持续压测(后台)
./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/perf-hotreload-wal \
    --source-dc=dc-perf &

GW_PID=$!
sleep 2

# 2. 持续压测(后台,5min)
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 --duration=300s --concurrency=8 &
LOADGEN_PID=$!

# 3. 压测中途热更新配置(修改 sample rate)
sleep 60
cp configs/rules/app-business.yaml /tmp/rules-v2.yaml
sed -i 's/rate: 0.1/rate: 0.05/' /tmp/rules-v2.yaml

# 通过 Admin API 热更新
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
    -H "Content-Type: application/yaml" \
    --data-binary @/tmp/rules-v2.yaml

echo "配置已热更新,观察压测是否中断..."

# 4. 再次热更新(改回)
sleep 60
curl -X PUT http://127.0.0.1:8082/v1/rulesets/app-business \
    -H "Content-Type: application/yaml" \
    --data-binary @configs/rules/app-business.yaml

# 5. 等待压测结束
wait $LOADGEN_PID

# 6. 检查结果
echo "检查 loadgen 输出:热更新期间不应有错误或延迟突增"

# 清理
kill $GW_PID
rm -rf /tmp/perf-hotreload-wal /tmp/rules-v2.yaml
```

**判定标准**:
- 热更新期间错误率 = 0
- 热更新瞬间 p99 延迟毛刺 < 100ms
- `gateway_ruleset_switch_total` 计数 +1
- `gateway_config_reload_total{status="ok"}` 计数 +1

### 7.5 长连接稳定性测试

验证 HTTP keep-alive 长连接在长时间运行下的稳定性:

```bash
# loadgen 默认使用 keep-alive(MaxIdleConns=200)
# 运行 30min,观察连接是否被重置
go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=1000000 \
    --duration=1800s \
    --concurrency=8 \
    --metrics-url=http://127.0.0.1:8080/metrics
```

**监控项**:

```bash
# HTTP 连接数(应稳定,不持续增长)
ss -tnp | grep :19201 | wc -l

# TIME_WAIT 连接(应 < 1000)
ss -tn state time-wait | wc -l

# FD 数(应稳定)
lsof -p $(pidof prom-gw) | wc -l
```

---

## 8. CI/CD 性能回归集成

### 8.1 集成方案

将性能冒烟测试集成到 GitHub Actions CI,每次 PR 自动运行,防止性能回归:

```
PR 提交
  │
  ├─ lint + unit test (已有)
  ├─ build (已有)
  └─ perf smoke test (新增)
       │
       ├─ 构建 prom-gw (带 pprof)
       ├─ 启动 prom-gw (WAL-only)
       ├─ 跑 loadgen (100K samples/s × 30s)
       ├─ 采集 metrics + profile
       └─ 判定:
            ├─ 吞吐 ≥ 100K samples/s → PASS
            ├─ p99 < 500ms → PASS
            ├─ 错误率 < 0.01% → PASS
            └─ 任一不达标 → FAIL(阻断合并)
```

### 8.2 GitHub Actions Workflow

在 `.github/workflows/ci.yml` 追加 perf job:

```yaml
  perf:
    name: perf smoke
    runs-on: ubuntu-latest
    needs: build
    # 仅 main 分支和 PR 时触发,避免资源浪费
    if: github.event_name == 'pull_request' || github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.23'
          cache: true

      # 下载 build job 产出的二进制
      - uses: actions/download-artifact@v4
        with:
          name: prom-gw-linux-amd64
          path: bin/

      - name: chmod +x
        run: chmod +x bin/prom-gw

      # 设置 Go 缓存目录(sandbox 兼容)
      - name: set go cache
        run: |
          echo "GOCACHE=/tmp/gocache-prom-gw" >> $GITHUB_ENV
          echo "GOMODCACHE=/tmp/gomodcache" >> $GITHUB_ENV

      # 启动 prom-gw(WAL-only 模式)
      - name: start prom-gw
        run: |
          ./bin/prom-gw \
            --config=configs/rules/app-business.yaml \
            --tokens=configs/tokens/local.yaml \
            --wal-dir=/tmp/perf-wal \
            --write-addr=:19201 \
            --metrics-addr=:8080 \
            --health-addr=:8081 \
            --admin-addr=:8082 \
            --admin-allow-cidr=127.0.0.1/32 \
            --source-dc=dc-ci \
            > /tmp/prom-gw.log 2>&1 &

          echo $! > /tmp/prom-gw.pid

          # 等待启动
          for i in $(seq 1 30); do
            curl -fsS http://127.0.0.1:8081/healthz && break
            sleep 0.5
          done

      # 执行压测
      - name: run loadgen
        id: loadgen
        run: |
          go run ./test/loadgen \
            --url=http://127.0.0.1:19201/api/v1/write \
            --token=tk_app_business_dev \
            --rate=100000 \
            --samples-per-batch=500 \
            --duration=30s \
            --concurrency=4 \
            --metrics-url=http://127.0.0.1:8080/metrics \
            2>&1 | tee /tmp/loadgen.log

      # 解析结果并判定
      - name: parse results
        run: |
          # 从 loadgen 输出解析最终结果
          RATE=$(grep "^rate=" /tmp/loadgen.log | tail -1 | awk -F'[= ]' '{print $2}')
          P99=$(grep "^latency" /tmp/loadgen.log | tail -1 | grep -oP 'p99=\K[0-9.]+')
          ERR=$(grep "err_batches" /tmp/loadgen.log | tail -1 | grep -oP '\(\K[0-9.]+')
          SENT=$(grep "sent_batches" /tmp/loadgen.log | tail -1 | grep -oP 'sent_batches=\K[0-9]+')

          echo "::notice::rate=${RATE} samples/s p99=${P99} err_rate=${ERR}%"

          # 判定
          PASS=true
          if [ -z "$RATE" ] || [ "$RATE" -lt 90000 ]; then
            echo "::error::吞吐不足: ${RATE} < 90000 samples/s"
            PASS=false
          fi
          if [ -z "$ERR" ] || (( $(echo "$ERR > 0.01" | bc -l) )); then
            echo "::error::错误率过高: ${ERR}%"
            PASS=false
          fi

          if [ "$PASS" != "true" ]; then
            echo "::error::性能冒烟测试未通过"
            exit 1
          fi
          echo "::notice::性能冒烟测试通过"

      # 上传 profile artifact(便于离线分析)
      - name: upload artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: perf-profiles
          path: |
            /tmp/prom-gw.log
            /tmp/loadgen.log

      # 清理
      - name: cleanup
        if: always()
        run: |
          kill $(cat /tmp/prom-gw.pid) 2>/dev/null || true
          rm -rf /tmp/perf-wal
```

### 8.3 性能门禁策略

| 检查项 | CI 阈值 | 发版阈值 | 说明 |
|---|---|---|---|
| 吞吐 | ≥ 90K samples/s | ≥ 1.5M samples/s | CI 资源有限,用低阈值 |
| p99 延迟 | < 1s | < 500ms | CI 共享 runner 延迟偏高 |
| 错误率 | < 0.1% | < 0.01% | CI 允许略高 |
| 进程崩溃 | 无 | 无 | 任何 panic 阻断 |

> **注意**:CI runner 是共享虚拟机,CPU/内存/磁盘性能远不如生产物理机。CI 性能阈值远低于 SLO,仅用于检测**回归**(如代码改动导致吞吐下降 50%),不用于验证 SLO 达标。SLO 验证应在专用压测环境执行。

### 8.4 本地 pre-push 性能检查

在本地提交前快速验证,避免 CI 等待:

```bash
# ~/.git/hooks/pre-push (创建后 chmod +x)
#!/bin/bash
echo ">>> 运行性能冒烟测试..."
make build

./bin/prom-gw \
    --config=configs/rules/app-business.yaml \
    --tokens=configs/tokens/local.yaml \
    --wal-dir=/tmp/pre-push-wal \
    --source-dc=dc-local > /tmp/prom-gw.log 2>&1 &

GW_PID=$!
sleep 2

go run ./test/loadgen \
    --url=http://127.0.0.1:19201/api/v1/write \
    --token=tk_app_business_dev \
    --rate=50000 --duration=10s --concurrency=2 \
    2>&1 | tee /tmp/loadgen.log

kill $GW_PID
rm -rf /tmp/pre-push-wal

# 检查是否有错误
if grep -q "err_batches=0" /tmp/loadgen.log; then
    echo ">>> 性能冒烟通过"
    exit 0
else
    echo ">>> 性能冒烟失败,检查 /tmp/loadgen.log"
    exit 1
fi
```

---

## 9. 压测报告自动化

### 9.1 自动化脚本

以下脚本自动解析 `profile.sh` 输出,生成 Markdown 格式的压测报告:

```bash
#!/bin/bash
# scripts/gen-perf-report.sh
# 用法: bash scripts/gen-perf-report.sh <perf-out-dir>
# 生成: <perf-out-dir>/REPORT.md

set -euo pipefail

DIR="${1:-$(ls -td perf-out/*/ | head -1)}"
DIR="${DIR%/}"

if [ ! -f "$DIR/loadgen.log" ]; then
    echo "错误: $DIR/loadgen.log 不存在"
    exit 1
fi

LOG="$DIR/loadgen.log"
METRICS="$DIR/metrics.txt"
GW_LOG="$DIR/prom-gw.log"
REPORT="$DIR/REPORT.md"

# ===== 解析 loadgen 输出 =====
FINAL_LINE=$(grep "=== Final ===" -A 10 "$LOG" | tail -n +2)
RATE=$(echo "$FINAL_LINE" | grep "^rate=" | awk '{print $1}' | cut -d= -f2)
SENT_BATCHES=$(echo "$FINAL_LINE" | grep "sent_batches" | grep -oP 'sent_batches=\K[0-9]+')
ERR_BATCHES=$(echo "$FINAL_LINE" | grep "err_batches" | grep -oP 'err_batches=\K[0-9]+')
ERR_RATE=$(echo "$FINAL_LINE" | grep "err_batches" | grep -oP '\(\K[0-9.]+')
BYTES=$(echo "$FINAL_LINE" | grep "bytes=" | grep -oP 'bytes=\K[0-9]+')
LATENCY=$(echo "$FINAL_LINE" | grep "^latency")
P50=$(echo "$LATENCY" | grep -oP 'p50=\K[0-9a-z]+')
P95=$(echo "$LATENCY" | grep -oP 'p95=\K[0-9a-z]+')
P99=$(echo "$LATENCY" | grep -oP 'p99=\K[0-9a-z]+')
MAX_LAT=$(echo "$LATENCY" | grep -oP 'max=\K[0-9a-z]+')
SAMPLES_SENT=$(echo "$FINAL_LINE" | grep "samples_sent" | grep -oP 'samples_sent=\K[0-9]+')

# ===== 解析 GW metrics =====
GW_PARSE_OK=""
GW_REQ_P99=""
GW_ERRORS=""
GW_BACKPRESSURE=""
GW_GOROUTINES=""
GW_MEM=""
GW_CPU=""
GW_WAL_BYTES=""

if [ -f "$METRICS" ]; then
    GW_PARSE_OK=$(grep 'gateway_samples_total{stage="parse",status="ok"' "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_GOROUTINES=$(grep "^gateway_goroutines" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_MEM=$(grep "^gateway_mem_bytes" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_CPU=$(grep "^gateway_cpu_ratio" "$METRICS" | awk '{print $2}' || echo "N/A")
    GW_WAL_BYTES=$(grep "^gateway_wal_bytes" "$METRICS" | awk '{print $2}' | tail -1 || echo "N/A")
fi

# ===== 解析 prom-gw 版本 =====
VERSION=$(grep "starting prom-gw" "$GW_LOG" 2>/dev/null | grep -oP 'version=\\?"?\K[^" ]+' || echo "unknown")
START_TIME=$(grep "starting prom-gw" "$GW_LOG" 2>/dev/null | grep -oP 'ts=\\?"?\K[^" ]+' || echo "unknown")

# ===== 格式化辅助 =====
fmt_bytes() {
    local b=$1
    if [ -z "$b" ] || [ "$b" = "N/A" ]; then echo "N/A"; return; fi
    if [ "$b" -gt 1073741824 ]; then
        echo "$(echo "scale=2; $b / 1073741824" | bc) GB"
    elif [ "$b" -gt 1048576 ]; then
        echo "$(echo "scale=2; $b / 1048576" | bc) MB"
    elif [ "$b" -gt 1024 ]; then
        echo "$(echo "scale=2; $b / 1024" | bc) KB"
    else
        echo "$b B"
    fi
}

fmt_mem() {
    local b=$1
    if [ -z "$b" ] || [ "$b" = "N/A" ]; then echo "N/A"; return; fi
    echo "$(echo "scale=2; $b / 1073741824" | bc) GB"
}

fmt_cpu() {
    local r=$1
    if [ -z "$r" ] || [ "$r" = "N/A" ]; then echo "N/A"; return; fi
    echo "$(echo "scale=1; $r * 100" | bc)%"
}

# ===== 判定 =====
PASS=true
[ -n "$RATE" ] && [ "$RATE" -ge 1500000 ] 2>/dev/null || PASS=false
[ -n "$ERR_RATE" ] && (( $(echo "$ERR_RATE < 0.01" | bc -l) )) 2>/dev/null || PASS=false
RESULT=$([ "$PASS" = "true" ] && echo "PASS" || echo "FAIL")

# ===== 生成报告 =====
cat > "$REPORT" << EOF
# prom-gw 压力测试报告(自动生成)

## 测试信息

| 项 | 值 |
|---|---|
| 生成时间 | $(date '+%Y-%m-%d %H:%M:%S') |
| prom-gw 版本 | ${VERSION} |
| 启动时间 | ${START_TIME} |
| 数据目录 | ${DIR} |

## 压测参数

| 参数 | 值 |
|---|---|
| 目标 rate | ${RATE} samples/s |
| sent_batches | ${SENT_BATCHES} |
| err_batches | ${ERR_BATCHES} (${ERR_RATE}%) |
| bytes_sent | $(fmt_bytes "$BYTES") |
| samples_sent | ${SAMPLES_SENT} |

## 客户端结果(loadgen)

| 指标 | 值 |
|---|---|
| 实际吞吐 | ${RATE} samples/s |
| p50 延迟 | ${P50} |
| p95 延迟 | ${P95} |
| p99 延迟 | ${P99} |
| max 延迟 | ${MAX_LAT} |
| 错误率 | ${ERR_RATE}% |

## 服务端指标(/metrics)

| 指标 | 值 |
|---|---|
| 解析成功 sample 数 | ${GW_PARSE_OK} |
| Goroutines | ${GW_GOROUTINES} |
| 内存 | $(fmt_mem "$GW_MEM") |
| CPU | $(fmt_cpu "$GW_CPU") |
| WAL 积压 | $(fmt_bytes "$GW_WAL_BYTES") |

## 判定结果

**${RESULT}**

| 判定项 | 阈值 | 实测 | 结果 |
|---|---|---|---|
| 吞吐 | ≥ 1,500,000 | ${RATE} | $([ -n "$RATE" ] && [ "$RATE" -ge 1500000 ] 2>/dev/null && echo "✅" || echo "❌") |
| 错误率 | < 0.01% | ${ERR_RATE}% | $(( $(echo "${ERR_RATE:-1} < 0.01" | bc -l) ) && echo "✅" || echo "❌") |

## Profile 文件

| 文件 | 路径 |
|---|---|
| prom-gw 日志 | ${DIR}/prom-gw.log |
| loadgen 日志 | ${DIR}/loadgen.log |
| CPU profile | ${DIR}/cpu.pprof |
| Heap profile | ${DIR}/heap.pprof |
| metrics 快照 | ${DIR}/metrics.txt |

## 分析命令

\`\`\`bash
# 查看 CPU 火焰图
go tool pprof -http=:9999 ${DIR}/cpu.pprof

# 查看 Heap 分配热点
go tool pprof -top -nodecount=20 -sample_index=alloc_space ${DIR}/heap.pprof

# 对比基线
go tool pprof -base <baseline>/cpu.pprof ${DIR}/cpu.pprof
\`\`\`
EOF

echo "报告已生成: $REPORT"
echo "判定结果: $RESULT"
```

### 9.2 使用方式

```bash
# 方式 1:压测后自动生成(集成到 profile.sh)
bash test/perf/profile.sh
bash scripts/gen-perf-report.sh perf-out/<latest>/

# 方式 2:指定目录生成
bash scripts/gen-perf-report.sh perf-out/20260812-143000/

# 方式 3:生成后直接查看
cat perf-out/20260812-143000/REPORT.md
```

### 9.3 批量对比脚本

对比多次压测结果,生成趋势报告:

```bash
#!/bin/bash
# scripts/perf-compare.sh
# 用法: bash scripts/perf-compare.sh perf-out/run-a/ perf-out/run-b/

RUN_A="${1:?usage: $0 <run-a> <run-b>}"
RUN_B="${2:?usage: $0 <run-a> <run-b>}"

echo "# 性能对比报告"
echo ""
echo "| 指标 | Run A ($(basename $RUN_A)) | Run B ($(basename $RUN_B)) | 变化 |"
echo "|---|---|---|---|"

# 提取各指标的函数
extract_metric() {
    local dir=$1
    local pattern=$2
    grep "$pattern" "$dir/loadgen.log" | tail -1 | grep -oP "$3" || echo "N/A"
}

RATE_A=$(extract_metric "$RUN_A" "^rate=" '[0-9]+')
RATE_B=$(extract_metric "$RUN_B" "^rate=" '[0-9]+')
P99_A=$(extract_metric "$RUN_A" "latency" 'p99=\K[0-9a-z]+')
P99_B=$(extract_metric "$RUN_B" "latency" 'p99=\K[0-9a-z]+')
ERR_A=$(extract_metric "$RUN_A" "err_batches" '\(\K[0-9.]+')
ERR_B=$(extract_metric "$RUN_B" "err_batches" '\(\K[0-9.]+')

# 计算变化率
if [ "$RATE_A" != "N/A" ] && [ "$RATE_B" != "N/A" ] && [ "$RATE_A" -gt 0 ]; then
    RATE_DELTA=$(echo "scale=1; ($RATE_B - $RATE_A) * 100 / $RATE_A" | bc)
    echo "| 吞吐 | $RATE_A | $RATE_B | ${RATE_DELTA}% |"
else
    echo "| 吞吐 | $RATE_A | $RATE_B | N/A |"
fi

echo "| p99 延迟 | $P99_A | $P99_B | - |"
echo "| 错误率 | ${ERR_A}% | ${ERR_B}% | - |"

echo ""
echo "## Profile 对比"
echo ""
echo '```bash'
echo "# CPU profile diff"
echo "go tool pprof -base $RUN_A/cpu.pprof $RUN_B/cpu.pprof"
echo ""
echo "# Heap profile diff"
echo "go tool pprof -base $RUN_A/heap.pprof -sample_index=alloc_space $RUN_B/heap.pprof"
echo '```'
```

**使用**:

```bash
# 对比两次发版的性能
bash scripts/perf-compare.sh perf-out/v1.0-baseline/ perf-out/v1.1-candidate/

# 输出示例:
# | 指标 | Run A (v1.0-baseline) | Run B (v1.1-candidate) | 变化 |
# |---|---|---|---|
# | 吞吐 | 1502400 | 1485000 | -1.2% |
# | p99 延迟 | 180ms | 195ms | - |
# | 错误率 | 0.000% | 0.000% | - |
```

### 9.4 性能回归判定

| 变化幅度 | 判定 | 动作 |
|---|---|---|
| 吞吐下降 < 5% | 正常波动 | 无需处理 |
| 吞吐下降 5-10% | 需关注 | 检查 profile,定位原因 |
| 吞吐下降 > 10% | 性能回归 | 阻断发版,必须修复 |
| p99 延迟增加 < 20% | 正常波动 | 无需处理 |
| p99 延迟增加 > 50% | 性能回归 | 阻断发版 |
| 内存增长 > 20% | 可能泄漏 | 阻断发版,检查 heap profile |

---

## 10. 附录

### 10.1 常用命令速查

```bash
# 冒烟压测(30s)
bash test/perf/profile.sh

# 基线回归(1.5M × 5min)
RATE=1500000 DURATION=300s bash test/perf/profile.sh

# 稳定性测试(1.5M × 1h)
RATE=1500000 DURATION=3600s bash test/perf/profile.sh

# 容量阶梯
for r in 100000 500000 1000000 1500000 2000000; do
    RATE=$r DURATION=180s bash test/perf/profile.sh; sleep 10
done

# 查看 CPU 火焰图
go tool pprof -http=:9999 perf-out/<latest>/cpu.pprof

# 实时查看 GW 指标
watch -n 1 'curl -s http://127.0.0.1:8080/metrics | grep -E "gateway_samples_total|gateway_goroutines|gateway_mem_bytes"'

# 查看 admin stats
curl -s http://127.0.0.1:8082/v1/stats | jq .
```

### 10.2 故障排查

| 现象 | 可能原因 | 解决方法 |
|---|---|---|
| loadgen 报 `connection refused` | prom-gw 未启动或端口错误 | 检查 `curl http://127.0.0.1:8081/healthz` |
| 实际吞吐远低于 rate | CPU 打满或限流 | 检查 `gateway_cpu_ratio`、`gateway_rate_limit_rejected_total` |
| 错误率 > 0 | payload 格式错误或 token 无效 | 检查 loadgen 日志和 prom-gw 日志 |
| p99 延迟突增 | GC stop-the-world 或 WAL 积压 | 查看 `gateway_wal_bytes`、heap profile |
| 内存持续增长 | series 泄漏或 buffer 未释放 | 对比 heap profile,grep `runtime.malg` |
| goroutine 持续增长 | channel 泄漏或 goroutine 未退出 | `go tool pprof goroutine.pprof` 查看堆栈 |

### 10.3 输出目录归档

建议按日期归档压测结果,便于版本对比:

```bash
# 归档
tar -czf perf-archive/$(date +%Y%m%d)-baseline.tar.gz \
    -C perf-out/<latest> .

# 对比两次 CPU profile
go tool pprof -base perf-archive/20260805-baseline/cpu.pprof \
    perf-out/20260812-baseline/cpu.pprof
```

### 10.4 相关文档

- [SLO 指标定义](slo.md) — 性能目标与告警分级
- [配置参数说明](configuration-reference.md) — 全部启动参数与调优
- [本地部署指南](local-dev-guide.md) — 本地环境搭建
- [生产部署指南](production-guide.md) — 生产环境部署
- [高可用与负载均衡](ha-lb-deployment.md) — Nginx/Keepalived 部署
- [运维手册](runbook.md) — 日常运维与故障处理
- [混沌测试](../chaos/README.md) — 故障注入测试

### 10.5 自动化脚本索引

| 脚本 | 用途 | 位置 |
|---|---|---|
| `profile.sh` | 一键压测 + profile 采集 | [test/perf/profile.sh](../../test/perf/profile.sh) |
| `gen-perf-report.sh` | 自动生成压测报告 | `scripts/gen-perf-report.sh`(见 §9.1) |
| `perf-compare.sh` | 两次压测结果对比 | `scripts/perf-compare.sh`(见 §9.3) |
| `loadgen` | RemoteWrite 协议压测客户端 | [test/loadgen/main.go](../../test/loadgen/main.go) |
