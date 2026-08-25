# SLO 指标定义
## 1. 可用性

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 实例可用性 | 99.95% 月度 | systemd 运行时间 / 总时间 |
| 端到端可用性(含 Kafka) | 99.9% 月度 | `success_2xx` / `total` |

**误差预算**:0.05% × 30 天 = 21.6 分钟/月
超预算时,触发非紧急变更冻结(只允许修复故障)。

## 2. 性能

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 吞吐 | ≥ 1.5M samples/s 单实例 | `rate(gateway_samples_total{stage="parse",status="ok"})[1m]` |
| p99 延迟 | < 500ms | `histogram_quantile(0.99, ...)` |
| p50 延迟 | < 50ms | 同上 |
| 错误率 | < 0.01% | `rate(gateway_errors_total) / rate(gateway_samples_total)` |
| 背压拒绝率 | < 0.1% | `rate(gateway_backpressure_rejected_total) / rate(gateway_samples_total)` |

## 3. 数据完整性

| 指标 | 目标 | 测量方法 |
|---|---|---|
| 数据丢失率 | 0(Kafka 不可用时落 WAL) | chaos 测试 + count 校验 |
| Kafka 写 ack | all + idempotent | producer 配置 |
| WAL 硬拒绝率 | 0(否则 503) | `increase(gateway_wal_hard_reject_total[1d])` |
| TraceID 端到端传递率 | 100% | OTel 测试 |

## 4. 资源

| 指标 | 目标 | 测量方法 |
|---|---|---|
| CPU | < 70%(1.5M samples/s 持续) | `process_cpu_seconds_total` |
| 内存 | < 8G | `go_memstats_heap_inuse_bytes` |
| Goroutines | < 5000 | `gateway_goroutines` |
| FD | 增量 < 100 / 24h | `lsof -p $PID \| wc -l` |

## 5. 告警分级

### 5.1 严重 (Critical)

- 错误率 > 1% 持续 5 分钟
- p99 延迟 > 1s 持续 5 分钟
- WAL 硬拒绝率 > 0
- 实例 down(healthz 503)

**响应 SLA**:on-call 5 分钟内确认,15 分钟内开始处置。

### 5.2 警告 (Warning)

- 4xx/5xx 持续 > 10/s
- 鉴权失败 > 50/s
- p99 延迟 0.5-1s
- Goroutines > 5000
- Config reload 失败

**响应 SLA**:30 分钟内确认,4 小时内处置。

### 5.3 信息 (Info)

- 单次 config reload 失败(后自动恢复)
- 单次 token reload 失败(后自动恢复)
- 短时(< 1m)背压拒绝

**响应 SLA**:下次工作日处理。

## 6. 容量规划

| 规模 | 实例数 | 备注 |
|---|---|---|
| < 500K samples/s | 1 | 单机 + 限流 |
| 500K - 1.5M | 2 | 主备 |
| 1.5M - 3M | 4 | LB 后 |
| > 3M | 8+ | 按 Kafka partition 数扩展 |

## 7. 性能基线回归

每次发版前跑性能回归:
```bash
RATE=1500000 DURATION=300s bash test/perf/profile.sh
```

判定标准:
- 持续 5 分钟 ≥ 1.5M samples/s
- p99 < 500ms
- 错误率 < 0.01%
- 内存 < 8G
- CPU < 70%

未达标时阻断发版。



---

