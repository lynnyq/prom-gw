# 故障排查 Runbook

## 1. 高错误率 (PromGwHighErrorRate 触发)

**症状**:`gateway_errors_total` 速率 > 1%。

**排查**:
```bash
# 1. 看错误分类
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total

# 2. 看具体 type
# gateway_errors_total{stage="decode",type="..."}  # 解码失败
# gateway_errors_total{stage="auth",type="..."}     # 鉴权失败
# gateway_errors_total{stage="kafka",type="..."}    # Kafka 写失败
```

**常见根因**:
- `decode`:客户端发送非 snappy/protobuf 字节 → 检查 Prometheus remote_write 配置
- `auth`:token 失效或被吊销 → 更新 `tokens.yaml` 并 HUP
- `kafka`:Kafka 不可达 → 检查 `gateway_kafka_*` 指标(若有)和 Kafka 集群
- `wal_full`:WAL 满 → 清理或扩容

## 2. 503 背压拒绝持续

**症状**:`gateway_backpressure_rejected_total` 持续 > 0。

**排查**:
```bash
# 看 channel 深度
curl -s http://127.0.0.1:8082/v1/stats  # admin API

# 看 Kafka 是否慢
# 如果有 kafka exporter,看 consumer lag
```

**处置**:
- 短期:扩容 prom-gw 实例数(横向扩展)
- 中期:增加 Kafka partition 数 / 扩容 Kafka 集群
- 长期:优化下游消费,降低反压

## 3. WAL 卡住不排空

**症状**:`gateway_wal_oldest_age_seconds` 持续 > 60s。

**排查**:
```bash
# 看 WAL 段数 / 大小
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal

# 看 WAL 目录
ls -lah /data/wal
```

**常见根因**:
- Kafka 不可达:检查 `kafka brokers` 配置和网络
- Kafka 慢:Kafka 集群压力过大,看 broker 指标
- Nacos 推送错误配置:回滚到上一版本

**手动 drain**(应急):
```bash
# 1. 停 prom-gw
sudo systemctl stop prom-gw

# 2. 启动时强制走 WAL→Kafka 模式
# (默认行为,Kafka 恢复后自动 replay)

# 3. 启动
sudo systemctl start prom-gw

# 4. 观察日志
sudo journalctl -u prom-gw -f | grep -i "replay\|wal"
```

## 4. 规则版本不切换

**症状**:修改 ruleset 文件后,`gateway_ruleset_version` 没变。

**排查**:
```bash
# 1. 看 fsnotify 是否触发
sudo journalctl -u prom-gw -f | grep "apply snapshot"

# 2. 看是否校验失败
sudo journalctl -u prom-gw --since "10m ago" | grep -i "warn\|error"

# 3. 手动 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app:reload
```

**常见根因**:
- 文件权限:prom-gw 进程读不到文件
- YAML 语法错误:看日志里 `apply snapshot failed`
- version 必须递增:用 `gateway_ruleset_version` 确认

## 5. 实例 OOM

**症状**:prom-gw 进程被 OOM kill。

**排查**:
```bash
# 1. 看 heap profile
go tool pprof http://127.0.0.1:8080/debug/pprof/heap

# 2. 看 goroutine 数
curl -s http://127.0.0.1:8080/metrics | grep gateway_goroutines

# 3. 看 RSS
ps -o rss= -p $(pidof prom-gw)
```

**常见根因**:
- Downsample 状态过大:减少并发 ruleset 或缩短 interval
- DeadValue LRU 满:调整 LRU 容量
- WAL 段未清理:确认 Kafka 正常消费

## 6. 鉴权失败激增

**症状**:`gateway_auth_fail_total{reason="invalid"}` 突增。

**排查**:
```bash
# 看 reason 分类
curl -s http://127.0.0.1:8080/metrics | grep gateway_auth_fail_total
```

**常见根因**:
- 客户端 token 拼写错
- 客户端还在用旧 token(已被轮换)
- Prometheus remote_write URL 配错(漏了 `Bearer`)

## 7. 性能不达标 (QPS 不到 1.5M)

**排查**:
```bash
# 1. CPU profile
go tool pprof -top -cum http://127.0.0.1:8080/debug/pprof/profile?seconds=30

# 2. 看 stage 耗时
curl -s http://127.0.0.1:8080/metrics | grep gateway_stage_duration
```

**常见瓶颈**:
- parser 单线程:确认 `-concurrency` 启动参数(本项目 N pipeline goroutine,默认 = 1)
- Kafka 慢:看 broker 端的 `produce latency`
- 大量 label string 分配:确认 stringpool 启用

## 8. p99 延迟 > 1s

**症状**:`histogram_quantile(0.99, ...)` > 1s。

**排查**:
```bash
# 1. 看 p50/p95/p99
# Grafana: prom-gw 大盘 "端到端延迟"

# 2. 看 stage 耗时分布
curl -s http://127.0.0.1:8080/metrics | grep gateway_stage_duration_seconds_sum

# 3. 看规则引擎 stages 数
curl -s http://127.0.0.1:8082/v1/stats
```

**常见根因**:
- Kafka 慢:ack=all 时 broker 慢会传染
- 复杂规则:relabel 规则太多,每 sample 耗时高
- 状态型 stage 内存抖动:看 `gateway_goroutines` 是否有 GC 暂停

## 9. 紧急处置清单

| 现象 | 立即动作 |
|---|---|
| prom-gw 全实例 down | 检查 systemd / 网络;LB 上游 fallback |
| 错误率 > 5% | 摘流,回滚上一版;查 Nacos / 配置文件 |
| Kafka 不可用 | prom-gw 自动降级 WAL-only;无需人工 |
| WAL 满 | 查 Kafka 恢复;临时调大 `wal-max-bytes` |
| Admin API 503 | IP 白名单被改? 查 `gateway_admin_auth_fail_total` |
| OOM | 抓 heap profile,临时加机器 / 重启 |

## 10. 联系 / 升级

如果 runbook 没覆盖:
- 提交 issue:附上 `gateway_*` 指标 + heap profile(若 OOM)
- 紧急联系:见 `docs/operations/slo.md` 中的 on-call 列表
