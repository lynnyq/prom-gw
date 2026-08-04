# 混沌测试 Runbook

> 本文件是 system-level 混沌测试的执行手册(脱离 in-process 测试,需要真实环境)。
> 配合 `test/chaos/chaos_test.go` 一起使用:in-process 测基本能力,系统级测极端组合。

## 0. 准备

- 1 套可压测环境(允许短暂故障)
- 1 个 prom-gw 实例 + 1 个 Kafka 集群
- 已配置好 token、ruleset(参考 `configs/`)
- `loadgen` 工具(本仓库 `test/loadgen/main.go`)

## 1. 杀 GW 实例

**目的**:验证 LB 后多实例的接管能力 + 数据不丢。

```bash
# 1. 启压测
go run ./test/loadgen --rate=100000 --duration=600s &

# 2. 等 30s 让流量稳定
sleep 30

# 3. 杀 1 个实例(模拟半数)
sudo kill -9 $(pidof prom-gw)

# 4. 观察 LB 是否摘流
curl -s http://lb/v1/healthz   # 应仍 200

# 5. 流量应被 LB 重分发到其他实例
# 6. 启动新实例补回
systemctl start prom-gw
```

**期望**:
- LB 摘流时间 < 5s
- 杀实例期间错误率 < 0.1%
- Kafka 消费数据连续无空洞(看 Kafka offset)

**告警验证**:
- 杀实例 30s 内触发 `PromGwHighErrorRate`(如果 error 率 > 1%)
- 不会触发 `PromGwDown`(只要还有 1 个实例在)

## 2. Kafka 不可用

**目的**:验证 WAL 自动接管 + 恢复后排空。

```bash
# 1. 启压测
go run ./test/loadgen --rate=50000 --duration=600s &

# 2. 等 30s 流量稳定
sleep 30

# 3. 停 Kafka(模拟 broker 全挂)
sudo systemctl stop kafka

# 4. 观察 GW 行为
# 期望: GW 自动降级到 WAL-only 模式,日志 "kafka connect failed"
sudo journalctl -u prom-gw -f | grep -E "kafka|wal"

# 5. 验证 GW 仍接受写(写到 WAL)
curl -s http://prom-gw:8080/metrics | grep gateway_wal_bytes
# 期望: gateway_wal_bytes 持续增长

# 6. 等 5 分钟,确保 WAL 持续接收
sleep 300

# 7. 启 Kafka
sudo systemctl start kafka

# 8. 观察 drain
sudo journalctl -u prom-gw -f | grep -E "replay|drain"
# 期望: WAL 段被消费,排空

# 9. 验证 Kafka 数据连续
# 用 kafka consumer 看 offset 增长是否匹配
```

**期望**:
- Kafka 停机期间 0 错误(写到 WAL 不返回 5xx)
- WAL 字节增长 ≈ 5min × 50K samples/s × 平均 sample 大小
- Kafka 恢复后 WAL 段被 drain,字节回到 0
- Kafka 数据无空洞(顺序连续)

## 3. 磁盘满

**目的**:验证 WAL 满后硬拒绝 503,而不是无限增长。

```bash
# 1. 把 WAL 目录缩到 1GB
sudo umount /data/wal
sudo mount -o size=1G tmpfs /data/wal
# 注: tmpfs 重启丢失,仅用于测试,完成后恢复

# 2. 启压测
go run ./test/loadgen --rate=200000 --duration=120s &

# 3. 观察 WAL 满
sleep 60
curl -s http://prom-gw:8080/metrics | grep gateway_wal_hard_reject_total
# 期望: 计数 > 0

# 4. 观察 503 比例
# 压测客户端会显示错误率
```

**期望**:
- WAL 字节到 1GB 之前全部 200/204
- WAL 满后开始 503(`gateway_wal_hard_reject_total` 增长)
- 不会触发 OOM / 进程崩溃

**收尾**:
```bash
sudo umount /data/wal
sudo mount /data/wal  # 恢复原 mount point
```

## 4. CPU 打满

**目的**:验证资源压力下数据不丢,只是延迟上升。

```bash
# 1. 启压测(基线)
go run ./test/loadgen --rate=100000 --duration=120s &
sleep 30

# 2. 抓基线 p99
curl -s http://prom-gw:8080/metrics | grep gateway_request_duration_seconds

# 3. 用 stress 打满 4 核(假设总 8 核,剩 4 核给 GW)
stress -c 4 -t 120s &

# 4. 等 60s,看 p99 是否上升
sleep 60
curl -s http://prom-gw:8080/metrics | grep gateway_request_duration_seconds
```

**期望**:
- p99 上升 2-5 倍(从 100ms → 300-500ms)
- 错误率 < 0.1%(无 5xx)
- 不丢数据(Kafka 数据条数匹配)

## 5. 网络分区(同机房)

**目的**:验证 GW 实例间隔离 + 客户端重试兜底。

```bash
# 1. 多实例部署,假设 4 个 GW 实例 + 1 Kafka
# 2. 启压测
go run ./test/loadgen --rate=100000 --duration=300s &

# 3. 用 iptables 隔离实例 3 与 Kafka
sudo iptables -A OUTPUT -d kafka -p tcp --dport 9092 -j DROP
# 实例 3 失去 Kafka 通路,但仍能接收 Prometheus 写入

# 4. 观察 3 个正常实例(实例 1/2/4) + 1 个隔离实例(3)
# 实例 3 应自动降级到 WAL-only
# 客户端持续写入 → 4 个实例都收到 → 3 个写 Kafka,1 个写 WAL
```

**期望**:
- 实例 3 日志 "kafka connect failed",进入 WAL 模式
- 其他实例正常写 Kafka
- 客户端无感(写哪个实例由 LB 决定)

**恢复**:
```bash
sudo iptables -D OUTPUT -d kafka -p tcp --dport 9092 -j DROP
# 实例 3 检测到 Kafka 恢复,自动 drain WAL 到 Kafka
```

## 6. 慢 Kafka(toxiproxy 注入)

**目的**:验证 Kafka 慢时背压 + WAL 兜底。

```bash
# 1. 部署 toxiproxy(参考: https://github.com/Shopify/toxiproxy)
# 2. 配置 toxiproxy 把 Kafka 9092 转到真实 Kafka
toxiproxy-cli -h toxiproxy-host:8474 create -l 0.0.0.0:29092 -u kafka:9092 kafka-proxy
# 3. GW 配置 --kafka-brokers=toxiproxy-host:29092

# 4. 启压测
go run ./test/loadgen --rate=100000 --duration=300s &

# 5. 注入延迟
toxiproxy-cli -h toxiproxy-host:8474 toxic add -t latency -a latency=1000 kafka-proxy
# Kafka 写入延迟 1s

# 6. 观察
# GW 内部 channel 满 → 503 拒绝
# 持续慢 → 切 WAL
```

**期望**:
- 慢 Kafka 触发 503 比例上升,但不超阈值(< 1%)
- WAL 段缓慢增长(因 Kafka 慢)
- 客户端 retry 兜底
- 移除延迟后,GW 自动恢复 + drain WAL

**恢复**:
```bash
toxiproxy-cli -h toxiproxy-host:8474 toxic remove -n latency kafka-proxy
```

## 7. Nacos 不可用

**目的**:验证 Nacos 故障时降级到本地文件(plan T4.1 行为)。

```bash
# 1. GW 启动时配 Nacos(--nacos-addr=10.0.0.1:8848)
# 2. 拉取 ruleset 成功
# 3. 停 Nacos
sudo systemctl stop nacos

# 4. 观察 GW 行为
sudo journalctl -u prom-gw -f | grep -E "nacos|snapshot"
# 期望: 持续 Nacos 拉取失败,但 ruleset 仍生效(用上次成功快照)

# 5. 启 Nacos
sudo systemctl start nacos
# 期望: GW 在 30s 内重新拉到 Nacos(长轮询)
```

**期望**:
- Nacos 停机期间,现有 ruleset 持续生效
- `gateway_config_reload_total{status="error"}` 增长
- 不影响 RemoteWrite 流量

## 8. 内存打爆(模拟 OOM 临界)

**目的**:验证状态型 stage 在内存压力下行为。

```bash
# 1. GW 配大 downsample (max_series=10M)
# 2. 启压测,持续 10 分钟
go run ./test/loadgen --rate=500000 --duration=600s

# 3. 观察内存
ps -o rss= -p $(pidof prom-gw)
# 期望: < 8G(按 plan 约束)

# 4. 观察 downsample 状态大小
curl -s http://prom-gw:8080/metrics | grep gateway_state_series
# 期望: < max_series,LRU 驱逐正常
```

**期望**:
- 内存稳定 < 8G
- LRU 驱逐工作,metrics 反映
- 不发生 OOM kill

## 9. 组合场景(回归)

下面这些场景是上线前必跑的:

| 场景 | 同时 | 验证 |
|---|---|---|
| 杀 50% 实例 + Kafka 慢 | ✓ ✓ | LB + WAL 双层降级 |
| 磁盘满 + Nacos 不可用 | ✓ ✓ | 503 优先于 ruleset 降级 |
| CPU 80% + Kafka 慢 | ✓ ✓ | p99 升高但数据不丢 |

每个组合跑 1 小时,记录:
- 错误率(必须 < 0.01%)
- 数据丢失条数(必须 = 0)
- 恢复时间(实例恢复 → 流量恢复 < 30s)

## 10. 收尾

每次混沌测试完成后:

```bash
# 1. 收集指标快照
curl -s http://prom-gw:8080/metrics > /tmp/chaos-metrics-$(date +%s).txt

# 2. 导出日志
sudo journalctl -u prom-gw --since "1h ago" > /tmp/chaos-journal-$(date +%s).log

# 3. 收集 heap profile(若怀疑内存问题)
go tool pprof -text -cum http://prom-gw:9090/debug/pprof/heap > /tmp/heap-$(date +%s).txt

# 4. 检查 Kafka 消费完整性
# 比较 push_count vs consume_count,差值 = 丢失数
```

报告产出:`/docs/operations/chaos-report-<date>.md` 包含:
- 测试场景
- 实际观察指标
- 失败 case 详细根因
- 改进项 follow-up
