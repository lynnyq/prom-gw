# 混沌测试执行

## 1. 自动化测试(in-process)

最常用,跑得快,CI 友好:

```bash
go test -tags=chaos ./test/chaos/... -count=1 -v
```

覆盖:
- `TestChaos_DiskFull_WalHardReject`:WAL 满 → 503
- `TestChaos_KafkaDown_FallbackToWAL`:无 Kafka 客户端时数据落 WAL
- `TestChaos_AuthInvalid_Returns401`:非法 token 401
- `TestChaos_BadPayload_Returns400`:坏字节 400
- `TestChaos_Concurrent_NoLeak`:100 并发无泄漏
- `TestChaos_PipelineSwitch_NoPanic`:100 次规则切换不 panic

## 2. 系统级场景(脚本驱动)

需要部署环境,见 `chaos_runbook.md`:

| 场景 | 方法 | 期望 |
|---|---|---|
| 杀 GW 实例 | `kill -9 $(pidof prom-gw)` | LB 摘流,其他实例接管 |
| Kafka 不可用 | 断网 / stop kafka | GW 落 WAL,恢复后 drain |
| 磁盘满 | 写满 /data/wal | 503,告警触发 |
| CPU 打满 | `stress -c 8` | p99 上升,但不丢数据 |
| 网络分区 | iptables drop | 分区内实例降级,恢复后合并 |
| 慢 Kafka | toxiproxy 加延迟 | 背压拒绝,但 WAL 兜底 |
