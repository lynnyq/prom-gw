# 故障响应与排查手册 (Runbook)
> 配套文档:**SLO 指标**(见 §12)、**生产部署指南**(见 §1)。
> 本文档覆盖"故障来了怎么办"——按严重程度分级响应(确认 → 隔离 → 恢复 → 复盘),并提供常见问题的具体排查步骤。

## 0. 通用原则

1. **先止血,后查因**。先恢复业务,再排查根因,不要在故障期做无关变更。
2. **保留现场**。抓 heap profile、metric 快照、相关日志段,留作复盘材料。
3. **所有变更记录到 incident 文档**(时间、操作人、命令、观察)。
4. **变更前先摘流**。无 load balancer 直连时,提前协调上游 Prometheus 暂停 remote_write。
5. **rollback 优先**。如果变更后故障,先 `make release` 拉上一版回滚,再分析。
6. **告警升级按 `slo.md` §5 分级**。

## 1. 严重故障 (SEV-1):服务整体不可用

**判定标准**:
- 全部实例 healthz/readyz 503
- 数据完全中断(写入返回 5xx > 5min)
- 错误率 > 5% 持续 5 分钟

**on-call 5 分钟内确认,15 分钟内开始处置**。

### 1.1 确认现场

```bash
# 1. 进程状态
systemctl status prom-gw
ps -ef | grep prom-gw | grep -v grep

# 2. 端口监听
ss -tlnp | grep -E ':(19201|8080|8081|8082)\s'

# 3. 健康检查
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -fsS -m 3 http://$h:8081/healthz || echo "FAIL: $h"
  curl -fsS -m 3 http://$h:8081/readyz || echo "NOT READY: $h"
done

# 4. 最近日志(找 panic / fatal)
sudo journalctl -u prom-gw --since "10m ago" --no-pager | tail -200

# 5. 资源
ps -o pid,rss,vsz,pcpu,pmem,comm -p $(pidof prom-gw)
dmesg | tail -20 | grep -i "oom\|killed"
```

### 1.2 隔离 (止血)

按优先级选择,每步 5 分钟内观察:

| 现象 | 立即动作 |
|---|---|
| 进程不存在 | `sudo systemctl restart prom-gw` |
| 进程存在但无响应 | `sudo systemctl restart prom-gw`(等 systemd 超时后强杀) |
| 启动反复 fail | 先看日志定位 → 临时把启动命令里的 `--kafka-brokers` 改成空,让 GW 走 WAL-only |
| OOM | 临时加机器或减少并发(`-concurrency=1`),重启 |
| 全部实例 down | LB 上游 fallback 到备用集群 |

### 1.3 恢复 (查因 + 修复)

1. **看启动日志**:`journalctl -u prom-gw -n 1000`,找 panic / fatal / error
2. **看 panic 类型**:
   - `kafka.New() probe failed` → Kafka 集群故障,GW 应已自动降级 WAL,查 Kafka
   - `config: failed to load ruleset` → 配置文件语法错,回滚
   - `port already in use` → 端口冲突,查同机其他进程
   - `out of memory` → heap profile 定位
3. **看依赖**:
   - Kafka: `kafka-broker-api-versions --bootstrap-server $BROKER`
   - Nacos: `curl http://$NACOS:8848/nacos/v1/cs/configs?dataId=prom-gw-rules`
   - 本地磁盘: `df -h /data/wal`
4. **修复后**:`systemctl restart prom-gw`,观察 5 分钟

### 1.4 复盘

填入 incident doc:故障时间线、影响面、root cause、修复动作、follow-up 项。
挂到团队复盘会议,2 个工作日内完成。

## 2. 严重故障 (SEV-2):部分功能不可用

**判定标准**:
- 错误率 1-5% 持续 5 分钟
- p99 延迟 > 1s
- WAL 硬拒绝 > 0
- 某 ruleset 不工作(其他 ruleset 正常)

**on-call 15 分钟内确认,1 小时内开始处置**。

### 2.1 高错误率

```bash
# 1. 错误分类
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total

# 2. 按 stage 拆解
for stage in decode auth parse kafka wal; do
  echo "=== $stage ==="
  curl -s http://127.0.0.1:8080/metrics | grep "gateway_errors_total{stage=\"$stage\""
done
```

**常见根因**:
- `decode`:客户端发送非 snappy/protobuf 字节 → 检查 Prometheus remote_write 配置
- `auth`:token 失效或被吊销 → 更新 `tokens.yaml` 并 HUP
- `kafka`:Kafka 不可达 → 检查 `gateway_kafka_*` 指标和 Kafka 集群
- `wal_full`:WAL 满 → 清理或扩容

### 2.2 p99 延迟高

```bash
# 1. CPU profile(30s 抓样)
go tool pprof -top -cum http://127.0.0.1:8080/debug/pprof/profile?seconds=30

# 2. stage 耗时
curl -s http://127.0.0.1:8080/metrics | grep gateway_stage_duration

# 3. 看是否有 GC 暂停
curl -s http://127.0.0.1:8080/metrics | grep go_gc_duration
```

**常见根因**:
- Kafka 慢:ack=all 时 broker 慢会传染
- 复杂规则:relabel 规则太多,每 sample 耗时高
- 状态型 stage 内存抖动:看 `gateway_goroutines` 是否有 GC 暂停

如果是某条 ruleset 引起(其他正常),临时 `POST /v1/rulesets/{name}:reload` 强制重载;
不行就 `POST /v1/rulesets/{name}:rollback?to_version=N` 回滚。

### 2.3 WAL 硬拒绝

```bash
# 1. WAL 状态
curl -s http://127.0.0.1:8080/metrics | grep -E "gateway_wal_(bytes|oldest|hard_reject)"

# 2. 磁盘
df -h /data/wal

# 3. WAL 目录
ls -lah /data/wal | head
```

**常见根因**:
- Kafka 不可达:检查 `kafka brokers` 配置和网络
- Kafka 慢:Kafka 集群压力过大,看 broker 指标
- Nacos 推送错误配置:回滚到上一版本

**短期止血**(降级背压):
- 調大 `wal.max_bytes`(临时改配置,SIGHUP 生效)
- 加挂一块盘,迁移 WAL 目录(需停机)
- 扩容 Kafka,加速排空

**禁止**:`rm -rf /data/wal` — 会丢所有未确认消息。

## 3. 警告 (SEV-3):告警但不阻塞

**判定标准**:
- 4xx/5xx 持续 > 10/s
- 鉴权失败 > 50/s
- p99 延迟 0.5-1s
- Goroutines > 5000
- Config reload 失败

**on-call 30 分钟内确认,4 小时内处置**。

### 3.1 鉴权失败激增

```bash
# 1. 看 reason
curl -s http://127.0.0.1:8080/metrics | grep gateway_auth_fail_total

# 2. 看具体 token(日志中脱敏,这里只能看 IP)
sudo journalctl -u prom-gw --since "10m ago" | grep "auth fail" | head
```

**常见根因**:
- 客户端 token 拼写错
- 客户端还在用旧 token(已被轮换)
- Prometheus remote_write URL 配错(漏了 `Bearer`)

确认是配置问题(token 拼错)还是被攻击(单一 IP 高频),按需 HUP tokens.yaml。

### 3.2 Config reload 失败

```bash
# 1. Nacos 拉取
sudo journalctl -u prom-gw --since "5m ago" | grep -i "nacos\|snapshot"

# 2. 本地文件监听
sudo journalctl -u prom-gw --since "5m ago" | grep -i "fsnotify\|apply snapshot"

# 3. 手动 reload
curl -X POST http://127.0.0.1:8082/v1/rulesets/app:reload
```

如果 Nacos 推了非法 YAML,GW 会保留旧版 + 告警,不影响业务。处理:
1. 修 YAML
2. 手动 reload 验证
3. 复盘 Nacos 发布流程(谁推的,有没有 review)

## 4. 计划性变更 (变更窗口)

| 变更类型 | 提前通知 | 风险评估 | 变更窗口 |
|---|---|---|---|
| 升级 binary | 24h | 中 | 工作日 02:00-04:00 |
| 修改默认配置 | 48h | 中 | 工作日 02:00-04:00 |
| Nacos 推 ruleset | 即时 | 低 | 任何时间(可秒级回滚) |
| Kafka 容量调整 | 72h | 高 | 业务低峰期,需 DBA + SRE 同时在场 |
| 端口/路由变更 | 1 周 | 高 | 业务低峰期 + 上游协同 |

变更后必须:
- 观察 30 分钟,确认指标未劣化
- 在变更日志里记录变更人 / 变更内容 / 观察结论
- 保留旧 binary 包至少 7 天(回滚用)

## 5. 容量告警处置

参考 `slo.md §6` 容量规划表。

| 信号 | 含义 | 处置 |
|---|---|---|
| CPU 持续 > 70% | 单机到上限 | 加实例 / 增加 partition |
| Goroutines 持续 > 5000 | 资源泄漏嫌疑 | 抓 goroutine profile 排查 |
| p99 抖动但 p50 稳 | 下游慢传染 | 查 Kafka broker 端 |
| 错误率突增但 p99 稳 | 鉴权/解析错误 | 查 `gateway_auth_fail_total` / 客户端配置 |
| WAL 段数持续增长 | 下游消费慢 | 扩容 Kafka consumer |

## 6. 沟通模板

### 6.1 故障通知(发给业务方)

```
【prom-gw 告警】SEV-X
- 现象:<错误率 / 延迟 / 不可用>
- 开始时间:<HH:MM>
- 影响面:<哪几个租户 / 哪条规则>
- 当前状态:<正在处置 / 已恢复>
- 预计恢复:<HH:MM 或 评估中>
- 负责人:<on-call 名字>
- 进展:<每 15 分钟更新一次>
```

### 6.2 恢复通知

```
【prom-gw 恢复】
- 故障时间:<HH:MM - HH:MM,共 X 分钟>
- root cause:<一句话>
- 修复动作:<已变更内容 / 配置 / 代码>
- 数据丢失:<无 / WAL 落盘 N 条已重放>
- 复盘文档:<链接>
```

## 7. 升级路径

| 严重度 | 第一响应 | 升级条件 | 第二响应 |
|---|---|---|---|
| SEV-1 | on-call 工程师 | 15 分钟无进展 | 团队负责人 + SRE lead |
| SEV-2 | on-call 工程师 | 1 小时无进展 | 团队负责人 |
| SEV-3 | on-call 工程师 | 4 小时无进展 | 下个工作日复盘 |
| 数据完整性事件 | on-call 工程师 | 即时 | 数据 owner + SRE lead + 团队负责人 |

## 8. 工具速查

```bash
# 全实例 healthz 巡检
HOSTS="10.0.1.1,10.0.1.2,10.0.1.3"
for h in $(echo $HOSTS | tr ',' ' '); do
  printf '%s\t' "$h"
  curl -fsS -m 3 http://$h:8081/readyz && echo OK || echo FAIL
done

# 全实例 admin 状态
for h in $(echo $HOSTS | tr ',' ' '); do
  echo "=== $h ==="
  curl -sS http://$h:8082/v1/stats | jq .
done

# 全实例 ruleset 一致性
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -sS http://$h:8082/v1/rulesets | jq -c '.data[] | {name, version}'
done | sort | uniq -c | sort -rn

# 全实例 5xx 计数
for h in $(echo $HOSTS | tr ',' ' '); do
  echo "=== $h ==="
  curl -s http://$h:8080/metrics | grep "gateway_errors_total{stage=\"kafka\""
done

# heap profile 远程拉
go tool pprof -text -cum http://127.0.0.1:8080/debug/pprof/heap > heap-$(date +%s).txt

# goroutine 数量
for h in $(echo $HOSTS | tr ',' ' '); do
  curl -s http://$h:8080/metrics | grep "^gateway_goroutines"
done
```

## 9. 复盘模板

每次 SEV-1/2 故障 24 小时内开复盘会,产出文档包含:

1. **时间线**(UTC+8):
   - HH:MM 告警触发
   - HH:MM on-call 确认
   - HH:MM 止血动作
   - HH:MM 业务恢复
   - HH:MM 修复完成
2. **影响面**:租户 / ruleset / 数据丢失条数 / 持续时间
3. **root cause**(一层、二层、最深层)
4. **为什么告警没提前**(MTTD 分析)
5. **为什么处置慢了**(MTTR 分析)
6. **Action items**(每条都有 owner + deadline)
7. **流程改进 / 自动化机会**

---

## 10. 常见问题排查

### 10.1 503 背压拒绝持续

**症状**:`gateway_backpressure_rejected_total` 持续 > 0。

```bash
# 看 channel 深度
curl -s http://127.0.0.1:8082/v1/stats  # admin API

# 看 Kafka 是否慢(若有 kafka exporter,看 consumer lag)
```

**处置**:
- 短期:扩容 prom-gw 实例数(横向扩展)
- 中期:增加 Kafka partition 数 / 扩容 Kafka 集群
- 长期:优化下游消费,降低反压

### 10.2 WAL 卡住不排空

**症状**:`gateway_wal_oldest_age_seconds` 持续 > 60s。

```bash
# 看 WAL 段数 / 大小
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal

# 看 WAL 目录
ls -lah /data/wal
```

**手动 drain**(应急):

```bash
# 1. 停 prom-gw
sudo systemctl stop prom-gw

# 2. 启动时强制走 WAL→Kafka 模式(默认行为,Kafka 恢复后自动 replay)

# 3. 启动
sudo systemctl start prom-gw

# 4. 观察日志
sudo journalctl -u prom-gw -f | grep -i "replay\|wal"
```

### 10.3 规则版本不切换

**症状**:修改 ruleset 文件后,`gateway_ruleset_version` 没变。

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

### 10.4 实例 OOM

**症状**:prom-gw 进程被 OOM kill。

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

### 10.5 性能不达标 (QPS 不到 1.5M)

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

### 10.6 紧急处置速查

| 现象 | 立即动作 |
|---|---|
| prom-gw 全实例 down | 检查 systemd / 网络;LB 上游 fallback |
| 错误率 > 5% | 摘流,回滚上一版;查 Nacos / 配置文件 |
| Kafka 不可用 | prom-gw 自动降级 WAL-only;无需人工 |
| WAL 满 | 查 Kafka 恢复;临时调大 `wal-max-bytes` |
| Admin API 503 | IP 白名单被改? 查 `gateway_admin_auth_fail_total` |
| OOM | 抓 heap profile,临时加机器 / 重启 |



---

