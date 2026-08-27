# 端到端测试验证

### 7.1 测试环境准备

```bash
# 1. 确认 Kafka 可达
/appdata/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092 | head

# 2. 确认 Topic 已创建
/appdata/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list | grep prom

# 3. 编译 prom-gw
make build

# 4. 确认配置文件
cat configs/tokens/local.yaml
cat configs/rules/app-business.yaml
```

### 7.2 测试 1:WAL-only 模式冒烟测试(无 Kafka)

> 验证 prom-gw 基本功能:接收、解码、鉴权、WAL 落盘、Admin API。

```bash
# 启动 prom-gw(无 KAFKA_BROKERS → WAL-only 模式)
KAFKA_BROKERS="" \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-test-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --admin-allow-cidr=127.0.0.1/32 \
  --source-dc=dc-test &

GW_PID=$!
echo "prom-gw pid=$GW_PID"
```

**验证步骤**:

```bash
# 1. 健康检查
curl http://127.0.0.1:8081/healthz
# 期望: {"status":"ok"}

# 2. 就绪检查
curl -o /dev/null -w "%{http_code}" http://127.0.0.1:8081/readyz
# 期望: 204

# 3. 构造 RemoteWrite 请求
RUN_ID=test-1 go run ./scripts/e2e_payload > /tmp/payload.bin
ls -la /tmp/payload.bin

# 4. 正常写入
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/payload.bin)
echo "写入返回: $HTTP_CODE"  # 期望: 200

# 5. 鉴权失败(无 token)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  --data-binary @/tmp/payload.bin)
echo "无 token 返回: $HTTP_CODE"  # 期望: 401

# 6. 鉴权失败(非法 token)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_invalid" \
  --data-binary @/tmp/payload.bin)
echo "非法 token 返回: $HTTP_CODE"  # 期望: 401

# 7. 错误请求(非 snappy)
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary "not-snappy-bytes")
echo "非法 snappy 返回: $HTTP_CODE"  # 期望: 400

# 8. 指标校验
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total
curl -s http://127.0.0.1:8080/metrics | grep gateway_bytes_in_total

# 9. WAL 落盘校验
sleep 1
ls -la /tmp/prom-gw-test-wal/
find /tmp/prom-gw-test-wal/ -name 'seg-*.log*' | wc -l  # 期望 ≥ 1

# 10. Admin API
curl -s http://127.0.0.1:8082/v1/rulesets | jq .
curl -s http://127.0.0.1:8082/v1/stats | jq .
curl -s http://127.0.0.1:8082/v1/businesses | jq .

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-test-wal /tmp/payload.bin
```

### 7.3 测试 2:完整端到端测试(Kafka + prom-gw)

> 验证数据从 Prometheus → prom-gw → Kafka 全链路。

**启动 prom-gw(连接 Kafka)**:

```bash
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-e2e-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-e2e-test &

GW_PID=$!

# 等待启动
for i in $(seq 1 50); do
  curl -fsS http://127.0.0.1:8081/healthz >/dev/null 2>&1 && break
  sleep 0.2
done
echo "prom-gw started (pid=$GW_PID)"
```

**模拟 Prometheus 写入**:

```bash
# 构造并写入 10 条 sample
for i in $(seq 1 10); do
  RUN_ID="e2e-$i-$(date +%s)" go run ./scripts/e2e_payload > /tmp/payload-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/payload-$i.bin
done
```

**验证 Kafka 消费**:

```bash
# 消费 Topic 验证数据到达
/appdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.bj.routed.app_business \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 15000 \
  | xxd | head -50
# 期望:能看到二进制数据(prompb.WriteRequest snappy 编码)
```

**验证 prom-gw 指标**:

```bash
# 1. sample 计数(应递增)
curl -s http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 2. Kafka 写入字节(应 > 0)
curl -s http://127.0.0.1:8080/metrics | grep gateway_bytes_out_total

# 3. 错误计数(应为 0 或极少)
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total

# 4. 请求延迟
curl -s http://127.0.0.1:8080/metrics | grep gateway_request_duration_seconds

# 5. WAL 状态(应无积压)
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
```

**清理**:

```bash
kill $GW_PID
rm -rf /tmp/prom-gw-e2e-wal /tmp/payload-*.bin
```

### 7.4 测试 3:WAL 故障切换测试

> 验证 Kafka 故障时 prom-gw 自动降级到 WAL,Kafka 恢复后自动 drain。

```bash
# 1. 启动 prom-gw(连接一个不存在的 Kafka 地址模拟故障)
KAFKA_BROKERS=127.0.0.1:9999 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-failover-wal \
  --write-addr=:19201 \
  --metrics-addr=:8080 \
  --health-addr=:8081 \
  --admin-addr=:8082 \
  --source-dc=dc-failover-test &

GW_PID=$!

# 2. 等待启动(应进入 WAL degraded mode)
sleep 3
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
# 期望:WAL 指标 > 0

# 3. 写入数据(应全部落 WAL)
for i in $(seq 1 5); do
  RUN_ID="failover-$i" go run ./scripts/e2e_payload > /tmp/failover-$i.bin
  curl -sS -o /dev/null -w "sample $i: HTTP %{http_code}\n" \
    -X POST http://127.0.0.1:19201/api/v1/write \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_app_business_dev" \
    --data-binary @/tmp/failover-$i.bin
done

# 4. 验证 WAL 落盘
sleep 1
ls -la /tmp/prom-gw-failover-wal/
WAL_SEGMENTS=$(find /tmp/prom-gw-failover-wal/ -name 'seg-*.log*' | wc -l)
echo "WAL segments: $WAL_SEGMENTS"  # 期望 ≥ 1

# 5. 检查指标:Kafka 写入失败,WAL 接管
curl -s http://127.0.0.1:8080/metrics | grep gateway_wal_bytes
curl -s http://127.0.0.1:8080/metrics | grep gateway_errors_total{stage=\"kafka\"}

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-failover-wal /tmp/failover-*.bin
```

### 7.5 测试 4:规则引擎验证

> 验证 relabel/route/sample 规则正确执行。

```bash
# 使用 app-business ruleset(包含 relabel + route + sample)
KAFKA_BROKERS=kafka-1:9092 \
./bin/prom-gw \
  --config=configs/rules/app-business.yaml \
  --tokens=configs/tokens/local.yaml \
  --wal-dir=/tmp/prom-gw-rule-wal \
  --write-addr=:19201 \
  --admin-addr=:8082 &

GW_PID=$!
sleep 2

# 构造带 team 标签的 sample
cat > /tmp/rule-test.go << 'EOF'
package main
import (
  "io"
  "os"
  "time"
  "github.com/klauspost/compress/snappy"
  "github.com/prometheus/prometheus/prompb"
)
func main() {
  req := &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{
    {Labels: []prompb.Label{
      {Name: "__name__", Value: "app_cpu_usage"},
      {Name: "team", Value: "core"},
      {Name: "env", Value: "prod"},
      {Name: "instance", Value: "10.0.0.1:9090"},
    }, Samples: []prompb.Sample{{Value: 88.5, Timestamp: time.Now().UnixMilli()}}},
  }}
  raw, _ := req.Marshal()
  encoded := snappy.Encode(nil, raw)
  io.Copy(os.Stdout, &byteReader{b: encoded})
}
type byteReader struct{ b []byte; i int }
func (r *byteReader) Read(p []byte) (int, error) {
  if r.i >= len(r.b) { return 0, io.EOF }
  n := copy(p, r.b[r.i:]); r.i += n; return n, nil
}
EOF
go run /tmp/rule-test.go > /tmp/rule-payload.bin

# 写入
curl -sS -o /dev/null -w "HTTP %{http_code}\n" \
  -X POST http://127.0.0.1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @/tmp/rule-payload.bin

# 验证路由到 prom.bj.routed.core(team=core)
/appdata/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic prom.bj.routed.core \
  --from-beginning --max-messages 1 --timeout-ms 10000 | xxd | head

# 验证 ruleset 指标
curl -s http://127.0.0.1:8080/metrics | grep gateway_ruleset_routed_total

# 清理
kill $GW_PID
rm -rf /tmp/prom-gw-rule-wal /tmp/rule-test.go /tmp/rule-payload.bin
```

### 7.6 测试 5:单元测试 + 集成测试

```bash
# 单元测试(快速,不需 Docker)
make test
# 期望:coverage > 60%,全部 PASS

# 集成测试(需要 Docker,启动 Kafka testcontainer)
make test-integration
# 期望:全部 PASS

# 压测(30s 冒烟)
make test-loadgen
# 期望:50000 samples/s 持续 30s 无错误

# 端到端手动脚本
bash test/manual/e2e.sh
# 期望:✅ 全部检查通过
```

### 7.7 测试 6:全链路验证清单

部署完成后,按以下清单逐项验证:

| 序号 | 验证项 | 命令 | 期望结果 |
|---|---|---|---|
| 1 | Kafka Broker 状态 | `kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092` | 3 个 Broker 在线 |
| 2 | Topic 列表 | `kafka-topics.sh --list --bootstrap-server kafka-1:9092 \| grep prom` | 包含 routed topic |
| 3 | prom-gw healthz | `curl http://prom-gw:8081/healthz` | `{"status":"ok"}` |
| 4 | prom-gw readyz | `curl -o /dev/null -w "%{http_code}" http://prom-gw:8081/readyz` | `204` |
| 5 | prom-gw metrics | `curl http://prom-gw:8080/metrics \| grep gateway_samples_total` | 指标可见 |
| 6 | Prometheus remote_write | `curl http://prom:9090/api/v1/query?query=prometheus_remote_storage_samples_total` | 计数递增 |
| 7 | 写入 200 | `curl -w "%{http_code}" -X POST .../api/v1/write` | `200` |
| 8 | 鉴权 401 | 无 token 写入 | `401` |
| 9 | Kafka 消费 | `kafka-console-consumer.sh --topic prom.bj.routed.app_business` | 收到数据 |
| 10 | Admin API | `curl http://prom-gw:8082/v1/rulesets` | 返回 ruleset 列表 |
| 11 | LVS VIP | `curl http://lvs-vip:19201/api/v1/write` | 可达 |
| 12 | Grafana 大盘 | 打开 Grafana → prom-gw dashboard | 数据有曲线 |
| 13 | 告警规则 | Prometheus → Alerts 页面 | 告警规则已加载 |
| 14 | WAL 状态 | `curl http://prom-gw:8080/metrics \| grep gateway_wal_bytes` | 正常为 0 |
| 15 | 跨机房 | 合肥 Prometheus → 合肥 prom-gw → 合肥 Kafka | 数据流通 |

---

