# 5 分钟接入 prom-gw

本文演示如何把现有 Prometheus 实例的 remote_write 流量指到 prom-gw。

## 1. 前置

- prom-gw 实例已部署并启动(参考 `docs/operations/deploy.md`)
- 已知 prom-gw 的 RemoteWrite 端点(如 `http://prom-gw-1:19201/api/v1/write`)
- 已申请 token(如有疑问联系运维)

## 2. 配置 Prometheus

修改 `prometheus.yml`:

```yaml
remote_write:
  - url: http://prom-gw-1:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_dev"   # 替换为实际 token
    write_relabel_configs:
      # 透传到 prom-gw(可选,本地也过滤一份)
      - source_labels: [__name__]
        regex: 'go_.*'
        action: keep
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

## 3. 验证

### 3.1 Prometheus 端

```bash
# 1. reload prometheus
curl -X POST http://prometheus:9090/-/reload

# 2. 看 remote_write 状态
curl http://prometheus:9090/api/v1/status/config | jq '.data.yaml' | grep -A 5 remote_write
```

### 3.2 prom-gw 端

```bash
# 1. 看 sample 计数(应该递增)
curl http://prom-gw-1:8080/metrics | grep gateway_samples_total

# 2. 看 Kafka 写入字节
curl http://prom-gw-1:8080/metrics | grep gateway_bytes_out

# 3. 看延迟
curl http://prom-gw-1:8080/metrics | grep gateway_request_duration_seconds

# 4. 消费 Kafka 验证(如果有 kafka 客户端)
kafka-console-consumer --bootstrap-server kafka:9092 --topic prom.raw.app_business --from-beginning --max-messages 5
```

## 4. 常见配置

### 4.1 高可用(多实例)

```yaml
remote_write:
  - url: http://prom-gw-1:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_dev"}
  - url: http://prom-gw-2:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_dev"}
```

### 4.2 跨机房

```yaml
remote_write:
  # 本机房优先
  - url: http://prom-gw-dc-a:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_dev"}
  # 备机:同机房
  - url: http://prom-gw-dc-a-2:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_dev"}
```

### 4.3 限流适配

Prometheus 默认 `max_samples_per_send=100`,如果 prom-gw 出现背压(503):

- 减小 `max_samples_per_send` 到 200
- 增大 `batch_send_deadline` 到 10s(允许更大 batch)
- 联系运维扩容 prom-gw

## 5. 监控

接入后,在 Grafana 导入 `deploy/grafana/dashboards/prom-gw.json`,看:
- 总吞吐 (samples/s)
- 端到端延迟 p50/p95/p99
- 错误率
- 背压拒绝率
- WAL 容量

## 6. 排错速查

| Prometheus 现象 | prom-gw 指标 | 排查 |
|---|---|---|
| 写入报错 5xx | `gateway_errors_total{type=~"decode\|parse"}` | 检查 content-encoding |
| 写入报错 401 | `gateway_auth_fail_total` | token 错 |
| 写入报错 503 | `gateway_backpressure_rejected_total` | 背压,扩容或限速 |
| 写入报错 4xx(其他) | `gateway_errors_total{type="..."}` | 看具体 type |

更详细: `docs/operations/troubleshooting.md`
