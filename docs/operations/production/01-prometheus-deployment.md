# Prometheus 生产部署与配置详解

### 4.1 Prometheus 安装(全新部署,已有环境可跳过)

> 若机房已运行 Prometheus,直接跳到 [4.2](#42-remote_write-配置对接-prom-gw) 配置 `remote_write`。
> 北京 2 套 + 深圳 2 套 + 合肥 1 套均为已有环境,本节供新机房扩容或灾备重建使用。

**下载并安装**:

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/prometheus/conf /appdata/prometheus/data
sudo wget https://github.com/prometheus/prometheus/releases/download/v2.51.0/prometheus-2.51.0.linux-amd64.tar.gz
sudo tar -xzf prometheus-2.51.0.linux-amd64.tar.gz -C /appdata/prometheus --strip-components=1
sudo chown -R bdops:bdops /appdata/prometheus
```

**基础配置** `/appdata/prometheus/conf/prometheus.yml`(最小可用,`remote_write` 在 §4.2 补充):

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source_dc: dc-bj-dongba          # 按机房修改:dc-bj-dongba / dc-sz-wulian / dc-hf

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ['localhost:9090']
  # 业务 exporter 按需追加 node_exporter / kube-state-metrics / pushgateway 等
```

**systemd 服务** `/etc/systemd/system/prometheus.service`:

```ini
[Unit]
Description=Prometheus
After=network.target

[Service]
Type=simple
User=bdops
ExecStart=/appdata/prometheus/prometheus \
  --config.file=/appdata/prometheus/conf/prometheus.yml \
  --storage.tsdb.path=/appdata/prometheus/data \
  --storage.tsdb.retention.time=15d \
  --web.enable-lifecycle
Restart=always
RestartSec=5
LimitNOFILE=65535
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/appdata/prometheus/data

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now prometheus
curl http://localhost:9090/-/healthy
# 期望: Prometheus is Healthy.
```

### 4.2 remote_write 配置对接 prom-gw

修改每套 Prometheus 的 `prometheus.yml`,添加 `remote_write` 段:

**北京东坝 Prometheus**:

```yaml
# /appdata/prometheus/conf/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    source_dc: dc-bj-dongba          # 标识机房,会被 prom-gw 读取

remote_write:
  - url: http://lvs-bj-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"    # 替换为实际 token
    write_relabel_configs:
      # 可选:本地过滤一份(如丢弃内部指标)
      - source_labels: [__name__]
        regex: 'go_.*|prometheus_.*'
        action: drop
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
      min_backoff: 500ms
      max_backoff: 10s
    metadata_config:
      send: true
      send_interval: 1m
```

**深圳五联 Prometheus**:

```yaml
global:
  external_labels:
    source_dc: dc-sz-wulian

remote_write:
  - url: http://lvs-sz-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

**合肥 Prometheus**:

```yaml
global:
  external_labels:
    source_dc: dc-hf

remote_write:
  - url: http://lvs-hf-vip:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_prod"
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

### 4.3 高可用配置(多实例)

```yaml
remote_write:
  # 主:LVS VIP(LB 到 prom-gw-1 ~ prom-gw-4)
  - url: http://lvs-bj-vip:19201/api/v1/write
    authorization: {type: Bearer, credentials: "tk_app_business_prod"}
    queue_config:
      capacity: 10000
      max_samples_per_send: 500
      batch_send_deadline: 5s
```

> prom-gw 的 Kafka producer 开启幂等写,多实例重复消息在 Kafka 端去重。

### 4.4 Prometheus 验证

```bash
# 1. reload prometheus
curl -X POST http://prometheus:9090/-/reload

# 2. 查看 remote_write 配置
curl -s http://prometheus:9090/api/v1/status/config | jq '.data.yaml' | grep remote_write -A 15

# 3. 查看 remote_write 状态(发送/失败/排队)
curl -s http://prometheus:9090/api/v1/status/runtimeinfo | jq '.data.remoteWrite'

# 4. 看指标
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_total | jq .
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_pending | jq .
curl -s http://prometheus:9090/api/v1/query?query=prometheus_remote_storage_samples_dropped_total | jq .
```

---

