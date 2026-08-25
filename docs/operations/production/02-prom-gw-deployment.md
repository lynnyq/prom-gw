# prom-gw 生产部署与配置详解

### 5.1 编译

```bash
# 依赖 Go 1.22+
make build           # 产物:bin/prom-gw
make test            # 单元测试 + 覆盖率(-race)
make lint            # golangci-lint
make release         # 产物:dist/prom-gw-<version>.tar.gz
```

版本注入:

```bash
VERSION=v1.2.3 make build
./bin/prom-gw --version  # prom-gw v1.2.3
```

### 5.2 Token 配置

路径:`/appdata/prom-gw/conf/tokens.yaml`

```yaml
tokens:
  "tk_app_business_prod":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.bj.raw.app_business
    rate_limit: 80000          # 该 tenant 的 samples/s 上限

  "tk_infra_prod":
    tenant: infra
    tenant_id: "1002"
    default_topic: prom.bj.raw.infra
    rate_limit: 50000
```

修改后通过 `kill -HUP <pid>` 热重载,**不重启进程**。

### 5.3 Ruleset 配置

路径:`/appdata/prom-gw/conf/config-<city>.yaml`(按城市分目录)

```yaml
rulesets:
  - name: app-business
    tenant: app-business
    input_topic: prom.bj.raw.app_business
    default_topic: prom.bj.routed.app_business
    version: 1
    match:
      metric_prefix: ""        # 空 = 全量接收
    stages:
      - type: relabel
        drop_labels: [env, instance, pod]
        keep_labels: []
        label_map:
          kubernetes_io_cluster: cluster

      - type: route
        rules:
          - match: { team: "core" }
            topic: prom.bj.routed.core
          - match: { team: "infra" }
            topic: prom.bj.routed.infra

      - type: sample
        rate: 0.1               # 保留 10%

global:
  rate_limit_per_instance: 100000
  channel_buffer: 65535
```

**stage 执行顺序**(固定):`relabel → enrich → route → sample → downsample → deadvalue`

**topic 命名规范**:`prom.<city>.<stage>.<tenant>`

### 5.4 systemd template 部署

prom-gw 使用 **template unit**(`prom-gw@.service`,`%i` 为城市标识):

**步骤 1:创建目录**

```bash
# bdops 用户(uid 6000)已由基础环境预先创建,所有组件统一使用 bdops 部署
sudo mkdir -p /appdata/prom-gw/bin /appdata/prom-gw/conf /appdata/prom-gw/wal /applog/prom-gw
sudo chown -R bdops:bdops /appdata/prom-gw /applog/prom-gw
```

**步骤 2:放置二进制和配置**

```bash
sudo cp bin/prom-gw /appdata/prom-gw/bin/
sudo cp configs/tokens/local.yaml /appdata/prom-gw/conf/tokens.yaml
sudo cp configs/rules/bj/default.yaml /appdata/prom-gw/conf/config-bj.yaml
sudo chmod 600 /appdata/prom-gw/conf/tokens.yaml
```

**步骤 3:配置 Kafka broker 地址**

```bash
# /appdata/prom-gw/conf/prom-gw.env
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
GOMAXPROCS=8
GOMEMLIMIT=6GiB
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317   # 可选
```

**步骤 4:安装 systemd unit**

`/etc/systemd/system/prom-gw@.service`(由仓库 `deploy/systemd/prom-gw@.service` 拷贝):

```ini
[Unit]
Description=Prometheus RemoteWrite Gateway (city=%i)
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=bdops
Group=bdops
Environment=INGEST_CITY=%i
Environment=PROM_GW_CONFIG=/appdata/prom-gw/conf/config-%i.yaml
Environment=PROM_GW_TOKENS=/appdata/prom-gw/conf/tokens.yaml
ExecStart=/appdata/prom-gw/bin/prom-gw \
  --config=/appdata/prom-gw/conf/config-%i.yaml \
  --tokens=/appdata/prom-gw/conf/tokens.yaml \
  --ingest-city=%i
Restart=always
RestartSec=5
LimitNOFILE=65535
MemoryMax=8G
KillSignal=SIGTERM
TimeoutStopSec=30
EnvironmentFile=-/appdata/prom-gw/conf/prom-gw.env

# 安全加固
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/appdata/prom-gw/wal /applog/prom-gw
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload

# 北京机房
sudo systemctl enable --now prom-gw@bj

# 查看状态
sudo systemctl status prom-gw@bj

# 看日志
sudo journalctl -u prom-gw@bj -f
```

### 5.5 启动参数速查

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `--config` | `PROM_GW_CONFIG` | `configs/rules/default.yaml` | ruleset 配置文件 |
| `--tokens` | `PROM_GW_TOKENS` | `configs/tokens/local.yaml` | token 配置文件 |
| `--write-addr` | - | `:19201` | RemoteWrite 接入地址 |
| `--metrics-addr` | - | `:8080` | Prometheus 指标 + pprof |
| `--health-addr` | - | `:8081` | healthz / readyz |
| `--admin-addr` | - | `:8082` | Admin API |
| `--admin-allow-cidr` | - | `127.0.0.1/32,10.0.0.0/8` | Admin API IP 白名单 |
| `--source-dc` | - | `dc-unknown` | 本实例所属机房标识 |
| `--ingest-city` | `INGEST_CITY` | `dc-unknown` | 城市标识(bj/sz/hf) |
| `--wal-dir` | - | `/appdata/prom-gw/wal` | WAL 数据目录 |
| `--wal-max-bytes` | - | `50GB` | WAL 总字节上限 |
| `--nacos-addr` | - | (空) | Nacos 地址,逗号分隔 |

Kafka broker 列表通过 `KAFKA_BROKERS` 环境变量注入,未设置时进入 **WAL-only 模式**。

---

