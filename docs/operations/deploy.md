# 部署与升级

## 1. 部署形态

- **非 K8s**,采用 VM/bare-metal + Ansible + systemd
- 典型部署:每机房 2~4 个 prom-gw 实例 + LB(nginx/haproxy) + 中心 Kafka
- WAL 目录建议独立挂载 SSD(/data/wal),与系统盘分离,避免 fsync 阻塞

## 2. 硬件基线

| 资源 | 建议 | 备注 |
|---|---|---|
| CPU | 8 核 | p99 < 500ms @ 1.5M samples/s |
| 内存 | 8G | 状态型 stage(Downsample/DeadValue)按 series 数扩张 |
| 磁盘(WAL) | 100G SSD,IOPS ≥ 5K | 独立挂载 /data/wal |
| 网络 | ≥ 1Gbps | 多机房时按专线带宽规划 |

## 3. 端口规划

| 端口 | 用途 | 监听端 |
|---|---|---|
| `19201` | RemoteWrite 接入(主流量) | 全部实例,LB 后端 |
| `8080` | `/metrics` + `/debug/pprof` | 全部实例,内网 |
| `8081` | `/healthz` + `/readyz` | 全部实例,LB health check |
| `8082` | Admin API | 全部实例,内网,IP 白名单 |

## 4. 配置文件

### 4.1 tokens 文件

路径:`/etc/prom-gw/tokens.yaml`(建议)

```yaml
tokens:
  "tk_app_business_dev":
    tenant: app-business
    tenant_id: "1001"
    default_topic: prom.raw.app_business
    rate_limit: 80000
```

修改后用 `kill -HUP <pid>` 热重载,**不重启进程**。

### 4.2 ruleset 文件

路径:`/etc/prom-gw/rules/app-business.yaml`

详见 `docs/user/ruleset-reference.md`。

修改后,fsnotify 自动检测并切换(5s 内),校验失败保留旧版本。

### 4.3 Nacos 接入(可选)

启动参数:
```bash
prom-gw \
  --nacos-addr=10.0.0.1:8848,10.0.0.2:8848 \
  --nacos-namespace=prod \
  --nacos-username=admin \
  --nacos-password=*** \
  --nacos-data-id=prom-gw-rules \
  --nacos-group=GATEWAY
```

Nacos 推送后 5s 内生效;Nacos 不可用时降级到本地文件(保留 last_good_snapshot)。

## 5. systemd 部署

prom-gw 使用 **template unit**(`prom-gw@.service`,`%i` 占位为城市标识),
每城一个独立 systemd 实例,启动时通过 `INGEST_CITY` 环境变量注入 ingest_city。

`/etc/systemd/system/prom-gw@.service`(由仓库 `deploy/systemd/prom-gw@.service` 拷贝):

```ini
[Unit]
Description=Prometheus RemoteWrite Gateway (city=%i)
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=prom-gw
Group=prom-gw
Environment=INGEST_CITY=%i
Environment=PROM_GW_CONFIG=/etc/prom-gw/config-%i.yaml
Environment=PROM_GW_TOKENS=/etc/prom-gw/tokens.yaml
ExecStart=/opt/prom-gw/bin/prom-gw \
  --config=/etc/prom-gw/config-%i.yaml \
  --tokens=/etc/prom-gw/tokens.yaml \
  --ingest-city=%i
Restart=always
RestartSec=5
StartLimitIntervalSec=60
StartLimitBurst=10
LimitNOFILE=65535
MemoryMax=8G
KillSignal=SIGTERM
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

启用与启动:
```bash
sudo systemctl daemon-reload

# 每城启用一个实例(例:bj)
sudo systemctl enable --now prom-gw@bj
sudo systemctl status prom-gw@bj

# 看日志
sudo journalctl -u prom-gw@bj -f
```

> **单实例部署(测试/小流量)**:直接用环境变量覆盖 `INGEST_CITY`,可绕过 template:
>
> ```bash
> INGEST_CITY=dc-test ./bin/prom-gw \
>   --config=configs/rules/default.yaml \
>   --tokens=configs/tokens/local.yaml
> ```

## 6. 滚动升级

### 6.1 灰度

1. **第一阶段**:只升级 1 个实例(10% 流量),观察 30 分钟
2. **第二阶段**:升级 50% 实例
3. **第三阶段**:全量升级

每个阶段检查:
- `/v1/stats` 的 drop_rate_estimate
- `gateway_errors_total` 增量
- `gateway_request_duration_seconds` p99

### 6.2 升级步骤

```bash
# 1. LB 摘流(nginx 配 health check fail 后自动摘,或手动)
sudo systemctl reload nginx  # 如果 nginx 上游用 health check

# 2. 停实例
sudo systemctl stop prom-gw

# 3. 替换二进制
sudo cp /tmp/prom-gw /opt/prom-gw/bin/prom-gw
sudo systemctl start prom-gw

# 4. 验证
curl http://127.0.0.1:8081/healthz
curl http://127.0.0.1:8080/metrics | grep gateway_samples_total

# 5. LB 上线(等 health check 通过)
```

## 7. 配置变更

### 7.1 Token 变更

```bash
# 1. 编辑 /etc/prom-gw/tokens.yaml
sudo vim /etc/prom-gw/tokens.yaml

# 2. 热重载(不重启)
sudo kill -HUP $(pidof prom-gw)

# 3. 验证
curl http://127.0.0.1:8080/metrics | grep gateway_samples_total
```

### 7.2 Ruleset 变更

修改 YAML 文件后,fsnotify 5s 内自动检测:
- 校验失败:旧版本保留,日志有 `WARN: apply snapshot failed`
- 校验成功:5s 内切换,日志有 `INFO: rule engine: applied via source`

## 8. 备份与恢复

- **WAL 目录**:`/data/wal` 不需要手动备份,启动时会自动 replay
- **Nacos snapshot**:`/data/nacos_snapshot.json`(启用 `--nacos-snapshot-path` 时)
- **Token / Ruleset**:跟随 Ansible,Git 仓库是源头

## 9. 监控接入

### 9.1 Prometheus 抓取

```yaml
scrape_configs:
  - job_name: prom-gw
    scrape_interval: 15s
    static_configs:
      - targets:
        - prom-gw-1:8080
        - prom-gw-2:8080
        - prom-gw-3:8080
```

### 9.2 告警规则

复制 `deploy/grafana/alerts/prom-gw.yaml` 到 Prometheus 的 rule_files 目录,reload 即可。

### 9.3 Grafana 大盘

导入 `deploy/grafana/dashboards/prom-gw.json`,选 Prometheus 数据源。

## 10. 跨机房部署

每机房独立部署 prom-gw 实例,通过专线写中心 Kafka:

```
DC-A Prometheus → DC-A prom-gw ──┐
                                  ├──> 中心 Kafka (跨机房专线)
DC-B Prometheus → DC-B prom-gw ──┘
DC-C Prometheus → DC-C prom-gw ──┘
```

关键点:
- 每个 GW 用 `--source-dc=dc-A` 标识机房
- Kafka 客户端配 `acks=all` + `enable.idempotence=true`
- 跨机房专线带宽建议 ≥ 1Gbps(2M samples/s × ~200B/sample ≈ 400MB/s)
