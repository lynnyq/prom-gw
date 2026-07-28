# prom-gw

多机房 Prometheus RemoteWrite 协议网关。

`prom-gw` 作为多机房 Prometheus 与中心 Kafka 之间的统一接入层,提供:

- 单机 ≥ 1.5M samples/s 持续
- 多租户路由、按业务分 topic
- 标签/指标/路由/采样/下采样/死值等多维清洗
- 配置热更新(本地 + Nacos)
- 端到端可观测(指标 + Trace + 日志)

## 快速启动

```bash
# 1. 编译
make build

# 2. 跑默认空 ruleset(只暴露 healthz/metrics)
./bin/prom-gw

# 3. 验证
curl localhost:8081/healthz   # 200
curl localhost:8080/metrics | head -5
```

## 端口

| 端口 | 用途 |
|---|---|
| `19201` | RemoteWrite 接入(`/api/v1/write`) |
| `8080`  | Prometheus self-export `/metrics` |
| `8081`  | `/healthz` + `/readyz` |
| `8082`  | Admin API(本机/内网,白名单) |
| `9090`  | pprof(debug build only) |

## 配置

启动参数:

| flag | 默认 | 说明 |
|---|---|---|
| `--config` | `configs/rules/default.yaml` | ruleset 配置文件路径 |
| `--tokens` | `configs/tokens/local.yaml` | token 配置文件路径 |
| `--metrics-addr` | `:8080` | metrics 监听地址 |
| `--health-addr` | `:8081` | healthz 监听地址 |
| `--version` | - | 打印版本后退出 |

## 文档

- 设计:[docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md](docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md)
- 实施计划:[docs/superpowers/plans/2026-07-28-prometheus-multidc-remotewrite-gateway-plan.md](docs/superpowers/plans/2026-07-28-prometheus-multidc-remotewrite-gateway-plan.md)
- 开发指南:[DEVELOPING.md](DEVELOPING.md)

## License

内部项目。
