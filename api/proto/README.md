# prom-gw API / Protocol Definitions

本目录保留 Prometheus 官方 RemoteWrite v1 / v2 协议定义,作为协议参考。

## 文件说明

| 文件 | 用途 |
|---|---|
| [remote.proto](remote.proto) | RemoteWrite v1 协议定义(`WriteRequest`、`ReadRequest` 等) |
| [types.proto](types.proto) | RemoteWrite v1 类型定义(`TimeSeries`、`Sample`、`Label` 等) |
| [io/prometheus/client/metrics.proto](io/prometheus/client/metrics.proto) | Prometheus metrics protobuf 定义 |
| [io/prometheus/write/v2/types.proto](io/prometheus/write/v2/types.proto) | RemoteWrite v2 类型定义(OpenMetrics 2) |
| [gogoproto/gogo.proto](gogoproto/gogo.proto) | gogo/protobuf 扩展选项 |

## 当前实现策略

`prom-gw` **不直接**用 `protoc` 从这里生成代码,而是通过 Go module 依赖官方
`github.com/prometheus/prometheus/prompb` 包,原因:

1. **版本一致性**:Prometheus 官方包随 Prometheus 主版本迭代,自行生成易出现字段漂移
2. **避免 CGO**:gogoproto 生成需要 `protoc-gen-gogo` 等额外工具链,增加构建复杂度
3. **上下游兼容**:Prometheus / Cortex / VM 等客户端共用同一份 prompb,天然兼容

## 若需重新生成

安装 buf / protoc 后:

```bash
# 1) 安装 protoc + protoc-gen-go + protoc-gen-gogo
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install github.com/gogo/protobuf/protoc-gen-gogo@latest

# 2) 用 protoc 生成
protoc \
  --proto_path=api/proto \
  --gogo_out=. \
  --go_out=. \
  api/proto/remote.proto api/proto/types.proto
```

## 协议参考

- [Prometheus RemoteStorage Spec](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)
- [WriteRequest proto 注释](remote.proto#L22)
