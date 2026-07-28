# DEVELOPING

## 流程

1. 切分支:`git checkout -b feat/<name>`
2. 改代码(每个 task 对应一个 PR,PR 大小控制在 200~500 行)
3. `make fmt && make lint && make test`
4. 提 PR,需 1 人 review
5. CI 全绿后合并

## 工具链

- Go 1.22+(本机 1.26.4)
- `golangci-lint` v1.61+
- `buf`(T1.1 实施时)
- `oapi-codegen` + `redocly/cli`(T4.7)
- `toxiproxy`(T5.5 混沌测试)
- Docker(testcontainers 跑 Kafka / Nacos)

## 测试分层

| 层级 | 命令 | 范围 |
|---|---|---|
| 单元 | `make test` | 每个包 `*_test.go` |
| 集成 | `make test-integration` | testcontainers 启 Kafka/Nacos,需 Docker |
| 性能 | `make perf` | 自研 loadgen,30s 冒烟 |
| 混沌 | `make chaos` | toxiproxy 注入网络故障 |
| 兼容 | `make test-integration` 内 compat tag | 多 Prometheus 版本 |

## 调试

```bash
# 启 debug build(含 pprof)
go build -tags=debug -gcflags="all=-N -l" -o bin/prom-gw ./cmd/prom-gw

# 抓 profile
curl localhost:9090/debug/pprof/heap > heap.pprof
go tool pprof heap.pprof
```

## 提交规范

Conventional Commits:

```
feat(safego):  add Stats() for panic counter
fix(receiver): 避免 channel 满时无限阻塞
docs(plan):    补 WAL 容量配置
chore(ci):     升级 golangci-lint 到 v1.61
```

## 关键约束(用户规范)

1. `_ =` 禁止,error 必须处理
2. context.Context 必须贯穿 IO
3. 所有 goroutine 必须 `safego.Go` 包裹
4. 敏感信息(token / key)禁止入日志
5. 每个新函数必带单测
6. `make lint` 0 error 才能 commit
