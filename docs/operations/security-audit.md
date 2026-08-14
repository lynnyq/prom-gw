# prom-gw 安全评估报告

> 审计日期：2026-08-13
> 审计方式：静态代码分析（未修改任何代码）
> 审计范围：认证授权、输入验证、配置密钥、网络传输、依赖并发五大维度
> 发现总数：**56 项**（高 11 / 中 25 / 低 20）

---

## 总体评估

项目在业务逻辑层有基本安全意识（RE2 正则免疫 ReDoS、yaml.v3 免疫反序列化、请求体大小限制、panic recovery 覆盖完整、限流双层防护），但在**密钥生命周期管理**和**网络传输安全**上存在系统性缺口。

最严重的攻击链：**伪造 X-Forwarded-For 绕过 Admin IP 白名单 → 无独立鉴权直接篡改路由规则**。

---

## 风险分布

| 攻击面 | 高 | 中 | 低 | 合计 |
|---|---|---|---|---|
| 认证与授权 | 3 | 4 | 3 | 10 |
| 配置与密钥 | 6 | 6 | 5 | 17 |
| 输入验证 | 1 | 6 | 4 | 11 |
| 网络与传输 | 1 | 3 | 1 | 5 |
| 依赖与并发 | 0 | 6 | 7 | 13 |
| **合计** | **11** | **25** | **20** | **56** |

---

## 关键高风险问题（11 项，建议立即修复）

### 1. Admin 安全边界可被完全绕过

**[高] 1.1 X-Forwarded-For 无条件信任导致 IP 白名单绕过**
- 位置：`internal/admin/helpers.go:37-49`、`internal/receiver/server.go:367-380`
- 描述：`parseClientIP()` 优先取 X-Forwarded-For 首段，未校验上游可信链。攻击者只需 `X-Forwarded-For: 127.0.0.1` 即可绕过 Admin IP 白名单。
- 影响：完全绕过 Admin 安全边界，可篡改规则配置、注入恶意规则、回滚到有漏洞的历史版本。
- 修复建议：
  1. 移除 Admin 的 XFF 解析，仅使用 `r.RemoteAddr`
  2. 增加 `--trusted-proxies` 配置，仅当 RemoteAddr 在可信代理网段时才解析 XFF
  3. 增加测试：`X-Forwarded-For: 127.0.0.1` + 外网 RemoteAddr 应被拒绝

**[高] 1.2 Admin 接口仅靠 IP 白名单，无独立鉴权**
- 位置：`internal/admin/server.go:240-262`
- 描述：Admin 暴露 `PUT /v1/rulesets/{name}`、`POST /v1/rulesets/{name}:reload`、`POST /v1/rulesets/{name}:rollback`、`GET /v1/tenants` 等敏感操作，均无 Token/mTLS/BasicAuth 保护。
- 修复建议：
  1. 在 `allowlistMW` 后增加 `authMW`，要求 Admin 请求携带独立 admin token
  2. 长期按 `internal/admin/doc.go:2` 注释规划，接入 mTLS + OIDC
  3. 对写操作增加审计日志（who/when/what）

### 2. 全部 HTTP 服务明文监听，Token 网络明文传输

**[高] 2.1 四个 HTTP server 均无 TLS**
- 位置：
  - `internal/receiver/server.go:103-107`（receiver `:19201`）
  - `internal/admin/server.go:155-159`（admin `:8082`）
  - `cmd/prom-gw/main.go:470-474`（metrics `:8080`）
  - `cmd/prom-gw/main.go:490-494`（health `:8081`）
- 描述：全部使用 `ListenAndServe()`，无 `ListenAndServeTLS()`。客户端 `Authorization: Bearer tk_xxx` 在网络完全明文。
- 修复建议：
  1. 增加 `--tls-cert` / `--tls-key` flag
  2. 默认拒绝明文模式，通过 `--insecure` 显式开启（仅 dev）
  3. 配置 `tls.Config{MinVersion: tls.VersionTLS12, CipherSuites: ...}` 禁用弱套件
  4. 至少为 receiver 和 admin 强制 TLS

### 3. 默认 Token 入仓且可猜测

**[高] 3.1 开发 Token 明文入仓**
- 位置：`configs/tokens/local.yaml:5-15`
- 描述：明文存放 `tk_app_business_dev`、`tk_infra_dev`，命名可猜测，不符合 `docs/user/auth.md:14-20` 规定的 `tk_<team>_<env>_<random>` 格式。
- 修复建议：
  1. 从 git 历史移除该文件（`git filter-repo`），改用 `local.yaml.example` 模板
  2. dev token 也应包含随机段，如 `tk_app_business_dev_a3f7b2e9c1`
  3. 启动时检测 token 格式，不匹配则拒绝启动

**[高] 3.2 .gitignore 规则与实际文件路径不匹配**
- 位置：`.gitignore:38-39`
- 描述：规则 `configs/tokens.local.yaml` 锚定仓库根，匹配的是 `configs/` 下名为 `tokens.local.yaml` 的文件，而非 `configs/tokens/local.yaml`（子目录形式），导致文件实际已入仓。
- 修复建议：修正为
  ```
  configs/tokens/local.yaml
  configs/tokens/prod.yaml
  configs/tokens/*.local.yaml
  configs/tokens/*.prod.yaml
  ```

### 4. Token 明文存储，无加密无文件权限校验

**[高] 4.1 Token 明文存储（内存+配置）**
- 位置：`internal/config/token.go:67-83`
- 描述：以明文 token 作为 map key，配置文件也是明文。内存 dump 或配置文件泄露时 Token 立即可用。
- 修复建议：存储时只保留 `SHA-256(token)` 作为 key，配置文件中使用 `hashed_token` 字段，或通过 secret manager 注入。

**[高] 4.2 无文件权限校验**
- 位置：`internal/config/token.go:56`、`internal/config/source.go:163`
- 描述：加载 Token 和 ruleset 文件时不检查文件权限。生产指南声称"token 文件权限 0600"，但代码未强制。
- 修复建议：在 `Reload` 中调用 `os.Stat(path)`，检查 `mode & 0o077 == 0`，否则返回 error 或 warn。

### 5. Nacos 凭据通过命令行 flag 传入

**[高] 5.1 Nacos 用户名/密码通过 CLI flag**
- 位置：`cmd/prom-gw/main.go:73-74`
- 描述：`--nacos-username` / `--nacos-password` 会被 `ps aux` 和 `/proc/<pid>/cmdline` 看到。
- 修复建议：改为从环境变量 `NACOS_USERNAME` / `NACOS_PASSWORD` 读取，或支持从 Vault/K8s Secret 拉取。

### 6. Nacos 通信未加密

**[高] 6.1 Nacos 无 TLS 配置**
- 位置：`internal/config/nacos.go:98-120`
- 描述：`NewNacosSDKClient` 构造 ServerConfig 时未设置 TLS 选项，Nacos 通信默认走明文 HTTP，凭据和配置明文传输。
- 修复建议：在 `NacosConfig` 中增加 `TLSEnable bool` / `TLSConfig *tls.Config`，生产环境强制开启。

### 7. Ansible systemd 模板缺失安全加固

**[高] 7.1 Ansible 模板与 systemd 模板安全姿态不一致**
- 位置：`deploy/ansible/roles/prom_gw/templates/prom-gw.service.j2`
- 描述：完全缺失 `NoNewPrivileges`、`ProtectSystem`、`ProtectHome`、`PrivateTmp`、`PrivateDevices`、`ProtectKernelTunels`、`ProtectKernelModules`、`ProtectControlGroups`、`RestrictSUIDSGID`、`LockPersonality`、`RestrictRealtime`、`RestrictNamespaces`、`MemoryMax`、`TasksMax`、`ReadWritePaths` 等加固项。
- 修复建议：将 `prom-gw@.service` 中的所有安全加固项同步到 `prom-gw.service.j2`，用变量参数化。

### 8. WAL 文件权限过宽

**[高] 8.1 WAL 文件 0o644 含敏感业务数据**
- 位置：`internal/wal/wal.go:282`、`internal/wal/wal.go:369`
- 描述：WAL 段文件以 `0o644` 创建。WAL 存储原始 Prometheus WriteRequest（可能含 PII label 如 user_id、email）、tenant 名、traceparent 等，同机任何用户可读。
- 修复建议：文件权限改为 `0o600`，目录 `0o700`，确保 systemd `ReadWritePaths=/data/wal` 且 owner 为 `prom-gw`。

### 9. kafkasink 不支持 SASL/SSL

**[高] 9.1 Kafka 客户端无 SASL/SSL，与生产文档矛盾**
- 位置：`internal/kafkasink/producer.go:179-194`
- 描述：`kgo.NewClient` 的 opts 未包含任何 `kgo.SASL` 选项或 TLS 配置。`docs/operations/production-guide.md:96` 声称"9094 Kafka 客户端访问(SSL/SASL)"，但代码无法连接 SSL/SASL Kafka。
- 修复建议：在 `kafkasink.Config` 中增加 `SASL` / `TLS` 字段，构造 client 时注入对应 `kgo.Opt`。

### 10. pprof/metrics 端点无鉴权

**[高] 10.1 /debug/pprof/* 和 /metrics 完全无鉴权**
- 位置：`cmd/prom-gw/main.go:463-474`
- 描述：`:8080` 端口暴露的 `/debug/pprof/*` 可触发 heap/profile 抓取，泄露 goroutine 栈、内存布局；`/metrics` 暴露 `gateway_auth_fail_total{reason}`、`gateway_samples_total{tenant}` 等，泄露 tenant 列表和失败率。
- 修复建议：
  1. pprof 端点单独绑 `127.0.0.1:8080`
  2. 或为 `/debug/pprof/*` 加独立 BasicAuth / Token 中间件
  3. metrics 端口默认绑内网接口

---

## 中风险问题（25 项，建议 1-2 周内修复）

### 认证与授权

| 编号 | 问题 | 位置 |
|---|---|---|
| M-1 | Token 比较未用常量时间比较 | `internal/config/token.go:101-104` |
| M-2 | 无 Token 过期机制，泄露后永久有效 | `internal/auth/authenticator.go:23-26` |
| M-3 | 默认白名单 `10.0.0.0/8` 过宽（1600 万 IP） | `internal/admin/server.go:132-134` |
| M-4 | Admin 写操作成功路径无审计日志 | `internal/admin/server.go:335-399` |

### 输入验证

| 编号 | 问题 | 位置 |
|---|---|---|
| M-5 | X-Source-DC 头无校验，可污染指标和 Kafka header | `internal/receiver/server.go:221-223` |
| M-6 | HTTP server 缺 WriteTimeout/ReadTimeout/IdleTimeout，slowloris 风险 | `internal/receiver/server.go:103-107`、`internal/admin/server.go:155-159` |
| M-7 | protobuf 解码无 TimeSeries 数量上限，64MB payload 可含数十万条 series 导致 OOM | `internal/parser/parser.go:68` |
| M-8 | 无 Labels 数量限制，单条 series 可含数万 label | `internal/parser/parser.go:86-128` |
| M-9 | Kafka topic 名称无正则校验 | `internal/kafkasink/producer.go:264-266` |
| M-10 | AllowAutoTopicCreation 静默创建错误 topic | `internal/kafkasink/producer.go:187` |
| M-11 | 规则执行无超时限制 | `internal/ruleengine/pipeline.go:134-246` |

### 配置与密钥

| 编号 | 问题 | 位置 |
|---|---|---|
| M-12 | Nacos 快照文件权限 `0o644` | `internal/config/nacos.go:305,314` |
| M-13 | Nacos 快照无完整性校验（无 HMAC/签名） | `internal/config/nacos.go:284-298` |
| M-14 | OTLP Tracing 硬编码 `Insecure: true` | `cmd/prom-gw/main.go:99` |
| M-15 | prom-gw@.service 允许 `LimitCORE=infinity`，core dump 可能含 Token | `deploy/systemd/prom-gw@.service:30` |
| M-16 | 缺少 Capability 限制与 SystemCallFilter | `deploy/systemd/prom-gw@.service:45-58` |
| M-17 | Ansible 未渲染/校验 Token 文件 | `deploy/ansible/roles/prom_gw/tasks/main.yml:38-54` |

### 依赖与并发

| 编号 | 问题 | 位置 |
|---|---|---|
| M-18 | 限流单位为"请求/秒"而非"样本/秒"，与设计不符 | `internal/receiver/tenant_rl.go:77` |
| M-19 | WAL fsync 持锁串行化，吞吐瓶颈 200-1000 写/秒 | `internal/wal/wal.go:453-483` |
| M-20 | readSegment 缺 totalLen 上界，损坏文件可触发 4GB 内存分配 | `internal/wal/wal.go:702-706` |
| M-21 | gogo/protobuf 已废弃，无上游修复 | `go.mod:7` |
| M-22 | 缺少 bodyclose/depguard/noctx 等关键安全 linter | `.golangci.yml` |
| M-23 | Token error message 泄露明文到日志 | `internal/config/token.go:70,73,76` |
| M-24 | WAL 目录权限过宽 `0o755` | `internal/wal/wal.go:204` |
| M-25 | WAL 无静态加密 | `internal/wal/wal.go:795-832` |

---

## 低风险问题（20 项，逐步改进）

| 编号 | 问题 | 位置 |
|---|---|---|
| L-1 | 鉴权失败响应泄露 reason 分类 | `internal/receiver/server.go:197` |
| L-2 | Bearer 前缀匹配区分大小写 | `internal/receiver/server.go:359-365` |
| L-3 | 路由无显式鉴权白名单 | `internal/receiver/server.go:123-127` |
| L-4 | /v1/tenants 暴露 tenant 元数据 | `internal/admin/server.go:84-90` |
| L-5 | Token 强度不足（无随机熵） | `configs/tokens/local.yaml:5,11` |
| L-6 | computeMD5 命名误导（非加密哈希） | `internal/config/source.go:504-511` |
| L-7 | 默认 Token 路径指向开发文件 | `cmd/prom-gw/main.go:59` |
| L-8 | systemd EnvironmentFile 使用 `-` 前缀（可选） | `deploy/systemd/prom-gw@.service:43` |
| L-9 | WAL 文件路径可预测 | `internal/wal/wal.go:367` |
| L-10 | 启动日志输出配置文件路径 | `cmd/prom-gw/main.go:114-121` |
| L-11 | OTLP exporter 无证书校验配置 | `internal/obs/tracing.go:93-95` |
| L-12 | 文档引导内网不启用 TLS | `docs/operations/ha-lb-deployment.md:968` |
| L-13 | 错误消息泄露内部实现细节 | `internal/receiver/server.go:252,268` |
| L-14 | 自定义 readAll 用字符串比较检测 EOF | `internal/receiver/server.go:403-419` |
| L-15 | decoder.Decode 无独立 panic recovery | `internal/decoder/decoder.go:49-74` |
| L-16 | 测试代码中硬编码 Token 字面量 | `internal/config/token_test.go:14-26` |
| L-17 | group_vars 与 env.j2 无硬编码密钥（良好） | `deploy/ansible/inventory/group_vars/all.yml` |
| L-18 | 鉴权失败日志不含 Token（良好） | `internal/receiver/server.go:193-196` |
| L-19 | Admin API 不返回 Token 明文（良好） | `internal/config/token.go:120-138` |
| L-20 | pipeline.go buffer 交换逻辑空 slice index 风险（被 recover 兜底） | `internal/ruleengine/pipeline.go:174,182` |

---

## 正面发现（设计正确的部分）

- **RE2 正则引擎**：Go regexp 天然免疫 ReDoS（`internal/ruleengine/stage.go:578`）
- **yaml.v3**：免疫 yaml.v2 反序列化漏洞（`internal/ruleengine/compiler.go:14`）
- **请求体大小限制**：16MB + Content-Type/Encoding 严格校验（`internal/receiver/server.go:95,236,248`）
- **状态型 stage 有内存上限**：deadvalue/downsample LRU 默认 1M series
- **原子规则热更新**：`atomic.Pointer[CompiledRuleSet]` 无锁切换，批次内一致
- **Panic recovery 覆盖完整**：safego 封装所有后台 goroutine，HTTP 中间件 + stage 级 recover
- **WAL 双阈值有界**：50GB + 80% 磁盘使用率，retention 24h 自动清理
- **TenantInfo 显式排除 token 字段**：`internal/admin/server.go:84-90`
- **限流 key 不可伪造**：tenant 由服务端 token 鉴权决定，不读取 HTTP header
- **Kafka producer 队列有界**：65535 + 100ms BlockTimeout
- **Graceful shutdown**：SIGINT/SIGTERM 触发，30s 超时，defer 链有序关闭

---

## 修复优先级建议

### P0 立即修复（1-3 天）

1. **移除 Admin `parseClientIP` 的 XFF 信任**（`internal/admin/helpers.go:37-49`），仅用 `r.RemoteAddr`
2. **从 git 移除 `configs/tokens/local.yaml`**，修正 `.gitignore`，所有环境签发强 Token
3. **WAL 文件权限 `0o644` → `0o600`**，目录 `0o700`（一行改动）
4. **Nacos 凭据改环境变量**，移除 `--nacos-username`/`--nacos-password` CLI flag

### P1 短期修复（1-2 周）

5. 为 receiver + admin 增加 TLS 监听入口，生产强制启用
6. Admin 增加独立 Token 鉴权层
7. 所有 HTTP server 添加 `ReadTimeout`/`WriteTimeout`/`IdleTimeout`
8. protobuf 解码增加 series/label 数量上限
9. 限流改为按样本数计费 `limiter.AllowN(len(samples))`
10. Token 存储改哈希 + 常量时间比较
11. pprof 绑定 127.0.0.1，metrics 加鉴权
12. Kafka topic 名称正则校验，生产禁用 AllowAutoTopicCreation

### P2 长期改进（1 个月）

13. kafkasink 增加 SASL/SSL 支持
14. Nacos 通信启用 TLS
15. Ansible systemd 模板补齐安全加固项
16. Token 过期机制 + 审计日志
17. mTLS 支持
18. golangci.yml 增加 bodyclose/depguard/noctx linter
19. 迁移 gogo/protobuf 到 google.golang.org/protobuf

---

## 待办清单

- [ ] P0-1: 移除 Admin XFF 信任
- [ ] P0-2: 从 git 移除默认 Token，修正 .gitignore
- [ ] P0-3: WAL 文件权限收紧到 0o600
- [ ] P0-4: Nacos 凭据改环境变量
- [ ] P1-5: receiver + admin 增加 TLS
- [ ] P1-6: Admin 独立 Token 鉴权
- [ ] P1-7: HTTP server 超时配置
- [ ] P1-8: protobuf series/label 数量上限
- [ ] P1-9: 限流改为按样本数计费
- [ ] P1-10: Token 哈希存储 + 常量时间比较
- [ ] P1-11: pprof 绑定 127.0.0.1
- [ ] P1-12: Kafka topic 正则校验
- [ ] P2-13: kafkasink SASL/SSL
- [ ] P2-14: Nacos TLS
- [ ] P2-15: Ansible systemd 加固
- [ ] P2-16: Token 过期 + 审计日志
- [ ] P2-17: mTLS 支持
- [ ] P2-18: golangci.yml linter 补充
- [ ] P2-19: 迁移 gogo/protobuf
