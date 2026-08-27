# 鉴权 (v1: 本地 Token)

> v1.0 仅做本地 Token 校验,未来接入公司 IAM 体系。

## 1. Token 申请

联系运维(目前仅 `prom-gw-admin`),提供:
- 业务名(business)
- 需要的 rate_limit
- 默认投递的 Kafka topic

获取一个 token,形如 `tk_app_business_dev`。

## 2. Token 格式

```
tk_<team>_<env>_<random>
  ↑    ↑      ↑       ↑
  固定 团队  环境  随机串
```

- 必须以 `tk_` 开头
- 中间含团队/环境便于人工识别
- 末尾随机串保证不可猜

## 3. 配置示例

`/etc/prom-gw/tokens.yaml`:

```yaml
tokens:
  "tk_app_business_dev":
    business: app-business         # 业务名,会作为 request 标签
    business_id: "1001"            # 未来 IAM 主键
    default_topic: prom.raw.app_business
    rate_limit: 80000            # 单business 80K samples/s
```

字段说明:
| 字段 | 必填 | 说明 |
|---|---|---|
| `business` | ✓ | 业务名,进入 request 标签 |
| `business_id` | | 未来 IAM 主键,v1 可空 |
| `default_topic` | ✓ | 该 token 默认投递 topic |
| `rate_limit` | | 单business限流(samples/s),默认 = 全局配置 |

## 4. 客户端使用

### 4.1 Prometheus remote_write

```yaml
remote_write:
  - url: http://prom-gw-1:19201/api/v1/write
    authorization:
      type: Bearer
      credentials: "tk_app_business_dev"
```

### 4.2 自研客户端

```bash
curl -X POST http://prom-gw-1:19201/api/v1/write \
  -H "Content-Type: application/x-protobuf" \
  -H "Content-Encoding: snappy" \
  -H "Authorization: Bearer tk_app_business_dev" \
  --data-binary @payload.bin
```

## 5. Token 轮换

```bash
# 1. 在 tokens.yaml 加入新 token
sudo vim /etc/prom-gw/tokens.yaml

# 2. HUP 重载(无需重启)
sudo kill -HUP $(pidof prom-gw)

# 3. 客户端切换到新 token,验证 OK 后删除旧 token

# 4. 再 HUP 一次
```

## 6. Token 吊销

直接删除 entries,HUP 即可,**已签发但被吊销的 token 立即 401**。

## 7. 失败原因

401 响应里的 `message`:
- `auth failed: missing`:没带 Authorization header
- `auth failed: invalid`:token 不存在或已吊销
- `auth failed: expired`:v1 暂不实现,占位

指标:`gateway_auth_fail_total{reason}`(v1 只 emit `invalid`)。

## 8. 未来 IAM 接入

详见 design 文档 F.3 / F.4:
- v1 接口签名已与未来 IAM 实现兼容
- 切换时只需改 receiver.Auth 中间件,下游不变
- IAM 接入会引入 mTLS / OIDC JWT,这里只做占位
