// Package auth 定义鉴权抽象接口(预留 IAM 接入扩展点)。
// v1 只实现 LocalTokenAuthenticator(读 tokens.yaml);
// 未来 IAM 接入只需新增 OIDCAuthenticator,receiver.Auth 中间件不变。
//
// 接口设计原则:
//   - Authenticator.Verify 必须线程安全
//   - 返回 auth.Tenant 而非 string,扩展字段无需改 receiver
//   - 错误用 sentinels(ErrTokenMissing / ErrTokenInvalid / ErrTokenExpired / ErrTokenRevoked),
//     方便 metric 分类(gateway_auth_fail_total{reason})
package auth

import (
	"context"
	"errors"
)

// Sentinel 错误(receiver 根据类型映射 HTTP 状态码 + metric reason 标签)。
var (
	// ErrTokenMissing Authorization 头缺失或格式错误
	ErrTokenMissing = errors.New("auth: token missing")
	// ErrTokenInvalid token 不存在 / 不匹配
	ErrTokenInvalid = errors.New("auth: token invalid")
	// ErrTokenExpired token 已过期(本地实现暂不感知,预留给 IAM)
	ErrTokenExpired = errors.New("auth: token expired")
	// ErrTokenRevoked token 已吊销(预留给 IAM,本地实现同 ErrTokenInvalid)
	ErrTokenRevoked = errors.New("auth: token revoked")
)

// Tenant 鉴权成功后的租户信息。
// 字段顺序与 yaml 对齐;新增字段时注意 json/yaml 双向兼容。
type Tenant struct {
	Name         string // 租户短名,作为 sample.Tenant
	DefaultTopic string // 路由未命中时的兜底 topic
	TenantID     string // IAM 主键(本地模式为占位)
	RateLimit    int    // samples/s 上限
}

// Authenticator 鉴权接口。
//
// Verify 在 ctx 取消时应立即返回 ctx.Err();其他错误见 sentinels。
type Authenticator interface {
	Verify(ctx context.Context, token string) (Tenant, error)
}
