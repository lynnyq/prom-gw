// Package config 集中管理所有配置:
//   - tokens.yaml 启动加载 + SIGHUP 重载
//   - rules/*.yaml fsnotify 监听 + 编译 + 原子切换
//   - Nacos 长轮询(Phase 4 接入)
//   - 启动参数 /etc/prom-gw/config.yaml
package config

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"github.com/lynnyq/prom-gw/internal/auth"
	"gopkg.in/yaml.v3"
)

// tokensFile 与 configs/tokens/local.yaml 结构对应。
type tokensFile struct {
	Tokens map[string]tokenEntry `yaml:"tokens"`
}

type tokenEntry struct {
	Business     string `yaml:"business"`
	BusinessID   string `yaml:"business_id"`
	DefaultTopic string `yaml:"default_topic"`
	RateLimit    int    `yaml:"rate_limit"`
}

// LocalTokenAuthenticator 本地 token 鉴权器,实现 auth.Authenticator。
//
// 特性:
//   - 启动时从 yaml 加载全量 token,后台运行期通过 Reload 热加载
//   - 读路径完全 lock-free(atomic.Pointer 持有 map)
//   - 不感知过期/吊销(token 与 IAM 切换后由 IAM 端保证);为兼容性仍返回对应 sentinel
type LocalTokenAuthenticator struct {
	path string

	// tokens atomic 持有 map;Reload 整体替换,无锁读
	tokens atomic.Pointer[map[string]auth.Business]
}

// NewLocalTokenAuthenticator 加载 path,失败返回 error。
// 成功后的对象可被并发调用 Verify;Reload 用于 SIGHUP 热加载。
func NewLocalTokenAuthenticator(path string) (*LocalTokenAuthenticator, error) {
	a := &LocalTokenAuthenticator{path: path}
	if err := a.Reload(path); err != nil {
		return nil, fmt.Errorf("load tokens from %s: %w", path, err)
	}
	return a, nil
}

// Reload 重读 path 并原子替换内部 map;返回 error 不影响旧版本。
func (a *LocalTokenAuthenticator) Reload(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var f tokensFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("unmarshal %s: %w", path, err)
	}
	if len(f.Tokens) == 0 {
		return fmt.Errorf("no tokens in %s", path)
	}
	m := make(map[string]auth.Business, len(f.Tokens))
	for token, e := range f.Tokens {
		if e.Business == "" {
			return fmt.Errorf("token %q has empty business", token)
		}
		if e.DefaultTopic == "" {
			return fmt.Errorf("token %q has empty default_topic", token)
		}
		if e.RateLimit <= 0 {
			return fmt.Errorf("token %q has invalid rate_limit %d", token, e.RateLimit)
		}
		m[token] = auth.Business{
			Name:         e.Business,
			DefaultTopic: e.DefaultTopic,
			BusinessID:   e.BusinessID,
			RateLimit:    e.RateLimit,
		}
	}
	a.tokens.Store(&m)
	return nil
}

// Verify 查表;ctx 仅用于 cancel 检查(本地实现不调外部,极快)。
func (a *LocalTokenAuthenticator) Verify(ctx context.Context, token string) (auth.Business, error) {
	if err := ctx.Err(); err != nil {
		return auth.Business{}, err
	}
	if token == "" {
		return auth.Business{}, auth.ErrTokenMissing
	}
	m := a.tokens.Load()
	if m == nil {
		return auth.Business{}, auth.ErrTokenInvalid
	}
	t, ok := (*m)[token]
	if !ok {
		return auth.Business{}, auth.ErrTokenInvalid
	}
	return t, nil
}

// Path 返回配置文件路径(用于日志/诊断)。
func (a *LocalTokenAuthenticator) Path() string { return a.path }

// Size 返回当前加载的 token 数(监控用)。
func (a *LocalTokenAuthenticator) Size() int {
	m := a.tokens.Load()
	if m == nil {
		return 0
	}
	return len(*m)
}

// ListBusinesses 列出所有已知 business(去重)。不返回 token 本身。
//
// 用于 admin /v1/businesses endpoint(plan T4.5)。
func (a *LocalTokenAuthenticator) ListBusinesses() []auth.Business {
	m := a.tokens.Load()
	if m == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(*m))
	out := make([]auth.Business, 0, len(*m))
	for _, t := range *m {
		if _, dup := seen[t.Name]; dup {
			continue
		}
		seen[t.Name] = struct{}{}
		out = append(out, t)
	}
	return out
}

// BusinessLimits 返回当前所有 business → RateLimit 映射(plan T5.1 per-business 限流)。
//
// 用于 receiver.UpdateBusinessLimits 在 SIGHUP 时刷新 per-business 限流配置;
// 该映射只关心 name + rate_limit,其他字段丢弃。
// 重复 business 时按 token key 字典序取首个,行为确定(避免 Go map 随机迭代顺序导致的热重载结果抖动)。
func (a *LocalTokenAuthenticator) BusinessLimits() map[string]int {
	m := a.tokens.Load()
	if m == nil {
		return nil
	}
	// 排序 token key,保证多次调用顺序一致
	keys := make([]string, 0, len(*m))
	for k := range *m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]int, len(*m))
	for _, k := range keys {
		t := (*m)[k]
		if t.RateLimit <= 0 {
			continue
		}
		if _, dup := out[t.Name]; dup {
			continue
		}
		out[t.Name] = t.RateLimit
	}
	return out
}

// 编译期检查: LocalTokenAuthenticator 实现 auth.Authenticator。
var _ auth.Authenticator = (*LocalTokenAuthenticator)(nil)
