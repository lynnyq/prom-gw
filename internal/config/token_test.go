package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validYAML = `
tokens:
  "tk_test_a":
    business: app-business
    business_id: "1001"
    default_topic: prom.routed.app_business
    rate_limit: 80000
  "tk_test_b":
    business: infra
    business_id: "1002"
    default_topic: prom.routed.infra
    rate_limit: 50000
`

func writeTempTokens(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestNewLocalTokenAuthenticator_LoadsTokens(t *testing.T) {
	path := writeTempTokens(t, validYAML)
	a, err := NewLocalTokenAuthenticator(path)
	require.NoError(t, err)
	assert.Equal(t, 2, a.Size())
}

func TestVerify_HappyPath(t *testing.T) {
	path := writeTempTokens(t, validYAML)
	a, _ := NewLocalTokenAuthenticator(path)

	business, err := a.Verify(context.Background(), "tk_test_a")
	require.NoError(t, err)
	assert.Equal(t, "app-business", business.Name)
	assert.Equal(t, "1001", business.BusinessID)
	assert.Equal(t, "prom.routed.app_business", business.DefaultTopic)
	assert.Equal(t, 80000, business.RateLimit)
}

func TestVerify_EmptyToken(t *testing.T) {
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, validYAML))
	_, err := a.Verify(context.Background(), "")
	assert.ErrorIs(t, err, auth.ErrTokenMissing)
}

func TestVerify_UnknownToken(t *testing.T) {
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, validYAML))
	_, err := a.Verify(context.Background(), "tk_unknown")
	assert.ErrorIs(t, err, auth.ErrTokenInvalid)
}

func TestVerify_ContextCanceled(t *testing.T) {
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, validYAML))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := a.Verify(ctx, "tk_test_a")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReload_ReplacesTokens(t *testing.T) {
	path := writeTempTokens(t, validYAML)
	a, _ := NewLocalTokenAuthenticator(path)
	assert.Equal(t, 2, a.Size())

	// 写入更新后的文件,Reload
	updated := `
tokens:
  "tk_test_c":
    business: app-new
    default_topic: prom.routed.app_new
    rate_limit: 1000
`
	require.NoError(t, os.WriteFile(path, []byte(updated), 0o600))
	require.NoError(t, a.Reload(path))
	assert.Equal(t, 1, a.Size())

	// 旧 token 已失效
	_, err := a.Verify(context.Background(), "tk_test_a")
	assert.ErrorIs(t, err, auth.ErrTokenInvalid)

	// 新 token 生效
	business, err := a.Verify(context.Background(), "tk_test_c")
	require.NoError(t, err)
	assert.Equal(t, "app-new", business.Name)
}

func TestNew_InvalidYAML(t *testing.T) {
	_, err := NewLocalTokenAuthenticator("/nonexistent/path/tokens.yaml")
	assert.Error(t, err)
}

func TestNew_EmptyTokens(t *testing.T) {
	path := writeTempTokens(t, "tokens: {}\n")
	_, err := NewLocalTokenAuthenticator(path)
	assert.Error(t, err, "should reject empty tokens")
}

func TestNew_InvalidRateLimit(t *testing.T) {
	bad := `
tokens:
  "tk_x":
    business: t
    default_topic: p
    rate_limit: 0
`
	_, err := NewLocalTokenAuthenticator(writeTempTokens(t, bad))
	assert.Error(t, err, "rate_limit <= 0 should be rejected")
}

func TestBusinessLimits_ReturnsAllBusinesses(t *testing.T) {
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, validYAML))
	limits := a.BusinessLimits()
	require.Len(t, limits, 2)
	assert.Equal(t, 80000, limits["app-business"])
	assert.Equal(t, 50000, limits["infra"])
}

func TestBusinessLimits_DeduplicatesBusiness(t *testing.T) {
	yaml := `
tokens:
  "tk_a1":
    business: app
    default_topic: prom.routed.app
    rate_limit: 100
  "tk_a2":
    business: app
    default_topic: prom.routed.app
    rate_limit: 200
`
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, yaml))
	limits := a.BusinessLimits()
	// 同一 business 多 token 时取 token key 字典序首个,后续不覆盖
	assert.Equal(t, 100, limits["app"])
}

func TestBusinessLimits_DeterministicOrder(t *testing.T) {
	// 即便 yaml 顺序不同 / 多次调用,结果必须一致(token key 字典序首个获胜)
	yaml := `
tokens:
  "tk_zzz":
    business: app
    default_topic: prom.routed.app
    rate_limit: 999
  "tk_aaa":
    business: app
    default_topic: prom.routed.app
    rate_limit: 50
  "tk_mmm":
    business: other
    default_topic: prom.routed.other
    rate_limit: 300
`
	a, _ := NewLocalTokenAuthenticator(writeTempTokens(t, yaml))
	for i := 0; i < 50; i++ {
		limits := a.BusinessLimits()
		assert.Equal(t, 50, limits["app"], "iteration %d", i)
		assert.Equal(t, 300, limits["other"], "iteration %d", i)
	}
}
