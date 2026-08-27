// Package receiver - business_rl_test.go: per-business 限流测试。
package receiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// 构造一个无网络依赖的 Server,仅测试 businessLimiter 逻辑。
func newRLTestServer(t *testing.T, defaultRPS int) *Server {
	t.Helper()
	s, err := New(Config{
		Addr:            ":0",
		Authenticator:   &fakeAuth{},
		Logger:          zap.NewNop(),
		GlobalRateLimit: defaultRPS,
	})
	require.NoError(t, err)
	return s
}

type fakeAuth struct{}

func (*fakeAuth) Verify(_ context.Context, _ string) (auth.Business, error) {
	return auth.Business{}, nil
}

func TestBusinessLimiter_Default(t *testing.T) {
	s := newRLTestServer(t, 100)
	lim := s.businessLimiter("unknown")
	require.NotNil(t, lim)
	// burst=100,前 100 个 Allow 应该成功
	ok := 0
	for i := 0; i < 150; i++ {
		if lim.Allow() {
			ok++
		}
	}
	// burst + 短时间恢复
	assert.GreaterOrEqual(t, ok, 100)
	assert.Less(t, ok, 151)
}

func TestBusinessLimiter_PerBusinessConfig(t *testing.T) {
	s := newRLTestServer(t, 1000)
	s.UpdateBusinessLimits(map[string]int{
		"big":   5000,
		"small": 5,
	})
	limSmall := s.businessLimiter("small")
	ok := 0
	for i := 0; i < 50; i++ {
		if limSmall.Allow() {
			ok++
		}
	}
	// burst=5,前 5 个 allow,后 ~45 个拒绝
	assert.LessOrEqual(t, ok, 10, "small 限流应大幅拒绝")
}

func TestUpdateBusinessLimits_ClearsCache(t *testing.T) {
	s := newRLTestServer(t, 100)
	s.UpdateBusinessLimits(map[string]int{"t": 10})
	lim1 := s.businessLimiter("t")
	s.UpdateBusinessLimits(map[string]int{"t": 1000})
	lim2 := s.businessLimiter("t")
	// limiter 重建 → 不同对象
	assert.NotSame(t, lim1, lim2)
}

func TestBusinessRateLimitMW_Rejects429(t *testing.T) {
	s := newRLTestServer(t, 1000)
	s.UpdateBusinessLimits(map[string]int{"tiny": 1})

	mw := s.businessRateLimitMW(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	hits := 0
	rejected := 0
	for i := 0; i < 10; i++ {
		r := httptest.NewRequest(http.MethodPost, "/x", nil)
		// 直接用 ctxKeyBusiness 把 business 注入(避免依赖 authMW)
		ctx := context.WithValue(r.Context(), ctxKeyBusiness{}, auth.Business{Name: "tiny"})
		r = r.WithContext(ctx)
		rr := httptest.NewRecorder()
		mw.ServeHTTP(rr, r)
		if rr.Code == http.StatusOK {
			hits++
		} else if rr.Code == http.StatusTooManyRequests {
			rejected++
			assert.Contains(t, rr.Header().Get("Retry-After"), "1")
		}
	}
	assert.Greater(t, rejected, 0, "tiny business 应被限流")
	assert.GreaterOrEqual(t, hits, 1, "至少 1 个 burst 命中")
}

func TestBusinessRateLimitMW_AllowsOK(t *testing.T) {
	s := newRLTestServer(t, 1000)
	mw := s.businessRateLimitMW(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	ctx := context.WithValue(r.Context(), ctxKeyBusiness{}, auth.Business{Name: "unknown"})
	r = r.WithContext(ctx)
	rr := httptest.NewRecorder()
	mw.ServeHTTP(rr, r)
	assert.Equal(t, http.StatusOK, rr.Code)
}