// Package receiver - tenant_rl.go: per-tenant 限流(plan T5.1)。
//
// 设计要点:
//   - 每个 tenant 一个 *rate.Limiter,基于 token 配置的 RateLimit 创建
//   - lazy 初始化:第一次见到该 tenant 时创建
//   - SIGHUP 重载后,RateLimit 变更:用 atomic.Pointer 持有 tenant → 配置,
//     实际 limiter 用 sync.Map 缓存;重载时清空缓存,让新请求重建
//   - 拒绝时打 metric gateway_rate_limit_rejected_total{tenant} + 429
package receiver

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/lynnyq/bigdata/internal/obs"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// rateLimitConfig 全局 per-tenant 限流配置(由 main.go 在 New 后注入)。
type rateLimitConfig struct {
	// configs 持有 tenant → RateLimit 映射,每次 SIGHUP 由 auth.Reload 触发
	// main.go 调 UpdateTenantLimits 重新写入此 map
	configs atomic.Pointer[map[string]int]
	// limiters 缓存 *rate.Limiter;key = tenant
	limiters sync.Map
	// default 默认兜底 rate(samples/s),防止未配置 tenant 拿不到限流
	defaultRPS int
}

// UpdateTenantLimits 由 main.go SIGHUP 时调,刷新 tenant → rate 映射。
//
// 传 nil 不变更(防御性)。
func (s *Server) UpdateTenantLimits(limits map[string]int) {
	if limits == nil {
		return
	}
	// 拷一份(避免外部修改)
	cp := make(map[string]int, len(limits))
	for k, v := range limits {
		cp[k] = v
	}
	s.rlCfg.configs.Store(&cp)
	// 清空旧 limiter,让下次请求以新 rate 重建
	s.rlCfg.limiters.Range(func(k, _ any) bool {
		s.rlCfg.limiters.Delete(k)
		return true
	})
}

// tenantLimiter 取 tenant 对应 limiter;无配置时给 default。
func (s *Server) tenantLimiter(tenant string) *rate.Limiter {
	if v, ok := s.rlCfg.limiters.Load(tenant); ok {
		return v.(*rate.Limiter)
	}
	rps := s.rlCfg.defaultRPS
	if cfg := s.rlCfg.configs.Load(); cfg != nil {
		if v, ok := (*cfg)[tenant]; ok && v > 0 {
			rps = v
		}
	}
	// burst = rps 即可;高吞吐下 burst 没必要 > rps
	lim := rate.NewLimiter(rate.Limit(rps), rps)
	actual, _ := s.rlCfg.limiters.LoadOrStore(tenant, lim)
	return actual.(*rate.Limiter)
}

// tenantRateLimitMW 鉴权通过后调,按 tenant 限流。
//
// 命中 429 + gateway_rate_limit_rejected_total{tenant} + Retry-After。
// 默认不放行无 tenant 的请求(兜底走 defaultRPS)。
func (s *Server) tenantRateLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant := tenantName(r.Context())
		lim := s.tenantLimiter(tenant)
		if !lim.Allow() {
			obs.RateLimitRejected.WithLabelValues(tenant, s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			obs.BackpressureRejected.WithLabelValues("tenant_rl", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			s.cfg.Logger.Warn("tenant rate limit exceeded",
				zap.String("tenant", tenant),
			)
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, errorBody(4292, "tenant rate limit exceeded"))
			return
		}
		next.ServeHTTP(w, r)
	})
}
