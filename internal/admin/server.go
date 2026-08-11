// Package admin - server.go: Admin HTTP server。
//
// 设计要点(plan T4.3 / T4.5):
//   - 监听 :8082,默认白名单 127.0.0.1/32, 10.0.0.0/8
//   - 中间件:recover + tracing + 统一响应 + 来源 IP 白名单
//   - endpoint:
//     GET    /v1/healthz
//     GET    /v1/rulesets
//     GET    /v1/rulesets/{name}
//     PUT    /v1/rulesets/{name}
//     POST   /v1/rulesets/{name}:reload
//     POST   /v1/rulesets/{name}:rollback?to_version=N
//     GET    /v1/rulesets/{name}/history
//     GET    /v1/tenants
//     GET    /v1/stats
//   - 业务能力委托给 Service 接口(便于 mock 测试)
package admin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/config"
	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"github.com/lynnyq/bigdata/pkg/httpx"
	"github.com/lynnyq/bigdata/pkg/safego"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Service Admin 依赖的业务能力(便于测试 mock)。
//
// 实现由 main.go 注入(基于 config.Manager + auth.LocalTokenAuthenticator + ruleengine.Pipeline)。
type Service interface {
	// ListRuleSets 列出所有 known ruleset 名称及当前 version。
	ListRuleSets(ctx context.Context) []RuleSetSummary
	// GetRuleSet 读单个(返回 name+version+raw yaml+stages count)。
	GetRuleSet(ctx context.Context, name string) (RuleSetDetail, error)
	// PutRuleSet 提交新 ruleset(YAML 字节);version 必须严格大于当前最新。
	// 成功返回新 version。
	PutRuleSet(ctx context.Context, name string, version int64, rawYAML []byte) (int64, error)
	// ReloadRuleSet 强制从 source 重新拉一次(重走 Nacos / File pipeline)。
	ReloadRuleSet(ctx context.Context, name string) error
	// RollbackRuleSet 回到指定 version(必须 ≤ 最新 且 > 0)。
	RollbackRuleSet(ctx context.Context, name string, toVersion int64) error
	// ListHistory 列历史(从新到旧)。
	ListHistory(ctx context.Context, name string) []config.HistoryRecord
	// ListTenants 列当前 token → tenant 映射(仅元数据,不返回明文 token)。
	ListTenants(ctx context.Context) []TenantInfo
	// Stats 运行时统计(per ruleset QPS/drop rate 等)。
	Stats(ctx context.Context) StatsResp
}

// RuleSetSummary 列表项。
type RuleSetSummary struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
	Stages  int    `json:"stages"`
	Source  string `json:"source"`
}

// RuleSetDetail 单个详情。
type RuleSetDetail struct {
	Name        string `json:"name"`
	Version     int64  `json:"version"`
	DefaultTopic string `json:"default_topic"`
	Stages      int    `json:"stages"`
	RawYAML     string `json:"raw_yaml"`
	Source      string `json:"source"`
}

// TenantInfo tenant 元数据(不含 token)。
type TenantInfo struct {
	Tenant       string `json:"tenant"`
	TenantID     string `json:"tenant_id,omitempty"`
	DefaultTopic string `json:"default_topic"`
	RateLimit    int    `json:"rate_limit"`
}

// StatsResp 统计。
type StatsResp struct {
	CurrentRuleSet string         `json:"current_ruleset"`
	CurrentVersion int64          `json:"current_version"`
	BySource       map[string]int `json:"by_source"`
	HistorySize    int            `json:"history_size"`
	// 简单 drop rate 估算(用 metric snapshot)
	DropRate float64 `json:"drop_rate_estimate"`
}

// --- Server ---

// Server Admin HTTP server。
type Server struct {
	cfg     Config
	service Service
	http    *http.Server
	// authFailTotal 计数器(独立于 obs 指标,便于单元测试断言)
	authFailTotal atomic.Int64
}

// Config 启动参数。
type Config struct {
	Addr string
	// AllowCIDR 逗号分隔的 CIDR 列表,默认 "127.0.0.1/32,10.0.0.0/8"
	AllowCIDR []string
	Logger    *zap.Logger
	// ReadHeaderTimeout 头读取超时(默认 5s)
	ReadHeaderTimeout time.Duration
	// IngestCity 城市标识(bj/sz/hf),用于 admin 内部错误指标(spec 7.1)
	IngestCity string
	// SourceDC 机房标识,用于 admin 内部错误指标(spec 7.1)
	SourceDC string
}

// New 构造 admin server。
func New(cfg Config, svc Service) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":8082"
	}
	if len(cfg.AllowCIDR) == 0 {
		cfg.AllowCIDR = []string{"127.0.0.1/32", "10.0.0.0/8"}
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.ReadHeaderTimeout == 0 {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	if svc == nil {
		return nil, errors.New("admin: Service required")
	}

	// 编译期解析 CIDR(失败 fast-fail)
	allowed, err := parseCIDRs(cfg.AllowCIDR)
	if err != nil {
		return nil, fmt.Errorf("admin: parse allow cidr: %w", err)
	}

	s := &Server{
		cfg:     cfg,
		service: svc,
	}
	s.http = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.middleware(s.routes(), allowed),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	return s, nil
}

// ListenAndServe 阻塞。
func (s *Server) ListenAndServe() error { return s.http.ListenAndServe() }

// Shutdown 优雅停机。
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

// Addr 返回实际监听地址(:0 时有用)。
func (s *Server) Addr() string { return s.http.Addr }

// Handler 返回底层 http.Handler(便于 httptest)。
func (s *Server) Handler() http.Handler { return s.http.Handler }

// AuthFailCount 暴露给测试(返回 IP 白名单拒绝计数)。
func (s *Server) AuthFailCount() int64 { return s.authFailTotal.Load() }

// --- middleware / routes ---

func (s *Server) middleware(next http.Handler, allowed []*net.IPNet) http.Handler {
	return s.recoverMW(s.tracingMW(s.allowlistMW(next, allowed)))
}

func (s *Server) recoverMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				// spec §6.6: panic 通过 safego 统一计数 gateway_panic_recovered_total
				safego.ReportPanic("admin-handler", rec, debugStack())
				obs.ErrorsTotal.WithLabelValues("admin", "panic", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
				s.cfg.Logger.Error("admin: handler panic",
					zap.Any("recover", rec),
					zap.String("path", r.URL.Path),
					zap.String("trace_id", tracex.TraceIDFromContext(r.Context())),
					zap.String("ingest_city", s.cfg.IngestCity),
					zap.String("source_dc", s.cfg.SourceDC),
					zap.String("stage", "admin"),
				)
				httpx.WriteErr(w, r, httpx.CodeInternal, MsgInternal)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// debugStack returns a byte slice of the current goroutine stack.
func debugStack() []byte {
	return runtimeStack()
}

// runtimeStack is a helper to avoid importing runtime/debug in multiple places.
func runtimeStack() []byte {
	return debug.Stack()
}

func (s *Server) tracingMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// spec §7.2: 每请求 TraceID; admin 响应体 trace_id 与入口请求一致
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := obs.Tracer.Start(ctx, "admin.request",
			trace.WithAttributes(
				attribute.String("ingest_city", s.cfg.IngestCity),
				attribute.String("source_dc", s.cfg.SourceDC),
				attribute.String("stage", "admin"),
				attribute.String("http.method", r.Method),
				attribute.String("http.path", r.URL.Path),
			),
		)
		defer span.End()

		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = fmt.Sprintf("admin-%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-Id", rid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) allowlistMW(next http.Handler, allowed []*net.IPNet) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := parseClientIP(r)
		if !ipInNets(ip, allowed) {
			s.authFailTotal.Add(1)
			// plan T4.3: 单独的 gateway_admin_auth_fail_total 指标便于告警分桶;
			// 与 ErrorsTotal{admin,authz_fail} 互补,后者用于总错误率,前者用于 IP 拒绝率。
			obs.AdminAuthFailTotal.WithLabelValues("ip_not_allowed", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			obs.ErrorsTotal.WithLabelValues("admin", "authz_fail", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			s.cfg.Logger.Warn("admin: source ip not allowed",
				zap.String("ip", ip),
				zap.String("path", r.URL.Path),
				zap.String("trace_id", tracex.TraceIDFromContext(r.Context())),
				zap.String("ingest_city", s.cfg.IngestCity),
				zap.String("source_dc", s.cfg.SourceDC),
				zap.String("stage", "admin"),
			)
			httpx.WriteErr(w, r, httpx.CodeForbidden, MsgAuthzForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", s.handleHealthz)
	mux.HandleFunc("/v1/rulesets", s.handleRulesets)
	mux.HandleFunc("/v1/rulesets/", s.handleRulesetSub)
	mux.HandleFunc("/v1/tenants", s.handleTenants)
	mux.HandleFunc("/v1/stats", s.handleStats)
	return mux
}

// --- handlers ---

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	httpx.Write(w, r, map[string]any{"status": "ok"})
}

func (s *Server) handleRulesets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		httpx.Write(w, r, map[string]any{"rulesets": s.service.ListRuleSets(r.Context())})
	case http.MethodPut:
		// PUT /v1/rulesets/{name} 的路径走到 handleRulesetSub,这里只处理 root
		httpx.WriteErr(w, r, httpx.CodeBadRequest, "name required in path")
	default:
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
	}
}

func (s *Server) handleRulesetSub(w http.ResponseWriter, r *http.Request) {
	// 路径: /v1/rulesets/{name} 或 /v1/rulesets/{name}:reload 或 /v1/rulesets/{name}/history
	rest := strings.TrimPrefix(r.URL.Path, "/v1/rulesets/")
	if rest == "" {
		httpx.WriteErr(w, r, httpx.CodeBadRequest, "name required")
		return
	}
	// 拆 ":action" 或 "/history"
	var name, action, sub string
	if i := strings.IndexAny(rest, ":/"); i >= 0 {
		name = rest[:i]
		rest = rest[i:]
		if strings.HasPrefix(rest, ":") {
			action = rest[1:]
		} else if strings.HasPrefix(rest, "/") {
			sub = rest[1:]
		}
	} else {
		name = rest
	}
	if name == "" {
		httpx.WriteErr(w, r, httpx.CodeBadRequest, "name required")
		return
	}

	switch {
	case action == "reload":
		s.handleReload(w, r, name)
	case action == "rollback":
		s.handleRollback(w, r, name)
	case sub == "history":
		s.handleHistory(w, r, name)
	case action == "" && sub == "":
		s.handleOne(w, r, name)
	default:
		httpx.WriteErr(w, r, httpx.CodeNotFound, "unknown sub path")
	}
}

func (s *Server) handleOne(w http.ResponseWriter, r *http.Request, name string) {
	switch r.Method {
	case http.MethodGet:
		d, err := s.service.GetRuleSet(r.Context(), name)
		if err != nil {
			s.writeServiceErr(w, r, err)
			return
		}
		httpx.Write(w, r, d)
	case http.MethodPut:
		version, raw, err := parsePutBody(r)
		if err != nil {
			httpx.WriteErr(w, r, httpx.CodeBadRequest, err.Error())
			return
		}
		newVer, err := s.service.PutRuleSet(r.Context(), name, version, raw)
		if err != nil {
			s.writeServiceErr(w, r, err)
			return
		}
		httpx.WriteStatus(w, r, http.StatusOK, map[string]any{
			"name":    name,
			"version": newVer,
		})
	default:
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
	}
}

func (s *Server) handleReload(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	if err := s.service.ReloadRuleSet(r.Context(), name); err != nil {
		s.writeServiceErr(w, r, err)
		return
	}
	httpx.Write(w, r, map[string]any{"reloaded": name})
}

func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	toV, err := parseInt64Query(r, "to_version")
	if err != nil {
		httpx.WriteErr(w, r, httpx.CodeBadRequest, "to_version required")
		return
	}
	if err := s.service.RollbackRuleSet(r.Context(), name, toV); err != nil {
		s.writeServiceErr(w, r, err)
		return
	}
	httpx.Write(w, r, map[string]any{"rolled_back": name, "to_version": toV})
}

func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodGet {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	httpx.Write(w, r, map[string]any{"history": s.service.ListHistory(r.Context(), name)})
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	httpx.Write(w, r, map[string]any{"tenants": s.service.ListTenants(r.Context())})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteErr(w, r, httpx.CodeMethodNotAllowed, MsgBadRequest)
		return
	}
	httpx.Write(w, r, s.service.Stats(r.Context()))
}

// writeServiceErr 把 service 层 error 映射到 httpx 错误码。
func (s *Server) writeServiceErr(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}
	s.cfg.Logger.Warn("admin: service error",
		zap.Error(err),
		zap.String("trace_id", tracex.TraceIDFromContext(r.Context())),
		zap.String("ingest_city", s.cfg.IngestCity),
		zap.String("source_dc", s.cfg.SourceDC),
		zap.String("stage", "admin"),
	)
	var sentinel *httpx.Sentinel
	if errors.As(err, &sentinel) {
		httpx.WriteErr(w, r, sentinel.Code, sentinel.Message)
		return
	}
	// 常见错误 fallback
	switch {
	case errors.Is(err, ruleengine.ErrRuleSetNotFound) || strings.Contains(err.Error(), "not found"):
		httpx.WriteErr(w, r, httpx.CodeRuleSetNotFound, MsgRuleSetNotFound)
	case errors.Is(err, ruleengine.ErrRuleSetConflict) || strings.Contains(err.Error(), "conflict"):
		httpx.WriteErr(w, r, httpx.CodeRuleSetConflict, MsgRuleSetConflict)
	case strings.Contains(err.Error(), "invalid") || strings.Contains(err.Error(), "yaml"):
		httpx.WriteErr(w, r, httpx.CodeRuleSetInvalid, MsgRuleSetInvalid)
	default:
		httpx.WriteErr(w, r, httpx.CodeInternal, MsgInternal)
	}
}
