// Package receiver 实现 HTTP 接入层(POST /api/v1/write),
// 负责鉴权、限流、请求级上下文注入。
//
// 中间件链(顺序敏感):
//   RequestID -> RealIP -> Recoverer -> RateLimit -> Auth -> handler
//
// 端口固定 :19201(见 plan T1.4 端口分配表)。
// Auth 注入的 Tenant 走 ctx.Meta,parser/kafkasink 通过 ctx 读取。
package receiver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/lynnyq/bigdata/internal/decoder"
	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Config 接收 server 配置。
type Config struct {
	// Addr 监听地址,默认 ":19201"
	Addr string
	// Authenticator 必填(由 main.go 注入)
	Authenticator auth.Authenticator
	// Logger 必填
	Logger *zap.Logger
	// Registry 注入自定义 prom registry,用于 /metrics
	// 如未指定,使用 prometheus.DefaultRegisterer(只读)
	Registry *prometheus.Registry
	// SourceDC 实例所属机房,写入 Meta.SourceDC(spec 4.1 来自 instance tag / X-Source-DC 头)
	SourceDC string
	// IngestCity 城市标识(bj/sz/hf),spec 4.3 / 7.1
	IngestCity string
	// GlobalRateLimit 全局限流(samples/s),默认 100000
	GlobalRateLimit int
	// Handler 业务处理函数(由 main.go 注入,通常为 pipeline.Submit 入口)
	// 参数:
	//   - ctx: 含 Meta(tenant / source_dc / remote_ip / ingest_ts / ingest_city)
	//   - raw: 原始 prompb.WriteRequest 字节(T1.10 集成测试要求字节级相等)
	//   - samples: parser 解析后的 samples
	//   - defaultTopic: 该 token 关联的兜底 topic
	// 返回:
	//   - nil: 已入队
	//   - 其他 error: receiver 映射 503
	// 注:Meta.IngestTs 单位为纳秒(ns),发送 Kafka header 时 main.go 转 ms(spec 4.3 ingest_time_ms)。
	Handler func(ctx context.Context, raw []byte, samples []parser.Sample, defaultTopic string) error
	// ReadHeaderTimeout HTTP 头读取超时
	ReadHeaderTimeout time.Duration
	// MaxBodyBytes 限制请求体大小,默认 16MB
	MaxBodyBytes int64
}

// Server HTTP server 封装,便于测试用 httptest。
type Server struct {
	cfg     Config
	limiter *rate.Limiter
	http    *http.Server
	// rlCfg per-tenant 限流配置(T5.1);init 时给 defaultRPS 一个兜底
	rlCfg rateLimitConfig
}

// New 创建 receiver.Server。
func New(cfg Config) (*Server, error) {
	if cfg.Authenticator == nil {
		return nil, errors.New("receiver: Authenticator required")
	}
	if cfg.Logger == nil {
		return nil, errors.New("receiver: Logger required")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":19201"
	}
	if cfg.GlobalRateLimit <= 0 {
		cfg.GlobalRateLimit = 100_000
	}
	if cfg.ReadHeaderTimeout == 0 {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 16 * 1024 * 1024
	}

	s := &Server{
		cfg:     cfg,
		limiter: rate.NewLimiter(rate.Limit(cfg.GlobalRateLimit), cfg.GlobalRateLimit/2),
		rlCfg:   rateLimitConfig{defaultRPS: cfg.GlobalRateLimit},
	}
	s.http = &http.Server{
		Addr:              cfg.Addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	return s, nil
}

// ListenAndServe 阻塞监听;由 main.go 在 safego.Go 中调用。
func (s *Server) ListenAndServe() error { return s.http.ListenAndServe() }

// Shutdown 优雅停机。
func (s *Server) Shutdown(ctx context.Context) error { return s.http.Shutdown(ctx) }

// Addr 监听地址(若为 ":0" 可用于测试时取实际端口)。
func (s *Server) Addr() string { return s.http.Addr }

// Handler 返回底层 http.Handler,便于 httptest 与集成测试。
func (s *Server) Handler() http.Handler { return s.routes() }

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/write", s.handleWrite)
	return s.middleware(mux)
}

// middleware 串联通用中间件。
func (s *Server) middleware(next http.Handler) http.Handler {
	return s.recovererMW(s.requestIDMW(s.realIPMW(s.rateLimitMW(s.authMW(s.tenantRateLimitMW(next))))))
}

func (s *Server) requestIDMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-Id")
		if rid == "" {
			rid = generateRequestID()
		}
		w.Header().Set("X-Request-Id", rid)
		ctx := context.WithValue(r.Context(), ctxKeyRequestID{}, rid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) realIPMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		ctx := context.WithValue(r.Context(), ctxKeyRemoteIP{}, ip)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) recovererMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				obs.ErrorsTotal.WithLabelValues("receiver", "panic", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
				s.cfg.Logger.Error("handler panic",
					zap.Any("panic", rec),
					zap.String("path", r.URL.Path),
				)
				http.Error(w, `{"code":1500,"message":"internal error"}`, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) rateLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.limiter.Allow() {
			obs.BackpressureRejected.WithLabelValues("global_rl", s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusTooManyRequests, errorBody(4291, "rate limit exceeded"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractBearer(r.Header.Get("Authorization"))
		tenant, err := s.cfg.Authenticator.Verify(r.Context(), token)
		if err != nil {
			reason := classifyAuthError(err)
			obs.AuthFailTotal.WithLabelValues(reason, s.cfg.IngestCity, s.cfg.SourceDC).Inc()
			status := http.StatusUnauthorized
			if errors.Is(err, auth.ErrTokenRevoked) {
				status = http.StatusForbidden
			}
			s.cfg.Logger.Warn("auth failed",
				zap.String("reason", reason),
				zap.String("remote_ip", clientIP(r)),
			)
			writeJSON(w, status, errorBody(4001, "auth failed: "+reason))
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyTenant{}, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorBody(4003, "method not allowed"))
		return
	}
	start := time.Now()

	// 0. 链路追踪:从上游 traceparent 续 trace(若没有则开 root span)
	incomingTP := r.Header.Get("traceparent")
	ctx := tracex.ExtractTraceparent(r.Context(), incomingTP)
	ctx, span := tracex.StartSpan(ctx, "receive", "write")
	defer span.End()

	// spec 4.1: 解析 X-Source-DC 头(优先于 --source-dc 启动参数,允许 Prometheus external_labels 注入)
	// spec 4.1: 解析 X-Prometheus-Remote-Write-Version 头(协议版本协商,记录到日志便于排查兼容问题)
	sourceDC := s.cfg.SourceDC
	if headerDC := r.Header.Get("X-Source-DC"); headerDC != "" {
		sourceDC = headerDC
	}
	if rwVersion := r.Header.Get("X-Prometheus-Remote-Write-Version"); rwVersion != "" {
		span.SetAttributes(attribute.String("rw_version", rwVersion))
	}
	ingestCity := s.cfg.IngestCity
	span.SetAttributes(
		attribute.String("http.method", r.Method),
		attribute.String("http.path", r.URL.Path),
		attribute.String("source_dc", sourceDC),
		attribute.String("ingest_city", ingestCity),
	)

	// 1. 校验 Content-Type / Content-Encoding
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		obs.ErrorsTotal.WithLabelValues("decode", "content_type", ingestCity, sourceDC).Inc()
		writeJSON(w, http.StatusUnsupportedMediaType, errorBody(4004, "invalid content-type"))
		return
	}
	if r.Header.Get("Content-Encoding") != "snappy" {
		obs.ErrorsTotal.WithLabelValues("decode", "content_encoding", ingestCity, sourceDC).Inc()
		writeJSON(w, http.StatusUnsupportedMediaType, errorBody(4005, "invalid content-encoding"))
		return
	}

	// 2. 限长读 body
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	body, err := readAll(r.Body)
	if err != nil {
		obs.ErrorsTotal.WithLabelValues("decode", "body_read", ingestCity, sourceDC).Inc()
		writeJSON(w, http.StatusBadRequest, errorBody(4006, "body read: "+err.Error()))
		return
	}
	obs.BytesIn.WithLabelValues(tenantName(r.Context()), ingestCity, sourceDC).Add(float64(len(body)))
	span.SetAttributes(attribute.Int("http.body_bytes", len(body)))

	// 3. decode
	decodeCtx, decodeSpan := tracex.StartSpan(ctx, "decode", "snappy_protobuf")
	_ = decodeCtx
	req, err := decoder.Decode(body)
	if err != nil {
		decodeSpan.RecordError(err)
		decodeSpan.SetStatus(codes.Error, err.Error())
		decodeSpan.End()
		obs.ErrorsTotal.WithLabelValues("decode", decodeErrorType(err), ingestCity, sourceDC).Inc()
		s.cfg.Logger.Warn("decode failed", zap.Error(err))
		writeJSON(w, http.StatusBadRequest, errorBody(4007, "decode: "+err.Error()))
		return
	}
	decodeSpan.SetAttributes(attribute.Int("timeseries", len(req.Timeseries)))
	decodeSpan.End()
	obs.StageDuration.WithLabelValues("decode", "ok", ingestCity).Observe(time.Since(start).Seconds())

	// 4. 注入 Meta 到 ctx
	tenant := tenantFromCtx(r.Context())
	remoteIP, _ := r.Context().Value(ctxKeyRemoteIP{}).(string)
	meta := parser.Meta{
		Tenant:     tenant.Name,
		TenantID:   tenant.TenantID,
		SourceDC:   sourceDC,
		RemoteIP:   remoteIP,
		IngestTs:   time.Now().UnixNano(),
		TraceID:    tracex.TraceIDFromContext(ctx), // T1.12: 把当前 trace_id 记到 Meta 便于日志关联
		IngestCity: ingestCity,
	}
	ctx = parser.ContextWithMeta(ctx, meta)
	span.SetAttributes(
		attribute.String("tenant", meta.Tenant),
		attribute.String("trace_id", meta.TraceID),
	)

	// 5. parse
	parseCtx, parseSpan := tracex.StartSpan(ctx, "parse", "write_request")
	res, err := parser.Parse(parseCtx, req)
	if err != nil {
		// 只有 ErrMetaMissing 会到这里,视为内部 bug
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, err.Error())
		parseSpan.End()
		obs.ErrorsTotal.WithLabelValues("parse", "meta_missing", ingestCity, sourceDC).Inc()
		s.cfg.Logger.Error("parse failed (internal)", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, errorBody(5001, "internal: meta missing"))
		return
	}
	parseSpan.SetAttributes(
		attribute.Int("samples", len(res.Samples)),
		attribute.Int("parse_errors", int(res.ParseError)),
	)
	parseSpan.End()
	if res.ParseError > 0 {
		obs.ErrorsTotal.WithLabelValues("parse", "parse_series", ingestCity, sourceDC).Add(float64(res.ParseError))
	}
	obs.StageDuration.WithLabelValues("parse", "ok", ingestCity).Observe(time.Since(start).Seconds())
	obs.SamplesTotal.WithLabelValues("parse", meta.Tenant, "ok", ingestCity, sourceDC).Add(float64(len(res.Samples)))

	// 6. handler(由 main 注入: 通常是 rule engine + pipeline.Submit)
	if s.cfg.Handler != nil {
		if err := s.cfg.Handler(ctx, body, res.Samples, tenant.DefaultTopic); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			obs.ErrorsTotal.WithLabelValues("sink", "handler_error", ingestCity, sourceDC).Inc()
			s.cfg.Logger.Error("handler failed", zap.Error(err))
			writeJSON(w, http.StatusServiceUnavailable, errorBody(5031, "downstream unavailable"))
			return
		}
	}

	obs.RequestDuration.WithLabelValues("/api/v1/write", "ok", ingestCity).Observe(time.Since(start).Seconds())
	w.WriteHeader(http.StatusNoContent)
}

// --- helpers ---

type ctxKeyRequestID struct{}
type ctxKeyRemoteIP struct{}
type ctxKeyTenant struct{}

func tenantName(ctx context.Context) string {
	t, _ := ctx.Value(ctxKeyTenant{}).(auth.Tenant)
	if t.Name == "" {
		return "unknown"
	}
	return t.Name
}

func tenantFromCtx(ctx context.Context) auth.Tenant {
	t, _ := ctx.Value(ctxKeyTenant{}).(auth.Tenant)
	return t
}

func extractBearer(h string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

func clientIP(r *http.Request) string {
	// X-Forwarded-For 优先;否则 RemoteAddr
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func classifyAuthError(err error) string {
	switch {
	case errors.Is(err, auth.ErrTokenMissing):
		return "missing"
	case errors.Is(err, auth.ErrTokenExpired):
		return "expired"
	case errors.Is(err, auth.ErrTokenRevoked):
		return "revoked"
	default:
		return "invalid"
	}
}

func decodeErrorType(err error) string {
	var de *decoder.Error
	if errors.As(err, &de) {
		return de.Type
	}
	return "unknown"
}

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	const bufSize = 64 * 1024
	buf := make([]byte, 0, bufSize)
	tmp := make([]byte, bufSize)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			if errors.Is(err, http.ErrBodyReadAfterClose) || err.Error() == "EOF" {
				return buf, nil
			}
			return buf, err
		}
	}
}

var reqCounter atomic.Uint64

func generateRequestID() string {
	n := reqCounter.Add(1)
	return fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), n)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = encodeJSON(w, body)
}

func errorBody(code int, msg string) map[string]any {
	return map[string]any{
		"code":    code,
		"message": msg,
	}
}

// strconv 留作占位防止 unused
var _ = strconv.Itoa
