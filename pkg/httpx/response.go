// Package httpx - response.go: 统一 HTTP 响应包装 + 错误码。
//
// 设计要点(plan T4.4):
//   - 所有 API 响应统一为 Response{Code, Message, Data, TraceID} 结构体
//   - 公共错误码 1000-1999,业务码 4000-4999(GW)
//   - 业务码常量集中在本包,handler 仅引用常量
//   - JSON 编解码统一在 Write/WriteErr 内部,handler 不直接碰 json.Marshal
package httpx

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.opentelemetry.io/otel/trace"
)

// Response 统一响应结构体。
//
// 业务约定:
//   - Code=0 表示成功
//   - Code 非 0:Message 必填,Data 可选(便于客户端区分错误粒度)
//   - TraceID:从 ctx 拉取,无 trace 时为空
//
// 字段顺序与 JSON tag 顺序对齐,便于客户端按字段解析。
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// 公共错误码(plan T4.4: 1000-1999)。
const (
	CodeOK             = 0
	CodeBadRequest     = 1000 // 通用参数错误
	CodeUnauthorized   = 1001
	CodeForbidden      = 1002
	CodeNotFound       = 1003
	CodeConflict       = 1004
	CodeInternal       = 1500 // 通用 5xx
	CodeUnavailable    = 1501 // 503 语义
)

// GW 业务错误码(plan T4.4: 4000-4999)。
const (
	CodeAuthMissing     = 4001
	CodeAuthInvalid     = 4002
	CodeMethodNotAllowed = 4003
	CodeBadContentType   = 4004
	CodeBadContentEnc    = 4005
	CodeBodyRead         = 4006
	CodeDecodeFailed     = 4007

	CodeRuleSetNotFound    = 4101
	CodeRuleSetConflict    = 4102 // 同名 ruleset 已存在 / version 倒退
	CodeRuleSetInvalid     = 4103 // YAML / 编译失败
	CodeRuleSetVersionGone = 4104 // rollback 目标 version 不存在
	CodeRuleSetApplyFailed = 4105
)

// Write 写一个成功响应(200 OK)。
func Write(w http.ResponseWriter, r *http.Request, data any) {
	writeJSON(w, http.StatusOK, buildResponse(r, CodeOK, "ok", data))
}

// WriteStatus 写一个非 200 的成功响应(如 201 / 204)。
//
// 用法:admin 创建成功返回 201;recompile 重新生效返回 200。
func WriteStatus(w http.ResponseWriter, r *http.Request, httpStatus int, data any) {
	writeJSON(w, httpStatus, buildResponse(r, CodeOK, "ok", data))
}

// WriteErr 写一个错误响应(自动选 HTTP 状态码)。
//
// 优先级:httpStatus > 业务码默认映射。
// 业务码到 HTTP 状态码的默认映射:
//
//	1000-1999 → 400
//	4001       → 401
//	4002       → 401
//	4003       → 405
//	4101       → 404
//	4102       → 409
//	4103       → 400
//	4104       → 404
//	1500-1999  → 500
//	其他       → 500
func WriteErr(w http.ResponseWriter, r *http.Request, code int, msg string) {
	status := defaultHTTPStatus(code)
	writeJSON(w, status, buildResponse(r, code, msg, nil))
}

// WriteErrWithStatus 写一个错误响应(显式 HTTP 状态码,跳过默认映射)。
func WriteErrWithStatus(w http.ResponseWriter, r *http.Request, httpStatus, code int, msg string) {
	writeJSON(w, httpStatus, buildResponse(r, code, msg, nil))
}

func buildResponse(r *http.Request, code int, msg string, data any) Response {
	return Response{
		Code:    code,
		Message: msg,
		Data:    data,
		TraceID: traceIDFromRequest(r),
	}
}

func traceIDFromRequest(r *http.Request) string {
	// 优先 OTel span 上的 TraceID;其次 ctx;最后从 traceparent header 解析
	if r == nil {
		return ""
	}
	sc := trace.SpanContextFromContext(r.Context())
	if sc.HasTraceID() {
		return sc.TraceID().String()
	}
	if tid := tracex.TraceIDFromContext(r.Context()); tid != "" {
		return tid
	}
	return ""
}

func defaultHTTPStatus(code int) int {
	// 优先匹配精确码(避免被范围 case 抢先)
	switch {
	case code == CodeUnavailable:
		return http.StatusServiceUnavailable
	case code == CodeInternal:
		return http.StatusInternalServerError
	case code == CodeUnauthorized || code == CodeAuthMissing || code == CodeAuthInvalid:
		return http.StatusUnauthorized
	case code == CodeForbidden:
		return http.StatusForbidden
	case code == CodeNotFound || code == CodeRuleSetNotFound || code == CodeRuleSetVersionGone:
		return http.StatusNotFound
	case code == CodeConflict || code == CodeRuleSetConflict:
		return http.StatusConflict
	case code == CodeMethodNotAllowed:
		return http.StatusMethodNotAllowed
	case code == CodeBadRequest:
		return http.StatusBadRequest
	}
	// 范围兜底
	switch {
	case code >= 1000 && code < 1500:
		return http.StatusBadRequest
	case code >= 1500 && code < 2000:
		return http.StatusInternalServerError
	case code >= 4000 && code < 4200:
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// Sentinel 错误,handler 包装业务错误时携带,便于中间件提取 code。
type Sentinel struct {
	Code    int
	Message string
}

func (e *Sentinel) Error() string { return e.Message }

func NewSentinel(code int, msg string) error { return &Sentinel{Code: code, Message: msg} }

// AsSentinel 尝试从 err 中提取 *Sentinel;失败时回退到 1500 internal。
func AsSentinel(err error) *Sentinel {
	if err == nil {
		return nil
	}
	var s *Sentinel
	if errors.As(err, &s) {
		return s
	}
	return &Sentinel{Code: CodeInternal, Message: err.Error()}
}
