// Package admin - codes.go: Admin API 业务错误码。
//
// 错误码与 internal/admin handlers 共享,直接使用 pkg/httpx.CodeRuleSet* 常量;
// 本文件提供 admin 专用 message 模板(便于 handler 复用)。
package admin

// Admin 错误消息模板(集中维护,避免散落)。
const (
	MsgRuleSetNotFound     = "ruleset not found"
	MsgRuleSetConflict     = "ruleset name or version conflict"
	MsgRuleSetInvalid      = "ruleset yaml or stage invalid"
	MsgRuleSetVersionGone  = "target version not in history"
	MsgRuleSetApplyFailed  = "ruleset apply failed"
	MsgBadRequest          = "bad request"
	MsgInternal            = "internal error"
	MsgSourceUnavailable   = "config source unavailable"
	MsgAuthzForbidden      = "source ip not in allowlist"
)
