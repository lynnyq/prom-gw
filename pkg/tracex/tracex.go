// Package tracex 封装 OpenTelemetry tracer 初始化与 span context 透传工具。
//
// 设计要点(plan T1.12):
//   - TraceID 走 request-scoped context,不进 Sample(避免 string 分配压力)
//   - 在 Kafka message header 中以 `traceparent` 传递(W3C trace context 规范)
//   - 接收端从 traceparent header 解析 → 注入 ctx → 各 stage 透传
package tracex

import (
	"context"
	"fmt"

	"github.com/lynnyq/prom-gw/internal/obs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// HeaderCarrier 基于 map[string]string 的 TextMapCarrier(用于 W3C trace context 序列化)。
//
// 接收端用 IncomingTraceparent 解析上游 traceparent;
// 发送端用 InjectTraceparent 注入到 Kafka message header。
type HeaderCarrier map[string]string

// Get 实现 propagation.TextMapCarrier。
func (h HeaderCarrier) Get(key string) string { return h[key] }

// Set 实现 propagation.TextMapCarrier。
func (h HeaderCarrier) Set(key, value string) { h[key] = value }

// Keys 实现 propagation.TextMapCarrier。
func (h HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// IncomingTraceparent 从 header 中解析 traceparent,返回 traceparent 字符串与有效标志。
//
// 如果 header 中没有 traceparent,返回 ("", false) — 调用方应决定是否新建 root span。
func IncomingTraceparent(headers map[string]string) (string, bool) {
	tp, ok := headers["traceparent"]
	return tp, ok && tp != ""
}

// InjectTraceparent 从 ctx 提取当前 span context 并序列化为 traceparent,放入 headers。
//
// 行为:用全局 TextMapPropagator(由 obs.InitTracing 设置 W3C trace context)注入。
// 当 ctx 无 active span 时,headers 不被修改。
func InjectTraceparent(ctx context.Context, headers map[string]string) {
	if headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(headers))
}

// ExtractTraceparent 从 traceparent 字符串还原 ctx,用于接收端续 trace。
//
// 返回的 ctx 已包含远程 SpanContext,后续 obs.Tracer.Start 会作为 child span 挂到上游。
func ExtractTraceparent(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := HeaderCarrier{"traceparent": traceparent}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// TraceIDFromContext 从 ctx 提取当前 span 的 traceID(32 hex 字符串)。
//
// 用途:Kafka message header `traceparent` 注入 / 日志 / metric labels。
// 当 ctx 无 active span 或 tracing 未启用时,返回空字符串。
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanIDFromContext 从 ctx 提取当前 span 的 spanID(16 hex 字符串)。
func SpanIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// StartSpan 是 obs.Tracer.Start 的便捷封装,自动加 stage 属性。
//
// stage 取值: receive / decode / parse / rule / pipeline / sink。
func StartSpan(ctx context.Context, stage, op string) (context.Context, trace.Span) {
	return obs.Tracer.Start(ctx, fmt.Sprintf("gw.%s.%s", stage, op),
		trace.WithAttributes(
			attribute.String("stage", stage),
			attribute.String("op", op),
		),
	)
}

// EndSpan 结束 span 并在 err != nil 时记录错误。
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
