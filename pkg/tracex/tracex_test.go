// tracex 单测: 覆盖 T1.12 trace 注入 / 提取 / 序列化工具。
//
// 覆盖点:
//   - ExtractTraceparent 续 trace(空字符串不改变 ctx)
//   - InjectTraceparent 把当前 span context 写回 header
//   - InjectTraceparent nil headers 不 panic
//   - TraceIDFromContext / SpanIDFromContext 在无 active span 时返回空
//   - HeaderCarrier 实现 propagation.TextMapCarrier
package tracex

import (
	"context"
	"testing"

	"github.com/lynnyq/prom-gw/internal/obs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setupTestTracer(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// StartSpan / EndSpan 走 obs.Tracer(全局单例),这里要同步覆盖,否则 span
	// 仍发到旧的全局 noop,recorder 收不到。
	prev := obs.Tracer
	obs.Tracer = tp.Tracer("prom-gw")
	t.Cleanup(func() {
		obs.Tracer = prev
		_ = tp.Shutdown(context.Background())
	})
	return rec
}

func TestExtractTraceparent_Empty_NoChange(t *testing.T) {
	_ = setupTestTracer(t)
	ctx := context.Background()
	out := ExtractTraceparent(ctx, "")
	assert.Equal(t, ctx, out, "空 traceparent 必须原样返回 ctx")
}

func TestExtractTraceparent_RoundTrip(t *testing.T) {
	_ = setupTestTracer(t)
	ctx := context.Background()
	_, span := otel.Tracer("test").Start(ctx, "root")
	tid := span.SpanContext().TraceID().String()
	span.End()

	// 用 inject 拿到 traceparent 字符串
	headers := map[string]string{}
	otel.GetTextMapPropagator().Inject(traceContextWithSpan(ctx, span), HeaderCarrier(headers))
	require.NotEmpty(t, headers["traceparent"], "OTel inject 必须填 traceparent")

	// 用 ExtractTraceparent 续 trace
	childCtx := ExtractTraceparent(context.Background(), headers["traceparent"])
	_, child := otel.Tracer("test").Start(childCtx, "child")
	defer child.End()

	// child 的 TraceID 必须等于 root
	assert.Equal(t, tid, child.SpanContext().TraceID().String(),
		"续 trace 后 child.TraceID == root.TraceID")
}

// traceContextWithSpan 把一个 SpanContext 包装到 ctx(用于 OTel inject)。
//
// 旧 API 有 SpanContext.WithContext,但新版本改用 trace.ContextWithSpanContext。
func traceContextWithSpan(ctx context.Context, sc trace.Span) context.Context {
	return trace.ContextWithSpanContext(ctx, sc.SpanContext())
}

func TestInjectTraceparent_NilHeaders_NoPanic(t *testing.T) {
	_ = setupTestTracer(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "s")
	defer span.End()
	assert.NotPanics(t, func() { InjectTraceparent(ctx, nil) })
}

func TestInjectTraceparent_FillsHeader(t *testing.T) {
	_ = setupTestTracer(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	headers := map[string]string{}
	InjectTraceparent(ctx, headers)
	assert.NotEmpty(t, headers["traceparent"], "有 active span 时必须填 traceparent")
}

func TestTraceIDFromContext_NoSpan(t *testing.T) {
	_ = setupTestTracer(t)
	assert.Equal(t, "", TraceIDFromContext(context.Background()))
	assert.Equal(t, "", SpanIDFromContext(context.Background()))
}

func TestTraceIDFromContext_WithSpan(t *testing.T) {
	_ = setupTestTracer(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	tid := TraceIDFromContext(ctx)
	sid := SpanIDFromContext(ctx)
	assert.NotEmpty(t, tid, "有 active span 时必须返回 traceID")
	assert.NotEmpty(t, sid, "有 active span 时必须返回 spanID")
	assert.True(t, isHex(tid, 32), "traceID 必须是 32 hex 字符")
	assert.True(t, isHex(sid, 16), "spanID 必须是 16 hex 字符")
}

func TestHeaderCarrier_TextMapCarrierInterface(t *testing.T) {
	c := HeaderCarrier{"a": "1", "b": "2"}
	assert.Equal(t, "1", c.Get("a"))
	assert.Equal(t, "", c.Get("missing"))

	c.Set("c", "3")
	assert.Equal(t, "3", c.Get("c"))

	keys := c.Keys()
	assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}

func TestStartSpan_PropagatesContext(t *testing.T) {
	rec := setupTestTracer(t)
	_, span := StartSpan(context.Background(), "test", "op")
	span.End()

	// span 必须已启动并被 recorder 捕获
	ended := rec.Ended()
	require.NotEmpty(t, ended)
	gotAttrs := map[string]string{}
	for _, kv := range ended[0].Attributes() {
		gotAttrs[string(kv.Key)] = kv.Value.Emit()
	}
	assert.Equal(t, "test", gotAttrs["stage"])
	assert.Equal(t, "op", gotAttrs["op"])
}

func TestEndSpan_RecordsError(t *testing.T) {
	rec := setupTestTracer(t)
	_, span := StartSpan(context.Background(), "test", "op")

	EndSpan(span, assertAnError{})
	ended := rec.Ended()
	require.NotEmpty(t, ended)
	// 验证 status = Error
	assert.Equal(t, codes.Error, ended[0].Status().Code, "EndSpan(err) 必须设置 Error status")
}

// --- helpers ---

type assertAnError struct{}

func (assertAnError) Error() string { return "sentinel" }

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
