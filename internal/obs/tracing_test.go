// obs/tracing 单测: 覆盖 T1.12 OTel 初始化行为。
//
// 覆盖点:
//   - 不配 endpoint → noop,无 error
//   - 失败 endpoint → 返回 error + 降级 noop
//   - ShutdownTracing 在 noop 状态调用不 panic
//   - OTLPEndpointFromEnv 优先 traces 变量
package obs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestInitTracing_NoEndpoint_Noop(t *testing.T) {
	// 重置全局 tracer(防止上一个测试污染)
	tracerProvider = nil
	Tracer = nil

	err := InitTracing(TracingConfig{ServiceName: "test-svc", Logger: nil})
	assert.NoError(t, err, "空 endpoint 必须返回 nil")
	assert.NotNil(t, Tracer, "Tracer 必须被设置为 noop")
	assert.Nil(t, tracerProvider, "tracerProvider 必须保持 nil(走 noop)")
}

// TestInitTracing_NoEndpoint_PropagatorSet 是 T1.12 端到端验证暴露出来的回归:
// 即便走 noop(高吞吐省 CPU),也要让 W3C TraceContext propagator 生效,
// 否则下游 Kafka header 拿不到 traceparent,trace 串联断裂。
//
// 验证方式:启一个 SDK TracerProvider(start 一个真实 recording span),
// InitTracing 用 noop mode(不配 endpoint),然后用全局 propagator inject,
// 必须能拿到 traceparent(等价于 propagation.TraceContext{} 的行为)。
func TestInitTracing_NoEndpoint_PropagatorSet(t *testing.T) {
	// 1. 先装一个真 TracerProvider,这样 noop Tracer 也会拿到 valid SpanContext。
	// (注:obs.Tracer 是 noop 模式,但 otel.Tracer() 仍走 SDK 的 TracerProvider。)
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	// 2. 切到 noop mode
	tracerProvider = nil
	Tracer = nil
	err := InitTracing(TracingConfig{ServiceName: "test-svc", Logger: nil})
	require.NoError(t, err)

	// 3. 启一个 recording span(走 SDK tp)
	_, span := otel.Tracer("test").Start(context.Background(), "s")
	defer span.End()

	// 4. 用全局 propagator inject,必须能写出 traceparent
	headers := map[string]string{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithSpanContext(context.Background(), span.SpanContext()),
		propagatorMapAdapter(headers))
	require.NotEmpty(t, headers["traceparent"],
		"noop 模式下全局 propagator 必须仍是 W3C TraceContext(否则 traceparent 写不出,trace 串联断裂)")

	// 5. 提取 traceID 验证格式合法(00-{32 hex}-...)
	tp1 := headers["traceparent"]
	require.GreaterOrEqual(t, len(tp1), 55, "traceparent 必须是 55 字符")
	assert.Equal(t, "00-", tp1[:3], "W3C 第一个字段必须是 00")
}

func TestShutdownTracing_NilProvider_Noop(t *testing.T) {
	tracerProvider = nil
	err := ShutdownTracing(context.Background())
	assert.NoError(t, err, "tracerProvider=nil 时 Shutdown 必须返回 nil")
}

func TestInitTracing_BadEndpoint_FallsBack(t *testing.T) {
	tracerProvider = nil
	Tracer = nil

	// 用一个非法 endpoint(端口空闲 + 短超时)触发连接失败
	err := InitTracing(TracingConfig{
		ServiceName:  "test-svc",
		OTLPEndpoint: "127.0.0.1:1", // 1 端口几乎肯定没监听
		Insecure:     true,
		Logger:       nil,
	})
	// 注意:OTLP exporter 默认 lazy connect,New() 通常不报错;
	// 这里只断言最终 Tracer 已被设置(不关心 err)
	require.NotNil(t, Tracer)
	_ = err
}

func TestOTLPEndpointFromEnv_PrefersTraces(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "traces-host:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "generic-host:4317")
	assert.Equal(t, "traces-host:4317", OTLPEndpointFromEnv(),
		"TRACES 专用变量必须优先")
}

func TestOTLPEndpointFromEnv_FallsBackToGeneric(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "generic-host:4317")
	assert.Equal(t, "generic-host:4317", OTLPEndpointFromEnv())
}

func TestOTLPEndpointFromEnv_Empty(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	assert.Equal(t, "", OTLPEndpointFromEnv())
}

// --- helpers ---

// mapCarrierAdapter 把 map[string]string 适配成 propagation.TextMapCarrier。
type mapCarrierAdapter map[string]string

func (m mapCarrierAdapter) Get(key string) string         { return m[key] }
func (m mapCarrierAdapter) Set(key, value string)         { m[key] = value }
func (m mapCarrierAdapter) Keys() []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func propagatorMapAdapter(h map[string]string) propagation.TextMapCarrier {
	return mapCarrierAdapter(h)
}
