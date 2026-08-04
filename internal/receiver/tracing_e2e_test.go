// 端到端 tracing 测试(T1.12):验证 traceparent 从 HTTP 入口贯穿到 sink。
//
// 测试目标:
//   - 客户端在请求头传入 traceparent 时,所有 stage( receiver / decode / parse /
//     rule / pipeline / sink )的 span 共享同一 traceID
//   - handler 调用 tracex.InjectTraceparent 把当前 trace 写进 Kafka message header
//   - 客户端不传 traceparent 时,接收端开 root span,后续 stage 全部挂在它下面
//   - 错误路径(handler 失败)不会让 trace 中断:接收端仍能 RecordError
//
// 覆盖点(全链路拓扑):
//
//	traceparent → receive.write → decode.snappy_protobuf
//	                          → parse.write_request
//	                          → handler(InjectTraceparent) → rule.process
//	                          → pipeline.submit → sink.send
package receiver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

// captureSink 内存 sink,用于断言"最终下游收到了什么"。
//
// Send 是同步的;不持有 ctx 取消语义,只把消息丢到带锁切片里。
type captureSink struct {
	mu   sync.Mutex
	msgs []sink.Message
}

func (c *captureSink) Send(_ context.Context, m sink.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, m)
	return nil
}

func (c *captureSink) Close() error { return nil }

func (c *captureSink) snapshot() []sink.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]sink.Message, len(c.msgs))
	copy(out, c.msgs)
	return out
}

// tracingE2ESetup 构造一个最小的"接收端 → 规则引擎 → pipeline → 假 sink"栈,
// 并挂上内存 SpanRecorder,方便断言 spans 拓扑。
type tracingE2ESetup struct {
	recorder *tracetest.SpanRecorder
	sink     *captureSink
	pipeline *sink.Pipeline
	rule     *ruleengine.Pipeline
	server   *Server
}

func newTracingE2ESetup(t *testing.T) *tracingE2ESetup {
	t.Helper()

	// 1. 装测试 tracer:全局 + 同步覆盖 obs.Tracer(因为 tracex.StartSpan 用的是后者)
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	prevTracer := obs.Tracer
	obs.Tracer = tp.Tracer("prom-gw")
	t.Cleanup(func() {
		obs.Tracer = prevTracer
		_ = tp.Shutdown(context.Background())
	})

	// 2. 假 sink
	cs := &captureSink{}

	// 3. sink.Pipeline(单 worker 串行,异步)
	pipe := sink.NewPipeline(sink.PipelineConfig{
		BufferSize: 1024,
		Logger:     zap.NewNop(),
	}, cs)
	pipe.Start()
	t.Cleanup(pipe.Stop)

	// 4. rule engine(空规则,透传)
	rule := ruleengine.NewPipeline(zap.NewNop(), pipe.Submit)

	// 5. 鉴权 stub
	a := &stubAuth{tokens: map[string]auth.Tenant{
		"good": {Name: "t1", DefaultTopic: "prom.raw.t1", RateLimit: 100000},
	}}

	// 6. receiver(handler 走 rule.Process,并注入 traceparent)
	srv, err := New(Config{
		Authenticator: a,
		Logger:        zap.NewNop(),
		SourceDC:      "dc-test",
		Handler: func(ctx context.Context, raw []byte, samples []parser.Sample, defaultTopic string) error {
			meta, _ := parser.MetaFromContext(ctx)
			headers := map[string]string{
				"tenant":    meta.Tenant,
				"source_dc": meta.SourceDC,
			}
			// 与 main.go 一致:InjectTraceparent 把当前 trace 写进 header
			tracex.InjectTraceparent(ctx, headers)
			msg := sink.Message{
				Topic:   defaultTopic,
				Key:     []byte(meta.Tenant),
				Headers: headers,
			}
			return rule.Process(ctx, samples, raw, msg)
		},
	})
	require.NoError(t, err)

	return &tracingE2ESetup{
		recorder: rec,
		sink:     cs,
		pipeline: pipe,
		rule:     rule,
		server:   srv,
	}
}

// findSpanByName 在 recorder 里找指定 name 的 span(测试用,顺序不一定)。
func findSpanByName(ended []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, sp := range ended {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

// waitForSpan 轮询直到 recorder 收到至少 n 条 span,避免时序 flake。
func waitForSpan(t *testing.T, rec *tracetest.SpanRecorder, n int, timeout time.Duration) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := rec.Ended()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 span 超时: 期望 >=%d, 实际 %d", n, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func writeRequest() *prompb.WriteRequest {
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			},
		},
	}
}

func TestE2E_Tracing_UpstreamTraceparent_PropagatesAllStages(t *testing.T) {
	setup := newTracingE2ESetup(t)

	// 1. 客户端在 OTel SDK 里开一个 root span,inject 出 traceparent,再发出请求
	upstreamCtx, upstreamSpan := otel.Tracer("client").Start(context.Background(), "client.test")
	upstreamTID := upstreamSpan.SpanContext().TraceID().String()
	upstreamSpan.End()

	// 用项目的 tracex.HeaderCarrier(map[string]string),避免 http.Header 对 key 的大小写规范化。
	clientHeaders := tracex.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(upstreamCtx, clientHeaders)
	require.NotEmpty(t, clientHeaders["traceparent"], "客户端 inject 必须填 traceparent: %v", clientHeaders)
	clientTP := clientHeaders["traceparent"]

	// 2. 发请求(把第一个值取出来传给 doRequest)
	w := doRequestWithHeaders(t, setup.server, "good", map[string]string{"traceparent": clientTP},
		encodeWriteRequest(writeRequest()))
	require.Equal(t, http.StatusNoContent, w.Code, "端到端必须 204")

	// 3. 等待 worker 把消息投递到 sink
	msgs := waitForMessages(t, setup.sink, 1, 2*time.Second)
	require.Len(t, msgs, 1)
	require.NotEmpty(t, msgs[0].Headers["traceparent"], "handler 必须 InjectTraceparent")

	// 4. message header 的 traceparent traceID 必须等于客户端的 traceID
	headerTID := traceIDFromTraceparent(msgs[0].Headers["traceparent"])
	assert.Equal(t, upstreamTID, headerTID,
		"message header 的 traceparent traceID 必须等于客户端的 traceID")

	// 5. 验证 spans:6 个 stage 全在,共享同一 traceID
	ended := waitForSpan(t, setup.recorder, 6, 2*time.Second)

	spanNames := make([]string, 0, len(ended))
	for _, sp := range ended {
		spanNames = append(spanNames, sp.Name())
	}
	wantStages := []string{
		"gw.receive.write",
		"gw.decode.snappy_protobuf",
		"gw.parse.write_request",
		"gw.rule.process",
		"gw.pipeline.submit",
		"gw.sink.send",
	}
	for _, want := range wantStages {
		assert.NotNil(t, findSpanByName(ended, want), "必须捕获到 span: %s", want)
	}

	// 所有 span 共享同一 traceID(因为客户端传了 traceparent 进来)
	for _, sp := range ended {
		assert.Equal(t, upstreamTID, sp.SpanContext().TraceID().String(),
			"span %q 的 traceID 必须等于客户端 traceID", sp.Name())
	}

	// 6. 父子关系:receive 是 root,rule/pipeline/sink 都是它的后代
	receiveSpan := findSpanByName(ended, "gw.receive.write")
	require.NotNil(t, receiveSpan)
	for _, name := range []string{"gw.decode.snappy_protobuf", "gw.parse.write_request"} {
		child := findSpanByName(ended, name)
		require.NotNil(t, child)
		assert.Equal(t, receiveSpan.SpanContext().SpanID(), child.Parent().SpanID(),
			"%s 必须是 receive 的 child", name)
	}
	// pipeline/sink 是 rule 的孙 / 子
	ruleSpan := findSpanByName(ended, "gw.rule.process")
	require.NotNil(t, ruleSpan)
	pipelineSpan := findSpanByName(ended, "gw.pipeline.submit")
	require.NotNil(t, pipelineSpan)
	assert.Equal(t, ruleSpan.SpanContext().SpanID(), pipelineSpan.Parent().SpanID(),
		"pipeline 必须是 rule 的 child")
}

func TestE2E_Tracing_NoUpstreamTraceparent_StartsRoot(t *testing.T) {
	setup := newTracingE2ESetup(t)

	// 1. 不传 traceparent,服务端应自开 root span
	w := doRequestWithHeaders(t, setup.server, "good", nil,
		encodeWriteRequest(writeRequest()))
	require.Equal(t, http.StatusNoContent, w.Code)

	// 2. 等待消息落地
	msgs := waitForMessages(t, setup.sink, 1, 2*time.Second)
	require.Len(t, msgs, 1)
	require.NotEmpty(t, msgs[0].Headers["traceparent"], "服务端自开 root 后 Inject 也要有 traceparent")

	// 3. 解析出 message header 的 traceID,所有 span 必须等于这个 traceID
	rootTID := traceIDFromTraceparent(msgs[0].Headers["traceparent"])
	assert.Len(t, rootTID, 32, "traceID 必须是 32 hex")

	ended := waitForSpan(t, setup.recorder, 6, 2*time.Second)
	for _, sp := range ended {
		assert.Equal(t, rootTID, sp.SpanContext().TraceID().String(),
			"无上游 trace 时所有 span 共享 receiver 开出的 root traceID")
	}
}

func TestE2E_Tracing_HandlerError_RecordsError(t *testing.T) {
	// 单独搭一个"handler 主动返回 error"的栈,验证 receive span 仍能 RecordError 并标 Error。
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	prevTracer := obs.Tracer
	obs.Tracer = tp.Tracer("prom-gw")
	t.Cleanup(func() {
		obs.Tracer = prevTracer
		_ = tp.Shutdown(context.Background())
	})

	a := &stubAuth{tokens: map[string]auth.Tenant{
		"good": {Name: "t1", DefaultTopic: "t1", RateLimit: 100000},
	}}
	srv, err := New(Config{
		Authenticator: a,
		Logger:        zap.NewNop(),
		SourceDC:      "dc-test",
		Handler: func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error {
			return assert.AnError
		},
	})
	require.NoError(t, err)

	w := doRequestWithHeaders(t, srv, "good", nil,
		encodeWriteRequest(writeRequest()))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)

	// handler 失败时,server.go 内部对 receive span RecordError + SetStatus(Error);
	// 验证 span.Status().Code 非 Unset 即可。
	ended := waitForSpan(t, rec, 1, 2*time.Second)
	receive := findSpanByName(ended, "gw.receive.write")
	require.NotNil(t, receive)
	assert.NotEqual(t, uint32(0), uint32(receive.Status().Code),
		"handler 失败时 receive span 的 Status.Code 必须非 Unset")
}

// --- helpers ---

// doRequestWithHeaders 与 server_test 里的 doRequest 几乎一样,只是允许自定义
// headers(给客户端用 traceparent 注入)。
func doRequestWithHeaders(t *testing.T, s *Server, token string, headers map[string]string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("Content-Encoding", "snappy")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	s.routes().ServeHTTP(w, r)
	return w
}

// waitForMessages 轮询 captureSink 直至收到 n 条。
func waitForMessages(t *testing.T, s *captureSink, n int, timeout time.Duration) []sink.Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := s.snapshot()
		if len(got) >= n {
			return got[:n]
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 sink 消息超时: 期望 >=%d, 实际 %d", n, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// traceIDFromTraceparent 解析 W3C traceparent 字符串取 traceID。
//
// W3C trace context 格式: "00-{traceID(32hex)}-{spanID(16hex)}-{flags(2hex)}"
// 拿第二段(traceID,共 32 hex 字符)。
func traceIDFromTraceparent(tp string) string {
	if len(tp) < 55 {
		return ""
	}
	return tp[3:35]
}
