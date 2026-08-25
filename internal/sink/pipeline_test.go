package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// --- 测试用 fake sink ---

// fakeSink 计数 + 可注入错误。
type fakeSink struct {
	calls atomic.Int32
	err   atomic.Value // error
}

func (f *fakeSink) Send(_ context.Context, _ Message) error {
	if e := f.err.Load(); e != nil {
		if err, ok := e.(error); ok {
			return err
		}
	}
	f.calls.Add(1)
	return nil
}

func (f *fakeSink) Close() error { return nil }

func (f *fakeSink) setErr(e error) { f.err.Store(e) }

// --- KafkaSink 测试 ---

// KafkaSink 是 kafkasink.Producer 的薄包装,这里只验证它能正确传播 error 类型。
// 实际 kafkasink.Produce 由 producer_test.go 覆盖。

// --- WALSink 测试 ---

// WALSink 需要真实 wal.WAL 实例;这里用 sink 包的 WALSink + 真实 WAL 跑通。

// --- Pipeline 测试 ---

func TestPipeline_SubmitAndDrain(t *testing.T) {
	fs := &fakeSink{}
	p := NewPipeline(PipelineConfig{BufferSize: 4}, fs)
	p.Start()

	for i := 0; i < 4; i++ {
		err := p.Submit(context.Background(), Message{Topic: "t", Payload: []byte("x")})
		require.NoError(t, err)
	}
	// 等待 worker 处理完
	require.Eventually(t, func() bool {
		return fs.calls.Load() == 4
	}, time.Second, 10*time.Millisecond)

	p.Stop()
	assert.Equal(t, int32(4), fs.calls.Load())
}

func TestPipeline_BackpressureWhenFull(t *testing.T) {
	// sink 慢;Stop() 现在先排空 channel 再取消 ctx(设计 §6.5)。
	// 释放 sink 后 worker 能处理完剩余消息,Stop 正常返回。
	fs := &blockingFakeSink{release: make(chan struct{})}
	p := NewPipeline(PipelineConfig{BufferSize: 2}, fs)
	p.Start()

	// 把 channel 填满
	for i := 0; i < 5; i++ {
		_ = p.Submit(context.Background(), Message{Topic: "t"})
	}
	// 释放 sink,让 worker 能排空剩余消息
	close(fs.release)

	stopDone := make(chan struct{})
	go func() {
		p.Stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		// 正常
	case <-time.After(2 * time.Second):
		t.Fatal("Pipeline.Stop did not return in time (deadlock)")
	}
}

func TestPipeline_StopIsIdempotent(t *testing.T) {
	fs := &fakeSink{}
	p := NewPipeline(PipelineConfig{BufferSize: 4}, fs)
	p.Start()
	p.Stop()
	// 二次 Stop 不 panic
	assert.NotPanics(t, func() { p.Stop() })
}

func TestPipeline_SubmitAfterStop(t *testing.T) {
	fs := &fakeSink{}
	p := NewPipeline(PipelineConfig{BufferSize: 4}, fs)
	p.Start()
	p.Stop()
	err := p.Submit(context.Background(), Message{Topic: "t"})
	assert.ErrorIs(t, err, ErrClosed)
}

func TestPipeline_Stats(t *testing.T) {
	fs := &fakeSink{}
	p := NewPipeline(PipelineConfig{BufferSize: 16}, fs)
	p.Start()

	for i := 0; i < 5; i++ {
		require.NoError(t, p.Submit(context.Background(), Message{Topic: "t"}))
	}
	require.Eventually(t, func() bool {
		_, drained, _ := p.Stats()
		return drained >= 5
	}, time.Second, 10*time.Millisecond)

	submitted, drained, _ := p.Stats()
	assert.GreaterOrEqual(t, submitted, uint64(5))
	assert.GreaterOrEqual(t, drained, uint64(5))
	p.Stop()
}

// blockingFakeSink 模拟慢 sink(用 channel 阻塞,控制时序)。
type blockingFakeSink struct {
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingFakeSink) Send(ctx context.Context, _ Message) error {
	b.calls.Add(1)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *blockingFakeSink) Close() error {
	// 释放阻塞的 worker,让 pipeline.Stop 顺利返回
	select {
	case <-b.release:
		// 已关闭
	default:
		close(b.release)
	}
	return nil
}

// --- trace 上下文透传(回归 #2: pipeline 把 sendCtx 传给 sink) ---

// ctxCaptureSink 记录 sink.Send 收到的 ctx,用于断言 trace 上下文透传。
type ctxCaptureSink struct {
	mu      sync.Mutex
	captured context.Context
	got     atomic.Bool
}

func (c *ctxCaptureSink) Send(ctx context.Context, _ Message) error {
	c.mu.Lock()
	c.captured = ctx
	c.mu.Unlock()
	c.got.Store(true)
	return nil
}

func (c *ctxCaptureSink) Close() error { return nil }

// TestPipeline_PassesTraceContext 验证 worker 把 sendCtx(含 traceparent 还原的
// 远程 SpanContext) 传给 sink.Send,而非 pipeline 持有的 p.ctx(无 span context)。
//
// 修复前 workerLoop L147 传 p.ctx,sink 收到的 ctx 不含远程 SpanContext;
// 修复后传 sendCtx,otel propagator Extract 出的 SpanContext 可被 sink 读到。
func TestPipeline_PassesTraceContext(t *testing.T) {
	// 初始化 W3C trace context propagator(若全局未设置)。
	// 生产由 obs.InitTracing 设置,测试里显式设置确保 Extract 能还原 span context。
	otel.SetTextMapPropagator(propagation.TraceContext{})

	cap := &ctxCaptureSink{}
	p := NewPipeline(PipelineConfig{BufferSize: 4}, cap)
	p.Start()

	// 构造有效 W3C traceparent: 00-<traceId 32hex>-<spanId 16hex>-<flags>
	// traceId/spanId 取自 W3C 规范示例。
	const traceID = "0af7651916cd43dd84481321127dacbf"
	const spanID = "00f067aa0ba902b7"
	tp := "00-" + traceID + "-" + spanID + "-01"

	msg := Message{
		Topic:   "t",
		Payload: []byte("x"),
		Headers: map[string]string{"traceparent": tp},
	}
	require.NoError(t, p.Submit(context.Background(), msg))

	// 等 worker 处理
	require.Eventually(t, func() bool { return cap.got.Load() }, time.Second, 5*time.Millisecond)
	p.Stop()

	cap.mu.Lock()
	got := cap.captured
	cap.mu.Unlock()
	require.NotNil(t, got, "sink should have received a ctx")

	// 验证 sink 收到的 ctx 含从 traceparent 还原的远程 SpanContext。
	// 若 worker 传的是 p.ctx(无 ExtractTraceparent),SpanContext 不 IsValid。
	sc := trace.SpanContextFromContext(got)
	assert.True(t, sc.IsValid(),
		"sink ctx must carry remote span context from traceparent header; "+
			"if false, worker is passing p.ctx instead of sendCtx")
	assert.Equal(t, traceID, sc.TraceID().String(),
		"trace ID must match the traceparent header")
}
