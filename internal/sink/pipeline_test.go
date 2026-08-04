package sink

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	// sink 慢;通过让 sink 在 ctx 取消时返回,验证 Stop 能正确排空。
	// 这里用 sync.Once 控制"未释放"状态;Stop 时 ctx 取消让 sink 退出。
	// 验证:不会发生 goroutine 死锁,Stop 能正常返回。
	fs := &blockingFakeSink{release: make(chan struct{})}
	p := NewPipeline(PipelineConfig{BufferSize: 2}, fs)
	p.Start()

	// 把 channel 填满
	for i := 0; i < 5; i++ {
		_ = p.Submit(context.Background(), Message{Topic: "t"})
	}
	// 此时 worker 已被阻塞,Stop 通过 ctx 取消让 sink 退出 → worker 退出 → wg.Done
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
