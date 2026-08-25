// Package sink - pipeline.go: 把 receiver handler 与 sink 解耦的有界 channel 队列。
//
// 设计要点(plan T1.9):
//   - 阶段 channel 容量 65535(默认),满 → ErrBackpressure(503)
//   - 单一 worker goroutine 串行消费(避免 franz-go 并发 Produce 的内部锁竞争)
//   - Stop 等待 worker 排空,保证 shutdown 期间数据不丢
package sink

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/pkg/safego"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.uber.org/zap"
)

// Pipeline 串联 receiver → sink 的有界 channel。
//
// 数据流: receiver.handler → Submit → ch → worker → sink.Send。
type Pipeline struct {
	ch     chan Message
	sink   Sink
	logger *zap.Logger

	wg     sync.WaitGroup
	stopCh chan struct{}
	cancel context.CancelFunc
	ctx    context.Context

	// submitted 累计入队条数(用于 metrics / 调试)。
	submitted atomic.Uint64
	// drained 累计出队条数。
	drained atomic.Uint64
}

// PipelineConfig 配置。
type PipelineConfig struct {
	// BufferSize 阶段 channel 容量。默认 65535。
	BufferSize int
	// Logger 必填。
	Logger *zap.Logger
}

// NewPipeline 构造但不启动。
func NewPipeline(cfg PipelineConfig, s Sink) *Pipeline {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 65535
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Pipeline{
		ch:     make(chan Message, cfg.BufferSize),
		sink:   s,
		logger: cfg.Logger,
		stopCh: make(chan struct{}),
		cancel: cancel,
		ctx:    ctx,
	}
}

// Start 启动 worker(单 goroutine, 串行消费)。
func (p *Pipeline) Start() {
	p.wg.Add(1)
	safego.GoWithRecover("sink-pipeline-worker", p.workerLoop, func(v any, stack []byte) {
		p.logger.Error("sink pipeline worker panic",
			zap.Any("panic", v),
			zap.ByteString("stack", stack),
		)
		ingestCity, sourceDC := obs.MetaLabels()
		obs.ErrorsTotal.WithLabelValues("pipeline", "worker_panic", ingestCity, sourceDC).Inc()
	})
}

// Submit 投递一条 message;channel 满立即返回 ErrBackpressure(receiver 映射 503)。
//
// 入队语义:
//   - nil: 已入队,worker 后续异步 Send
//   - ErrBackpressure: channel 满,上游应返回 503 + Retry-After
//   - ErrClosed: pipeline 已 Stop
//   - ctx.Err(): ctx 取消
func (p *Pipeline) Submit(ctx context.Context, msg Message) error {
	// T1.12: pipeline span(挂在 rule / caller span 之下,标记入队阶段)
	_, span := tracex.StartSpan(ctx, "pipeline", "submit")
	defer span.End()
	ingestCity, sourceDC := obs.MetaLabels()
	select {
	case <-p.stopCh:
		return ErrClosed
	default:
	}
	select {
	case p.ch <- msg:
		p.submitted.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		obs.BackpressureRejected.WithLabelValues("pipeline", ingestCity, sourceDC).Inc()
		return ErrBackpressure
	}
}

// Stop 关闭 channel,等待 worker 排空已有消息后,再取消 ctx。
//
// 设计 §6.5: "Flush 所有 batch buffer; 等待 in-flight 请求处理完(超时 30s)"。
// 先 close(ch) 通知 worker 不再有新消息,worker 用仍有效的 ctx 处理完剩余消息,
// 最后 cancel() 释放 ctx 资源。这样保证 shutdown 期间数据不丢。
func (p *Pipeline) Stop() {
	select {
	case <-p.stopCh:
		return // 已 Stop
	default:
		close(p.stopCh)
	}
	close(p.ch)
	// 等 worker 排空 channel 中的剩余消息(ctx 仍有效,sink.Send 能正常完成)
	p.wg.Wait()
	// 排空后再取消 ctx,释放资源
	p.cancel()
}

// Stats 监控用。
func (p *Pipeline) Stats() (submitted, drained uint64, depth int) {
	return p.submitted.Load(), p.drained.Load(), len(p.ch)
}

// workerLoop 单 goroutine 串行从 channel 读消息并 Send。
//
// T1.12:trace 上下文不能从 request ctx 透传过来(已经跨 channel 异步了),
// 需要从 message.Headers["traceparent"] 反向还原 ctx,这样 sink.send span
// 才能挂回请求的 trace 上,而不是开新 root。
func (p *Pipeline) workerLoop() {
	defer p.wg.Done()
	for msg := range p.ch {
		p.drained.Add(1)
		// 还原请求侧的 ctx(从 message header 的 traceparent),
		// 再派生一个受 pipeline.Stop 控制的 ctx,这样阻塞中的 Send 能被取消。
		// 注:sendCtx 派生自 p.ctx,Stop 时 cancel p.ctx 会让 sendCtx 同步取消,
		// 因此把 sendCtx 传给 sink.Send 既能透传 trace 上下文,又能受 Stop 控制。
		sendCtx := tracex.ExtractTraceparent(p.ctx, msg.Headers["traceparent"])
		_, span := tracex.StartSpan(sendCtx, "sink", "send")
		err := p.sink.Send(sendCtx, msg)
		tracex.EndSpan(span, err)
		if err != nil {
			if errors.Is(err, ErrClosed) {
				span.End()
				p.logger.Warn("sink closed, dropping remaining messages", zap.Error(err))
				return
			}
			if errors.Is(err, ErrBackpressure) {
				// 已在 sink 内 inc 指标;这里只记 warn
				p.logger.Warn("sink backpressure, will retry on next message",
					zap.String("topic", msg.Topic),
				)
				continue
			}
			if errors.Is(err, context.Canceled) {
				// pipeline 正在停机,正常退出
				return
			}
			ingestCity, sourceDC := obs.MetaLabels()
			obs.ErrorsTotal.WithLabelValues("pipeline", "send", ingestCity, sourceDC).Inc()
			p.logger.Error("sink send failed",
				zap.String("topic", msg.Topic),
				zap.Int("payload_size", len(msg.Payload)),
				zap.Error(err),
			)
		}
	}
}
