// Package sink 抽象数据出口。
//
// 设计要点(plan T1.9):
//   - Sink.Send(ctx, msg) 返回 ErrBackpressure → receiver 映射 503
//   - AdapterSink 包装 KafkaSink + WALSink,kafka 故障时降级到 WAL
//   - kafka 恢复后由 monitor goroutine 周期性尝试 drain WAL
//   - M1 简版: 单调阈值判断故障与恢复(不引入 sliding window)
package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynnyq/prom-gw/internal/kafkasink"
	"github.com/lynnyq/prom-gw/internal/obs"
	"github.com/lynnyq/prom-gw/internal/wal"
	"github.com/lynnyq/prom-gw/pkg/safego"
	"go.uber.org/zap"
)

// 公共错误。
var (
	// ErrBackpressure 出口超载(receiver 映射 503)。
	// 来自 WAL 容量满或 kafka 内部 channel 满。
	ErrBackpressure = errors.New("sink: backpressure")
	// ErrClosed sink 已关闭。
	ErrClosed = errors.New("sink: closed")
)

// Message 一条要写入 Kafka/WAL 的消息。
//
// Payload 承载 prompb.WriteRequest 原始字节(plan T1.10 要求字节级相等)。
// Headers 在 kafka 投递时透传到 message header,WAL 重放时再带回到 kafka。
type Message struct {
	Topic   string
	Key     []byte
	Payload []byte
	Headers map[string]string
}

// Sink 抽象出口接口。
//
// Send 必须保证:
//   - ctx 取消立即返回 ctx.Err()
//   - 已 Close 后调用返回 ErrClosed
//   - 真背压(底层 capacity 满)返回 ErrBackpressure
type Sink interface {
	Send(ctx context.Context, msg Message) error
	Close() error
}

// --- KafkaSink ---

// KafkaSink 直接转给 kafkasink.Producer。
// kafkasink.Produce 已是异步批量,Send 即"入 channel"。
type KafkaSink struct {
	p *kafkasink.Producer
}

// NewKafkaSink 构造一个 KafkaSink。
func NewKafkaSink(p *kafkasink.Producer) *KafkaSink { return &KafkaSink{p: p} }

// Send 投递到 Kafka。kafka channel 满时返回 kafkasink.ErrProduceBackpressure。
func (k *KafkaSink) Send(ctx context.Context, msg Message) error {
	return k.p.Produce(ctx, msg.Topic, string(msg.Key), msg.Payload, msg.Headers)
}

// Close 关闭底层 producer。
func (k *KafkaSink) Close() error { return k.p.Close() }

// --- WALSink ---

// WALSink 把消息转成 wal.Record 落盘。
// 落盘语义: wal.Write 内部已 fsync,返回 nil 即保证数据已落到 segment 文件。
type WALSink struct {
	w wal.WAL
}

// NewWALSink 构造一个 WALSink。
func NewWALSink(w wal.WAL) *WALSink { return &WALSink{w: w} }

// Send 同步落盘。WAL 满时返回 wal.ErrWALFull,receiver 应映射 503。
func (s *WALSink) Send(ctx context.Context, msg Message) error {
	return s.w.Write(ctx, wal.Record{
		Topic:   msg.Topic,
		Key:     msg.Key,
		Payload: msg.Payload,
		Headers: msg.Headers,
	})
}

// Close 关闭底层 WAL。
func (s *WALSink) Close() error { return s.w.Close() }

// WAL 内部访问(供 AdapterSink drain 使用)。
func (s *WALSink) WAL() wal.WAL { return s.w }

// --- AdapterSink ---

// AdapterState 适配器状态。
const (
	// StateNormal 正常模式,投递到 Kafka。
	StateNormal int32 = 0
	// StateDegraded 降级模式,投递到 WAL。
	StateDegraded int32 = 1
)

// AdapterConfig AdapterSink 配置。
type AdapterConfig struct {
	// FailThreshold kafka 连续失败次数超过此值 → 切 WAL。默认 3。
	FailThreshold int
	// RecoverCheck kafka 恢复检查间隔。默认 1s。
	RecoverCheck time.Duration
	// RecoverSuccessThreshold 恢复期连续成功次数超过此值 → 切回 Kafka 并 drain。默认 3。
	RecoverSuccessThreshold int
	// DrainTimeout drain WAL 到 Kafka 的最大等待时间(spec §6.5)。
	// 超时后 Close 不再阻塞,未 drain 的数据保留在 WAL 供下次启动重放。默认 30s。
	DrainTimeout time.Duration
	// Logger 必填。
	Logger *zap.Logger
}

// AdapterSink 包装 Kafka + WAL,做自动故障切换与恢复 drain。
//
// 状态机:
//
//	StateNormal  --(连续 N 次失败)-->  StateDegraded
//	StateDegraded --(probe 连续 M 次成功)-->  StateNormal + drain WAL
type AdapterSink struct {
	cfg     AdapterConfig
	kafka   *kafkasink.Producer
	walSink *WALSink

	state atomic.Int32

	// 失败/成功计数(单调递增;到达阈值后清零并切换状态)。
	failCount    atomic.Int32
	successCount atomic.Int32

	drainCh chan struct{}
	done    chan struct{}
	wg      sync.WaitGroup // 跟踪 monitor goroutine
}

// NewAdapterSink 构造 AdapterSink 并启动 monitor goroutine。
func NewAdapterSink(cfg AdapterConfig, k *kafkasink.Producer, w *WALSink) *AdapterSink {
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = 3
	}
	if cfg.RecoverCheck <= 0 {
		cfg.RecoverCheck = 1 * time.Second
	}
	if cfg.RecoverSuccessThreshold <= 0 {
		cfg.RecoverSuccessThreshold = 3
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	a := &AdapterSink{
		cfg:     cfg,
		kafka:   k,
		walSink: w,
		drainCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	a.wg.Add(1)
	safego.GoWithRecover("sink-adapter-monitor", func() {
		defer a.wg.Done()
		a.monitorLoop()
	}, func(v any, stack []byte) {
		cfg.Logger.Error("sink adapter monitor panic",
			zap.Any("panic", v),
			zap.ByteString("stack", stack),
		)
	})
	return a
}

// State 返回当前状态(StateNormal / StateDegraded),用于监控/调试。
func (a *AdapterSink) State() int32 { return a.state.Load() }

// Send 按当前状态路由到 Kafka 或 WAL。
//
// 当 Kafka 不可用(broker 故障 → 内部 channel 堆满 → ErrProduceBackpressure,
// 或其他 kafka 错误)时,降级到 WAL 落盘。WAL 也满时才返回 ErrBackpressure(503)。
// 设计 §6.1: "同城 Kafka 暂时不可用 → 200 (内部) 内存 + WAL 重试; 长期不可用 → 503 当 WAL 满"。
func (a *AdapterSink) Send(ctx context.Context, msg Message) error {
	if a.state.Load() == StateNormal {
		err := a.sendToKafka(ctx, msg)
		if err == nil {
			return nil
		}
		// ErrClosed 是不可恢复的,直接返回(不落 WAL)
		if errors.Is(err, kafkasink.ErrClosed) {
			return ErrClosed
		}
		// kafka 故障(含 ErrProduceBackpressure: broker 不可达 → channel 堆满)
		// → 累计失败 + 降级到 WAL。WAL 满时 sendToWAL 返回 ErrBackpressure(503)。
		if errors.Is(err, kafkasink.ErrProduceBackpressure) {
			ingestCity, sourceDC := obs.MetaLabels()
			obs.BackpressureRejected.WithLabelValues("kafka", ingestCity, sourceDC).Inc()
		}
		a.recordFailure()
		return a.sendToWAL(ctx, msg)
	}
	// 降级模式:全部走 WAL
	return a.sendToWAL(ctx, msg)
}

// sendToKafka 投递到 kafka,通过 callback 跟踪异步错误。
//
// 返回:
//   - nil: 消息已入 producer channel(后续异步投递,可能异步失败但不影响本次返回)
//   - ErrBackpressure: kafka 内部 channel 满
//   - ErrClosed: producer 已关闭
//   - ctx.Err(): 上下文取消
func (a *AdapterSink) sendToKafka(ctx context.Context, msg Message) error {
	return a.kafka.Produce(ctx, msg.Topic, string(msg.Key), msg.Payload, msg.Headers)
}

// sendToKafkaSync 同步投递到 kafka,等待 broker ack 后返回。
// 用于 drainWAL 场景,保证 at-least-once 语义(设计 §6.3):
// 只有 broker 确认接收后才返回 nil,否则返回错误让段保留供下次重试。
func (a *AdapterSink) sendToKafkaSync(ctx context.Context, msg Message) error {
	ackCh := make(chan error, 1)
	err := a.kafka.ProduceWithCallback(ctx, msg.Topic, string(msg.Key), msg.Payload, msg.Headers, func(ackErr error) {
		ackCh <- ackErr
	})
	if err != nil {
		return err
	}
	select {
	case ackErr := <-ackCh:
		return ackErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendToWAL 投递到 WAL,容量满时返回 ErrBackpressure(503)。
func (a *AdapterSink) sendToWAL(ctx context.Context, msg Message) error {
	err := a.walSink.Send(ctx, msg)
	if err == nil {
		return nil
	}
	if errors.Is(err, wal.ErrWALFull) {
		obs.WalHardReject.Inc()
		ingestCity, sourceDC := obs.MetaLabels()
		obs.BackpressureRejected.WithLabelValues("wal", ingestCity, sourceDC).Inc()
		return ErrBackpressure
	}
	if errors.Is(err, wal.ErrClosed) {
		return ErrClosed
	}
	return err
}

// recordFailure 累计失败次数,超过阈值切到降级。
func (a *AdapterSink) recordFailure() {
	n := a.failCount.Add(1)
	a.successCount.Store(0)
	if int(n) >= a.cfg.FailThreshold && a.state.CompareAndSwap(StateNormal, StateDegraded) {
		ingestCity, sourceDC := obs.MetaLabels()
		a.cfg.Logger.Warn("sink adapter: switched to WAL degraded mode",
			zap.Int32("fail_count", n),
		)
		obs.ErrorsTotal.WithLabelValues("sink", "kafka_failover", ingestCity, sourceDC).Inc()
	}
}

// recordSuccess 累计成功次数(降级期间 probe 成功时调用)。
func (a *AdapterSink) recordSuccess() {
	n := a.successCount.Add(1)
	a.failCount.Store(0)
	if int(n) >= a.cfg.RecoverSuccessThreshold && a.state.CompareAndSwap(StateDegraded, StateNormal) {
		a.cfg.Logger.Info("sink adapter: kafka recovered, switching back + draining WAL",
			zap.Int32("success_count", n),
		)
		// 触发 drain
		select {
		case a.drainCh <- struct{}{}:
		default:
		}
	}
}

// Close 关闭 monitor + 底层 wal/kafka。
//
// 关闭顺序(设计 §6.5): Stop monitor → drain WAL → Close WAL → Close Kafka。
// 先关闭 monitor(done + wg.Wait),再 drain WAL(把未 .done 的段重放到 Kafka,
// 超时 DrainTimeout),然后关 WAL(确保所有 pending 数据落盘),最后关 Kafka producer。
//
// spec §6.5: Sink Stop() must drain all messages before canceling context to avoid data loss。
func (a *AdapterSink) Close() error {
	close(a.done)
	a.wg.Wait() // 等 monitor goroutine 退出
	// drain WAL → Kafka(spec §6.5: 停机前把 WAL 数据回灌 Kafka)
	a.drainWAL()
	var errs []error
	if err := a.walSink.Close(); err != nil {
		errs = append(errs, fmt.Errorf("wal close: %w", err))
	}
	if err := a.kafka.Close(); err != nil {
		errs = append(errs, fmt.Errorf("kafka close: %w", err))
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// monitorLoop 周期检查 kafka 健康并触发 drain。
//
// 行为:
//   - 正常模式: 不做事(由 Send 的失败累积触发降级)
//   - 降级模式: 周期性尝试向 kafka 发"ping 消息"检查连通性;
//     连续成功 → 触发 recordSuccess → 切回 normal + drain
func (a *AdapterSink) monitorLoop() {
	t := time.NewTicker(a.cfg.RecoverCheck)
	defer t.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-t.C:
			if a.state.Load() == StateDegraded {
				a.probeKafka()
			}
		case <-a.drainCh:
			a.drainWAL()
		}
	}
}

// probeKafka 尝试向 kafka 发一条 ping 消息,成功则 recordSuccess。
//
// v1 实现:发一条 1B 消息到一个固定 _gw_probe topic,handler 忽略。
// kafka 故障时 Produce 会立即返回错误(producer 内部 channel 探测已能反映)。
func (a *AdapterSink) probeKafka() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// 简化: 复用 ping 思路,Send 内部用 Produce,如果 produce 立即失败则记失败。
	// 由于 Produce 是 async,无法从 Send 内部同步知道结果,所以这里仅作为触发器,
	// 真正的成功由后续 Send 累积触发 recordSuccess(通过 successCount)。
	// v2 优化: 用 franz-go 的 Ping 做 active probing。
	if err := a.sendToKafka(ctx, Message{Topic: "_gw_probe", Payload: []byte("p")}); err == nil {
		a.recordSuccess()
	}
}

// drainWAL 把 WAL 里所有未 .done 的段重放回 kafka。
//
// 使用同步投递(sendToKafkaSync)等待 broker ack,保证 at-least-once 语义(设计 §6.3):
// 只有 broker 确认接收后才返回 nil → 段被 mark .done;
// broker ack 失败 → 返回错误 → 段保留(下次 drain 再试)。
//
// spec §6.5: drain 有 DrainTimeout 硬上限(默认 30s),超时后未 drain 的数据
// 保留在 WAL 供下次启动重放,避免 Close 永久阻塞。
func (a *AdapterSink) drainWAL() {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.DrainTimeout)
	defer cancel()
	a.cfg.Logger.Info("sink adapter: draining WAL to Kafka", zap.Duration("timeout", a.cfg.DrainTimeout))
	err := a.walSink.WAL().Replay(ctx, func(rec wal.Record) error {
		msg := Message{
			Topic:   rec.Topic,
			Key:     rec.Key,
			Payload: rec.Payload,
			Headers: rec.Headers,
		}
		// 同步等待 broker ack;失败则停止 drain,保留段(下次再试)
		if err := a.sendToKafkaSync(ctx, msg); err != nil {
			a.cfg.Logger.Warn("drain send failed, will retry later",
				zap.String("topic", rec.Topic),
				zap.Error(err),
			)
			return err
		}
		return nil
	})
	if err != nil {
		a.cfg.Logger.Error("drain incomplete", zap.Error(err))
		ingestCity, sourceDC := obs.MetaLabels()
		obs.ErrorsTotal.WithLabelValues("sink", "drain_incomplete", ingestCity, sourceDC).Inc()
		return
	}
	a.cfg.Logger.Info("sink adapter: WAL drained successfully")
	ingestCity, sourceDC := obs.MetaLabels()
	obs.SamplesTotal.WithLabelValues("wal", "drain", "ok", ingestCity, sourceDC).Inc()
}
