// Package kafkasink 封装 franz-go 异步批量 producer。
//
// 设计要点(plan T1.7):
//   - 入内部有界 channel 即返回 nil error,异步批量由后台 goroutine 真正投递到 Kafka
//   - 真正的错误通过 gateway_produce_errors_total{reason} 指标和可选 callback 反馈
//   - 同步等待用 Flush(timeout),仅在停机 + WAL 落盘场景使用
//   - Channel 满且超过 produce_block_timeout 默认 100ms → ErrProduceBackpressure
//   - Headers map 透传(traceparent / tenant / source_dc / ingest_ts 等)
//   - 启动参数: linger=50ms, batch=1MB, acks=all, 压缩=zstd
//   - 幂等写:v1 默认开启 enable.idempotence(franz-go 默认值,无需显式设置)
package kafkasink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/pkg/safego"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// 背压 / 关闭错误。
var (
	// ErrProduceBackpressure 内部 channel 满且超时。
	// receiver 将其映射为 HTTP 503(spec 6.1)。
	ErrProduceBackpressure = errors.New("kafkasink: produce backpressure (channel full)")
	// ErrClosed producer 已关闭。
	ErrClosed = errors.New("kafkasink: producer closed")
	// ErrNoBrokers 启动时未配置 brokers。
	ErrNoBrokers = errors.New("kafkasink: no brokers configured")
	// ErrConnectTimeout 启动时连接 Kafka 超时。
	ErrConnectTimeout = errors.New("kafkasink: connect to brokers timeout")
)

// 默认配置常量(可被 Config 覆盖)。
const (
	DefaultBufferSize             = 65535
	DefaultBlockTimeout           = 100 * time.Millisecond
	DefaultConnectTimeout         = 10 * time.Second
	DefaultBatchMaxBytes          = 1 * 1024 * 1024 // 1MB
	DefaultLinger                 = 50 * time.Millisecond
	DefaultFlushTimeout           = 30 * time.Second
	DefaultRequestTimeoutOverhead = 10 * time.Second
)

// Config producer 配置。
type Config struct {
	// Brokers 必填,形如 ["10.0.0.1:9092", "10.0.0.2:9092"]
	Brokers []string
	// ClientID 写入 Kafka 协议,便于 broker 端日志识别。
	// 默认 "prom-gw"。
	ClientID string
	// BufferSize 内部 channel 容量(同时 in-flight 消息上限)。
	// 默认 65535。压测可调到 131072。
	BufferSize int
	// BlockTimeout 内部 channel 满时,Produce 阻塞等待空闲槽的最大时间。
	// 超时返回 ErrProduceBackpressure。默认 100ms。
	BlockTimeout time.Duration
	// ConnectTimeout 启动时建立连接的最大等待。
	// 默认 10s。WAL 启动模式下探测失败由 main.go 切到 WAL。
	ConnectTimeout time.Duration
	// BatchMaxBytes 单批最大字节数,默认 1MB。
	BatchMaxBytes int32
	// Linger 批次最大等待时间,默认 50ms。
	Linger time.Duration
	// RequestTimeoutOverhead 单次 Produce 请求的额外超时叠加到 linger。
	// 默认 10s。
	RequestTimeoutOverhead time.Duration
	// Compression 压缩算法,默认 zstd。可选:zstd, snappy, lz4, gzip, none。
	Compression string
	// Idempotent 是否开启幂等写。默认 true。
	Idempotent bool
	// Logger 必填,用于连接 / 关闭 / 错误日志。
	Logger *zap.Logger
}

// envelope 内部消息封装,在 channel 中传递。
// 注意: key/headers 持有 []byte 是为了让 kgo.Record 不复制持有;
// envelope 在 channel 之后由 flusher 独占消费,无并发问题。
type envelope struct {
	topic   string
	key     []byte
	payload []byte
	headers []kgo.RecordHeader
	// onAck 为 nil 时丢弃 ack(纯 fire-and-forget 模式);
	// 非 nil 时 Produce 调用方得到 callback。
	onAck func(err error)
}

// Producer 异步批量 Kafka producer。
//
// 生命周期: New -> Produce*N -> Flush -> Close。
// Close 后所有 Produce 返回 ErrClosed。
type Producer struct {
	cfg    Config
	client *kgo.Client
	ch     chan *envelope

	// inFlight 当前未 ack 的消息数,Flush 等待其归零。
	inFlight sync.WaitGroup

	closed atomic.Bool
	done   chan struct{} // flusher 退出信号
}

// New 初始化 producer 并启动后台 flusher goroutine。
//
// 启动行为(plan T1.7 启动行为):
//   - 建立 franz-go client(若 ConnectTimeout 内未成功 → 返回 ErrConnectTimeout)
//   - 启动 flusher goroutine(从 channel 读 envelope,转 kgo.Record 投递)
func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrNoBrokers
	}
	if cfg.Logger == nil {
		return nil, errors.New("kafkasink: Logger required")
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}
	if cfg.BlockTimeout <= 0 {
		cfg.BlockTimeout = DefaultBlockTimeout
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = DefaultConnectTimeout
	}
	if cfg.BatchMaxBytes <= 0 {
		cfg.BatchMaxBytes = DefaultBatchMaxBytes
	}
	if cfg.Linger <= 0 {
		cfg.Linger = DefaultLinger
	}
	if cfg.RequestTimeoutOverhead <= 0 {
		cfg.RequestTimeoutOverhead = DefaultRequestTimeoutOverhead
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "prom-gw"
	}
	compression, err := parseCompressionCodec(cfg.Compression)
	if err != nil {
		return nil, err
	}

	// 1. 建连(client 初始化时即会异步探活)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.ProducerLinger(cfg.Linger),
		kgo.RequestTimeoutOverhead(cfg.RequestTimeoutOverhead),
		kgo.ProducerBatchCompression(compression),
		kgo.AllowAutoTopicCreation(),
	}
	if !cfg.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	cli, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafkasink: new client: %w", err)
	}

	// 探活:发一个 metadata 请求,失败时返回超时错误。
	pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer pingCancel()
	if err := cli.Ping(pingCtx); err != nil {
		cli.Close()
		return nil, fmt.Errorf("%w: %v", ErrConnectTimeout, err)
	}

	p := &Producer{
		cfg:    cfg,
		client: cli,
		ch:     make(chan *envelope, cfg.BufferSize),
		done:   make(chan struct{}),
	}

	// 2. 启动 flusher goroutine
	safego.GoWithRecover("kafkasink-flusher", func() {
		p.flusherLoop()
	}, func(value any, stack []byte) {
		cfg.Logger.Error("kafkasink flusher panic",
			zap.Any("panic", value),
			zap.ByteString("stack", stack),
		)
		ingestCity, sourceDC := obs.MetaLabels()
		obs.ErrorsTotal.WithLabelValues("kafka", "flusher_panic", ingestCity, sourceDC).Inc()
	})

	cfg.Logger.Info("kafkasink started",
		zap.Strings("brokers", cfg.Brokers),
		zap.Int("buffer_size", cfg.BufferSize),
		zap.Duration("block_timeout", cfg.BlockTimeout),
		zap.Duration("linger", cfg.Linger),
		zap.Int32("batch_max_bytes", cfg.BatchMaxBytes),
		zap.Bool("idempotent", cfg.Idempotent),
	)
	return p, nil
}

// Produce 入队一条消息,语义为"入 channel 即返回 nil"。
//
// 真正的投递由后台 flusher goroutine 异步完成,broker ack 错误通过
// gateway_produce_errors_total{reason} 指标反馈(若 onAck != nil 则同时回调)。
//
// 返回值:
//   - nil: 消息已入 channel(成功)
//   - ErrProduceBackpressure: channel 满且超过 BlockTimeout(receiver 映射 503)
//   - ErrClosed: producer 已 Close
//   - ctx.Err(): ctx 被取消
func (p *Producer) Produce(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error {
	return p.ProduceWithCallback(ctx, topic, key, payload, headers, nil)
}

// ProduceWithCallback 与 Produce 等价,但支持 onAck 异步回调(在 broker ack 完成后触发)。
// v1 主流模式不传;预留扩展点用于 WAL 同步等待场景。
func (p *Producer) ProduceWithCallback(
	ctx context.Context,
	topic, key string,
	payload []byte,
	headers map[string]string,
	onAck func(err error),
) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if topic == "" {
		return errors.New("kafkasink: topic required")
	}

	// headers → kgo.RecordHeader 切片
	var rh []kgo.RecordHeader
	if len(headers) > 0 {
		rh = make([]kgo.RecordHeader, 0, len(headers))
		for k, v := range headers {
			rh = append(rh, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
	}

	env := &envelope{
		topic:   topic,
		key:     []byte(key),
		payload: payload,
		headers: rh,
		onAck:   onAck,
	}

	// 阻塞入 channel,BlockTimeout 内等不到空闲槽 → ErrProduceBackpressure
	t := time.NewTimer(p.cfg.BlockTimeout)
	defer t.Stop()
	select {
	case p.ch <- env:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		ingestCity, sourceDC := obs.MetaLabels()
		obs.BackpressureRejected.WithLabelValues("kafkasink", ingestCity, sourceDC).Inc()
		return ErrProduceBackpressure
	}
}

// Flush 阻塞等待所有 in-flight 消息获得 broker ack(成功或最终失败)。
//
// 语义:
//   - timeout 内所有消息获得 ack → nil
//   - 超时 → 仍有未 ack 消息,client 仍存活;返回 context.DeadlineExceeded
//
// 实现:franz-go 的 Flush 内部会同步等待所有 in-flight 消息 ack 完成。
//
// 用途: shutdown 路径,确保最后一波消息不丢;WAL drain 完成后切回 kafka 时的同步点。
func (p *Producer) Flush(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultFlushTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// franz-go 的 Flush 内部会等待所有 in-flight 消息 ack 完成
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("kafkasink: flush: %w", err)
	}
	return nil
}

// Close 排空 in-flight 消息并关闭 client。幂等。
//
// 阻塞直到:
//   - 所有 in-flight 消息获得 ack(或最终失败)
//   - franz-go client 内部资源释放
func (p *Producer) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil // 已关闭
	}
	close(p.ch)
	<-p.done
	p.client.Close()
	p.cfg.Logger.Info("kafkasink closed")
	return nil
}

	// flusherLoop 后台单 goroutine:从 channel 读 envelope → 调 client.Produce。
	//
	// 设计要点:
	//   - 单 goroutine 串行投递,避免 franz-go 的并发 Produce 引发的内部锁竞争
	//     (franz-go 本身并发安全,但 kgo.Produce 每次调用都加锁,串行更便宜)
	//   - broker ack 回调用于 inFlight 计数 + 错误指标
	//   - inFlight 在每条消息进入 batch 时 +1,ack(成功或最终失败)时 -1
func (p *Producer) flusherLoop() {
	defer close(p.done)
	for env := range p.ch {
		p.inFlight.Add(1)
		rec := &kgo.Record{
			Topic:   env.topic,
			Key:     env.key,
			Value:   env.payload,
			Headers: env.headers,
		}
		// Produce 异步投递:把 record 放进取胜 batch,真正 broker ack 走 callback。
		p.client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
			// broker ack 完成(成功或最终失败)时触发。
			defer p.inFlight.Done()
			if err != nil {
				ingestCity, sourceDC := obs.MetaLabels()
				// plan T1.7: 单独的 gateway_produce_errors_total 指标便于告警分桶
				// (kafka produce / timeout / backpressure 等细分 reason)。
				obs.ProduceErrorsTotal.WithLabelValues("kafka_produce", ingestCity, sourceDC).Inc()
				obs.ErrorsTotal.WithLabelValues("kafka", "produce", ingestCity, sourceDC).Inc()
				if p.cfg.Logger != nil {
					p.cfg.Logger.Warn("kafka produce failed",
						zap.String("topic", r.Topic),
						zap.Error(err),
					)
				}
			} else {
				// 提取 ingest_city(若 envelope.headers 已带)
				ingestCity := ""
				for _, h := range r.Headers {
					if h.Key == "ingest_city" {
						ingestCity = string(h.Value)
						break
					}
				}
				obs.BytesOut.WithLabelValues(r.Topic, ingestCity).Add(float64(len(r.Value)))
			}
			if env.onAck != nil {
				env.onAck(err)
			}
		})
	}
	// channel 关闭后等所有 in-flight 完成
	p.inFlight.Wait()
}

// parseCompressionCodec 把字符串映射到 franz-go 压缩 codec。
func parseCompressionCodec(s string) (kgo.CompressionCodec, error) {
	switch s {
	case "", "zstd":
		return kgo.ZstdCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	default:
		return kgo.CompressionCodec{}, fmt.Errorf("kafkasink: unknown compression %q", s)
	}
}
