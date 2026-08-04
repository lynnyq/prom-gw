// Package kafkasink 封装 franz-go 异步批量 producer,作为 prom-gw 写入中心 Kafka 的出口。
//
// # 设计要点(plan T1.7)
//
//   - Produce 语义为"入内部有界 channel 即返回 nil error",真正的 broker ack 异步由后台 flusher 完成
//   - 真正的错误通过 gateway_produce_errors_total{reason} 指标反馈(也可选 callback)
//   - 同步等待用 Flush(timeout),仅在停机 + WAL drain 场景使用
//   - Channel 满且超过 produce_block_timeout 默认 100ms → ErrProduceBackpressure(receiver 映射 503)
//   - Headers map 透传(traceparent / tenant / source_dc / ingest_ts 等)
//   - 启动参数:linger=50ms / batch=1MB / acks=all / 压缩=zstd / 幂等=true
//
// # 启动行为
//
// New 内部:
//  1. 解析默认配置(BufferSize/BlockTimeout/Linger 等)
//  2. 用 kgo.NewClient 建连 franz-go(SeedBrokers + 压缩 + 幂等)
//  3. 调 cli.Ping(ConnectTimeout)探活;失败 → ErrConnectTimeout
//  4. 启动后台 flusher goroutine(单 goroutine 串行投递,降低 franz-go 内部锁竞争)
//
// # 与 WAL 的关系(T1.8 引入)
//
// WAL 启动模式:WAL 不可用时 main.go 不应启动 kafkasink,而是先 wal.New() → 落盘。
// WAL 恢复时:kafkasink.Produce + wal.Write 形成 sink adapter,由 T1.9 实现。
package kafkasink
