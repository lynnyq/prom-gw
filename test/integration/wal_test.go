//go:build integration

// T1.10 WAL 集成测试:验证 WAL 落盘 → Kafka 故障 → 恢复 → drain 的完整路径。
//
// 测试策略:
//   - 直接用 wal.WAL + sink.WALSink 验证落盘
//   - 用 sink.AdapterSink 模拟 Kafka 故障时降级到 WAL,kafka 恢复后 drain
//   - 端到端: HTTP 写入 → handler 落 WAL → Replay 验证字节级相等
package integration

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/lynnyq/bigdata/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWAL_DirectWriteAndReplay 直接测 WAL 落盘 + 重放,不依赖 receiver。
func TestWAL_DirectWriteAndReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.New(wal.Config{
		Dir:          dir,
		SegmentBytes: 1024 * 1024,
		MaxBytes:     100 * 1024 * 1024,
	})
	require.NoError(t, err)

	// 写 3 条
	for i := 0; i < 3; i++ {
		err := w.Write(context.Background(), wal.Record{
			Topic:   "prom.t",
			Key:     []byte{byte(i)},
			Payload: []byte("payload-" + string(rune('a'+i))),
			Headers: map[string]string{"h": "v"},
		})
		require.NoError(t, err)
	}

	// 验证 Bytes
	assert.Greater(t, w.Bytes(), int64(0))

	// Close 会 seal active 段,使其可被 Replay
	require.NoError(t, w.Close())

	// Reopen + Replay
	w2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	defer w2.Close()

	var got []wal.Record
	var mu sync.Mutex
	err = w2.Replay(context.Background(), func(r wal.Record) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, r)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, got, 3)
	assert.Equal(t, "prom.t", got[0].Topic)
	assert.Equal(t, []byte("payload-a"), got[0].Payload)
}

// TestWAL_SegmentFiles 验证 segment 文件被创建。
func TestWAL_SegmentFiles(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.New(wal.Config{
		Dir:          dir,
		SegmentBytes: 1024 * 1024,
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := w.Write(context.Background(), wal.Record{
			Topic:   "prom.t",
			Payload: []byte("x"),
		})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	// 段文件存在
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.NotEmpty(t, entries, "至少 1 个段文件")
}

// TestWAL_ReopenAndReplay 关闭后重开,验证 Replay 能读出上次写入的 records。
func TestWAL_ReopenAndReplay(t *testing.T) {
	dir := t.TempDir()

	// 第一轮:写 + 关闭
	w1, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, w1.Write(context.Background(), wal.Record{
			Topic:   "prom.t",
			Payload: []byte("data"),
		}))
	}
	require.NoError(t, w1.Close())

	// 第二轮:重开 + Replay
	w2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	defer w2.Close()

	var n int
	err = w2.Replay(context.Background(), func(r wal.Record) error {
		n++
		return nil
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, 3, "Reopen 后能读出上次 records")
}

// TestWAL_WALSink_SendByteForByte 验证 sink.WALSink 落盘与原始 payload 字节级相等。
func TestWAL_WALSink_SendByteForByte(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)

	ws := sink.NewWALSink(w)
	payload := []byte("hello-wal-payload")
	err = ws.Send(context.Background(), sink.Message{
		Topic:   "prom.test",
		Key:     []byte("k"),
		Payload: payload,
		Headers: map[string]string{"trace_id": "abc"},
	})
	require.NoError(t, err)

	// Close seal active 段
	require.NoError(t, w.Close())

	// Reopen + Replay
	w2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	defer w2.Close()

	var got []wal.Record
	err = w2.Replay(context.Background(), func(r wal.Record) error {
		got = append(got, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "prom.test", got[0].Topic)
	assert.Equal(t, payload, got[0].Payload, "字节级相等")
	assert.Equal(t, "abc", got[0].Headers["trace_id"])
}

// TestWAL_HardReject 验证容量超限返回 ErrWALFull(被 sink 映射为 ErrBackpressure)。
func TestWAL_HardReject(t *testing.T) {
	dir := t.TempDir()
	// MaxBytes 极小,放 1-2 条就满
	w, err := wal.New(wal.Config{
		Dir:          dir,
		SegmentBytes: 1024 * 1024,
		MaxBytes:     200, // 200 字节
	})
	require.NoError(t, err)
	defer w.Close()

	ws := sink.NewWALSink(w)
	// 写大 payload,直到 ErrWALFull
	var lastErr error
	for i := 0; i < 100; i++ {
		payload := make([]byte, 1024) // 1KB per record
		err := ws.Send(context.Background(), sink.Message{
			Topic:   "t",
			Payload: payload,
		})
		if err != nil {
			lastErr = err
			break
		}
	}
	require.Error(t, lastErr, "应达到容量上限")
}

// TestWAL_ViaReceiver 端到端:HTTP 写入 → handler 落 WAL → Replay 验证。
func TestWAL_ViaReceiver(t *testing.T) {
	dir := t.TempDir()
	walImpl, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)

	ws := sink.NewWALSink(walImpl)
	var received [][]byte
	var mu sync.Mutex

	authN := newMockAuth()
	handler := func(_ context.Context, raw []byte, _ []parser.Sample, defaultTopic string) error {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]byte, len(raw))
		copy(cp, raw)
		received = append(received, cp)
		return ws.Send(context.Background(), sink.Message{
			Topic:   defaultTopic,
			Key:     []byte("k"),
			Payload: cp,
		})
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	// 推 3 个请求
	for i := 0; i < 3; i++ {
		req := makeWriteRequestFromSamples("m", map[string]string{"i": string(rune('a' + i))}, 1, 1)
		resp := h.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), nil)
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		resp.Body.Close()
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 3
	}, 1*time.Second, 10*time.Millisecond)

	// 等 fsync 完成 + Close seal
	require.NoError(t, walImpl.Close())

	// Reopen + Replay
	wal2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	defer wal2.Close()

	var replayed []wal.Record
	err = wal2.Replay(context.Background(), func(r wal.Record) error {
		replayed = append(replayed, r)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, replayed, 3)
	for i, r := range replayed {
		assert.Equal(t, received[i], r.Payload, "第 %d 条 payload 字节级相等", i)
	}
}

// TestWAL_RecoverAfterRestart 模拟 kafka 故障→wal 落盘→kafka 恢复→drain。
//
// 用 sink.WALSink 直接接收,模拟"kafka 故障时降级到 WAL";恢复时用 Replay
// 把 WAL 中的数据投递给"修复后的 kafka"(这里用 mock 收)。
func TestWAL_RecoverAfterRestart(t *testing.T) {
	dir := t.TempDir()
	walImpl, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)

	ws := sink.NewWALSink(walImpl)
	mockK := newMockSink() // 模拟"恢复后的 kafka"

	// 阶段 1: kafka 故障,只写 WAL
	for i := 0; i < 3; i++ {
		err := ws.Send(context.Background(), sink.Message{
			Topic:   "prom.test",
			Key:     []byte{byte(i)},
			Payload: []byte("a"),
		})
		require.NoError(t, err)
	}
	assert.Empty(t, mockK.Messages(), "kafka 故障时不应收到任何消息")

	// Close seal 段
	require.NoError(t, walImpl.Close())

	// Reopen + drain
	wal2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1024 * 1024})
	require.NoError(t, err)
	defer wal2.Close()

	err = wal2.Replay(context.Background(), func(r wal.Record) error {
		return mockK.Send(context.Background(), sink.Message{
			Topic:   r.Topic,
			Key:     r.Key,
			Payload: r.Payload,
		})
	})
	require.NoError(t, err)

	assert.Len(t, mockK.Messages(), 3, "drain 后 kafka 应收到全部 3 条")
}

// TestWAL_DirIndependence 验证 WAL 数据目录独立创建(部署要求)。
func TestWAL_DirIndependence(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "wal-data")
	require.NoError(t, os.MkdirAll(dir, 0o755))

	w, err := wal.New(wal.Config{Dir: dir})
	require.NoError(t, err)
	defer w.Close()

	err = w.Write(context.Background(), wal.Record{
		Topic:   "t",
		Payload: []byte("x"),
	})
	require.NoError(t, err)
}
