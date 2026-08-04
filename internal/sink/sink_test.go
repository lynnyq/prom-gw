package sink

import (
	"context"
	"testing"
	"time"

	"github.com/lynnyq/bigdata/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- WALSink ---

func TestWALSink_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ws := NewWALSink(w)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		msg := Message{
			Topic:   "t1",
			Key:     []byte("k"),
			Payload: []byte{byte(i)},
			Headers: map[string]string{"h": "v"},
		}
		require.NoError(t, ws.Send(ctx, msg))
	}
	assert.Greater(t, w.Bytes(), int64(0))

	// 强制 seal active 段(否则 Replay 读不到)
	require.NoError(t, w.Replay(ctx, func(wal.Record) error { return nil })) // no-op,无 sealed 时不做事
	// 实际 seal 通过 RotateSegmentSize: 改用小段触发轮转
	// 这里直接调 wal 内部 seal 不便(非导出),改用 Replay: 先写大让 rotate,再读。
	// 简化:直接 Close → close 会封段
	require.NoError(t, w.Close())

	// 重开读
	w2, err := wal.New(wal.Config{Dir: dir, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w2.Close() })

	var seen []byte
	err = w2.Replay(ctx, func(rec wal.Record) error {
		seen = append(seen, rec.Payload...)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 1, 2, 3, 4}, seen)
}

// fakeWAL 实现 wal.WAL 接口,用于驱动错误路径。
type fakeWAL struct {
	fail bool
}

func (f *fakeWAL) Write(_ context.Context, _ wal.Record) error {
	if f.fail {
		return wal.ErrWALFull
	}
	return nil
}
func (f *fakeWAL) Replay(_ context.Context, _ func(wal.Record) error) error { return nil }
func (f *fakeWAL) Bytes() int64                                             { return 0 }
func (f *fakeWAL) OldestAge() time.Duration                                 { return 0 }
func (f *fakeWAL) Segments() []wal.SegmentInfo                              { return nil }
func (f *fakeWAL) Close() error                                             { return nil }

var _ wal.WAL = (*fakeWAL)(nil)

func TestWALSink_FullReturnsWalFull(t *testing.T) {
	// WALSink.Send 透传 wal 错误(由 AdapterSink 统一映射到 ErrBackpressure)。
	// 这里只验证 raw 错误透传。
	ws := NewWALSink(&fakeWAL{fail: true})
	err := ws.Send(context.Background(), Message{Topic: "t"})
	assert.ErrorIs(t, err, wal.ErrWALFull)
}

func TestWALSink_NormalSendReturnsNil(t *testing.T) {
	ws := NewWALSink(&fakeWAL{fail: false})
	err := ws.Send(context.Background(), Message{Topic: "t"})
	assert.NoError(t, err)
}

// --- AdapterSink 状态机 ---
// AdapterSink 接收具体类型 *kafkasink.Producer,无法注入 fake;
// 因此状态机的 e2e 验证留给 test/integration。
// 这里只验证轻量方法(NewAdapterSink, State)。

func TestAdapterSink_NewStartsNormal(t *testing.T) {
	a := NewAdapterSink(AdapterConfig{Logger: zap.NewNop()}, nil, nil)
	assert.Equal(t, int32(StateNormal), a.State())
}

func TestAdapterSink_Defaults(t *testing.T) {
	a := NewAdapterSink(AdapterConfig{Logger: zap.NewNop()}, nil, nil)
	assert.Equal(t, 3, a.cfg.FailThreshold)
	assert.Equal(t, 1*time.Second, a.cfg.RecoverCheck)
	assert.Equal(t, 3, a.cfg.RecoverSuccessThreshold)
}
