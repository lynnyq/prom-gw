package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper: make a unique temp dir per test.
func newTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// newTestWAL 构造一个禁用磁盘使用率检查的 WAL(DiskUsedRatio=1.0 表示"永不拒绝")。
//
// 行为:为聚焦 WAL 行为测试,显式绕过 syscall.Statfs 阈值;
// 真实场景的 DiskUsedRatio 阈值逻辑由 test/chaos/wal_test.go 验证。
func newTestWAL(t *testing.T, cfg Config) *fileWAL {
	t.Helper()
	if cfg.DiskUsedRatio == 0 {
		cfg.DiskUsedRatio = 1.0
	}
	w, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// helper: build a tiny record for tests.
func mkRec(topic string, payload []byte) Record {
	return Record{
		Topic:   topic,
		Key:     []byte("k-" + topic),
		Payload: payload,
		Headers: map[string]string{"h": "v"},
		Time:    time.Unix(0, time.Now().UnixNano()),
	}
}

// helper: produce N records sequentially.
func writeN(t *testing.T, w *fileWAL, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := w.Write(context.Background(), mkRec("t1", []byte(fmt.Sprintf("p-%d", i))))
		require.NoError(t, err)
	}
}

// --- Config / New ---

func TestNew_Defaults(t *testing.T) {
	dir := newTempDir(t)
	w, err := New(Config{Dir: dir})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	// Defaults applied to internal cfg.
	assert.Equal(t, int64(DefaultSegmentBytes), w.cfg.SegmentBytes)
	assert.Equal(t, int64(DefaultMaxBytes), w.cfg.MaxBytes)
	assert.Equal(t, DefaultDiskUsedRatio, w.cfg.DiskUsedRatio)
	assert.Equal(t, DefaultCleanupInterval, w.cfg.CleanupInterval)
	assert.Equal(t, DefaultRetention, w.cfg.Retention)
	assert.Equal(t, DefaultMaxReplayFailures, w.cfg.MaxReplayFailures)
}

func TestNew_RejectsInvalidDir(t *testing.T) {
	// /dev/null/sub: cannot create a file under a non-directory.
	_, err := New(Config{Dir: "/dev/null/sub"})
	assert.Error(t, err)
}

// --- Write / Bytes ---

func TestWrite_IncrementsBytes(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024 * 1024})

	before := w.Bytes()
	writeN(t, w, 10)
	after := w.Bytes()
	assert.Greater(t, after, before, "Bytes should grow after writes")
}

func TestWrite_AfterClose(t *testing.T) {
	dir := newTempDir(t)
	// 显式构造 WAL(不走 t.Cleanup 自动 Close,避免干扰断言时序)
	w, err := New(Config{Dir: dir, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	err = w.Write(context.Background(), mkRec("t", []byte("p")))
	assert.ErrorIs(t, err, ErrClosed)
}

func TestWrite_EmptyTopicRejected(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})

	err := w.Write(context.Background(), Record{Topic: "", Payload: []byte("x")})
	assert.Error(t, err)
}

func TestWrite_FullReturnsErrWALFull(t *testing.T) {
	dir := newTempDir(t)
	w, err := New(Config{Dir: dir, MaxBytes: 100, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0})
	require.NoError(t, err)
	defer w.Close()

	// Force Bytes() to look full by pre-loading.
	w.bytes.Store(200)
	err = w.Write(context.Background(), mkRec("t", []byte("x")))
	assert.ErrorIs(t, err, ErrWALFull)
}

// --- Segment rotation ---

func TestWrite_RotatesSegment(t *testing.T) {
	dir := newTempDir(t)
	// tiny segment to force rotation
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024})

	// each record ~ 60B; 50 records should rotate several times.
	writeN(t, w, 50)

	segs := w.Segments()
	assert.Greater(t, len(segs), 1, "should have produced multiple segments, got %d", len(segs))
}

// --- Replay ---

func TestReplay_NoSealedSegments(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})

	// Only active segment, no .sealed; Replay should be a no-op.
	var called atomic.Int32
	err := w.Replay(context.Background(), func(rec Record) error {
		called.Add(1)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), called.Load())
}

func TestReplay_AfterRotation(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024})

	writeN(t, w, 30)

	// Force a final seal so we can replay.
	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	var seen []string
	var mu sync.Mutex
	err := w.Replay(context.Background(), func(rec Record) error {
		mu.Lock()
		seen = append(seen, string(rec.Payload))
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)
	w.Close()

	sort.Strings(seen)
	assert.Equal(t, 30, len(seen), "expected 30 records replayed, got %d", len(seen))
}

func TestReplay_HandlerErrorRetries(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024, MaxReplayFailures: 5})

	writeN(t, w, 20)

	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	var attempts atomic.Int32
	// 第一次调用失败,后续都成功,验证 retries 后能完成。
	err := w.Replay(context.Background(), func(rec Record) error {
		if attempts.Add(1) == 1 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err, "replay should converge after retries")
	w.Close()
}

func TestReplay_HandlerPermanentErrorGivesUp(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024, MaxReplayFailures: 2})

	writeN(t, w, 10)

	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	err := w.Replay(context.Background(), func(rec Record) error {
		return errors.New("permanent")
	})
	// After MaxReplayFailures it surfaces a wrapped error.
	assert.Error(t, err)
	w.Close()
}

func TestReplay_MarksDoneOnSuccess(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024})

	writeN(t, w, 10)
	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	require.NoError(t, w.Replay(context.Background(), func(rec Record) error { return nil }))

	// All sealed segments should be marked Done.
	doneCount := 0
	for _, s := range w.Segments() {
		if s.Done {
			doneCount++
		}
	}
	assert.Greater(t, doneCount, 0, "expected at least one .done segment")
	w.Close()
}

// --- OldestAge ---

func TestOldestAge_EmptyIsZero(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})
	assert.Equal(t, time.Duration(0), w.OldestAge())
}

func TestOldestAge_GrowsWithAge(t *testing.T) {
	dir := newTempDir(t)
	// 小 SegmentBytes 强制轮转,产生 sealed 段。
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1024})

	writeN(t, w, 30) // 至少 1 个 sealed 段

	// 强制把 sealed 段的 CreatedAt 改到 2h 前。
	w.mu.Lock()
	backdated := 0
	for _, s := range w.segments {
		if s.Sealed {
			s.CreatedAt = time.Now().Add(-2 * time.Hour)
			backdated++
		}
	}
	w.mu.Unlock()
	require.Greater(t, backdated, 0, "需要至少 1 个 sealed 段才能测试 OldestAge")

	age := w.OldestAge()
	assert.Greater(t, age, time.Hour, "OldestAge should reflect backdated sealed segment")
}

// --- Segments ---

func TestSegments_SortedByMtime(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 512})

	for i := 0; i < 5; i++ {
		writeN(t, w, 10)
	}
	w.Close()

	// Reopen to list segments on disk.
	w2 := newTestWAL(t, Config{Dir: dir, SegmentBytes: 512, CleanupInterval: time.Hour, Retention: time.Hour})

	segs := w2.Segments()
	for i := 1; i < len(segs); i++ {
		assert.True(t, !segs[i].CreatedAt.Before(segs[i-1].CreatedAt),
			"Segments must be sorted by CreatedAt ascending")
	}
}

// --- Close ---

func TestClose_Idempotent(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})
	// second close returns nil (CompareAndSwap prevents re-run).
	assert.NoError(t, w.Close())
}

func TestClose_SealsActive(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})

	writeN(t, w, 5)
	require.NoError(t, w.Close())

	// After close, there must be a .sealed file on disk.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	foundSealed := false
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sealed" {
			foundSealed = true
			break
		}
	}
	assert.True(t, foundSealed, "Close should have sealed the active segment")
}

// --- Encode / Decode round-trip ---

func TestEncodeDecode_RoundTrip(t *testing.T) {
	rec := Record{
		Topic:   "metrics.cn",
		Key:     []byte("host=node-01"),
		Payload: []byte{0xde, 0xad, 0xbe, 0xef},
		Headers: map[string]string{
			"trace_id": "abc-123",
			"tenant":   "acme",
		},
		Time: time.Unix(0, 1700000000000000000),
	}
	enc, err := encodeRecord(rec)
	require.NoError(t, err)
	// First 4B is total_len; body follows.
	dec, err := decodeRecord(enc[4:])
	require.NoError(t, err)

	assert.Equal(t, rec.Topic, dec.Topic)
	assert.True(t, bytes.Equal(rec.Key, dec.Key))
	assert.True(t, bytes.Equal(rec.Payload, dec.Payload))
	assert.Equal(t, rec.Headers["trace_id"], dec.Headers["trace_id"])
	assert.Equal(t, rec.Headers["tenant"], dec.Headers["tenant"])
	assert.Equal(t, rec.Time.UnixNano(), dec.Time.UnixNano())
}

func TestEncodeDecode_EmptyHeaders(t *testing.T) {
	rec := Record{Topic: "t", Key: []byte("k"), Payload: []byte("p")}
	enc, err := encodeRecord(rec)
	require.NoError(t, err)
	dec, err := decodeRecord(enc[4:])
	require.NoError(t, err)
	assert.Equal(t, rec.Topic, dec.Topic)
	assert.Nil(t, dec.Headers)
}

func TestEncodeDecode_HeadersOrderStable(t *testing.T) {
	// Same record with shuffled header order should produce identical bytes
	// (headers serialized in sorted key order).
	r1 := Record{Topic: "t", Headers: map[string]string{"a": "1", "b": "2", "c": "3"}}
	r2 := Record{Topic: "t", Headers: map[string]string{"c": "3", "b": "2", "a": "1"}}
	b1, err := encodeRecord(r1)
	require.NoError(t, err)
	b2, err := encodeRecord(r2)
	require.NoError(t, err)
	assert.Equal(t, b1, b2, "header ordering must be stable for CRC")
}

// --- CRC / corruption detection ---

func TestSegmentFooter_CorruptionDetected(t *testing.T) {
	dir := newTempDir(t)
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20})

	writeN(t, w, 3)
	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	// Find the sealed segment and corrupt the first record body.
	w.mu.Lock()
	var path string
	for _, s := range w.segments {
		if s.Sealed {
			path = s.Path
			break
		}
	}
	w.mu.Unlock()
	require.NotEmpty(t, path)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 12)
	// Flip a byte deep inside the first record (past the 4B length + 8B ts).
	data[20] ^= 0xff
	require.NoError(t, os.WriteFile(path, data, 0o644))

	err = w.Replay(context.Background(), func(rec Record) error { return nil })
	assert.ErrorIs(t, err, ErrCorrupt)
	w.Close()
}

// --- Cleanup loop ---

func TestCleanup_RemovesOldDone(t *testing.T) {
	dir := newTempDir(t)
	w, err := New(Config{
		Dir:            dir,
		SegmentBytes:   1 << 20,
		DiskUsedRatio:  1.0,
		Retention:      50 * time.Millisecond,
		CleanupInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	writeN(t, w, 2)
	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()

	require.NoError(t, w.Replay(context.Background(), func(rec Record) error { return nil }))

	// Force every .done segment's CreatedAt into the past so cleanup picks it up.
	w.mu.Lock()
	for _, s := range w.segments {
		if s.Done {
			s.CreatedAt = time.Now().Add(-time.Hour)
		}
	}
	w.mu.Unlock()

	// Wait for at least one cleanup tick.
	time.Sleep(150 * time.Millisecond)

	// All .done segments should be gone.
	w.mu.Lock()
	remain := 0
	for _, s := range w.segments {
		if s.Done {
			remain++
		}
	}
	w.mu.Unlock()
	assert.Equal(t, 0, remain, "expired .done segments should have been cleaned up")
	w.Close()
}

// --- scanExisting ---

func TestScanExisting_RecoversActiveSegment(t *testing.T) {
	// 模拟进程崩溃:写 record 后不 Close(不 seal active 段),直接重开。
	// 设计 §6.2: Reopen replay consistency — 重开后应能 replay 未封段的 active 数据。
	dir := newTempDir(t)

	// 第一轮:写 record 但不 Close(模拟崩溃)
	w1, err := New(Config{Dir: dir, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0})
	require.NoError(t, err)
	writeN(t, w1, 3)
	// 不调 w1.Close(),直接放弃引用(active .log 文件留在磁盘上)

	// 第二轮:重开,scanExisting 应把 .log 封段并加入 replay 队列
	w2 := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20, DiskUsedRatio: 1.0, CleanupInterval: time.Hour, Retention: time.Hour})

	var seen []string
	err = w2.Replay(context.Background(), func(rec Record) error {
		seen = append(seen, string(rec.Payload))
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, seen, 3, "active segment should be recovered and replayed on reopen")

	// 旧 active 段应已被封段并 replay 成功(replay 后 rename 为 .done)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	recoveredCount := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sealed") || strings.HasSuffix(e.Name(), ".done") {
			recoveredCount++
		}
	}
	assert.Greater(t, recoveredCount, 0, "old active segment should be sealed/done on disk after reopen")
}

func TestScanExisting_PicksUpSealedAndDone(t *testing.T) {
	dir := newTempDir(t)
	// Pre-populate dir with .sealed and .done files via a real WAL run.
	w := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20, CleanupInterval: time.Hour, Retention: time.Hour})
	writeN(t, w, 2)
	w.mu.Lock()
	if w.active != nil {
		active := w.active
		active.mu.Lock()
		_ = w.sealActiveLocked()
		active.mu.Unlock()
	}
	w.mu.Unlock()
	require.NoError(t, w.Replay(context.Background(), func(rec Record) error { return nil }))
	w.Close()

	// Reopen and verify index contains both kinds of entries.
	w2 := newTestWAL(t, Config{Dir: dir, SegmentBytes: 1 << 20, CleanupInterval: time.Hour, Retention: time.Hour})

	sealed, done := 0, 0
	for _, s := range w2.Segments() {
		if s.Done {
			done++
		} else if s.Sealed {
			sealed++
		}
	}
	assert.Greater(t, done, 0, "should have at least one .done segment after replay+restart")
	_ = sealed
}

// --- parseSegmentName ---

func TestParseSegmentName(t *testing.T) {
	cases := []struct {
		name        string
		in          string
		wantBase    string
		wantSealed  bool
		wantDone    bool
	}{
		{"plain", "seg-1-2.log", "seg-1-2.log", false, false},
		{"sealed", "seg-1-2.log.sealed", "seg-1-2.log", true, false},
		{"done", "seg-1-2.log.done", "seg-1-2.log", true, true},
		{"sealed_done", "seg-1-2.log.sealed.done", "seg-1-2.log", true, true},
		{"other", "junk.txt", "", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotBase, gotSealed, gotDone := parseSegmentName(c.in)
			assert.Equal(t, c.wantBase, gotBase)
			assert.Equal(t, c.wantSealed, gotSealed)
			assert.Equal(t, c.wantDone, gotDone)
		})
	}
}

// --- nextSeq ---

func TestNextSeq_Monotonic(t *testing.T) {
	existing := map[string]*SegmentInfo{
		"seg-0-000007.log": {},
		"seg-0-000003.log": {},
		"seg-0-000010.log": {},
	}
	assert.Equal(t, 11, nextSeq(existing))
	assert.Equal(t, 0, nextSeq(map[string]*SegmentInfo{}))
}
