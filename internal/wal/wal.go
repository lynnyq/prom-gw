// Package wal 实现磁盘 WAL(Write-Ahead Log),作为 Kafka 不可用时的第三道防线(spec 6.2)。
//
// # 存储布局
//
//   - <dir>/seg-<ts>-<seq>.log          // active 段,正在写
//   - <dir>/seg-<ts>-<seq>.log.sealed   // 已关闭段,fsync'd,可被 replay
//   - <dir>/seg-<ts>-<seq>.log.done     // replay 成功,等待后台清理
//
// # Record 格式(per record, 大端 binary)
//
//	[4B total_len]      // 后续所有字节长度
//	[8B ts]             // record 写入时间(Unix nano)
//	[1B flags]          // 0=ok
//	[2B topic_len]
//	[topic_bytes]
//	[4B key_len]
//	[key_bytes]
//	[4B payload_len]
//	[payload_bytes]
//	[4B headers_len]
//	[headers_serialized]   // [2B name_len][name][4B value_len][value]...
//
// # Segment footer(per segment)
//
//	[4B magic = "PWAL"]
//	[4B record_count]
//	[8B CRC32 over all record bytes(不含 footer 自身)]
//
// # 容量控制
//
//   - Bytes() >= MaxBytes → ErrWALFull(receiver 映射 503)
//   - 磁盘使用率 >= DiskUsedRatio → ErrWALFull(v1.1 启用 syscall.Statfs)
//
// # 重放语义
//
// Replay 按 mtime 顺序逐段读取(过滤 .tmp),调 handler;handler 返回 nil 才把段标记为 .done;
// handler 失败则重试 + 退避,失败累计超 MaxReplayFailures 则告警。
package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// 错误定义。
var (
	// ErrWALFull 容量已满(双阈值:字节数 / 磁盘使用率)。
	// receiver 映射 503。
	ErrWALFull = errors.New("wal: capacity full")
	// ErrClosed WAL 已关闭。
	ErrClosed = errors.New("wal: closed")
	// ErrCorrupt 段文件 CRC 校验失败或 magic 不匹配。
	ErrCorrupt = errors.New("wal: segment corrupt")
	// ErrTruncated 段文件被截断(可能正在被写)。
	ErrTruncated = errors.New("wal: segment truncated")
)

// 默认配置常量(可被 Config 覆盖)。
const (
	DefaultDir               = "/data/wal"
	DefaultSegmentBytes      = 64 * 1024 * 1024        // 64MB
	DefaultMaxBytes          = 50 * 1024 * 1024 * 1024 // 50GB
	DefaultDiskUsedRatio     = 0.80
	DefaultCleanupInterval   = 60 * time.Second
	DefaultRetention         = 24 * time.Hour
	DefaultMaxReplayFailures = 10

	// 段 footer 大小(4B magic + 4B record_count + 8B CRC)
	segmentFooterSize = 16

	// record header 固定部分大小(不含 total_len 自身)
	// ts(8) + flags(1) + topic_len(2) + key_len(4) + payload_len(4) + headers_len(4) = 23
	recordFixedBody = 8 + 1 + 2 + 4 + 4 + 4
)

// segmentMagic 段 footer 4B 魔数。
var segmentMagic = [4]byte{'P', 'W', 'A', 'L'}

// Record 单条 WAL 记录。
type Record struct {
	Topic   string
	Key     []byte
	Payload []byte
	Headers map[string]string
	Time    time.Time // 写入时间
}

// Config WAL 配置。
type Config struct {
	// Dir 数据目录,需独立挂载(部署文档强制)。
	// 默认 /data/wal。
	Dir string
	// SegmentBytes 单段最大字节数,默认 64MB。
	SegmentBytes int64
	// MaxBytes WAL 总字节上限(到达后切硬拒绝),默认 50GB。
	MaxBytes int64
	// DiskUsedRatio 磁盘使用率硬阈值(0-1),默认 0.80。
	// v1.1: 使用 syscall.Statfs 实现。
	DiskUsedRatio float64
	// CleanupInterval 已确认段清理间隔,默认 60s。
	CleanupInterval time.Duration
	// Retention 已确认段保留时长(超过后被清理),默认 24h。
	Retention time.Duration
	// MaxReplayFailures 单段重放失败上限,默认 10。
	MaxReplayFailures int
}

// SegmentInfo 段文件元信息。
type SegmentInfo struct {
	Path        string    // 完整路径
	BaseName    string    // 文件名(不含 .sealed/.done 后缀)
	Size        int64     // 段大小(含 footer)
	RecordCount int       // footer 中的 record_count
	CreatedAt   time.Time // mtime
	Sealed      bool      // 是否有 .sealed 后缀
	Done        bool      // 是否有 .done 后缀
}

// fileWAL 内部实现。
type fileWAL struct {
	cfg Config

	mu       sync.Mutex
	active   *activeSegment
	segments map[string]*SegmentInfo // basename -> info(已 close 的段)
	bytes    atomic.Int64            // 当前总占用字节
	closed   atomic.Bool

	doneCh chan struct{} // 关闭信号
}

// activeSegment 当前正在写的段。
type activeSegment struct {
	mu          sync.Mutex
	f           *os.File
	path        string
	baseName    string
	written     int64     // 已写字节数(不含 footer)
	recordCount int       // 已写 record 数
	hasher      hashSum32 // 段 CRC32 累加
}

// hashSum32 接口化便于测试替换。
type hashSum32 interface {
	Write(p []byte) (n int, err error)
	Sum32() uint32
	Reset()
}

// WAL 接口。
type WAL interface {
	// Write 写入一条 record,语义同步: 落盘 + fsync 后返回。
	Write(ctx context.Context, rec Record) error
	// Replay 按 mtime 顺序逐段重放,handler 返回 nil 才把段标记为 .done。
	Replay(ctx context.Context, handler func(rec Record) error) error
	// Bytes 当前总占用字节(active + 已 close 段)。
	Bytes() int64
	// OldestAge 最老未确认段(未 .done)的存活时长;全 .done 时返回 0。
	OldestAge() time.Duration
	// Segments 返回所有已知段(便于运维查询)。
	Segments() []SegmentInfo
	// Close 排空并关闭文件;后台清理 goroutine 退出。
	Close() error
}

// New 创建并初始化 WAL。
func New(cfg Config) (*fileWAL, error) {
	if cfg.Dir == "" {
		cfg.Dir = DefaultDir
	}
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = DefaultSegmentBytes
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = DefaultMaxBytes
	}
	if cfg.DiskUsedRatio <= 0 {
		cfg.DiskUsedRatio = DefaultDiskUsedRatio
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = DefaultCleanupInterval
	}
	if cfg.Retention <= 0 {
		cfg.Retention = DefaultRetention
	}
	if cfg.MaxReplayFailures <= 0 {
		cfg.MaxReplayFailures = DefaultMaxReplayFailures
	}

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", cfg.Dir, err)
	}

	w := &fileWAL{
		cfg:      cfg,
		segments: make(map[string]*SegmentInfo),
		doneCh:   make(chan struct{}),
	}

	// 扫描已有段
	if err := w.scanExisting(); err != nil {
		return nil, fmt.Errorf("wal: scan: %w", err)
	}

	// 打开 active 段
	if err := w.openNewActive(); err != nil {
		return nil, fmt.Errorf("wal: open active: %w", err)
	}

	// 启动 cleanup goroutine
	w.startCleanup()

	return w, nil
}

// --- 启动辅助 ---

// scanExisting 扫描目录,把已有段加入索引。
//
// 对于未封段的 .log 文件(active 段,可能是上次进程崩溃遗留),
// 读取所有有效 record、写 footer、rename 为 .sealed,使其可被 Replay。
// 设计 §6.2: "Reopen replay consistency"。
func (w *fileWAL) scanExisting() error {
	entries, err := os.ReadDir(w.cfg.Dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		baseName, sealed, done := parseSegmentName(name)
		if baseName == "" {
			continue
		}
		// active(.log)段在重开时需要封段,使其可被 Replay。
		if !sealed && !done {
			path := filepath.Join(w.cfg.Dir, name)
			if err := w.sealRecoveredSegment(path, baseName); err != nil {
				// 封段失败(空文件 / 损坏)→ 删除并跳过
				_ = os.Remove(path)
				continue
			}
			continue // sealRecoveredSegment 已更新 segments 和 bytes
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		w.segments[baseName] = &SegmentInfo{
			Path:      filepath.Join(w.cfg.Dir, name),
			BaseName:  baseName,
			Size:      info.Size(),
			CreatedAt: info.ModTime(),
			Sealed:    sealed,
			Done:      done,
		}
		w.bytes.Add(info.Size())
	}
	return nil
}

// sealRecoveredSegment 把一个未封段的 .log 文件封段:
// 读取所有有效 record(遇到截断/损坏则停止)、计算 CRC、写 footer、rename 为 .sealed。
// 成功后把段加入 segments 索引并更新 bytes。
func (w *fileWAL) sealRecoveredSegment(path, baseName string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}

	hasher := crc32.NewIEEE()
	recordCount := 0
	var lastValidOffset int64 = 0

	for {
		header := make([]byte, 4)
		_, err := io.ReadFull(f, header)
		if err != nil {
			break // EOF 或截断
		}
		totalLen := binary.BigEndian.Uint32(header)
		if totalLen < uint32(recordFixedBody) || totalLen > 64*1024*1024 {
			break // 无效 record 头
		}
		body := make([]byte, totalLen)
		_, err = io.ReadFull(f, body)
		if err != nil {
			break // record 被截断
		}
		_, _ = hasher.Write(header)
		_, _ = hasher.Write(body)
		recordCount++
		lastValidOffset += int64(4 + totalLen)
	}

	// 空段(无有效 record)→ 无需封段,直接返回错误让调用方删除
	if recordCount == 0 {
		_ = f.Close()
		return fmt.Errorf("wal: no valid records in %s", baseName)
	}

	// 截断可能的部分写入(最后一条不完整的 record 之后的数据)
	if err := f.Truncate(lastValidOffset); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return err
	}

	// 写 footer
	crc := hasher.Sum32()
	footer := make([]byte, segmentFooterSize)
	copy(footer[0:4], segmentMagic[:])
	binary.BigEndian.PutUint32(footer[4:8], uint32(recordCount))
	binary.BigEndian.PutUint64(footer[8:16], uint64(crc))
	if _, err := f.Write(footer); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}

	sealedPath := path + ".sealed"
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, sealedPath); err != nil {
		return err
	}

	info, _ := os.Stat(sealedPath)
	w.segments[baseName] = &SegmentInfo{
		Path:        sealedPath,
		BaseName:    baseName,
		Size:        info.Size(),
		RecordCount: recordCount,
		CreatedAt:   info.ModTime(),
		Sealed:      true,
	}
	w.bytes.Add(info.Size())
	return nil
}

// openNewActive 打开一个新 active 段。
func (w *fileWAL) openNewActive() error {
	seq := nextSeq(w.segments)
	baseName := fmt.Sprintf("seg-%d-%06d.log", time.Now().UnixNano(), seq)
	path := filepath.Join(w.cfg.Dir, baseName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.active = &activeSegment{
		f:        f,
		path:     path,
		baseName: baseName,
		hasher:   crc32.NewIEEE(),
	}
	return nil
}

// nextSeq 返回下一个可用的段序号。
func nextSeq(existing map[string]*SegmentInfo) int {
	maxSeq := -1
	for n := range existing {
		parts := strings.Split(strings.TrimSuffix(n, ".log"), "-")
		if len(parts) < 3 {
			continue
		}
		seq, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq + 1
}

// parseSegmentName 解析段文件名,返回 (baseName, sealed, done)。
// 顺序重要:先匹配最长的复合后缀。
func parseSegmentName(name string) (string, bool, bool) {
	switch {
	case strings.HasSuffix(name, ".log.sealed.done"):
		return strings.TrimSuffix(name, ".sealed.done"), true, true
	case strings.HasSuffix(name, ".log.sealed"):
		return strings.TrimSuffix(name, ".sealed"), true, false
	case strings.HasSuffix(name, ".log.done"):
		return strings.TrimSuffix(name, ".done"), true, true
	case strings.HasSuffix(name, ".log"):
		return name, false, false
	default:
		return "", false, false
	}
}

// --- Write ---

// Write 写入一条 record。语义: 编码 → 容量检查 → 落盘 → fsync → 段轮转。
//
// 返回 ErrWALFull 时 receiver 映射 503(spec 6.1)。
func (w *fileWAL) Write(ctx context.Context, rec Record) error {
	if w.closed.Load() {
		return ErrClosed
	}
	if rec.Topic == "" {
		return errors.New("wal: record topic required")
	}

	// 1. 容量检查
	if err := w.checkCapacity(); err != nil {
		return err
	}

	// 2. 编码
	if rec.Time.IsZero() {
		rec.Time = time.Now()
	}
	encoded, err := encodeRecord(rec)
	if err != nil {
		return fmt.Errorf("wal: encode: %w", err)
	}

	// 3. 写入 active 段(可能触发轮转)
	w.mu.Lock()
	active := w.active
	w.mu.Unlock()
	if active == nil {
		return errors.New("wal: no active segment")
	}

	active.mu.Lock()
	// 检查是否需要轮转
	if active.written+int64(len(encoded))+segmentFooterSize > w.cfg.SegmentBytes {
		if err := w.sealActiveLocked(); err != nil {
			active.mu.Unlock()
			return fmt.Errorf("wal: rotate: %w", err)
		}
		if err := w.openNewActive(); err != nil {
			active.mu.Unlock()
			return fmt.Errorf("wal: open new: %w", err)
		}
		active = w.active
		active.mu.Lock()
	}

	// 写文件 + 更新 CRC
	n, err := active.f.Write(encoded)
	if err != nil {
		active.mu.Unlock()
		return fmt.Errorf("wal: write: %w", err)
	}
	_, _ = active.hasher.Write(encoded)
	active.written += int64(n)
	active.recordCount++

	// 4. fsync
	if err := active.f.Sync(); err != nil {
		active.mu.Unlock()
		return fmt.Errorf("wal: sync: %w", err)
	}
	active.mu.Unlock()

	// 5. 更新全局字节数
	w.bytes.Add(int64(n))

	return nil
}

// checkCapacity 双阈值硬拒绝(spec 6.2 第三道防线 / plan T1.8):
//
//  1. WAL 字节数 ≥ MaxBytes
//  2. WAL 所在磁盘使用率 ≥ DiskUsedRatio
//
// 任意一个触发 → 返回 ErrWALFull(receiver 映射 503)。
//
// 性能:syscall.Statfs 是快速调用(< 1ms),调用频率与 Write 一致;
// 真实部署 wal.dir 一般是独立 SSD 挂载,Statfs 走底层 fs 统计无锁。
func (w *fileWAL) checkCapacity() error {
	if w.bytes.Load() >= w.cfg.MaxBytes {
		return ErrWALFull
	}
	if used, ok := diskUsedRatio(w.cfg.Dir); ok && used >= w.cfg.DiskUsedRatio {
		return ErrWALFull
	}
	return nil
}

// diskUsedRatio 返回 dir 所在文件系统的"已用比例"(used/total)。
// 调用 syscall.Statfs,darwin / linux 行为一致(statfs 块单位为 bsize)。
//
// 实现要点:
//   - 仅依赖 syscall.Statfs 块大小,不需要 bsize 计算 total
//   - 失败(权限 / unmount / 跨平台) → 返回 (0, false),让 checkCapacity 跳过此阈值
//   - 为避免 statfs 抖动,本函数不缓存(每次 Write 调用一次)
func diskUsedRatio(dir string) (float64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, false
	}
	// 避免 total==0 导致除零;此情形视为未知,跳过阈值检查
	if stat.Blocks == 0 {
		return 0, false
	}
	// 块使用率 = (Blocks - Bavail) / Blocks
	// Bavail 是非 root 用户可用的块数(可能小于 Bfree),更接近"实际已用"。
	used := float64(stat.Blocks-stat.Bavail) / float64(stat.Blocks)
	return used, true
}

// sealActiveLocked 把 active 段 flush + 写 footer + rename 为 .sealed。
// 调用方必须持有 active.mu;w.mu 由调用方按需锁定。
func (w *fileWAL) sealActiveLocked() error {
	return w.sealActiveSegment(w.active)
}

// sealActiveSegment 封指定段并把它从 active 摘除,加入到 sealed 索引。
// 抽象出来便于 Close 在不修改 w.active 的情况下封段。
func (w *fileWAL) sealActiveSegment(active *activeSegment) error {
	if active == nil {
		return nil
	}
	// 写 footer
	crc := active.hasher.Sum32()
	footer := make([]byte, segmentFooterSize)
	copy(footer[0:4], segmentMagic[:])
	binary.BigEndian.PutUint32(footer[4:8], uint32(active.recordCount))
	binary.BigEndian.PutUint64(footer[8:16], uint64(crc))
	if _, err := active.f.Write(footer); err != nil {
		return err
	}
	if err := active.f.Sync(); err != nil {
		return err
	}

	// rename 为 .sealed
	sealedPath := active.path + ".sealed"
	if err := active.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(active.path, sealedPath); err != nil {
		return err
	}

	// 加入 segments 索引
	info, _ := os.Stat(sealedPath)
	w.segments[active.baseName] = &SegmentInfo{
		Path:        sealedPath,
		BaseName:    active.baseName,
		Size:        info.Size(),
		RecordCount: active.recordCount,
		CreatedAt:   info.ModTime(),
		Sealed:      true,
	}
	// 只在封的就是当前 active 时才清空引用。
	if w.active == active {
		w.active = nil
	}
	return nil
}

// --- Replay ---

// Replay 按 mtime 顺序逐段读取,调 handler;handler 返回 nil 才标记 .done。
func (w *fileWAL) Replay(ctx context.Context, handler func(rec Record) error) error {
	if w.closed.Load() {
		return ErrClosed
	}

	// 1. 收集未 .done 的 sealed 段
	w.mu.Lock()
	type segItem struct {
		info *SegmentInfo
	}
	var segs []segItem
	for _, info := range w.segments {
		if info.Sealed && !info.Done {
			segs = append(segs, segItem{info: info})
		}
	}
	w.mu.Unlock()
	if len(segs) == 0 {
		return nil
	}
	// 按 mtime 排序
	sort.Slice(segs, func(i, j int) bool {
		return segs[i].info.CreatedAt.Before(segs[j].info.CreatedAt)
	})

	// 2. 逐段处理
	for _, s := range segs {
		if err := w.replaySegment(ctx, s.info, handler); err != nil {
			return err
		}
	}
	return nil
}

// replaySegment 处理单段;失败重试 + 退避。
func (w *fileWAL) replaySegment(ctx context.Context, info *SegmentInfo, handler func(rec Record) error) error {
	var lastErr error
	for attempt := 0; attempt < w.cfg.MaxReplayFailures; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := w.readSegment(info.Path, handler)
		if err == nil {
			// 成功:rename 为 .done
			donePath := info.Path + ".done"
			if err := os.Rename(info.Path, donePath); err == nil {
				w.mu.Lock()
				info.Done = true
				info.Path = donePath
				w.mu.Unlock()
			}
			return nil
		}
		lastErr = err
		// 退避
		backoff := time.Duration(100*(1<<attempt)) * time.Millisecond
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return fmt.Errorf("wal: replay %s failed after %d attempts: %w",
		info.BaseName, w.cfg.MaxReplayFailures, lastErr)
}

// readSegment 读单段并逐条调 handler。
//
// 先读 footer 拿到 recordCount 和 expected CRC,再按 recordCount 循环读 record,
// 避免把 footer 的 magic 误判成 record 头。
func (w *fileWAL) readSegment(path string, handler func(rec Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()
	if fileSize < int64(segmentFooterSize) {
		return fmt.Errorf("%w: file too small (%d bytes)", ErrTruncated, fileSize)
	}

	// 读 footer
	if _, err := f.Seek(fileSize-int64(segmentFooterSize), io.SeekStart); err != nil {
		return err
	}
	footer := make([]byte, segmentFooterSize)
	if _, err := io.ReadFull(f, footer); err != nil {
		return fmt.Errorf("%w: missing footer: %v", ErrTruncated, err)
	}
	if !bytes.Equal(footer[0:4], segmentMagic[:]) {
		return fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	recordCount := binary.BigEndian.Uint32(footer[4:8])
	expectedCRC := binary.BigEndian.Uint64(footer[8:16])

	// 回到开头,按 recordCount 逐条读
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hasher := crc32.NewIEEE()
	for i := uint32(0); i < recordCount; i++ {
		header := make([]byte, 4)
		if _, err := io.ReadFull(f, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("%w: missing header at record %d: %v", ErrTruncated, i, err)
			}
			return err
		}
		totalLen := binary.BigEndian.Uint32(header)
		if totalLen < uint32(recordFixedBody) {
			return fmt.Errorf("%w: invalid record total_len %d at %d", ErrCorrupt, totalLen, i)
		}
		body := make([]byte, totalLen)
		if _, err := io.ReadFull(f, body); err != nil {
			return fmt.Errorf("%w: mid-record at %d: %v", ErrTruncated, i, err)
		}
		// 累加 CRC(覆盖 total_len + body,跟写入端一致)
		_, _ = hasher.Write(header)
		_, _ = hasher.Write(body)

		// 解码
		rec, err := decodeRecord(body)
		if err != nil {
			return fmt.Errorf("%w: decode at %d: %v", ErrCorrupt, i, err)
		}
		if err := handler(rec); err != nil {
			return err
		}
	}

	// 验证 CRC
	if uint64(hasher.Sum32()) != expectedCRC {
		return fmt.Errorf("%w: crc mismatch (got %x, want %x)", ErrCorrupt, hasher.Sum32(), expectedCRC)
	}
	return nil
}

// --- Close ---

// Close 排空 active + 关闭所有文件 + 退出后台 goroutine。
func (w *fileWAL) Close() error {
	if !w.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(w.doneCh)

	w.mu.Lock()
	active := w.active
	w.mu.Unlock()
	if active != nil {
		active.mu.Lock()
		_ = w.sealActiveSegment(active)
		active.mu.Unlock()
	}
	return nil
}

// --- 查询 ---

// Bytes 当前总占用字节。
func (w *fileWAL) Bytes() int64 {
	return w.bytes.Load()
}

// OldestAge 最老未确认段(sealed 但未 .done)的存活时长。
// 监控语义:这些段是已落盘但还没成功投递到 Kafka 的数据。
func (w *fileWAL) OldestAge() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	var oldest time.Time
	for _, info := range w.segments {
		if info.Done || !info.Sealed {
			continue
		}
		if oldest.IsZero() || info.CreatedAt.Before(oldest) {
			oldest = info.CreatedAt
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest)
}

// Segments 返回所有已知段。
func (w *fileWAL) Segments() []SegmentInfo {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]SegmentInfo, 0, len(w.segments))
	for _, info := range w.segments {
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

// --- 编码 / 解码 ---

// encodeRecord 把 Record 序列化为二进制。
func encodeRecord(r Record) ([]byte, error) {
	topicB := []byte(r.Topic)
	headersB, err := encodeHeaders(r.Headers)
	if err != nil {
		return nil, err
	}
	bodyLen := recordFixedBody + len(topicB) + len(r.Key) + len(r.Payload) + len(headersB)
	out := make([]byte, 0, 4+bodyLen)
	buf := make([]byte, 8)

	// 1. total_len(占位,最后写)
	out = append(out, 0, 0, 0, 0)
	// 2. ts
	binary.BigEndian.PutUint64(buf, uint64(r.Time.UnixNano()))
	out = append(out, buf...)
	// 3. flags
	out = append(out, 0)
	// 4. topic_len(2B) + topic
	binary.BigEndian.PutUint16(buf[:2], uint16(len(topicB)))
	out = append(out, buf[:2]...)
	out = append(out, topicB...)
	// 5. key_len(4B) + key
	binary.BigEndian.PutUint32(buf, uint32(len(r.Key)))
	out = append(out, buf[:4]...)
	out = append(out, r.Key...)
	// 6. payload_len(4B) + payload
	binary.BigEndian.PutUint32(buf, uint32(len(r.Payload)))
	out = append(out, buf[:4]...)
	out = append(out, r.Payload...)
	// 7. headers_len(4B) + headers
	binary.BigEndian.PutUint32(buf, uint32(len(headersB)))
	out = append(out, buf[:4]...)
	out = append(out, headersB...)

	// 8. 写回 total_len
	binary.BigEndian.PutUint32(out[0:4], uint32(bodyLen))
	return out, nil
}

// decodeRecord 从 record body 解码 Record。body 不含 total_len 前缀。
func decodeRecord(body []byte) (Record, error) {
	if len(body) < recordFixedBody {
		return Record{}, errors.New("body too short")
	}
	r := bytes.NewReader(body)
	var tmp [8]byte

	// ts
	if _, err := io.ReadFull(r, tmp[:8]); err != nil {
		return Record{}, err
	}
	ts := int64(binary.BigEndian.Uint64(tmp[:8]))

	// flags
	flagsB := make([]byte, 1)
	if _, err := io.ReadFull(r, flagsB); err != nil {
		return Record{}, err
	}
	_ = flagsB[0]

	// topic_len
	if _, err := io.ReadFull(r, tmp[:2]); err != nil {
		return Record{}, err
	}
	topicLen := int(binary.BigEndian.Uint16(tmp[:2]))
	topicB := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicB); err != nil {
		return Record{}, err
	}

	// key_len
	if _, err := io.ReadFull(r, tmp[:4]); err != nil {
		return Record{}, err
	}
	keyLen := int(binary.BigEndian.Uint32(tmp[:4]))
	keyB := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyB); err != nil {
		return Record{}, err
	}

	// payload_len
	if _, err := io.ReadFull(r, tmp[:4]); err != nil {
		return Record{}, err
	}
	payloadLen := int(binary.BigEndian.Uint32(tmp[:4]))
	payloadB := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payloadB); err != nil {
		return Record{}, err
	}

	// headers_len
	if _, err := io.ReadFull(r, tmp[:4]); err != nil {
		return Record{}, err
	}
	headersLen := int(binary.BigEndian.Uint32(tmp[:4]))
	headersB := make([]byte, headersLen)
	if _, err := io.ReadFull(r, headersB); err != nil {
		return Record{}, err
	}

	headers, err := decodeHeaders(headersB)
	if err != nil {
		return Record{}, err
	}
	return Record{
		Topic:   string(topicB),
		Key:     keyB,
		Payload: payloadB,
		Headers: headers,
		Time:    time.Unix(0, ts),
	}, nil
}

// encodeHeaders 序列化 headers 为 [2B name_len][name][4B value_len][value]... 序列。
// 按 name 排序保证 CRC 稳定。
func encodeHeaders(h map[string]string) ([]byte, error) {
	if len(h) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []byte
	buf := make([]byte, 4)
	for _, k := range keys {
		v := h[k]
		binary.BigEndian.PutUint16(buf[:2], uint16(len(k)))
		out = append(out, buf[:2]...)
		out = append(out, k...)
		binary.BigEndian.PutUint32(buf, uint32(len(v)))
		out = append(out, buf...)
		out = append(out, v...)
	}
	return out, nil
}

// decodeHeaders 反序列化 headers。
func decodeHeaders(b []byte) (map[string]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r := bytes.NewReader(b)
	out := make(map[string]string)
	var tmp [4]byte
	for r.Len() > 0 {
		if _, err := io.ReadFull(r, tmp[:2]); err != nil {
			return nil, err
		}
		nameLen := int(binary.BigEndian.Uint16(tmp[:2]))
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(r, tmp[:4]); err != nil {
			return nil, err
		}
		valueLen := int(binary.BigEndian.Uint32(tmp[:4]))
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, err
		}
		out[string(name)] = string(value)
	}
	return out, nil
}

// --- Cleanup ---

// startCleanup 启动后台清理 goroutine。
func (w *fileWAL) startCleanup() {
	go func() {
		t := time.NewTicker(w.cfg.CleanupInterval)
		defer t.Stop()
		for {
			select {
			case <-w.doneCh:
				return
			case <-t.C:
				w.cleanup()
			}
		}
	}()
}

// cleanup 删除超 Retention 的 .done 段。
func (w *fileWAL) cleanup() {
	w.mu.Lock()
	defer w.mu.Unlock()
	cutoff := time.Now().Add(-w.cfg.Retention)
	for baseName, info := range w.segments {
		if !info.Done {
			continue
		}
		if info.CreatedAt.Before(cutoff) {
			if err := os.Remove(info.Path); err == nil {
				delete(w.segments, baseName)
				w.bytes.Add(-info.Size)
			}
		}
	}
}
