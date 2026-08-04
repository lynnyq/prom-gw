// Package config - history.go: 规则集历史版本 ring buffer(plan T4.6)。
//
// 设计要点:
//   - 内存 ring buffer,最近 N 版(默认 10),超出 LRU 驱逐
//   - 每条记录:R RuleSet(原始 YAML 字节) + 编译后的 CompiledRuleSet
//   - 切换时(Switch/Reload)写入一份;Rollback 时按 version 取回
//   - 线程安全:所有方法持有 mu,handler 走 history 不阻塞 pipeline
//   - 单版字节上限 1MB,超限拒绝入库(spec 默认)
package config

import (
	"errors"
	"sync"

	"github.com/lynnyq/bigdata/internal/ruleengine"
)

// HistoryConfig ring buffer 配置。
type HistoryConfig struct {
	// Capacity 最大保留版本数(默认 10,plan T4.6)
	Capacity int
	// MaxBytesPerVersion 单版字节上限(默认 1MB,plan T4.6)
	MaxBytesPerVersion int
	// OnEvict 驱逐时回调(用于 metric/日志);可为 nil
	OnEvict func(r HistoryRecord)
}

// HistoryRecord 一次保存的快照。
type HistoryRecord struct {
	Name    string
	Version int64
	Bytes   int          // 原始字节数
	RawYAML []byte       // 原始 YAML 字节(用于导出/重编译)
	Source  string       // 写入来源(nacos / file / api)
	Compiled *ruleengine.CompiledRuleSet // 编译产物;Rollback 时直接复用
}

// History 历史版本存储。
type History struct {
	cfg HistoryConfig
	mu  sync.Mutex
	buf []*HistoryRecord
	// 索引:name -> version -> 位置(便于按 version 查找)
	idx map[string]map[int64]int
}

// ErrVersionExists 保存时 version 与已有冲突(spec:仅在不重复时写入;否则覆盖旧条)。
//
// 实际行为:Save 时若同名同 version 已存在,覆盖之(不报错),保持 list 长度稳定。
// 此错误保留给外部调用方决策(目前未使用)。

// ErrVersionNotFound 按 version 取不到时返回。
var ErrVersionNotFound = errors.New("config: history: version not found")

// NewHistory 构造。
func NewHistory(cfg HistoryConfig) *History {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 10
	}
	if cfg.MaxBytesPerVersion <= 0 {
		cfg.MaxBytesPerVersion = 1 << 20 // 1MB
	}
	return &History{
		cfg: cfg,
		idx: make(map[string]map[int64]int),
	}
}

// Save 保存一个版本。
//
// 行为:
//   - 单版字节超过 MaxBytesPerVersion → 返回 error,不写入
//   - 同名同 version 已存在 → 覆盖(更新 RawYAML/Compiled,位置不变)
//   - 同名新 version → 头部插入;超过 Capacity 驱逐尾部最旧
func (h *History) Save(r HistoryRecord) error {
	if len(r.RawYAML) > h.cfg.MaxBytesPerVersion {
		return errors.New("config: history: raw yaml exceeds max bytes")
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	if r.Name == "" {
		return errors.New("config: history: name required")
	}
	if r.Compiled == nil {
		return errors.New("config: history: compiled ruleset nil")
	}

	if m, ok := h.idx[r.Name]; ok {
		if pos, dup := m[r.Version]; dup {
			h.buf[pos] = &r
			return nil
		}
	} else {
		h.idx[r.Name] = make(map[int64]int)
	}

	// 头部插入
	h.buf = append([]*HistoryRecord{&r}, h.buf...)
	// 索引全部右移 +1
	for _, m := range h.idx {
		for k, pos := range m {
			m[k] = pos + 1
		}
	}
	h.idx[r.Name][r.Version] = 0

	// 驱逐
	for len(h.buf) > h.cfg.Capacity {
		evicted := h.buf[len(h.buf)-1]
		h.buf = h.buf[:len(h.buf)-1]
		if m, ok := h.idx[evicted.Name]; ok {
			delete(m, evicted.Version)
			if len(m) == 0 {
				delete(h.idx, evicted.Name)
			}
		}
		if h.cfg.OnEvict != nil {
			h.cfg.OnEvict(*evicted)
		}
	}
	return nil
}

// Get 按 name + version 取一份。
func (h *History) Get(name string, version int64) (HistoryRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.idx[name]
	if !ok {
		return HistoryRecord{}, ErrVersionNotFound
	}
	pos, ok := m[version]
	if !ok {
		return HistoryRecord{}, ErrVersionNotFound
	}
	return *h.buf[pos], nil
}

// Latest 拿某 name 的最新版本(若没有返回 ErrVersionNotFound)。
func (h *History) Latest(name string) (HistoryRecord, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.idx[name]
	if !ok || len(m) == 0 {
		return HistoryRecord{}, ErrVersionNotFound
	}
	// m 是 map,顺序不稳定;找 buf 中 pos 最小的那条
	var best HistoryRecord
	bestPos := -1
	for v, pos := range m {
		if bestPos == -1 || pos < bestPos {
			bestPos = pos
			best = *h.buf[pos]
			_ = v
		}
	}
	return best, nil
}

// List 列出某 name 的所有版本(从新到旧)。
func (h *History) List(name string) []HistoryRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.idx[name]
	if !ok {
		return nil
	}
	// idx 内只存 name 自己的 key,每个 (version, pos) 唯一;不会重复 pos。
	out := make([]HistoryRecord, 0, len(m))
	positions := make([]int, 0, len(m))
	for _, pos := range m {
		positions = append(positions, pos)
	}
	// 按 pos 升序(pos 0 = 最新)
	for i := 0; i < len(positions); i++ {
		for j := i + 1; j < len(positions); j++ {
			if positions[j] < positions[i] {
				positions[i], positions[j] = positions[j], positions[i]
			}
		}
	}
	for _, pos := range positions {
		if pos < len(h.buf) {
			out = append(out, *h.buf[pos])
		}
	}
	return out
}

// Names 列出所有出现过的 name。
func (h *History) Names() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, 0, len(h.idx))
	for n := range h.idx {
		out = append(out, n)
	}
	return out
}

// Size 当前条数。
func (h *History) Size() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.buf)
}

// SizeByName 拿指定 name 的版本数(plan T4.6: 用于 gateway_ruleset_history_size 指标)。
func (h *History) SizeByName(name string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	m, ok := h.idx[name]
	if !ok {
		return 0
	}
	return len(m)
}
