// Package ruleengine - deadvalue.go: 状态型 dead value stage(plan T3.3)。
//
// 用途:跟踪 series 的"最后值 + 最后时间",在 window 时间内值未变 → 丢弃。
// 典型场景:某 metrics exporter 上报进程挂掉后,sample 持续"凝固"成同一个值;
//          死值丢弃可以减少下游 50%+ 的无效存储。
//
// 设计要点:
//
//   - 按 seriesKey 索引,每条 series 维护 (lastValue, lastTs, seenCount)
//   - LRU 控制内存:默认 1M 条 series,超出时驱逐最久未访问的 series
//   - "未变"语义:精确值相等(NaN/Inf 不参与,见 edge cases)
//   - 状态用 atomic.Pointer[deadvalueState] 装载,支持热更新
//   - 内存上界:1M × (8+8+4) ≈ 20MB,符合 spec 9.4 "状态型 stage 内存可控"
//
// 关键行为:
//
//   - 第一条 sample:总是发出(无历史,无法判断"是否变化")
//   - 后续 sample:
//       value 变化           → 发出 + 更新 lastValue/lastTs
//       value 不变 + ts 间隔 > window → 发出(认为是"新的相同值"而非"死值")
//       value 不变 + ts 间隔 ≤ window → 丢弃(死值)
//   - 上述语义等价于:丢弃连续 window 时间内值不变的 sample
//
// edge cases:
//
//   - NaN/Inf:与 lastValue 比较时,NaN != NaN,会触发"变化"分支 → 总是发出
//     (因为 NaN/Inf 通常表示 exporter 异常,值得上报;不静默丢弃)
//   - 同 ts 同 value 的重发:会被识别为死值并丢弃
//   - seriesKey 突变(同一 series 突然换 labels):被识别为新 series,总是发出
package ruleengine

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.opentelemetry.io/otel/attribute"
)

// --- LRU cache ---

// lruEntry 单条 LRU 节点。ll 指针供 list.Element 使用。
type lruEntry struct {
	key   uint64
	value *dvState
}

// dvState 单 series 的 last-emitted 状态。
//
//   - lastValue: 上次发出 sample 的 value(NaN 也按 bit 存,精确比较)
//   - emitTs: 上次发出 sample 的 timestamp(毫秒)
//   - 注意:drop 时不更新这两个字段,保证 window 计时从"上次发出"算
type dvState struct {
	lastValue float64
	emitTs    int64
}

// deadvalueLRU 固定容量的 LRU,使用 container/list 双向链表 + map。
//
// 线程安全:外部调用方需持有 state.mu(或 stage 自己的锁)。
type deadvalueLRU struct {
	cap   int
	items map[uint64]*list.Element
	ll    *list.List // front = 最近使用, back = 最久未用
}

func newDeadvalueLRU(cap int) *deadvalueLRU {
	if cap <= 0 {
		cap = 1
	}
	return &deadvalueLRU{
		cap:   cap,
		items: make(map[uint64]*list.Element, cap),
		ll:    list.New(),
	}
}

// get 取出 key 对应的值,顺便把它移到链表头(标记最近使用)。
// 返回 (state, true) 命中;(nil, false) 未命中。
func (l *deadvalueLRU) get(key uint64) (*dvState, bool) {
	el, ok := l.items[key]
	if !ok {
		return nil, false
	}
	l.ll.MoveToFront(el)
	return el.Value.(*lruEntry).value, true
}

// put 设置 key → state,若超容则驱逐最久未用。
func (l *deadvalueLRU) put(key uint64, state *dvState) {
	if el, ok := l.items[key]; ok {
		l.ll.MoveToFront(el)
		el.Value.(*lruEntry).value = state
		return
	}
	// 超容 → 驱逐末尾
	if l.ll.Len() >= l.cap {
		oldest := l.ll.Back()
		if oldest != nil {
			ent := oldest.Value.(*lruEntry)
			delete(l.items, ent.key)
			l.ll.Remove(oldest)
		}
	}
	el := l.ll.PushFront(&lruEntry{key: key, value: state})
	l.items[key] = el
}

// len 当前 LRU 中元素数(用于 metric)。
func (l *deadvalueLRU) len() int { return l.ll.Len() }

// --- state ---

// deadvalueState 一个编译期 + 运行期的不可变配置 + 运行时可变 LRU。
type deadvalueState struct {
	window time.Duration
	lru    *deadvalueLRU
	mu     sync.Mutex // 保护 LRU(单 state 内串行访问;多 state 由 atomic 切换)
}

func newDeadvalueState(window time.Duration, maxSeries int) *deadvalueState {
	return &deadvalueState{
		window: window,
		lru:    newDeadvalueLRU(maxSeries),
	}
}

// seriesCount 返回 LRU 当前追踪的 series 数(spec 7.1 gateway_state_series)。
//
// 加锁读 LRU 长度;调用频率低(metric 采集),无 hot path 风险。
func (s *deadvalueState) seriesCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.len()
}

// check 返回 shouldEmit:这条 sample 是否发往下游(否则视为死值丢弃)。
//
// 决策表:
//
//	| 命中 LRU | 值变化 | window 检查        | emit? |
//	| -------- | ------ | ------------------ | ----- |
//	| miss     | -      | -                  | true  |
//	| hit      | yes    | -                  | true  |
//	| hit      | no     | 间隔 > window      | true  |
//	| hit      | no     | 间隔 ≤ window      | false |
//
// window=0 含义:无时间阈值,只要值不变就丢。
//
// emit 时更新 lastValue + emitTs;drop 时不更新(保证 window 计时从"上次发出"算,
// 否则 drop 会重置计时,导致连续 dead value 永远不会被重新发出)。
func (s *deadvalueState) check(in parser.Sample) bool {
	key := in.SeriesKey()
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.lru.get(key)
	if !ok {
		// 新 series → 首条总是发出
		s.lru.put(key, &dvState{lastValue: in.Value, emitTs: in.Timestamp})
		return true
	}

	// 值变化(NaN/Inf 特殊:NaN != 任何值,会进这分支 → 总是发出,符合"异常不静默")
	if in.Value != cur.lastValue {
		cur.lastValue = in.Value
		cur.emitTs = in.Timestamp
		return true
	}

	// 值相同,看 ts 间隔是否超过 window(window 锚点是"上次发出")
	if s.window > 0 {
		elapsed := in.Timestamp - cur.emitTs
		if elapsed > s.window.Milliseconds() {
			cur.emitTs = in.Timestamp
			return true
		}
	}

	// 死值 → 丢,不更新 emitTs
	return false
}

// --- stage ---

// DeadValueStage 实现 plan T3.3: 死值丢弃。
type DeadValueStage struct{}

func (DeadValueStage) Name() string { return "deadvalue" }

func (DeadValueStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	windowStr, _ := cfg["window"].(string)
	var window time.Duration
	if windowStr == "" {
		window = 0
	} else {
		var err error
		window, err = time.ParseDuration(windowStr)
		if err != nil || window < 0 {
			return nil, fmt.Errorf("deadvalue: invalid window %q, use Go duration (5m, 1h)", windowStr)
		}
	}
	maxSeries := 0
	if v, ok := cfg["max_series"].(int); ok {
		maxSeries = v
	} else if v, ok := cfg["max_series"].(float64); ok {
		maxSeries = int(v)
	}
	if maxSeries <= 0 {
		maxSeries = 1_000_000 // 1M,符合 spec
	}

	// 状态对象(atomic 装载支持热更新)
	state := atomic.Pointer[deadvalueState]{}
	state.Store(newDeadvalueState(window, maxSeries))

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "deadvalue")
		defer span.End()
		span.SetAttributes(
			attribute.String("window", window.String()),
			attribute.Int("max_series", maxSeries),
		)

		out := prev[:0]
		dropped := 0
		st := state.Load()
		for _, s := range in {
			if st.check(s) {
				out = append(out, s)
			} else {
				dropped++
			}
		}

		span.SetAttributes(
			attribute.Int("input", len(in)),
			attribute.Int("kept", len(out)),
			attribute.Int("dropped", dropped),
		)
		// state 级 metric:当前 LRU 容量
		ingestCity, _ := obs.MetaLabels()
		obs.StageDuration.WithLabelValues("rule_deadvalue", "ok", ingestCity).Observe(0)
		obs.StateSeries.WithLabelValues("deadvalue", ingestCity).Set(float64(st.seriesCount()))
		return out, dropped, nil
	}, nil
}
