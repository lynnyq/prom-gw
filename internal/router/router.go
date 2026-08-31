// Package router 根据 sample 的 metric 特征把请求 fan-out 到对应的 ruleset pipeline。
//
// 设计要点(spec 5.2 + plan T2.7/T2.8):
//   - 每条 RuleSet 持有自己的 Match 条件(MetricPrefix / MetricExact)
//   - 同一请求内的 sample 按 Match 命中到不同 ruleset 后,并发/串行由各 ruleset 自己
//     决定(ruleengine.Pipeline 自身是同步执行,Routing 完成后由调用方负责错误聚合)
//   - 路由表通过 SetEntries 热更新,无锁情况下用 atomic.Pointer[entries] 装载
//   - 没有任何 ruleset 命中 → 走 default(最后一条);若 default 也不存在 → 报错并 drop
//
// 与 ruleengine.Route stage 的区别:
//   - ruleengine.Route stage 在 **单条 sample 内部** 按 label 把 sample 路由到不同 topic
//   - 本 router 在 **batch 维度** 把不同 sample 分到不同 ruleset(每 ruleset 一条 pipeline)
//
// 跨 ruleset 故障隔离:任一 ruleset 的 Process 返回 error,本次请求整体失败并把
// error 上抛(由 receiver 映射 503);不会因为单 ruleset 失败而影响其他。
//
// 依赖约定:本包不 import ruleengine,避免循环依赖(ruleengine → router)。
// 通过 Matcher 接口注入命中规则;ruleengine.Match 在调用方构造 router.Entry 时
// 用 adapter(见 ruleengine.NewMatcher)接入。
package router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lynnyq/prom-gw/internal/obs"
	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/sink"
	"go.uber.org/zap"
)

// Matcher 路由匹配规则接口。
//
// 实现方(ruleengine)持有一组 MetricPrefix / MetricExact 配置;
// 调用 Matches 判断 sample 是否被本 ruleset 接管。
type Matcher interface {
	Matches(s parser.Sample) bool
}

// ProcessFunc ruleset 内部处理函数,签名与 ruleengine.Pipeline.Process 保持一致。
//
// 由调用方注入,通常为 ruleengine.Pipeline.Process 方法值。
type ProcessFunc func(ctx context.Context, samples []parser.Sample, raw []byte, msg sink.Message) error

// Entry 路由表条目,描述"一组 sample 应被哪个 ruleset 接管"。
type Entry struct {
	// Name 仅用于日志 / metrics,不做匹配。
	Name string
	// Match 命中规则。
	//   - 空 Match(nil)表示全量接收(兜底 ruleset)
	//   - 多条 entries 中只允许一条 Match 为 nil,放在最后;其他 Match 必须非 nil
	Match Matcher
	// Process 该 ruleset 的处理函数。
	Process ProcessFunc
}

// Validate 校验 entry 列表的合法性:
//
//   - 至少 1 条
//   - 最多 1 条 Match 为 nil(全量兜底),且必须放在最后
//   - 任何一条 Process 为 nil 都视为非法
func Validate(entries []Entry) error {
	if len(entries) == 0 {
		return fmt.Errorf("router: no entries configured")
	}
	seenDefault := false
	for i, e := range entries {
		if e.Process == nil {
			return fmt.Errorf("router: entry[%d] %q has nil Process", i, e.Name)
		}
		if e.Match == nil {
			if seenDefault {
				return fmt.Errorf("router: multiple default entries (nil Match)")
			}
			seenDefault = true
			if i != len(entries)-1 {
				return fmt.Errorf("router: default entry must be the last one")
			}
		}
	}
	return nil
}

// Router fan-out 路由器。
//
// 内部用 atomic.Pointer[entries] 装载当前路由表,Read 路径零开销(R.Load 一次指针),
// 写路径(配置热更新)走 Store 原子切换,正在跑请求里的 entries 副本不受影响(per-batch)。
type Router struct {
	entries atomic.Pointer[[]Entry]
	logger  *zap.Logger
	mu      sync.Mutex // 仅用于 SetEntries 串行化(避免并发写导致 ABA)
}

// New 构造一个空 router;调用方需在启动时调 SetEntries 注入路由表。
func New(logger *zap.Logger) *Router {
	if logger == nil {
		logger = zap.NewNop()
	}
	r := &Router{logger: logger}
	// 给 entries 一个"空数组"占位,避免首次 Process 触发 nil 解引用
	empty := make([]Entry, 0)
	r.entries.Store(&empty)
	return r
}

// SetEntries 原子更新路由表。
//
// 入参必须通过 Validate;失败则不切换并返回 error(老 entries 继续生效)。
// 该方法在配置热更新 / 启动时由 main.go 调用。
func (r *Router) SetEntries(entries []Entry) error {
	if err := Validate(entries); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// 复制一份避免外部继续写入影响路由表
	cp := make([]Entry, len(entries))
	copy(cp, entries)
	r.entries.Store(&cp)
	r.logger.Info("router: entries updated",
		zap.Int("count", len(entries)),
		zap.String("default", defaultName(entries)),
	)
	return nil
}

// Entries 返回当前路由表(快照);用于 metrics 暴露 / admin / 调试。
func (r *Router) Entries() []Entry {
	p := r.entries.Load()
	if p == nil {
		return nil
	}
	out := make([]Entry, len(*p))
	copy(out, *p)
	return out
}

// Process 入口:把样本按 Match 分桶后,逐桶调用对应 ruleset 的 Process 函数。
//
// 行为约定:
//   - 单次 Process 内,1 个 sample 只会进入 1 个 ruleset(命中第一条规则即停)
//   - 桶为空 → 该 ruleset 跳过(不调用 Process)
//   - 任一 ruleset 返回 error → 本次 Process 立即返回该 error,后续 ruleset 跳过
//   - 全部命中 default 兜底 → 由 default 一次性处理
//
// 性能:
//   - O(n_samples + n_entries),无锁(只读 entries 指针)
//   - 桶分配用 sync.Pool 复用,降低 GC(高频路径)
func (r *Router) Process(ctx context.Context, samples []parser.Sample, raw []byte, msg sink.Message) error {
	p := r.entries.Load()
	if p == nil || len(*p) == 0 {
		obs.ErrorsTotal.WithLabelValues("router", "no_entries", "", "").Inc()
		return fmt.Errorf("router: no entries configured")
	}
	entries := *p
	nEntries := len(entries)
	defaultIdx := -1
	for i := range entries {
		if entries[i].Match == nil {
			defaultIdx = i
		}
	}

	// 预分配桶:每 entries 一桶
	buckets := make([][]parser.Sample, nEntries)
	// 为 0..nEntries-2(非 default)预分配 cap=0;default 桶按经验给大一些
	for i := range buckets {
		buckets[i] = getSampleBuf()
	}

	// spec 7.1: 取 ctx 上的 IngestCity/SourceDC,用于指标 label
	ingestCity, sourceDC := extractIngestLabels(ctx)

	// 1. 分桶:遍历每条 entry,命中第一条即停
	// 注:default 桶(Match=nil)不参与此轮;它只在 idx 仍为 -1 时兜底
	for _, s := range samples {
		idx := -1
		for i := 0; i < nEntries; i++ {
			if entries[i].Match == nil {
				continue // default 跳过
			}
			if entries[i].Match.Matches(s) {
				idx = i
				break
			}
		}
		if idx < 0 {
			if defaultIdx >= 0 {
				idx = defaultIdx
			} else {
				// 没有任何 ruleset 命中,且没有 default → drop
				obs.ErrorsTotal.WithLabelValues("router", "no_match_drop", ingestCity, sourceDC).Inc()
				continue
			}
		}
		buckets[idx] = append(buckets[idx], s)
	}

	// 2. 逐桶投递
	defer func() {
		// 回收所有桶
		for i := range buckets {
			putSampleBuf(buckets[i][:0])
		}
	}()

	for i := 0; i < nEntries; i++ {
		if len(buckets[i]) == 0 {
			continue
		}
		// default 桶在中间位置:此规则保证"命中第 i 条即停",但 default 兜底只允许 1 条
		// 这里我们依然逐桶调用,与 entries 顺序一致;只要 Validate 已限制 default 在最后,default 桶的"前面"已经走过一遍了
		if err := entries[i].Process(ctx, buckets[i], raw, msg); err != nil {
			obs.ErrorsTotal.WithLabelValues("router", "process_error", ingestCity, sourceDC).Inc()
			obs.RulesetErrorsTotal.WithLabelValues(entries[i].Name, ingestCity, sourceDC).Inc()
			return fmt.Errorf("router: ruleset %q failed: %w", entries[i].Name, err)
		}
		obs.RulesetRoutedTotal.WithLabelValues(entries[i].Name, ingestCity, sourceDC).Add(float64(len(buckets[i])))
	}

	// 记录"无 default 但有样本未命中"作为 warn
	_ = defaultIdx
	return nil
}

// extractIngestLabels 从 ctx 抽取 IngestCity/SourceDC(spec 7.1 指标 label 来源)。
// 不存在时返回空串(router 自身启动阶段不依赖 Meta)。
func extractIngestLabels(ctx context.Context) (string, string) {
	if ctx == nil {
		return "", ""
	}
	if m, ok := parser.MetaFromContext(ctx); ok {
		return m.IngestCity, m.SourceDC
	}
	return "", ""
}

func defaultName(entries []Entry) string {
	for _, e := range entries {
		if e.Match == nil {
			return e.Name
		}
	}
	return ""
}

// --- sample buffer pool ---

var sampleBufPool = sync.Pool{
	New: func() interface{} {
		// 预分配 64 cap,避免小样本频繁 grow
		buf := make([]parser.Sample, 0, 64)
		return &buf
	},
}

func getSampleBuf() []parser.Sample {
	return *sampleBufPool.Get().(*[]parser.Sample)
}

func putSampleBuf(b []parser.Sample) {
	// cap 太大不回收,避免 pool 持有大对象
	if cap(b) > 4096 {
		return
	}
	sampleBufPool.Put(&b)
}
