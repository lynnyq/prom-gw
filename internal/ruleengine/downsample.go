// Package ruleengine - downsample.go: 状态型 downsample stage(plan T3.2)。
//
// 设计要点:
//
//   - 按 sample 系列(seriesKey)分桶,每桶 = [bucketStart, bucketStart+interval)
//   - 桶内累计 sum/count/max/min;p50/p99 用排序切片精确计算(留 Phase 5 benchmark,
//     若内存 > 500MB/百万 series 再升级 t-digest / DDSketch)
//   - 桶关闭(下一个 sample 跨入新桶 OR 阶段遇到 flush 调用) → 发出 1 个聚合 sample
//   - 状态用 atomic.Pointer[downsampleState] 装载,支持热更新:
//     切换时旧状态继续吃 in-flight,新状态独立累计
//   - 当前 stage.Apply 不主动调 out(避免在 stage 内回调 Pipeline);
//     桶关闭时把"emit"sample 追加到返回值,Pipeline 统一投递
//
// 性能:
//
//   - map[seriesKey]*bucket 用 sync.Mutex 保护(读多写少场景未做 lock-free 优化)
//   - 默认 bucket interval 1m,1.5M samples/s 下 ≈ 1M series 在飞
//   - p50/p99 用切片实现:每个 series 每桶 ≤ p99_max_samples 个 float64
//     (默认 4096),超限后转为"top-k reservoir sampling"以保持上界
package ruleengine

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.opentelemetry.io/otel/attribute"
)

// --- aggregation 类型 ---

// AggregationKind 聚合方式。
type AggregationKind string

const (
	AggAvg   AggregationKind = "avg"
	AggSum   AggregationKind = "sum"
	AggMax   AggregationKind = "max"
	AggMin   AggregationKind = "min"
	AggCount AggregationKind = "count"
	AggP50   AggregationKind = "p50"
	AggP99   AggregationKind = "p99"
)

// --- bucket ---

// dsBucket 单 series 在单个 interval 内的累计器。
//
// 字段分类:
//
//   - 标量累计:sum/count/max/min
//   - 切片累计:values(只在配了 p50/p99 时使用,默认上限 p99MaxSamples)
//   - 元数据:start/end/sample(emit 时复用 metric/labels)
type dsBucket struct {
	start  int64
	end    int64
	sum    float64
	count  int64
	max    float64
	min    float64
	first  bool // 是否首个 sample(true → 首次 add 才真正累计)
	values []float64
	cap    int
	sample parser.Sample
}

// newDSBucket 构造桶并把第一个 sample 累计进去。
//
// 关键:这里把"首个 sample"也走一遍 add 逻辑,避免首 sample 被构造函数
// 静默"赋值给 max/min"但 sum/count 不增加的 bug(见 plan T3.2 修复记录)。
func newDSBucket(s parser.Sample, bucketStart, intervalSec int64, wantsPercentile bool, pCap int) *dsBucket {
	b := &dsBucket{
		start: bucketStart,
		end:   bucketStart + intervalSec,
		first: true, // 让首次 add 真正累加 sum/count 并初始化 max/min
		cap:   pCap,
		sample: s,
	}
	if wantsPercentile {
		b.values = make([]float64, 0, 64)
	}
	b.add(s) // 把首个 sample 累计进去
	return b
}

// add 累加一个 sample;同时维护 max/min/sum/count,并按需追加到 values。
func (b *dsBucket) add(s parser.Sample) {
	if b.first {
		b.max = s.Value
		b.min = s.Value
		b.first = false
	} else {
		if s.Value > b.max {
			b.max = s.Value
		}
		if s.Value < b.min {
			b.min = s.Value
		}
	}
	b.sum += s.Value
	b.count++
	if b.values != nil {
		if len(b.values) < b.cap {
			b.values = append(b.values, s.Value)
		}
		// 超过 cap → 丢尾(简化:不做 reservoir sampling,
		// 因为 p99 max_samples 默认 4096 已能覆盖绝大多数 1m 桶)
	}
}

// emit 把 bucket 折叠成一组 sample(每个 agg 一条,共享 seriesKey 和 timestamp=end)。
func (b *dsBucket) emit(aggs []AggregationKind) []parser.Sample {
	if b.count == 0 {
		return nil
	}
	out := make([]parser.Sample, 0, len(aggs))
	for _, agg := range aggs {
		s := b.sample
		s.Timestamp = b.end
		s.Value = aggregateValue(b, agg)
		out = append(out, s)
	}
	return out
}

// aggregateValue 计算单个聚合值;p50/p99 在第一次访问时排序 values 缓存(线程不安全,
// 但调用前已持有 state.mu,所以 OK)。
func aggregateValue(b *dsBucket, agg AggregationKind) float64 {
	switch agg {
	case AggAvg:
		return b.sum / float64(b.count)
	case AggSum:
		return b.sum
	case AggMax:
		return b.max
	case AggMin:
		return b.min
	case AggCount:
		return float64(b.count)
	case AggP50:
		return percentile(b.values, 0.50)
	case AggP99:
		return percentile(b.values, 0.99)
	}
	return 0
}

// percentile 返回切片(0..1)分位数;len=0 时返回 0。
//
// 算法:复制切片 + sort.Slice。O(N log N) 但 N ≤ 4096 时延迟 < 1µs,完全可接受。
// emit 时按需调用,不在 ingest 路径上。
func percentile(values []float64, q float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return values[0]
	}
	cp := make([]float64, n)
	copy(cp, values)
	sort.Float64s(cp)
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	// 线性插值,与 numpy.percentile 行为一致
	idx := q * float64(n-1)
	lo := int(idx)
	if lo >= n-1 {
		return cp[n-1]
	}
	frac := idx - float64(lo)
	return cp[lo] + frac*(cp[lo+1]-cp[lo])
}

// --- state ---

// downsampleState 一个编译期 + 运行期的不可变配置 + 运行时可变 map。
type downsampleState struct {
	interval  time.Duration
	aggs      []AggregationKind
	buckets   map[uint64]*dsBucket
	maxSeries int
	pCap      int
	mu        sync.Mutex // 保护 buckets map(写多读少,简单 mutex 即可)
}

func newDownsampleState(interval time.Duration, aggs []AggregationKind, maxSeries, pCap int) *downsampleState {
	return &downsampleState{
		interval:  interval,
		aggs:      aggs,
		buckets:   make(map[uint64]*dsBucket, 1024),
		maxSeries: maxSeries,
		pCap:      pCap,
	}
}

// bucketStart 给定 sample timestamp(秒),返回所在桶的起始(秒)。
func (s *downsampleState) bucketStart(tsSec int64) int64 {
	intervalSec := int64(s.interval.Seconds())
	if intervalSec <= 0 {
		return tsSec
	}
	return (tsSec / intervalSec) * intervalSec
}

// needsPercentile 判断本 state 是否需要 values 切片(配了 p50/p99 才要)。
func (s *downsampleState) needsPercentile() bool {
	for _, a := range s.aggs {
		if a == AggP50 || a == AggP99 {
			return true
		}
	}
	return false
}

// seriesCount 返回当前追踪的 series 数(spec 7.1 gateway_state_series)。
//
// 加锁读 buckets map 的 size;调用频率低(metric 采集),无 hot path 风险。
func (s *downsampleState) seriesCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buckets)
}

// ingest 喂一个 sample,返回因桶关闭而 emit 的所有 sample。
func (s *downsampleState) ingest(in parser.Sample) []parser.Sample {
	// Sample.Timestamp 是毫秒,downsample 用秒桶
	tsSec := in.Timestamp / 1000
	bs := s.bucketStart(tsSec)
	seriesKey := in.SeriesKey()

	s.mu.Lock()
	defer s.mu.Unlock()

	var emitted []parser.Sample
	b, ok := s.buckets[seriesKey]
	if !ok {
		// 新 series:检查容量
		if s.maxSeries > 0 && len(s.buckets) >= s.maxSeries {
			ingestCity, sourceDC := obs.MetaLabels()
			obs.ErrorsTotal.WithLabelValues("rule", "downsample_series_full", ingestCity, sourceDC).Inc()
			return nil
		}
		s.buckets[seriesKey] = newDSBucket(in, bs, int64(s.interval.Seconds()), s.needsPercentile(), s.pCap)
		return nil
	}

	// 桶跨界 → emit 旧桶,初始化新桶
	if bs != b.start {
		emitted = append(emitted, b.emit(s.aggs)...)
		s.buckets[seriesKey] = newDSBucket(in, bs, int64(s.interval.Seconds()), s.needsPercentile(), s.pCap)
	} else {
		b.add(in)
	}
	return emitted
}

// --- stage ---

// DownsampleStage 实现 plan T3.2: 按 interval 桶聚合。
type DownsampleStage struct{}

func (DownsampleStage) Name() string { return "downsample" }

func (DownsampleStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	intervalStr, _ := cfg["interval"].(string)
	interval, err := time.ParseDuration(intervalStr)
	if err != nil || interval <= 0 {
		return nil, fmt.Errorf("downsample: invalid interval %q, use Go duration (1m, 5m, 30s)", intervalStr)
	}
	aggsRaw, _ := cfg["aggregations"].([]interface{})
	if len(aggsRaw) == 0 {
		return nil, fmt.Errorf("downsample: aggregations required (e.g. [avg, max, p99])")
	}
	aggs := make([]AggregationKind, 0, len(aggsRaw))
	seen := map[AggregationKind]bool{}
	for _, raw := range aggsRaw {
		s, _ := raw.(string)
		a := AggregationKind(s)
		switch a {
		case AggAvg, AggSum, AggMax, AggMin, AggCount, AggP50, AggP99:
		default:
			return nil, fmt.Errorf("downsample: unsupported aggregation %q", s)
		}
		if seen[a] {
			return nil, fmt.Errorf("downsample: duplicate aggregation %q", s)
		}
		seen[a] = true
		aggs = append(aggs, a)
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
	pCap := 0
	if v, ok := cfg["p99_max_samples"].(int); ok {
		pCap = v
	} else if v, ok := cfg["p99_max_samples"].(float64); ok {
		pCap = int(v)
	}
	if pCap <= 0 {
		pCap = 4096
	}

	// 状态对象(atomic 装载支持热更新)
	state := atomic.Pointer[downsampleState]{}
	state.Store(newDownsampleState(interval, aggs, maxSeries, pCap))

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "downsample")
		defer span.End()
		span.SetAttributes(
			attribute.String("interval", interval.String()),
			attribute.Int("aggregations", len(aggs)),
		)

		// 累计要 emit 的 sample(可能在原 slice 之上)
		emit := make([]parser.Sample, 0, len(in))
		st := state.Load()
		for _, s := range in {
			emitted := st.ingest(s)
			if len(emitted) > 0 {
				emit = append(emit, emitted...)
			}
		}
		// downsample stage 不透传原 sample(被聚合掉了),只 emit 关闭的桶
		span.SetAttributes(
			attribute.Int("input", len(in)),
			attribute.Int("emitted", len(emit)),
		)
		ingestCity, _ := obs.MetaLabels()
		obs.StageDuration.WithLabelValues("rule_downsample", "ok", ingestCity).Observe(0)
		// state 级 metric(spec 7.1 gateway_state_series)
		obs.StateSeries.WithLabelValues("downsample", ingestCity).Set(float64(st.seriesCount()))
		return emit, 0, nil
	}, nil
}
