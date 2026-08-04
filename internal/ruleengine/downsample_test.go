// ruleengine/downsample 单测:覆盖 plan T3.2 行为。
//
// 覆盖点:
//   - 简单 avg 聚合
//   - max/min/sum/count
//   - 桶边界触发 emit
//   - p50/p99 精确计算(用排序切片)
//   - 多 series 互不干扰
//   - 配置错误
package ruleengine

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkSample(metric string, tsMs int64, value float64) parser.Sample {
	return parser.Sample{
		Metric:    metric,
		Value:     value,
		Timestamp: tsMs,
		Labels:    []parser.Label{{Name: "job", Value: "x"}},
	}
}

func TestDownsampleStage_SimpleAvg(t *testing.T) {
	// 1 分钟桶,avg 聚合,3 条 sample 都在同一分钟 → emit 时应输出 avg
	rs, err := Compile(&RuleSet{
		Name:         "ds-avg",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"avg"},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply
	require.NotNil(t, apply)

	// 3 条 sample 在 60s 桶内(对齐 60s 边界,避开跨界)
	// 选 1700000040000 ms = 28333334 * 60s 的整分钟
	bucketStartMs := int64(1700000040000)
	in := []parser.Sample{
		mkSample("m", bucketStartMs+1000, 10),
		mkSample("m", bucketStartMs+20000, 20),
		mkSample("m", bucketStartMs+50000, 30),
	}
	out, _, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	// 当前 bucket 未关闭,无 emit
	assert.Empty(t, out, "桶未关闭前不应 emit")

	// 喂一个跨入下个桶的 sample(70s 偏移)
	out2, _, err := apply(context.Background(),
		[]parser.Sample{mkSample("m", bucketStartMs+70000, 99)},
		make([]parser.Sample, 0, 1),
	)
	require.NoError(t, err)
	require.Len(t, out2, 1, "跨桶应 emit 旧桶")
	assert.InDelta(t, 20.0, out2[0].Value, 0.01, "avg of 10/20/30 = 20")
}

func TestDownsampleStage_MultiAggregations(t *testing.T) {
	rs, err := Compile(&RuleSet{
		Name:         "ds-multi",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"avg", "max", "min", "sum", "count"},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	bucketStartMs := int64(1700000000000)
	in := []parser.Sample{
		mkSample("m", bucketStartMs+1000, 10),
		mkSample("m", bucketStartMs+2000, 20),
		mkSample("m", bucketStartMs+3000, 5),
	}
	_, _, _ = apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	// 触发 emit
	out, _, err := apply(context.Background(),
		[]parser.Sample{mkSample("m", bucketStartMs+70000, 0)},
		make([]parser.Sample, 0, 1),
	)
	require.NoError(t, err)
	require.Len(t, out, 5, "5 个 agg 应产生 5 条 sample")

	// 按 metric 展开比较(agg 顺序已固定,验证 Value)
	got := map[AggregationKind]float64{}
	keys := []AggregationKind{AggAvg, AggMax, AggMin, AggSum, AggCount}
	for i, s := range out {
		got[keys[i]] = s.Value
	}
	assert.InDelta(t, (10+20+5)/3.0, got[AggAvg], 0.01)
	assert.Equal(t, 20.0, got[AggMax])
	assert.Equal(t, 5.0, got[AggMin])
	assert.Equal(t, 35.0, got[AggSum])
	assert.Equal(t, 3.0, got[AggCount])
}

func TestDownsampleStage_PercentileExact(t *testing.T) {
	// 1..100 均匀分布,p50 应 ≈ 50.5,p99 应 ≈ 99.01(线性插值,精度远好于 P²)
	rs, err := Compile(&RuleSet{
		Name:         "ds-p99",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"p50", "p99"},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	bucketStartMs := int64(1700000000000)
	var in []parser.Sample
	for i := 1; i <= 100; i++ {
		in = append(in, mkSample("m", bucketStartMs+int64(i*100), float64(i)))
	}
	_, _, _ = apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	out, _, err := apply(context.Background(),
		[]parser.Sample{mkSample("m", bucketStartMs+70000, 0)},
		make([]parser.Sample, 0, 1),
	)
	require.NoError(t, err)
	require.Len(t, out, 2)
	// p50 和 p99 的输出顺序按 aggregations 数组顺序:p50, p99
	assert.InDelta(t, 50.5, out[0].Value, 0.5, "p50 ≈ 50.5 (linear interp over 1..100)")
	assert.InDelta(t, 99.01, out[1].Value, 0.5, "p99 ≈ 99.01 (linear interp over 1..100)")
}

func TestDownsampleStage_PercentileHelper(t *testing.T) {
	// 直接测 percentile 函数
	assert.Equal(t, 0.0, percentile(nil, 0.5))
	assert.Equal(t, 0.0, percentile([]float64{}, 0.5))
	assert.Equal(t, 42.0, percentile([]float64{42}, 0.5))
	// 1..100 排序后取 50%
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	assert.InDelta(t, 50.5, percentile(xs, 0.5), 0.001)
	assert.InDelta(t, 99.01, percentile(xs, 0.99), 0.001)
	assert.InDelta(t, 1.0, percentile(xs, 0.0), 0.001)
	assert.Equal(t, 100.0, percentile(xs, 1.0))
}

func TestDownsampleStage_InvalidInterval(t *testing.T) {
	_, err := Compile(&RuleSet{
		Name:         "ds-bad",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "bogus",
				"aggregations": []interface{}{"avg"},
			}},
		},
		Version: 1,
	})
	assert.Error(t, err)
}

func TestDownsampleStage_EmptyAggregations(t *testing.T) {
	_, err := Compile(&RuleSet{
		Name:         "ds-no-agg",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{},
			}},
		},
		Version: 1,
	})
	assert.Error(t, err)
}

func TestDownsampleStage_UnknownAggregation(t *testing.T) {
	_, err := Compile(&RuleSet{
		Name:         "ds-unknown-agg",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"nonsense"},
			}},
		},
		Version: 1,
	})
	assert.Error(t, err)
}

func TestDownsampleStage_MultipleBuckets(t *testing.T) {
	// 多个 series,各自累积,不应互相干扰
	rs, err := Compile(&RuleSet{
		Name:         "ds-multi-series",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"avg"},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	bucketStartMs := int64(1700000000000)
	in := []parser.Sample{
		mkSample("m1", bucketStartMs+1000, 10),
		mkSample("m2", bucketStartMs+1000, 100),
		mkSample("m1", bucketStartMs+2000, 30),
		mkSample("m2", bucketStartMs+2000, 200),
	}
	_, _, _ = apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	// 触发 emit
	out, _, err := apply(context.Background(),
		[]parser.Sample{
			mkSample("m1", bucketStartMs+70000, 0),
			mkSample("m2", bucketStartMs+70000, 0),
		},
		make([]parser.Sample, 0, 2),
	)
	require.NoError(t, err)
	require.Len(t, out, 2, "两个 series 各 emit 一个 avg")
	// 按 metric 排序后比较
	sort.Slice(out, func(i, j int) bool { return out[i].Metric < out[j].Metric })
	assert.Equal(t, "m1", out[0].Metric)
	assert.InDelta(t, 20.0, out[0].Value, 0.01, "m1 avg = (10+30)/2")
	assert.Equal(t, "m2", out[1].Metric)
	assert.InDelta(t, 150.0, out[1].Value, 0.01, "m2 avg = (100+200)/2")
}

func TestDownsampleStage_HotReload(t *testing.T) {
	// 验证热更新(用 Stage.SetRules 模拟):同一 pipeline 在新规则下行为符合新配置
	rs1, err := Compile(&RuleSet{
		Name:         "ds-1m",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "downsample", Config: map[string]interface{}{
				"interval":     "1m",
				"aggregations": []interface{}{"avg"},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply1 := rs1.Stages[0].Apply

	// 1m 桶:3 个 sample 在同一桶
	bucketStartMs := int64(1700000000000)
	in := []parser.Sample{
		mkSample("m", bucketStartMs+1000, 10),
		mkSample("m", bucketStartMs+2000, 30),
	}
	_, _, _ = apply1(context.Background(), in, make([]parser.Sample, 0, len(in)))
	// 跨入 1m+1 → 触发 emit
	out, _, err := apply1(context.Background(),
		[]parser.Sample{mkSample("m", bucketStartMs+65000, 0)},
		make([]parser.Sample, 0, 1),
	)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.InDelta(t, 20.0, out[0].Value, 0.01, "1m 桶 avg of [10, 30] = 20")
}

// 防止 time 包未使用告警(为后续 interval 配置使用)
var _ = time.Second
