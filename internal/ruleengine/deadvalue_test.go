// ruleengine/deadvalue 单测:覆盖 plan T3.3 行为。
//
// 覆盖点:
//   - 首条 sample 总是发出
//   - 值变化的 sample 总是发出
//   - 值不变 + ts 间隔 > window → 发出
//   - 值不变 + ts 间隔 ≤ window → 丢弃
//   - window=0 语义(无时间阈值,值不变即丢)
//   - 多 series 互不干扰
//   - LRU 容量上限 + 驱逐
//   - 配置错误
package ruleengine

import (
	"context"
	"math"
	"testing"

	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeadValueStage_FirstSampleAlwaysKept(t *testing.T) {
	rs, err := Compile(&RuleSet{
		Name:         "dv-first",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "5m"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		mkSample("m", 1000, 42), // 首条 → 发出
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, 0, dropped)
}

func TestDeadValueStage_ValueChange(t *testing.T) {
	rs, err := Compile(&RuleSet{
		Name:         "dv-change",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "5m"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		mkSample("m", 1000, 42), // 首条 → 发出
		mkSample("m", 2000, 42), // 值不变 + ts 间隔 1s < 5m → 丢
		mkSample("m", 3000, 50), // 值变化 → 发出
		mkSample("m", 4000, 50), // 值不变 + ts 间隔 1s < 5m → 丢
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	assert.Equal(t, 2, dropped, "中间 2 条值不变且间隔短 → 丢")
	require.Len(t, out, 2, "首条 + 值变化各保留")
	assert.Equal(t, int64(1000), out[0].Timestamp)
	assert.Equal(t, int64(3000), out[1].Timestamp)
}

func TestDeadValueStage_WindowRespected(t *testing.T) {
	// window=1m:间隔 > 1m 的相同值要重新发出
	rs, err := Compile(&RuleSet{
		Name:         "dv-window",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "1m"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		mkSample("m", 0, 42),               // 首条 → 发出
		mkSample("m", 30_000, 42),          // 30s 间隔 < 1m → 丢
		mkSample("m", 65_000, 42),          // 65s 间隔 > 1m → 重新发出
		mkSample("m", 65_000+30_000, 42),   // 又 30s < 1m → 丢
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	assert.Equal(t, 2, dropped)
	require.Len(t, out, 2)
	assert.Equal(t, int64(0), out[0].Timestamp)
	assert.Equal(t, int64(65_000), out[1].Timestamp)
}

func TestDeadValueStage_ZeroWindow(t *testing.T) {
	// window=0 含义:无时间阈值,只要值不变就丢
	rs, err := Compile(&RuleSet{
		Name:         "dv-zerow",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "0s"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		mkSample("m", 1000, 42),
		mkSample("m", 1000+24*60*60*1000, 42), // 1 天后,值还是 42
		mkSample("m", 1000+24*60*60*1000+1, 99), // 值变
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	// 1 天后的 42 因为 window=0 仍然丢
	require.Len(t, out, 2)
	assert.Equal(t, 1, dropped, "1 天后值不变也丢(window=0)")
}

func TestDeadValueStage_MultipleSeries(t *testing.T) {
	rs, err := Compile(&RuleSet{
		Name:         "dv-multi",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "5m"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	mk := func(metric string, ts int64, val float64) parser.Sample {
		return parser.Sample{
			Metric:    metric,
			Value:     val,
			Timestamp: ts,
			Labels:    []parser.Label{{Name: "job", Value: "x"}},
		}
	}
	in := []parser.Sample{
		mk("m1", 1000, 10), // 1: 首条
		mk("m2", 1000, 20), // 2: 首条
		mk("m1", 2000, 10), // m1 值不变 → 丢
		mk("m2", 2000, 30), // m2 值变 → 发出
		mk("m1", 3000, 11), // m1 值变 → 发出
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 4)
	assert.Equal(t, 1, dropped, "只有 1 条死值")
	// 按 metric 排序后验证
	// 实际顺序:m1首条, m2首条, m2变化, m1变化
	assert.Equal(t, "m1", out[0].Metric)
	assert.Equal(t, int64(1000), out[0].Timestamp)
	assert.Equal(t, "m2", out[1].Metric)
	assert.Equal(t, int64(1000), out[1].Timestamp)
	assert.Equal(t, "m2", out[2].Metric)
	assert.Equal(t, int64(2000), out[2].Timestamp)
	assert.Equal(t, "m1", out[3].Metric)
	assert.Equal(t, int64(3000), out[3].Timestamp)
}

func TestDeadValueStage_LRUEviction(t *testing.T) {
	// max_series=3:装满后再插入新 series → 驱逐最久未用
	rs, err := Compile(&RuleSet{
		Name:         "dv-lru",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{
				"window":     "0s",
				"max_series": 3,
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	mk := func(metric string, ts int64, val float64) parser.Sample {
		return parser.Sample{
			Metric:    metric,
			Value:     val,
			Timestamp: ts,
			Labels:    []parser.Label{{Name: "job", Value: "x"}},
		}
	}
	in := []parser.Sample{
		mk("a", 1000, 1), // 首条 a
		mk("b", 1000, 1), // 首条 b
		mk("c", 1000, 1), // 首条 c
		mk("d", 1000, 1), // 插入 d → 驱逐最久未用(a,因为从未 get 过)
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 4, "都是首条,都发出")
	assert.Equal(t, 0, dropped)

	// 现在 a 已被驱逐,再发同样的 a → 被识别为"新 series",发出
	in2 := []parser.Sample{mk("a", 2000, 1)}
	out2, _, err := apply(context.Background(), in2, make([]parser.Sample, 0, 1))
	require.NoError(t, err)
	require.Len(t, out2, 1, "a 被驱逐后 → 重新发出")
}

func TestDeadValueStage_LRUEviction_AccessedNotEvicted(t *testing.T) {
	// 访问过的 key 不会被驱逐
	rs, err := Compile(&RuleSet{
		Name:         "dv-lru-acc",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{
				"window":     "0s",
				"max_series": 2,
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	mk := func(metric string, ts int64, val float64) parser.Sample {
		return parser.Sample{
			Metric:    metric,
			Value:     val,
			Timestamp: ts,
			Labels:    []parser.Label{{Name: "job", Value: "x"}},
		}
	}
	in := []parser.Sample{
		mk("a", 1000, 1),
		mk("b", 1000, 1),
		// 访问 a (值变,发出)
		mk("a", 2000, 2),
		// 插入 c → 驱逐最久未用(b)
		mk("c", 3000, 1),
		// 再访问 a (值变,应该发出 — a 仍在 LRU)
		mk("a", 4000, 3),
		// 再访问 b (已被驱逐,首条 → 发出)
		mk("b", 5000, 5),
	}
	out, _, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	// a 多次访问都发出(3 次:首条 + 变化2次)→ 3 条 a
	// b 首条 + 被驱逐后重发 → 2 条 b
	// c 首条 → 1 条 c
	// 总 6 条
	require.Len(t, out, 6, "所有 a/b/c 都应该发出(a 被访问所以未驱逐)")
}

func TestDeadValueStage_InvalidWindow(t *testing.T) {
	_, err := Compile(&RuleSet{
		Name:         "dv-bad",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "bogus"}},
		},
		Version: 1,
	})
	assert.Error(t, err)
}

func TestDeadValueStage_DefaultConfig(t *testing.T) {
	// 空配置也 OK(window 默认 0,max_series 默认 1M)
	rs, err := Compile(&RuleSet{
		Name:         "dv-def",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, rs.Stages[0].Apply)

	in := []parser.Sample{mkSample("m", 1000, 1)}
	out, _, err := applyDirect(rs.Stages[0], in)
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestDeadValueStage_NaNInfHandling(t *testing.T) {
	// NaN 与任何值比较都不等(包括自己)→ 总是发出
	rs, err := Compile(&RuleSet{
		Name:         "dv-nan",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "deadvalue", Config: map[string]interface{}{"window": "5m"}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	nan := math.NaN()
	in := []parser.Sample{
		mkSample("m", 1000, 42),  // 首条
		mkSample("m", 2000, nan), // NaN 与 42 不等 → 发出
	}
	out, dropped, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 2)
	assert.Equal(t, 0, dropped)
}

// applyDirect 直接从 CompiledStage 调 Apply,避免重新 Compile。
func applyDirect(s CompiledStage, in []parser.Sample) ([]parser.Sample, int, error) {
	out, dropped, err := s.Apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	return out, dropped, err
}
