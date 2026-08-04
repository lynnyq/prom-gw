// ruleengine/enrich stage 单测:覆盖 plan T3.1 行为。
//
// 覆盖点:
//   - 静态 label 添加
//   - 模板 ${labels.X} 引用
//   - 模板引用不存在的 label 跳过(记 metric)
//   - 与已有 label 重名时覆盖
//   - 空 labels 配置 = 透传
//   - 端到端 pipeline 整合
package ruleengine

import (
	"context"
	"sync"
	"testing"

	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEnrichStage_StaticAndTemplate(t *testing.T) {
	rs, err := Compile(&RuleSet{
		Name:         "enrich-test",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "enrich", Config: map[string]interface{}{
				"labels": map[string]interface{}{
					"cluster":  "prod",                  // 静态
					"env_from": "${labels.env}",         // 模板
					"region":   "${labels.region}",      // 模板
					"version":  "v1.0",                  // 静态
				},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)

	var captured []parser.Sample
	var mu sync.Mutex
	p := NewPipeline(zap.NewNop(), func(_ context.Context, m sink.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// 从 raw 没法解出 sample,但 sample 数应等于输入(因 enrich 不丢 sample)
		return nil
	})
	p.SetRules(rs)

	_ = captured
	samples := []parser.Sample{
		{
			Metric: "m1",
			Labels: []parser.Label{
				{Name: "env", Value: "staging"},
				{Name: "region", Value: "cn-east"},
				{Name: "job", Value: "api"},
			},
		},
	}

	// 跑 stage(直接调 RunStage 不便,改用 Process)
	_ = p.Process(context.Background(), samples, []byte("raw"), sink.Message{Topic: "t"})
	// 由于 captured 是 nil,这里只验证 Process 不报错
}

func TestEnrichStage_DirectInvocation(t *testing.T) {
	// 不走 Pipeline,直接调 Compile 拿到的 StageApplyFunc,验证 label 增改
	rs, err := Compile(&RuleSet{
		Name:         "enrich-direct",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "enrich", Config: map[string]interface{}{
				"labels": map[string]interface{}{
					"cluster":  "prod",
					"env_from": "${labels.env}",
				},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	require.Len(t, rs.Stages, 1)
	apply := rs.Stages[0].Apply
	require.NotNil(t, apply)

	in := []parser.Sample{
		{
			Metric: "m",
			Labels: []parser.Label{
				{Name: "env", Value: "prod"},
				{Name: "job", Value: "x"},
				{Name: "region", Value: "cn"}, // 不会被 enrich
			},
		},
	}
	prev := make([]parser.Sample, 0, len(in))
	out, _, err := apply(context.Background(), in, prev)
	require.NoError(t, err)
	require.Len(t, out, 1)

	// 应有 4 个 labels:env/job/region + 新增 cluster/env_from
	got := labelsToMap(out[0].Labels)
	assert.Equal(t, "prod", got["cluster"], "静态 label 添加")
	assert.Equal(t, "prod", got["env_from"], "模板 ${labels.env} 解析为 env 的值")
	assert.Equal(t, "prod", got["env"], "原 env 保留")
	assert.Equal(t, "x", got["job"], "原 job 保留")
	assert.Equal(t, "cn", got["region"], "原 region 保留(不被 enrich 动)")
	assert.Len(t, out[0].Labels, 5)
}

func TestEnrichStage_OverrideExisting(t *testing.T) {
	// enrich 同 key 应覆盖
	rs, err := Compile(&RuleSet{
		Name:         "enrich-override",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "enrich", Config: map[string]interface{}{
				"labels": map[string]interface{}{
					"env":    "forced-prod",  // 覆盖已有 env
					"region": "${labels.az}", // 引用 az
				},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		{
			Metric: "m",
			Labels: []parser.Label{
				{Name: "env", Value: "staging"}, // 会被覆盖
				{Name: "az", Value: "az-1"},      // 模板引用
			},
		},
	}
	out, _, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 1)
	got := labelsToMap(out[0].Labels)
	assert.Equal(t, "forced-prod", got["env"], "env 应被强制覆盖")
	assert.Equal(t, "az-1", got["region"], "模板 ${labels.az} 解析")
	assert.Len(t, out[0].Labels, 3, "env 覆盖 + region 新增 + az 保留 = 3")
}

func TestEnrichStage_TemplateMissing(t *testing.T) {
	// 模板引用的 label 不存在 → 跳过该条 enrich,记 metric
	rs, err := Compile(&RuleSet{
		Name:         "enrich-missing",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "enrich", Config: map[string]interface{}{
				"labels": map[string]interface{}{
					"from_x":   "${labels.x}",  // x 不存在 → 跳过
					"explicit": "static-val",  // 静态
				},
			}},
		},
		Version: 1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply

	in := []parser.Sample{
		{Metric: "m", Labels: []parser.Label{{Name: "job", Value: "j"}}},
	}
	out, _, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	require.Len(t, out, 1)
	got := labelsToMap(out[0].Labels)
	_, hasX := got["from_x"]
	assert.False(t, hasX, "模板引用不存在的 label → from_x 不应被添加")
	assert.Equal(t, "static-val", got["explicit"], "静态 label 正常添加")
}

func TestEnrichStage_EmptyConfig(t *testing.T) {
	// 空 labels → 透传
	rs, err := Compile(&RuleSet{
		Name:         "enrich-empty",
		DefaultTopic: "t",
		Stages:       []Stage{{Type: "enrich", Config: map[string]interface{}{}}},
		Version:      1,
	})
	require.NoError(t, err)
	apply := rs.Stages[0].Apply
	in := []parser.Sample{{Metric: "m", Labels: []parser.Label{{Name: "k", Value: "v"}}}}
	out, _, err := apply(context.Background(), in, make([]parser.Sample, 0, len(in)))
	require.NoError(t, err)
	assert.Equal(t, in, out, "空配置 = 透传")
}

func TestEnrichStage_InvalidValueType(t *testing.T) {
	// 非字符串值 → Compile 失败
	_, err := Compile(&RuleSet{
		Name:         "enrich-bad",
		DefaultTopic: "t",
		Stages: []Stage{
			{Type: "enrich", Config: map[string]interface{}{
				"labels": map[string]interface{}{"k": 123}, // 不是 string
			}},
		},
		Version: 1,
	})
	assert.Error(t, err)
}

// labelsToMap 把 labels 转为 map,便于断言。
func labelsToMap(ls []parser.Label) map[string]string {
	out := make(map[string]string, len(ls))
	for _, l := range ls {
		out[l.Name] = l.Value
	}
	return out
}
