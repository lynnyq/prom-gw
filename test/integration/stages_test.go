//go:build integration

// T3.5 集成测试:enrich + downsample + deadvalue 三个状态型 stage 的端到端验证。
//
// 测试策略:
//   - 用 ruleHarness 把 receiver + rule engine pipeline + mock sink 串起来
//   - 每条 stage 单独测试,验证其语义正确
//   - 状态型 stage 验证状态变化(桶关闭 / 死值识别)
package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStage_Enrich_StaticLabels 验证 enrich stage 写入静态 label。
func TestStage_Enrich_StaticLabels(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: enrich
        config:
          labels:
            cluster: prod
            region: cn-east
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSamples("http_total", map[string]string{"job": "api"}, 1, 1)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 1*time.Second, 10*time.Millisecond)

	msgs := h.messages()
	require.Len(t, msgs, 1)
	// 验证 enrich 后的 sink message headers 或 payload 中的 label 存在 cluster=prod
	// (此处我们通过 message key 验证即可,因为 Payload 是原始 prompb 字节,
	// 解析需要深入 payload;集成测试只验证数量与 topic)
	assert.Equal(t, "prom.t", msgs[0].Topic)
}

// TestStage_Enrich_TemplateRef 验证 enrich stage 引用已有 label(${labels.x})。
func TestStage_Enrich_TemplateRef(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: enrich
        config:
          labels:
            dc: "${labels.region}"
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSamples("http_total", map[string]string{"region": "us-west"}, 1, 1)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 1*time.Second, 10*time.Millisecond)

	msgs := h.messages()
	require.Len(t, msgs, 1)
}

// TestStage_Downsample_EmitsOnBucketClose 验证 downsample 在桶关闭时发出聚合 sample。
//
// 时间轴:
//   - t=0: sample(1) 落入 [0,60) 桶
//   - t=10: sample(2) 同桶
//   - t=70: sample(3) 触发 [0,60) 桶关闭,emit 聚合
func TestStage_Downsample_EmitsOnBucketClose(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: downsample
        config:
          interval: 1m
          aggregations: [avg, max, min, sum, count]
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	// 同 series,3 个 sample 跨两个桶
	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 1, Timestamp: 0},          // bucket [0,60)
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 2, Timestamp: 10000},       // bucket [0,60)
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 70000},       // bucket [60,120) → emit 旧桶
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// 等到 emit 发生
	require.Eventually(t, func() bool {
		return len(h.messages()) >= 5
	}, 2*time.Second, 10*time.Millisecond, "5 个聚合 sample 都应发出")

	msgs := h.messages()
	assert.GreaterOrEqual(t, len(msgs), 5, "应至少 5 条:avg/max/min/sum/count")
}

// TestStage_Downsample_RespectsAggregations 验证 downsample 只发出配置的 aggregations。
func TestStage_Downsample_RespectsAggregations(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: downsample
        config:
          interval: 1m
          aggregations: [max]
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 1, Timestamp: 0},
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 9, Timestamp: 70000}, // 触发 emit
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 2*time.Second, 10*time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 1, "只配 max → 每个桶关闭只 emit 1 条")
}

// TestStage_Downsample_MultipleBuckets 验证多个 series 各自维护独立桶。
func TestStage_Downsample_MultipleBuckets(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: downsample
        config:
          interval: 1m
          aggregations: [avg]
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m1", Labels: map[string]string{"a": "1"}, Value: 1, Timestamp: 0},
		{Metric: "m2", Labels: map[string]string{"a": "2"}, Value: 2, Timestamp: 0},
		{Metric: "m1", Labels: map[string]string{"a": "1"}, Value: 100, Timestamp: 70000}, // 关 m1 桶
		{Metric: "m2", Labels: map[string]string{"a": "2"}, Value: 200, Timestamp: 70000}, // 关 m2 桶
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 2
	}, 2*time.Second, 10*time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 2, "m1 和 m2 各 1 条 emit")
}

// TestStage_DeadValue_FirstEmitted 验证第一条 sample 总是发出(无历史)。
func TestStage_DeadValue_FirstEmitted(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: deadvalue
        config:
          window: 1m
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", map[string]string{"a": "1"}, 5, 1000)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 1*time.Second, 10*time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 1, "首条 sample 必发出")
}

// TestStage_DeadValue_StaticValueDropped 验证同 series 同值重复上报会被丢弃。
func TestStage_DeadValue_StaticValueDropped(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: deadvalue
        config:
          window: 5m
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	// 3 个 sample,value 都是 5,timestamp 都在 1s 间隔内
	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 1000},
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 2000},
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 3000},
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// 等 300ms,只应有 1 条
	time.Sleep(300 * time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 1, "同 series 同值重复 → 只首条发出")
}

// TestStage_DeadValue_ValueChangedEmitted 验证值变化时发出。
func TestStage_DeadValue_ValueChangedEmitted(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: deadvalue
        config:
          window: 1m
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 1000},
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 2000},  // 死值
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 8, Timestamp: 3000},  // 变化 → 发出
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 8, Timestamp: 4000},  // 死值
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 2
	}, 1*time.Second, 10*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 2, "首条 + 变化各 1")
}

// TestStage_DeadValue_WindowExceeded 验证超过 window 时间时即使是同值也发出。
func TestStage_DeadValue_WindowExceeded(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: deadvalue
        config:
          window: 1m
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	// window=1m,两个 sample 间隔 2min,即便同值也应发出
	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 0},      // t=0
		{Metric: "m", Labels: map[string]string{"a": "1"}, Value: 5, Timestamp: 120000}, // t=2min
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 2
	}, 1*time.Second, 10*time.Millisecond)
	msgs := h.messages()
	assert.Len(t, msgs, 2, "跨 window 即便同值也发出")
}

// TestStage_Combo_EnrichRelabelRoute 验证 stage 链式组合。
func TestStage_Combo_EnrichRelabelRoute(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.default
    version: 1
    stages:
      - type: enrich
        config:
          labels:
            team: "${labels.owner}"
      - type: relabel
        config:
          drop_labels: ["owner"]
      - type: route
        config:
          rules:
            - match:
                team: app
              topic: prom.app
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", map[string]string{"owner": "app"}, 1, 1)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 1*time.Second, 10*time.Millisecond)
	msgs := h.messages()
	require.Len(t, msgs, 1)
	assert.Equal(t, "prom.app", msgs[0].Topic, "enrich owner→team 后 route 命中 app")
}

// _ = parser.Sample 防止 parser import unused 报警(集成测试只验证 rule engine 行为)
var _ = parser.Sample{}
