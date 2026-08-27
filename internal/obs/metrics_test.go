// obs 单测: 验证 T1.13 第一批指标已注册到 default registerer 并能正常写入/读取。
//
// 覆盖点:
//   - 所有 spec 7.1 列出的指标都已注册(无 panic)
//   - Counter / CounterVec / HistogramVec / Gauge / GaugeFunc 都能 Inc/Set/Observe
//   - numGoroutines GaugeFunc 返回正数
package obs

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherMetric 从 default registerer 收集指定 name 的指标族。
func gatherMetric(t *testing.T, name string) *dto.MetricFamily {
	t.Helper()
	gathered, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range gathered {
		if mf.GetName() == name {
			return mf
		}
	}
	return nil
}

func TestMetrics_AllRegistered(t *testing.T) {
	// 触发一次写,确保 CounterVec 至少有一个 label 组合被注册
	SamplesTotal.WithLabelValues("test", "t1", "ok", "test_city", "test_dc").Inc()
	ErrorsTotal.WithLabelValues("test", "x", "test_city", "test_dc").Inc()
	BackpressureRejected.WithLabelValues("p", "test_city", "test_dc").Inc()
	AuthFailTotal.WithLabelValues("invalid", "test_city", "test_dc").Inc()
	BytesIn.WithLabelValues("t", "test_city", "test_dc").Add(0)
	BytesOut.WithLabelValues("topic", "test_city").Add(0)
	RulesetSwitchTotal.WithLabelValues("n", "v1", "v2", "test_city", "test_dc").Inc()
	RulesetVersion.WithLabelValues("n", "test_city").Set(1)
	StageDuration.WithLabelValues("s", "ok", "test_city").Observe(0.001)
	RequestDuration.WithLabelValues("e", "ok", "test_city").Observe(0.001)
	WalBytesVec.WithLabelValues("test_city").Set(0)
	WalOldestAgeVec.WithLabelValues("test_city").Set(0)
	WalHardReject.Inc()
	// Goroutines 是 GaugeFunc,无 Set 方法;读取走 gatherMetric 验证
	_ = Goroutines

	// 关键指标必须存在
	required := []string{
		"gateway_samples_total",
		"gateway_stage_duration_seconds",
		"gateway_request_duration_seconds",
		"gateway_errors_total",
		"gateway_auth_fail_total",
		"gateway_backpressure_rejected_total",
		"gateway_wal_bytes",
		"gateway_wal_oldest_age_seconds",
		"gateway_wal_hard_reject_total",
		"gateway_goroutines",
		"gateway_bytes_in_total",
		"gateway_bytes_out_total",
		"gateway_ruleset_switch_total",
		"gateway_ruleset_version",
	}
	for _, name := range required {
		mf := gatherMetric(t, name)
		assert.NotNil(t, mf, "指标 %s 必须已注册", name)
	}
}

func TestSamplesTotal_CounterVec(t *testing.T) {
	before := getCounterValue(t, "gateway_samples_total", "stage", "x_test", "business", "x_t", "status", "ok")

	SamplesTotal.WithLabelValues("x_test", "x_t", "ok", "test_city", "test_dc").Inc()
	SamplesTotal.WithLabelValues("x_test", "x_t", "ok", "test_city", "test_dc").Inc()
	SamplesTotal.WithLabelValues("x_test", "x_t", "ok", "test_city", "test_dc").Inc()

	after := getCounterValue(t, "gateway_samples_total", "stage", "x_test", "business", "x_t", "status", "ok")
	assert.Equal(t, before+3, after, "Inc 3 次应增加 3")
}

func TestStageDuration_Histogram(t *testing.T) {
	// 写一次,确保无 panic
	StageDuration.WithLabelValues("hist_test", "ok", "test_city").Observe(0.005)
	StageDuration.WithLabelValues("hist_test", "ok", "test_city").Observe(0.01)

	mf := gatherMetric(t, "gateway_stage_duration_seconds")
	require.NotNil(t, mf)
	found := false
	for _, m := range mf.Metric {
		for _, l := range m.Label {
			if l.GetName() == "stage" && l.GetValue() == "hist_test" {
				found = true
				break
			}
		}
	}
	assert.True(t, found, "stage=hist_test 必须出现")
}

func TestWalMetrics_Gauges(t *testing.T) {
	WalBytesVec.WithLabelValues("test_city").Set(1024)
	WalOldestAgeVec.WithLabelValues("test_city").Set(3.14)

	// 简单 sanity 校验:Set 后能再次 Set 不 panic
	WalBytesVec.WithLabelValues("test_city").Set(2048)
	WalOldestAgeVec.WithLabelValues("test_city").Set(6.28)
}

func TestGoroutines_GaugeFunc(t *testing.T) {
	mf := gatherMetric(t, "gateway_goroutines")
	require.NotNil(t, mf)
	require.NotEmpty(t, mf.Metric)
	val := mf.Metric[0].GetGauge().GetValue()
	assert.Greater(t, val, 0.0, "goroutines 必须 > 0")
}

func TestRulesetSwitch_RecordedOnSwap(t *testing.T) {
	RulesetSwitchTotal.WithLabelValues("rs_test", "v1", "v2", "test_city", "test_dc").Inc()
	RulesetSwitchTotal.WithLabelValues("rs_test", "v1", "v2", "test_city", "test_dc").Inc()

	val := getCounterValue(t, "gateway_ruleset_switch_total",
		"ruleset", "rs_test", "from_version", "v1", "to_version", "v2",
		"ingest_city", "test_city", "source_dc", "test_dc")
	assert.GreaterOrEqual(t, val, 2.0)
}

// --- helpers ---

// getCounterValue 按 label 维度收集特定 Counter 的值;找不到返回 0。
func getCounterValue(t *testing.T, name string, kv ...string) float64 {
	t.Helper()
	mf := gatherMetric(t, name)
	if mf == nil {
		return 0
	}
	for _, m := range mf.Metric {
		if matchLabels(m, kv...) {
			if m.Counter != nil {
				return m.Counter.GetValue()
			}
			if m.Gauge != nil {
				return m.Gauge.GetValue()
			}
		}
	}
	return 0
}

func matchLabels(m *dto.Metric, kv ...string) bool {
	if len(kv)%2 != 0 {
		return false
	}
	want := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		want[kv[i]] = kv[i+1]
	}
	for _, l := range m.Label {
		if v, ok := want[l.GetName()]; ok && v != l.GetValue() {
			return false
		}
	}
	// 简化版匹配:要求 want 中的 key 全在 metric 中,允许 metric 有额外 label
	for k := range want {
		found := false
		for _, l := range m.Label {
			if l.GetName() == k {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// 防 import strings 警告
var _ = strings.HasPrefix
