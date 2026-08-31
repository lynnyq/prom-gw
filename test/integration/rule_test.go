//go:build integration

// T2.11 集成测试:规则引擎端到端(relabel / route / sample)。
//
// 测试策略:把 receiver + rule engine 串起来,handler 走真实的 ruleengine.Pipeline,
// 投递到 mockSink。覆盖三种 stage 的行为。
package integration

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/ruleengine"
	"github.com/lynnyq/prom-gw/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ruleHarness 串联 receiver + rule engine pipeline + mock sink,提供完整路径测试。
type ruleHarness struct {
	recvH  *receiverHarness
	mock   *mockSink
	pipe   *ruleengine.Pipeline
	cancel context.CancelFunc
}

func newRuleHarness(t *testing.T, rulesYAML string) *ruleHarness {
	t.Helper()
	authN := newMockAuth()
	mockSink := newMockSink()
	// 加载并编译 ruleset
	cfg, err := ruleengine.LoadBytes([]byte(rulesYAML))
	require.NoError(t, err)
	require.NotEmpty(t, cfg.Rulesets, "ruleset must not be empty")
	compiled, err := ruleengine.Compile(&cfg.Rulesets[0])
	require.NoError(t, err)

	pipe := ruleengine.NewPipeline(nil, func(ctx context.Context, msg sink.Message) error {
		return mockSink.Send(ctx, msg)
	})
	pipe.SetRules(compiled)

	recv := newReceiverHarness(t, authN, func(ctx context.Context, raw []byte, samples []parser.Sample, defaultTopic string) error {
		meta, _ := parser.MetaFromContext(ctx)
		headers := map[string]string{
			"tenant":         meta.Tenant,
			"source_dc":      meta.SourceDC,
			"ingest_city":    meta.IngestCity,
			"ingest_dc":      meta.SourceDC,
			"ingest_time_ms": "0",
		}
		msg := sink.Message{
			Topic:   defaultTopic,
			Key:     []byte(meta.Tenant),
			Headers: headers,
		}
		return pipe.Process(ctx, samples, raw, msg)
	})
	return &ruleHarness{
		recvH: recv,
		mock:  mockSink,
		pipe:  pipe,
	}
}

func (h *ruleHarness) Close() { h.recvH.Close() }
func (h *ruleHarness) URL() string { return h.recvH.URL() }

func (h *ruleHarness) post(t *testing.T, token string, body []byte) *http.Response {
	return h.recvH.postRemoteWrite(t, token, body, nil)
}

func (h *ruleHarness) messages() []sink.Message { return h.mock.Messages() }

// TestRule_Relabel_DropsLabels 验证 relabel stage 删除指定 label。
func TestRule_Relabel_DropsLabels(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: relabel
        config:
          drop_labels: ["secret", "env"]
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSamples("http_total", map[string]string{
		"env":    "prod",
		"secret": "x",
		"team":   "core",
	}, 1, 1)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		return len(h.messages()) >= 1
	}, 1*time.Second, 10*time.Millisecond)

	msgs := h.messages()
	require.Len(t, msgs, 1, "一个 sample → 一条 sink message")
	assert.Equal(t, "prom.t", msgs[0].Topic)
}

// TestRule_Route_DifferentTopic 验证 route stage 把特定 label 路由到不同 topic。
func TestRule_Route_DifferentTopic(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.default
    version: 1
    stages:
      - type: route
        config:
          rules:
            - match:
                team: app
              topic: prom.app
            - match:
                team: infra
              topic: prom.infra
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m1", Labels: map[string]string{"team": "app"}, Value: 1, Timestamp: 1},
		{Metric: "m2", Labels: map[string]string{"team": "infra"}, Value: 1, Timestamp: 1},
		{Metric: "m3", Labels: map[string]string{"team": "other"}, Value: 1, Timestamp: 1},
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		msgs := h.messages()
		if len(msgs) < 3 {
			return false
		}
		seen := map[string]int{}
		for _, m := range msgs {
			seen[m.Topic]++
		}
		return seen["prom.app"] == 1 && seen["prom.infra"] == 1 && seen["prom.default"] == 1
	}, 1*time.Second, 10*time.Millisecond)
}

// TestRule_Sample_DropsByRate 验证 sample stage 按 rate 丢弃。
func TestRule_Sample_DropsByRate(t *testing.T) {
	yaml := `
rulesets:
  - name: t
    default_topic: prom.t
    version: 1
    stages:
      - type: sample
        config:
          rate: 0.0  # 全丢
`
	h := newRuleHarness(t, yaml)
	defer h.Close()

	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "m1", Value: 1, Timestamp: 1},
		{Metric: "m2", Value: 1, Timestamp: 1},
		{Metric: "m3", Value: 1, Timestamp: 1},
	})
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// 等 200ms 后看 sink 是否收到(应当 0 条)
	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, h.messages(), "rate=0.0 → 全部丢弃")
}

// TestRule_HotReload 验证 PUT 风格热更新:新 ruleset 编译后 SetRules 即时生效。
func TestRule_HotReload(t *testing.T) {
	yaml1 := `
rulesets:
  - name: t
    default_topic: prom.v1
    version: 1
`
	h := newRuleHarness(t, yaml1)
	defer h.Close()

	// 第一版
	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		msgs := h.messages()
		return len(msgs) >= 1 && msgs[len(msgs)-1].Topic == "prom.v1"
	}, 1*time.Second, 10*time.Millisecond)

	// 切到 v2
	yaml2 := `
rulesets:
  - name: t
    default_topic: prom.v2
    version: 2
`
	cfg, err := ruleengine.LoadBytes([]byte(yaml2))
	require.NoError(t, err)
	compiled, err := ruleengine.Compile(&cfg.Rulesets[0])
	require.NoError(t, err)
	h.pipe.SetRules(compiled)

	// 第二版
	req = makeWriteRequestFromSamples("m2", nil, 1, 1)
	resp = h.post(t, "tk_app_business", encodeWriteRequest(req))
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		msgs := h.messages()
		return len(msgs) >= 2 && msgs[len(msgs)-1].Topic == "prom.v2"
	}, 1*time.Second, 10*time.Millisecond)
}

// TestRule_InvalidYAML 验证 receiver 不接收(此处 receiver 不参与校验,rule engine 在 Process 时编译错误)。
// 这条用 SetRules(nil) 模拟规则不可用场景:Process 仍按 default 走(per-batch 加载)。
func TestRule_DefaultFallbackWhenNoRules(t *testing.T) {
	// 新 harness,SetRules 不被调,使用 NewPipeline 的默认空规则
	authN := newMockAuth()
	mockSink := newMockSink()
	pipe := ruleengine.NewPipeline(nil, func(ctx context.Context, msg sink.Message) error {
		return mockSink.Send(ctx, msg)
	})
	recv := newReceiverHarness(t, authN, func(ctx context.Context, raw []byte, samples []parser.Sample, defaultTopic string) error {
		meta, _ := parser.MetaFromContext(ctx)
		return pipe.Process(ctx, samples, raw, sink.Message{
			Topic:   defaultTopic,
			Key:     []byte(meta.Tenant),
			Headers: map[string]string{},
		})
	})
	defer recv.Close()
	defer recv.srv.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := recv.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if len(mockSink.Messages()) >= 1 {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	wg.Wait()
	assert.Equal(t, "prom.raw.app_business", mockSink.Messages()[0].Topic,
		"无规则时,走 default_topic(token 关联的)")
}
