//go:build integration

// Package integration 包含端到端集成测试。
//
// 这些测试在 `make test-integration` 时被启用(`-tags=integration`),
// 默认 `go test ./...` 不会跑,确保 PR 默认快速。
//
// 文件分类:
//   - passthrough_test.go: T1.10 接收 + 解码 + 解析 + 投递(用 mock sink 替代 Kafka)
//   - rule_test.go:        T2.11 规则引擎集成(relabel/route/sample 三类规则端到端)
//   - stages_test.go:      T3.5 高级 stage 集成(enrich/downsample/deadvalue)
//   - wal_test.go:         T1.10 WAL 端到端(写盘 → 重放 → 投递)
//   - admin_test.go:       T4.9 Admin API 端到端(httptest + 真实 service)
//
// 通用 mock 全部在本目录内,避免依赖 testcontainers / Docker。
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/lynnyq/prom-gw/internal/auth"
	"github.com/lynnyq/prom-gw/internal/obs"
	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/receiver"
	"github.com/lynnyq/prom-gw/internal/sink"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// TestMain 启动时初始化 OTel propagator(为 tracing test 服务)+ 全局 obs 注册。
func TestMain(m *testing.M) {
	// 设置 W3C propagator(为 traceparent 解析/注入)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	// 初始化 obs 指标(让 metric 测试可读 DefaultGatherer)
	obs.InitMetricsForTest()
	m.Run()
}

// mockAuth 实现 auth.Authenticator 接口,只接受预置 token。
type mockAuth struct {
	mu     sync.RWMutex
	tokens map[string]auth.Tenant
}

func newMockAuth() *mockAuth {
	return &mockAuth{
		tokens: map[string]auth.Tenant{
			"tk_app_business": {Name: "app-business", TenantID: "1001", DefaultTopic: "prom.raw.app_business", RateLimit: 80000},
			"tk_infra":        {Name: "infra", TenantID: "1002", DefaultTopic: "prom.raw.infra", RateLimit: 50000},
		},
	}
}

func (a *mockAuth) Verify(_ context.Context, token string) (auth.Tenant, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if token == "" {
		return auth.Tenant{}, auth.ErrTokenMissing
	}
	t, ok := a.tokens[token]
	if !ok {
		return auth.Tenant{}, auth.ErrTokenInvalid
	}
	return t, nil
}

func (a *mockAuth) add(token string, t auth.Tenant) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tokens[token] = t
}

// mockSink 实现 sink.Sink 接口,记录所有消息便于断言。
type mockSink struct {
	mu       sync.Mutex
	messages []sink.Message
	closed   atomic.Bool
	// rejectNext 临时把下一次 Send 返回 ErrBackpressure(模拟 503 路径)
	rejectNext atomic.Bool
	// alwaysReject 持续返回 ErrBackpressure(模拟持续背压)
	alwaysReject atomic.Bool
}

func newMockSink() *mockSink { return &mockSink{} }

func (m *mockSink) Send(_ context.Context, msg sink.Message) error {
	if m.closed.Load() {
		return sink.ErrClosed
	}
	if m.alwaysReject.Load() {
		return sink.ErrBackpressure
	}
	if m.rejectNext.Swap(false) {
		return sink.ErrBackpressure
	}
	m.mu.Lock()
	m.messages = append(m.messages, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockSink) Close() error {
	m.closed.Store(true)
	return nil
}

func (m *mockSink) Messages() []sink.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sink.Message, len(m.messages))
	copy(out, m.messages)
	return out
}

// --- receiver harness ---

// receiverHarness 用 httptest 把 receiver 串起来,提供 HTTP 客户端和指标读取。
//
// obs 指标走 DefaultGatherer(由 TestMain 的 InitMetricsForTest 触发),
// 所以测试可直接读 DefaultGatherer.Gather()。
type receiverHarness struct {
	srv     *httptest.Server
	server  *receiver.Server
	auth    *mockAuth
	handler func(context.Context, []byte, []parser.Sample, string) error
}

func newReceiverHarness(t *testing.T, authN auth.Authenticator, handler func(context.Context, []byte, []parser.Sample, string) error) *receiverHarness {
	t.Helper()
	srv, err := receiver.New(receiver.Config{
		Addr:            "127.0.0.1:0",
		Authenticator:   authN,
		Logger:          zap.NewNop(),
		SourceDC:        "dc-test",
		Handler:         handler,
		GlobalRateLimit: 100000,
		MaxBodyBytes:    16 * 1024 * 1024,
	})
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	return &receiverHarness{
		srv:     ts,
		server:  srv,
		auth:    authN.(*mockAuth),
		handler: handler,
	}
}

func (h *receiverHarness) Close() {
	h.srv.Close()
}

func (h *receiverHarness) URL() string { return h.srv.URL }

func (h *receiverHarness) postRemoteWrite(t *testing.T, token string, body []byte, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.URL()+"/api/v1/write", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// globalMetricValue 读 DefaultGatherer 的 metric 值(按 label 子集匹配)。
func globalMetricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prom.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if matchMetricLabels(m.GetLabel(), labels) {
				if m.Counter != nil {
					return m.Counter.GetValue()
				}
				if m.Gauge != nil {
					return m.Gauge.GetValue()
				}
				return 0
			}
		}
	}
	return 0
}

func matchMetricLabels(have []*dto.LabelPair, want map[string]string) bool {
	got := map[string]string{}
	for _, l := range have {
		got[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// metricValue 在 harness 内调 globalMetricValue,便于测试代码引用。
func (h *receiverHarness) metricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	return globalMetricValue(t, name, labels)
}

// --- prompb helpers ---

// encodeWriteRequest 把 prompb.WriteRequest 序列化为 snappy+protobuf 字节(模拟 Prometheus client)。
func encodeWriteRequest(req *prompb.WriteRequest) []byte {
	raw, _ := proto.Marshal(req)
	return snappy.Encode(nil, raw)
}

// makeWriteRequestFromSamples 构造一个 WriteRequest 包含 1 series(1 sample)。
func makeWriteRequestFromSamples(metric string, labels map[string]string, value float64, ts int64) *prompb.WriteRequest {
	pl := make([]prompb.Label, 0, len(labels)+1)
	pl = append(pl, prompb.Label{Name: "__name__", Value: metric})
	for k, v := range labels {
		pl = append(pl, prompb.Label{Name: k, Value: v})
	}
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels:  pl,
			Samples: []prompb.Sample{{Value: value, Timestamp: ts}},
		}},
	}
}

// makeWriteRequestFromSerieses 构造含 N 个 series 的 WriteRequest。
func makeWriteRequestFromSerieses(serieses []seriesFixture) *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0, len(serieses)),
	}
	for _, s := range serieses {
		pl := make([]prompb.Label, 0, len(s.Labels)+1)
		pl = append(pl, prompb.Label{Name: "__name__", Value: s.Metric})
		for k, v := range s.Labels {
			pl = append(pl, prompb.Label{Name: k, Value: v})
		}
		req.Timeseries = append(req.Timeseries, prompb.TimeSeries{
			Labels:  pl,
			Samples: []prompb.Sample{{Value: s.Value, Timestamp: s.Timestamp}},
		})
	}
	return req
}

// seriesFixture 描述一个 series 构造参数。
type seriesFixture struct {
	Metric    string
	Labels    map[string]string
	Value     float64
	Timestamp int64
}

// encodeSamplesAsJSON 把 samples 序列化成 JSON 字节(测试 mock sink 使用)。
func encodeSamplesAsJSON(samples []parser.Sample) []byte {
	b, _ := json.Marshal(samples)
	return b
}

// runWithTimeout 限时跑 fn,超时返回 err(测试中的统一 wait 工具)。
func runWithTimeout(t *testing.T, d time.Duration, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(d):
		return errTimeout{d: d}
	}
}

type errTimeout struct{ d time.Duration }

func (e errTimeout) Error() string { return "timeout after " + e.d.String() }
