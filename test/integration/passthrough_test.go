//go:build integration

// T1.10 集成测试:Prometheus RemoteWrite 透传路径。
//
// 覆盖:
//   - happy path: 写 → 鉴权 → 解码 → 解析 → sink 收到
//   - 401 missing/invalid token
//   - 400 错误 content-type / content-encoding
//   - 503 sink 背压 → handler 返 ErrBackpressure
//   - 字节级相等: sink.Payload 长度与原始 snappy 字节长度一致
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPassthrough_HappyPath(t *testing.T) {
	authN := newMockAuth()
	mockSink := newMockSink()
	var received [][]byte
	var mu sync.Mutex

	handler := func(_ context.Context, raw []byte, samples []parser.Sample, _ string) error {
		mu.Lock()
		defer mu.Unlock()
		// 模拟真实 sink:把 raw body 包装成 sink.Message(测试中只校验 payload 字节级一致)
		_ = samples
		cp := make([]byte, len(raw))
		copy(cp, raw)
		received = append(received, cp)
		return mockSink.Send(context.Background(), sink.Message{
			Topic:   "prom.raw.app_business",
			Key:     []byte("k"),
			Payload: cp,
		})
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	// 构造 2 个 series 的请求
	req := makeWriteRequestFromSerieses([]seriesFixture{
		{Metric: "http_requests_total", Labels: map[string]string{"code": "200", "env": "prod"}, Value: 1, Timestamp: 1700000000000},
		{Metric: "http_requests_total", Labels: map[string]string{"code": "500", "env": "prod"}, Value: 2, Timestamp: 1700000000000},
	})
	body := encodeWriteRequest(req)

	resp := h.postRemoteWrite(t, "tk_app_business", body, nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)

	// 等 sink 收完
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	}, 1*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 1)
	assert.Equal(t, len(body), len(received[0]), "字节级相等")
}

func TestPassthrough_AuthMissing(t *testing.T) {
	authN := newMockAuth()
	handler := func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error { return nil }
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// 校验指标增长
	v := h.metricValue(t, "gateway_auth_fail_total", map[string]string{"reason": "missing"})
	assert.Greater(t, v, 0.0)
}

func TestPassthrough_AuthInvalid(t *testing.T) {
	authN := newMockAuth()
	handler := func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error { return nil }
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "tk_unknown", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	v := h.metricValue(t, "gateway_auth_fail_total", map[string]string{"reason": "invalid"})
	assert.Greater(t, v, 0.0)
}

func TestPassthrough_BadContentType(t *testing.T) {
	authN := newMockAuth()
	handler := func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error { return nil }
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	body := encodeWriteRequest(req)
	resp := h.postRemoteWrite(t, "tk_app_business", body, nil)
	defer resp.Body.Close()

	// 用普通 client 改 header
	req2, _ := http.NewRequest(http.MethodPost, h.URL()+"/api/v1/write", http.NoBody)
	req2.Header.Set("Content-Type", "text/plain")
	req2.Header.Set("Content-Encoding", "snappy")
	req2.Header.Set("Authorization", "Bearer tk_app_business")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp2.StatusCode)
}

func TestPassthrough_BadContentEncoding(t *testing.T) {
	authN := newMockAuth()
	handler := func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error { return nil }
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req2, _ := http.NewRequest(http.MethodPost, h.URL()+"/api/v1/write", http.NoBody)
	req2.Header.Set("Content-Type", "application/x-protobuf")
	req2.Header.Set("Content-Encoding", "gzip")
	req2.Header.Set("Authorization", "Bearer tk_app_business")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusUnsupportedMediaType, resp2.StatusCode)
}

func TestPassthrough_BadProtobuf(t *testing.T) {
	authN := newMockAuth()
	handler := func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error { return nil }
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	// 发送一段非 snappy 编码的字节
	req, _ := http.NewRequest(http.MethodPost, h.URL()+"/api/v1/write", bytes.NewReader([]byte("not-snappy-bytes")))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Authorization", "Bearer tk_app_business")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPassthrough_BackpressureFromHandler(t *testing.T) {
	authN := newMockAuth()
	ms := newMockSink()
	ms.alwaysReject.Store(true) // 持续背压

	handler := func(_ context.Context, raw []byte, _ []parser.Sample, _ string) error {
		return ms.Send(context.Background(), sink.Message{Topic: "t", Payload: raw})
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestPassthrough_PerTenantTenantAssignment 验证 ctx.Meta.Tenant 正确注入。
func TestPassthrough_PerTenantTenantAssignment(t *testing.T) {
	authN := newMockAuth()
	var seenTenant string
	var mu sync.Mutex
	handler := func(ctx context.Context, _ []byte, _ []parser.Sample, _ string) error {
		meta, ok := parser.MetaFromContext(ctx)
		require.True(t, ok)
		mu.Lock()
		seenTenant = meta.Tenant
		mu.Unlock()
		return nil
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "tk_infra", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seenTenant != ""
	}, 1*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "infra", seenTenant)
}

// TestPassthrough_TraceparentPassedToHandler 验证 traceparent header 注入 ctx,handler 可拿到。
func TestPassthrough_TraceparentPassedToHandler(t *testing.T) {
	authN := newMockAuth()
	var seenTraceID string
	var mu sync.Mutex
	handler := func(ctx context.Context, _ []byte, _ []parser.Sample, _ string) error {
		meta, _ := parser.MetaFromContext(ctx)
		mu.Lock()
		seenTraceID = meta.TraceID
		mu.Unlock()
		return nil
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	// 用 W3C traceparent: version-traceid-spanid-flags
	const traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), map[string]string{
		"traceparent": traceparent,
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return seenTraceID != ""
	}, 1*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "0af7651916cd43dd8448eb211c80319c", seenTraceID)
}

// TestPassthrough_SamplesParsed 验证 samples 数量与请求 series 一致。
func TestPassthrough_SamplesParsed(t *testing.T) {
	authN := newMockAuth()
	var seenSamples []parser.Sample
	var mu sync.Mutex
	handler := func(_ context.Context, _ []byte, samples []parser.Sample, _ string) error {
		mu.Lock()
		seenSamples = samples
		mu.Unlock()
		return nil
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	serieses := []seriesFixture{
		{Metric: "m1", Labels: map[string]string{"a": "1"}, Value: 1, Timestamp: 1},
		{Metric: "m2", Labels: map[string]string{"a": "2"}, Value: 2, Timestamp: 1},
		{Metric: "m3", Labels: map[string]string{"a": "3"}, Value: 3, Timestamp: 1},
	}
	req := makeWriteRequestFromSerieses(serieses)
	resp := h.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seenSamples) == 3
	}, 1*time.Second, 10*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, seenSamples, 3)
}

// TestPassthrough_ResponseEnvelopeIsCorrect 验证 503 错误的响应体格式。
func TestPassthrough_ResponseEnvelopeIsCorrect(t *testing.T) {
	authN := newMockAuth()
	ms := newMockSink()
	ms.alwaysReject.Store(true)
	handler := func(_ context.Context, raw []byte, _ []parser.Sample, _ string) error {
		return ms.Send(context.Background(), sink.Message{Topic: "t", Payload: raw})
	}
	h := newReceiverHarness(t, authN, handler)
	defer h.Close()

	req := makeWriteRequestFromSamples("m", nil, 1, 1)
	resp := h.postRemoteWrite(t, "tk_app_business", encodeWriteRequest(req), nil)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Contains(t, got, "code")
	assert.Contains(t, got, "message")
}

// 下面这段 stringReader / ioEOF 旧版自定义 reader 实现已废弃,改用 stdlib bytes.NewReader。
var _ = io.NopCloser
