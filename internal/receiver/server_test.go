package receiver

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// stubAuth 测试用 Authenticator。
type stubAuth struct {
	tokens map[string]auth.Tenant
}

func (s *stubAuth) Verify(_ context.Context, token string) (auth.Tenant, error) {
	if token == "" {
		return auth.Tenant{}, auth.ErrTokenMissing
	}
	t, ok := s.tokens[token]
	if !ok {
		return auth.Tenant{}, auth.ErrTokenInvalid
	}
	return t, nil
}

func newTestServer(t *testing.T, h func(context.Context, []byte, []parser.Sample, string) error) *Server {
	t.Helper()
	a := &stubAuth{tokens: map[string]auth.Tenant{
		"good": {Name: "t1", DefaultTopic: "prom.raw.t1", RateLimit: 100000},
	}}
	s, err := New(Config{
		Authenticator: a,
		Logger:        zap.NewNop(),
		SourceDC:      "dc-test",
		Handler:       h,
	})
	require.NoError(t, err)
	return s
}

func encodeWriteRequest(req *prompb.WriteRequest) []byte {
	raw, _ := proto.Marshal(req)
	return snappy.Encode(nil, raw)
}

func doRequest(t *testing.T, s *Server, token, ct, ce string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/write", bytes.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if ct != "" {
		r.Header.Set("Content-Type", ct)
	}
	if ce != "" {
		r.Header.Set("Content-Encoding", ce)
	}
	s.routes().ServeHTTP(w, r)
	return w
}

func TestAuth_MissingToken_Returns401(t *testing.T) {
	s := newTestServer(t, nil)
	w := doRequest(t, s, "", "application/x-protobuf", "snappy", nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_BadToken_Returns401(t *testing.T) {
	s := newTestServer(t, nil)
	w := doRequest(t, s, "nope", "application/x-protobuf", "snappy", nil)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuth_GoodToken_PassesThrough(t *testing.T) {
	s := newTestServer(t, nil)
	w := doRequest(t, s, "good", "application/x-protobuf", "snappy",
		encodeWriteRequest(&prompb.WriteRequest{}))
	if w.Code != http.StatusOK {
		t.Logf("body=%s", w.Body.String())
	}
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestContentType_Rejected(t *testing.T) {
	s := newTestServer(t, nil)
	w := doRequest(t, s, "good", "text/plain", "snappy",
		encodeWriteRequest(&prompb.WriteRequest{}))
	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestContentEncoding_Rejected(t *testing.T) {
	s := newTestServer(t, nil)
	w := doRequest(t, s, "good", "application/x-protobuf", "gzip",
		encodeWriteRequest(&prompb.WriteRequest{}))
	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestMethodNotAllowed(t *testing.T) {
	s := newTestServer(t, nil)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/write", nil)
	r.Header.Set("Authorization", "Bearer good")
	s.routes().ServeHTTP(w, r)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestHandler_Invoked(t *testing.T) {
	var got atomic.Int32
	s := newTestServer(t, func(_ context.Context, _ []byte, batch []parser.Sample, _ string) error {
		got.Add(int32(len(batch)))
		return nil
	})

	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			},
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "down"}},
				Samples: []prompb.Sample{{Value: 0, Timestamp: 1}},
			},
		},
	}
	w := doRequest(t, s, "good", "application/x-protobuf", "snappy",
		encodeWriteRequest(req))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, int32(2), got.Load())
}

func TestHandler_Error_Returns503(t *testing.T) {
	s := newTestServer(t, func(_ context.Context, _ []byte, _ []parser.Sample, _ string) error {
		return assert.AnError
	})
	w := doRequest(t, s, "good", "application/x-protobuf", "snappy",
		encodeWriteRequest(&prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{Labels: []prompb.Label{{Name: "__name__", Value: "x"}},
					Samples: []prompb.Sample{{Value: 1, Timestamp: 1}}},
			},
		}))
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestClientIP_XFF(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Forwarded-For", "1.2.3.4, 10.0.0.1")
	r.RemoteAddr = "127.0.0.1:1234"
	assert.Equal(t, "1.2.3.4", clientIP(r))
}

func TestClientIP_RemoteAddr(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.RemoteAddr = "127.0.0.1:1234"
	assert.Equal(t, "127.0.0.1", clientIP(r))
}

func TestExtractBearer(t *testing.T) {
	assert.Equal(t, "abc", extractBearer("Bearer abc"))
	assert.Equal(t, "abc", extractBearer("Bearer  abc  "))
	assert.Equal(t, "", extractBearer("Basic abc"))
	assert.Equal(t, "", extractBearer(""))
}

func TestRecoverer_CatchesPanic(t *testing.T) {
	s := newTestServer(t, nil)
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	wrapped := s.recovererMW(panicHandler)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	wrapped.ServeHTTP(w, r)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestReadAll(t *testing.T) {
	got, err := readAll(strings.NewReader("hello world"))
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(got))
}

func TestReadAll_Empty(t *testing.T) {
	got, err := readAll(strings.NewReader(""))
	require.NoError(t, err)
	assert.Empty(t, got)
}
