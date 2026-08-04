// Package httpx - response_test.go: 验证 Response / WriteErr / Sentinel 行为。
package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite_SuccessShape(t *testing.T) {
	rr := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/foo", nil)
	Write(rr, r, map[string]string{"hello": "world"})

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var body Response
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&body))
	assert.Equal(t, CodeOK, body.Code)
	assert.Equal(t, "ok", body.Message)
	assert.Equal(t, "world", body.Data.(map[string]any)["hello"])
}

func TestWriteErr_HTTPStatusMapping(t *testing.T) {
	cases := []struct {
		code   int
		status int
	}{
		{CodeUnauthorized, http.StatusUnauthorized},
		{CodeAuthMissing, http.StatusUnauthorized},
		{CodeAuthInvalid, http.StatusUnauthorized},
		{CodeForbidden, http.StatusForbidden},
		{CodeRuleSetNotFound, http.StatusNotFound},
		{CodeRuleSetVersionGone, http.StatusNotFound},
		{CodeRuleSetConflict, http.StatusConflict},
		{CodeMethodNotAllowed, http.StatusMethodNotAllowed},
		{CodeBadRequest, http.StatusBadRequest},
		{CodeRuleSetInvalid, http.StatusBadRequest},
		{CodeInternal, http.StatusInternalServerError},
		{CodeUnavailable, http.StatusServiceUnavailable},
		{99999, http.StatusInternalServerError},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		WriteErr(rr, r, c.code, "boom")
		assert.Equalf(t, c.status, rr.Code, "code=%d", c.code)
	}
}

func TestWriteErrWithStatus_OverridesDefault(t *testing.T) {
	rr := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	// CodeRuleSetNotFound 默认 404;override 成 410
	WriteErrWithStatus(rr, r, http.StatusGone, CodeRuleSetNotFound, "expired")
	assert.Equal(t, http.StatusGone, rr.Code)
}

func TestAsSentinel(t *testing.T) {
	s := NewSentinel(CodeRuleSetNotFound, "missing")
	out := AsSentinel(s)
	require.NotNil(t, out)
	assert.Equal(t, CodeRuleSetNotFound, out.Code)
	assert.Equal(t, "missing", out.Message)

	// 普通 error 包装成 internal
	other := AsSentinel(assertError("plain"))
	assert.Equal(t, CodeInternal, other.Code)

	// nil → nil
	assert.Nil(t, AsSentinel(nil))
}

func TestResponse_DataOmittedWhenNil(t *testing.T) {
	rr := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	WriteErr(rr, r, CodeBadRequest, "bad")
	raw := rr.Body.String()
	// 没有 "data" 字段,只有 code/message
	assert.NotContains(t, raw, "data")
}

func TestResponse_TraceIDFromContext(t *testing.T) {
	// context 没有 trace 时,trace_id 为空(不报错)
	rr := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/x", nil).WithContext(context.Background())
	Write(rr, r, nil)
	body := rr.Body.String()
	assert.Contains(t, body, `"code":0`)
	// trace_id 可能不出现(omitempty)
}

// assertError 辅助构造一个普通 error。
type assertError string

func (e assertError) Error() string { return string(e) }
