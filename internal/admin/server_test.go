// Package admin - server_test.go: Admin API 集成测试。
//
// 覆盖场景(plan T4.5 / T4.9):
//   - /v1/healthz
//   - /v1/rulesets 列表
//   - PUT /v1/rulesets/{name} 创建
//   - GET /v1/rulesets/{name} 读取
//   - POST /v1/rulesets/{name}:reload
//   - POST /v1/rulesets/{name}:rollback?to_version=N
//   - GET /v1/rulesets/{name}/history
//   - /v1/businesses /v1/stats
//   - IP 白名单拒绝
//   - 404 / 400 / 405
package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lynnyq/prom-gw/internal/auth"
	"github.com/lynnyq/prom-gw/internal/config"
	"github.com/lynnyq/prom-gw/internal/ruleengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mock Service ---

type mockService struct {
	mu          sync.Mutex
	ruleSets    map[string]config.HistoryRecord
	reloadCount atomic.Int32
	rollbackTo  int64
	businesses  []auth.Business
}

func newMockService() *mockService {
	return &mockService{
		ruleSets: make(map[string]config.HistoryRecord),
		businesses: []auth.Business{
			{Name: "app-business", BusinessID: "1001", DefaultTopic: "prom.routed.app_business", RateLimit: 80000},
			{Name: "infra", BusinessID: "1002", DefaultTopic: "prom.routed.infra", RateLimit: 50000},
		},
	}
}

func (m *mockService) addRuleset(name string, version int64, yaml string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg, err := ruleengine.LoadBytes([]byte(yaml))
	if err != nil {
		panic(err)
	}
	rs := &cfg.Rulesets[0]
	rs.Version = version
	compiled, err := ruleengine.Compile(rs)
	if err != nil {
		panic(err)
	}
	m.ruleSets[name] = config.HistoryRecord{
		Name: name, Version: version, RawYAML: []byte(yaml), Source: "api", Compiled: compiled,
	}
}

func (m *mockService) ListRuleSets(_ context.Context) []RuleSetSummary {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RuleSetSummary, 0, len(m.ruleSets))
	for n, r := range m.ruleSets {
		stages := 0
		if r.Compiled != nil {
			stages = len(r.Compiled.Stages)
		}
		out = append(out, RuleSetSummary{Name: n, Version: r.Version, Stages: stages, Source: r.Source})
	}
	return out
}

func (m *mockService) GetRuleSet(_ context.Context, name string) (RuleSetDetail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.ruleSets[name]
	if !ok {
		return RuleSetDetail{}, ruleengine.ErrRuleSetNotFound
	}
	return RuleSetDetail{
		Name: name, Version: r.Version,
		RawYAML: string(r.RawYAML), Source: r.Source,
	}, nil
}

func (m *mockService) PutRuleSet(_ context.Context, name string, version int64, raw []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.ruleSets[name]; ok && version <= r.Version {
		return 0, ruleengine.ErrRuleSetConflict
	}
	cfg, err := ruleengine.LoadBytes(raw)
	if err != nil {
		return 0, ruleengine.ErrRuleSetInvalid
	}
	var rs *ruleengine.RuleSet
	for i := range cfg.Rulesets {
		if cfg.Rulesets[i].Name == name {
			cfg.Rulesets[i].Version = version
			rs = &cfg.Rulesets[i]
			break
		}
	}
	if rs == nil {
		return 0, ruleengine.ErrRuleSetNotFound
	}
	compiled, err := ruleengine.Compile(rs)
	if err != nil {
		return 0, ruleengine.ErrRuleSetInvalid
	}
	m.ruleSets[name] = config.HistoryRecord{
		Name: name, Version: version, RawYAML: raw, Source: "api", Compiled: compiled,
	}
	return version, nil
}

func (m *mockService) ReloadRuleSet(_ context.Context, name string) error {
	m.reloadCount.Add(1)
	return nil
}

func (m *mockService) RollbackRuleSet(_ context.Context, name string, to int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.ruleSets[name]
	if !ok {
		return ruleengine.ErrRuleSetNotFound
	}
	if to >= r.Version {
		return ruleengine.ErrRuleSetVersionGone
	}
	m.rollbackTo = to
	return nil
}

func (m *mockService) ListHistory(_ context.Context, name string) []config.HistoryRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.ruleSets[name]
	if !ok {
		return nil
	}
	return []config.HistoryRecord{r}
}

func (m *mockService) ListBusinesses(_ context.Context) []BusinessInfo {
	out := make([]BusinessInfo, len(m.businesses))
	for i, t := range m.businesses {
		out[i] = BusinessInfo{
			Business: t.Name, BusinessID: t.BusinessID,
			DefaultTopic: t.DefaultTopic, RateLimit: t.RateLimit,
		}
	}
	return out
}

func (m *mockService) Stats(_ context.Context) StatsResp {
	return StatsResp{
		CurrentRuleSet: "app-business", CurrentVersion: 3,
		BySource: map[string]int{"api": 1}, HistorySize: 1,
	}
}

// --- helpers ---

func newTestServer(t *testing.T, svc Service, cidr ...string) *Server {
	t.Helper()
	if len(cidr) == 0 {
		cidr = []string{"127.0.0.1/32"}
	}
	s, err := New(Config{
		Addr:      ":0",
		AllowCIDR: cidr,
		Logger:    zap.NewNop(),
	}, svc)
	require.NoError(t, err)
	return s
}

func doRequest(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		switch v := body.(type) {
		case []byte:
			rdr = bytes.NewReader(v)
		case string:
			rdr = bytes.NewReader([]byte(v))
		default:
			b, err := json.Marshal(v)
			require.NoError(t, err)
			rdr = bytes.NewReader(b)
		}
	}
	var r *http.Request
	if rdr != nil {
		r = httptest.NewRequest(method, path, rdr)
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.RemoteAddr = "127.0.0.1:12345"
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	return rr
}

// --- tests ---

func TestServer_Healthz(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodGet, "/v1/healthz", nil)
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"status"`)
}

func TestServer_Healthz_MethodNotAllowed(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodPost, "/v1/healthz", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestServer_RulesetsList_Empty(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodGet, "/v1/rulesets", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"rulesets"`)
}

func TestServer_RulesetsList_AfterPut(t *testing.T) {
	svc := newMockService()
	svc.addRuleset("app-business", 1, `
rulesets:
  - name: app-business
    default_topic: prom.app
    version: 1
    stages: []
`)
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodGet, "/v1/rulesets", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "app-business")
}

func TestServer_PutRuleSet_Success(t *testing.T) {
	svc := newMockService()
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodPut, "/v1/rulesets/app-business", map[string]any{
		"version": 1,
		"raw_yaml": `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 1
    stages: []
`,
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, `"version":1`)
}

func TestServer_PutRuleSet_InvalidYAML(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodPut, "/v1/rulesets/app-business", map[string]any{
		"version": 1,
		"raw_yaml": "rulesets: : bad",
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "invalid")
}

func TestServer_PutRuleSet_VersionConflict(t *testing.T) {
	svc := newMockService()
	svc.addRuleset("app-business", 5, `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 5
    stages: []
`)
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodPut, "/v1/rulesets/app-business", map[string]any{
		"version": 3,
		"raw_yaml": `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 3
    stages: []
`,
	})
	assert.Equal(t, http.StatusConflict, rr.Code)
}

func TestServer_PutRuleSet_MissingVersion(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodPut, "/v1/rulesets/app-business", map[string]any{
		"raw_yaml": "rulesets: []\n",
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestServer_GetRuleSet_NotFound(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodGet, "/v1/rulesets/missing", nil)
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestServer_GetRuleSet_OK(t *testing.T) {
	svc := newMockService()
	svc.addRuleset("a", 1, `rulesets:
  - name: a
    default_topic: t
    version: 1
    stages: []
`)
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodGet, "/v1/rulesets/a", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"name":"a"`)
}

func TestServer_Reload(t *testing.T) {
	svc := newMockService()
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodPost, "/v1/rulesets/app-business:reload", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, int32(1), svc.reloadCount.Load())
}

func TestServer_Rollback_OK(t *testing.T) {
	svc := newMockService()
	svc.addRuleset("a", 5, `rulesets:
  - name: a
    default_topic: t
    version: 5
    stages: []
`)
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodPost, "/v1/rulesets/a:rollback?to_version=3", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, int64(3), svc.rollbackTo)
}

func TestServer_Rollback_MissingVersion(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodPost, "/v1/rulesets/a:rollback", nil)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestServer_History(t *testing.T) {
	svc := newMockService()
	svc.addRuleset("a", 1, `rulesets:
  - name: a
    default_topic: t
    version: 1
    stages: []
`)
	s := newTestServer(t, svc)
	rr := doRequest(t, s, http.MethodGet, "/v1/rulesets/a/history", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"history"`)
}

func TestServer_Businesses(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodGet, "/v1/businesses", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "app-business")
}

func TestServer_Stats(t *testing.T) {
	s := newTestServer(t, newMockService())
	rr := doRequest(t, s, http.MethodGet, "/v1/stats", nil)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), `"current_ruleset"`)
}

func TestServer_IPAllowList_BlocksExternal(t *testing.T) {
	s := newTestServer(t, newMockService(), "10.0.0.0/8")
	// mock 一个外网 IP
	r := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	r.RemoteAddr = "203.0.113.5:1234"
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Greater(t, s.AuthFailCount(), int64(0))
}

func TestServer_IPAllowList_AllowsLocal(t *testing.T) {
	s := newTestServer(t, newMockService(), "10.0.0.0/8")
	r := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	r.RemoteAddr = "10.1.2.3:9999"
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, r)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestServer_InvalidCIDR_FastFail(t *testing.T) {
	_, err := New(Config{Addr: ":0", AllowCIDR: []string{"not-a-cidr"}, Logger: zap.NewNop()}, newMockService())
	assert.Error(t, err)
}

// --- helpers_test.go: parseCIDRs / parseClientIP / parsePutBody ---

func TestParseCIDRs_AcceptsBareIP(t *testing.T) {
	nets, err := parseCIDRs([]string{"10.0.0.1"})
	require.NoError(t, err)
	require.Len(t, nets, 1)
	assert.True(t, nets[0].Contains(net.ParseIP("10.0.0.1")))
}

func TestParseCIDRs_RejectsInvalid(t *testing.T) {
	_, err := parseCIDRs([]string{"999.0.0.0/8"})
	assert.Error(t, err)
}

func TestParseInt64Query(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/x?to_version=42", nil)
	v, err := parseInt64Query(r, "to_version")
	require.NoError(t, err)
	assert.Equal(t, int64(42), v)

	r2 := httptest.NewRequest(http.MethodGet, "/x?to_version=abc", nil)
	_, err = parseInt64Query(r2, "to_version")
	assert.Error(t, err)

	r3 := httptest.NewRequest(http.MethodGet, "/x", nil)
	_, err = parseInt64Query(r3, "to_version")
	assert.Error(t, err)
}

func TestParsePutBody_JSON(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/x?version=3", strings.NewReader(`{"version":7,"raw_yaml":"k: v"}`))
	r.Header.Set("Content-Type", "application/json")
	v, raw, err := parsePutBody(r)
	require.NoError(t, err)
	assert.Equal(t, int64(7), v)
	assert.Equal(t, "k: v", string(raw))
}

func TestParsePutBody_YAML(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/x?version=9", strings.NewReader("rulesets: []\n"))
	r.Header.Set("Content-Type", "application/yaml")
	v, raw, err := parsePutBody(r)
	require.NoError(t, err)
	assert.Equal(t, int64(9), v)
	assert.Equal(t, "rulesets: []\n", string(raw))
}

func TestParsePutBody_Invalid(t *testing.T) {
	// bad content-type
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	r.Header.Set("Content-Type", "text/plain")
	_, _, err := parsePutBody(r)
	assert.Error(t, err)

	// json 缺 version
	r2 := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(`{"raw_yaml":"x"}`))
	r2.Header.Set("Content-Type", "application/json")
	_, _, err = parsePutBody(r2)
	assert.Error(t, err)
}
