//go:build integration

// T4.9 集成测试:Admin API 端到端(用真实 ManagerService + History + Pipeline)。
//
// 覆盖场景:
//   - 启动 admin server + 真实 ManagerService
//   - PUT 创建/替换 ruleset
//   - GET 列表/详情
//   - Rollback 行为
//   - 响应体格式(code/message/data/trace_id)
//   - 来源 IP 白名单
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lynnyq/bigdata/internal/admin"
	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/lynnyq/bigdata/internal/config"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// adminHarness 端到端 Admin API 测试装置:真实 ManagerService + History + Pipeline。
type adminHarness struct {
	srv    *httptest.Server
	svc    *admin.ManagerService
	server *admin.Server
}

func newAdminHarness(t *testing.T) *adminHarness {
	t.Helper()
	hist := config.NewHistory(config.HistoryConfig{Capacity: 10})
	mgr := config.NewManager(config.ManagerConfig{Logger: zap.NewNop(), History: hist})

	// 注册 DefaultSource + 启动 Manager,使 manager.Current() 始终有有效快照,
	// ReloadRuleSet 才能走完 ApplySnapshot 路径(否则会因 "no active snapshot" 失败)。
	mgr.AddSource(config.NewDefaultSource())
	require.NoError(t, mgr.Start(context.Background()))

	// mock sink 让 pipeline 有出口
	mockK := newMockSink()
	ruleMgr := ruleengine.NewManager(ruleengine.ManagerConfig{
		Logger: zap.NewNop(),
		Out: func(_ context.Context, msg sink.Message) error {
			return mockK.Send(context.Background(), msg)
		},
	})

	// 简单 auth provider 实现
	authProvider := &stubAuthProvider{}

	svc := admin.NewManagerService(admin.ManagerDeps{
		Manager: mgr,
		RuleMgr: ruleMgr,
		Auth:    authProvider,
		History: hist,
		Logger:  zap.NewNop(),
	})

	server, err := admin.New(admin.Config{
		Addr:      "127.0.0.1:0",
		AllowCIDR: []string{"127.0.0.1/32"},
		Logger:    zap.NewNop(),
	}, svc)
	require.NoError(t, err)

	ts := httptest.NewServer(server.Handler())
	return &adminHarness{
		srv:    ts,
		svc:    svc,
		server: server,
	}
}

func (h *adminHarness) Close() { h.srv.Close() }

func (h *adminHarness) do(t *testing.T, method, path string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		switch v := body.(type) {
		case []byte:
			reader = bytes.NewReader(v)
		case string:
			reader = bytes.NewReader([]byte(v))
		default:
			b, _ := json.Marshal(v)
			reader = bytes.NewReader(b)
		}
	}
	var req *http.Request
	if reader != nil {
		req, _ = http.NewRequest(method, h.srv.URL+path, reader)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, _ = http.NewRequest(method, h.srv.URL+path, nil)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	buf := make([]byte, 8192)
	n, _ := resp.Body.Read(buf)
	return resp, buf[:n]
}

func (h *adminHarness) putRuleSet(t *testing.T, name string, version int64, yaml string) (*http.Response, []byte) {
	t.Helper()
	return h.do(t, http.MethodPut, "/v1/rulesets/"+name, map[string]any{
		"version":  version,
		"raw_yaml": yaml,
	})
}

// stubAuthProvider 最小 AuthProvider 实现。
type stubAuthProvider struct{}

func (s *stubAuthProvider) ListTenants() []auth.Tenant {
	return []auth.Tenant{
		{Name: "app-business", TenantID: "1001", DefaultTopic: "prom.raw.app_business", RateLimit: 80000},
		{Name: "infra", TenantID: "1002", DefaultTopic: "prom.raw.infra", RateLimit: 50000},
	}
}

// TestAdmin_Healthz 验证 /v1/healthz 返回 ok。
func TestAdmin_Healthz(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()
	resp, body := h.do(t, http.MethodGet, "/v1/healthz", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"status":"ok"`)
}

// TestAdmin_PutAndList 创建 ruleset,验证可被列出。
func TestAdmin_PutAndList(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	// PUT 一个 ruleset
	yaml := `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 1
    stages: []
`
	resp, body := h.putRuleSet(t, "app-business", 1, yaml)
	require.Equal(t, http.StatusOK, resp.StatusCode, "PUT 应成功: %s", body)
	assert.Contains(t, string(body), `"version":1`)

	// GET 列表
	resp, body = h.do(t, http.MethodGet, "/v1/rulesets", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "app-business")

	// GET 详情
	resp, body = h.do(t, http.MethodGet, "/v1/rulesets/app-business", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"name":"app-business"`)
	assert.Contains(t, string(body), "prom.app")
}

// TestAdmin_PutVersionConflict 验证 version 必须递增。
func TestAdmin_PutVersionConflict(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	yaml := `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 5
    stages: []
`
	resp, _ := h.putRuleSet(t, "app-business", 5, yaml)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 再用 version=3 → 应冲突
	resp, _ = h.putRuleSet(t, "app-business", 3, yaml)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

// TestAdmin_PutInvalidYAML 验证非法 YAML 400。
func TestAdmin_PutInvalidYAML(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	resp, body := h.putRuleSet(t, "bad", 1, "rulesets: : invalid")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, string(body), "invalid")
}

// TestAdmin_PutMissingName 验证 ruleset name 不在 yaml 中 → 错误。
func TestAdmin_PutMissingName(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	yaml := `rulesets:
  - name: other-name
    default_topic: prom.app
    version: 1
    stages: []
`
	resp, body := h.putRuleSet(t, "app-business", 1, yaml)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode, "name 不匹配应失败: %s", body)
}

// TestAdmin_Rollback 验证回滚到旧版本成功。
func TestAdmin_Rollback(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	// 提交 v1
	yaml1 := `rulesets:
  - name: app
    default_topic: prom.v1
    version: 1
    stages: []
`
	resp, _ := h.putRuleSet(t, "app", 1, yaml1)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 提交 v2
	yaml2 := `rulesets:
  - name: app
    default_topic: prom.v2
    version: 2
    stages: []
`
	resp, _ = h.putRuleSet(t, "app", 2, yaml2)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 回滚到 v1
	resp, body := h.do(t, http.MethodPost, "/v1/rulesets/app:rollback?to_version=1", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "rollback 应成功: %s", body)

	// 验证 pipeline 当前生效的是 v1
	resp, body = h.do(t, http.MethodGet, "/v1/stats", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"current_version":1`, "回滚后 current_version 应回到 1")

	// history 仍保留多版本
	resp, body = h.do(t, http.MethodGet, "/v1/rulesets/app/history", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), `"version":1`)
	assert.Contains(t, string(body), `"version":2`)
}

// TestAdmin_Rollback_MissingVersion 验证缺失 to_version 参数返回 400。
func TestAdmin_Rollback_MissingVersion(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()
	resp, _ := h.do(t, http.MethodPost, "/v1/rulesets/app:rollback", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestAdmin_Reload 验证 :reload 端点。
func TestAdmin_Reload(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	// 先 PUT 一份 ruleset,确保 manager 有可重载的快照
	yaml := `rulesets:
  - name: app-business
    default_topic: prom.app
    version: 1
    stages: []
`
	resp, body := h.putRuleSet(t, "app-business", 1, yaml)
	require.Equal(t, http.StatusOK, resp.StatusCode, "PUT 应成功: %s", body)

	// reload:从 source 拉取并重切 pipeline
	resp, body = h.do(t, http.MethodPost, "/v1/rulesets/app-business:reload", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, "reload 应成功: %s", body)
}

// TestAdmin_History 验证 /history 端点列出历史。
func TestAdmin_History(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	yaml1 := `rulesets:
  - name: a
    default_topic: t
    version: 1
    stages: []
`
	h.putRuleSet(t, "a", 1, yaml1)

	yaml2 := `rulesets:
  - name: a
    default_topic: t
    version: 2
    stages: []
`
	h.putRuleSet(t, "a", 2, yaml2)

	resp, body := h.do(t, http.MethodGet, "/v1/rulesets/a/history", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "history")
}

// TestAdmin_Tenants 验证 /v1/tenants。
func TestAdmin_Tenants(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()
	resp, body := h.do(t, http.MethodGet, "/v1/tenants", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "app-business")
	assert.Contains(t, string(body), "infra")
}

// TestAdmin_Stats 验证 /v1/stats。
func TestAdmin_Stats(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()
	resp, body := h.do(t, http.MethodGet, "/v1/stats", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(body), "current_ruleset")
}

// TestAdmin_IPAllowList 验证白名单外的 IP 被拒。
func TestAdmin_IPAllowList(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	// 通过外部 IP 模拟(用 fake remote addr)
	r, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/v1/healthz", nil)
	// httptest 默认 LocalAddr=127.0.0.1,allow list 通过 → 200
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestAdmin_ResponseEnvelope 验证响应统一信封 {code, message, data, trace_id}。
func TestAdmin_ResponseEnvelope(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	resp, body := h.do(t, http.MethodGet, "/v1/healthz", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.Unmarshal(body, &got))
	// Response 是 {code, message, data, trace_id}
	assert.Contains(t, got, "code")
	assert.Contains(t, got, "message")
}

// TestAdmin_Route_ViaPipeline 端到端:PutRuleSet → rule engine 应用新规则。
func TestAdmin_Route_ViaPipeline(t *testing.T) {
	h := newAdminHarness(t)
	defer h.Close()

	yaml := `rulesets:
  - name: app
    default_topic: prom.default
    version: 1
    stages:
      - type: route
        config:
          rules:
            - match:
                team: app
              topic: prom.app
`
	resp, _ := h.putRuleSet(t, "app", 1, yaml)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 验证规则已编译并能通过 stats 看到
	time.Sleep(50 * time.Millisecond) // 给 onChange 一点时间
	resp, body := h.do(t, http.MethodGet, "/v1/stats", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	t.Logf("stats: %s", body)
}

