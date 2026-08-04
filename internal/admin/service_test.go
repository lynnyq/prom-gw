// Package admin - service_test.go: ManagerService 单元测试。
package admin

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/lynnyq/bigdata/internal/auth"
	"github.com/lynnyq/bigdata/internal/config"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mock AuthProvider ---

type mockAuth struct {
	tenants []auth.Tenant
}

func (m *mockAuth) ListTenants() []auth.Tenant { return m.tenants }

// --- helpers ---

func newSvc(t *testing.T) (*ManagerService, *config.History) {
	t.Helper()
	h := config.NewHistory(config.HistoryConfig{Capacity: 5})
	mgr := ruleengine.NewManager(ruleengine.ManagerConfig{Logger: zap.NewNop()})
	s := NewManagerService(ManagerDeps{
		Manager: config.NewManager(config.ManagerConfig{Logger: zap.NewNop(), History: h}),
		RuleMgr: mgr,
		Auth: &mockAuth{tenants: []auth.Tenant{
			{Name: "app-business", TenantID: "1001", DefaultTopic: "prom.app", RateLimit: 80000},
		}},
		History: h,
		Logger:  zap.NewNop(),
	})
	return s, h
}

const baseYAML = `
rulesets:
  - name: app-business
    default_topic: prom.app
    stages:
      - type: relabel
        config: { drop_labels: [pod] }
`

// --- tests ---

func TestService_PutRuleSet_NewName(t *testing.T) {
	s, h := newSvc(t)
	ver, err := s.PutRuleSet(context.Background(), "app-business", 1, []byte(baseYAML))
	require.NoError(t, err)
	assert.Equal(t, int64(1), ver)
	assert.Equal(t, 1, h.Size())
}

func TestService_PutRuleSet_VersionConflict(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "app-business", 1, []byte(baseYAML))
	require.NoError(t, err)
	_, err = s.PutRuleSet(context.Background(), "app-business", 1, []byte(baseYAML))
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetConflict)
}

func TestService_PutRuleSet_InvalidYAML(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "app-business", 1, []byte("rulesets: : bad"))
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetInvalid)
}

func TestService_PutRuleSet_StageInvalid(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "app-business", 1, []byte(`
rulesets:
  - name: app-business
    default_topic: prom.app
    stages:
      - type: not_a_stage
`))
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetInvalid)
}

func TestService_PutRuleSet_NotInYAML(t *testing.T) {
	s, _ := newSvc(t)
	// yaml 内 ruleset 名是 a,但路径里 PUT 的是 b
	_, err := s.PutRuleSet(context.Background(), "b", 1, []byte(`
rulesets:
  - name: a
    default_topic: t
`))
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetNotFound)
}

func TestService_GetRuleSet_NotFound(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.GetRuleSet(context.Background(), "missing")
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetNotFound)
}

func TestService_GetRuleSet_OK(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "app-business", 1, []byte(baseYAML))
	require.NoError(t, err)
	d, err := s.GetRuleSet(context.Background(), "app-business")
	require.NoError(t, err)
	assert.Equal(t, "app-business", d.Name)
	assert.Equal(t, int64(1), d.Version)
	assert.Equal(t, 1, d.Stages)
}

func TestService_ListRuleSets(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "a", 1, []byte(`
rulesets:
  - name: a
    default_topic: t1
`))
	require.NoError(t, err)
	_, err = s.PutRuleSet(context.Background(), "b", 1, []byte(`
rulesets:
  - name: b
    default_topic: t2
`))
	require.NoError(t, err)

	out := s.ListRuleSets(context.Background())
	assert.Len(t, out, 2)
	names := []string{out[0].Name, out[1].Name}
	assert.Contains(t, names, "a")
	assert.Contains(t, names, "b")
}

func TestService_Rollback_OK(t *testing.T) {
	s, h := newSvc(t)
	// 入两个版本
	_, err := s.PutRuleSet(context.Background(), "a", 1, []byte(`
rulesets:
  - name: a
    default_topic: t
    stages:
      - type: relabel
        config: { drop_labels: [pod] }
`))
	require.NoError(t, err)
	_, err = s.PutRuleSet(context.Background(), "a", 2, []byte(`
rulesets:
  - name: a
    default_topic: t
    stages:
      - type: relabel
        config: { drop_labels: [pod, instance] }
`))
	require.NoError(t, err)

	err = s.RollbackRuleSet(context.Background(), "a", 1)
	require.NoError(t, err)
	assert.Equal(t, 2, h.Size())
}

func TestService_Rollback_VersionGone(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "a", 1, []byte(`
rulesets:
  - name: a
    default_topic: t
`))
	require.NoError(t, err)
	err = s.RollbackRuleSet(context.Background(), "a", 99)
	assert.ErrorIs(t, err, ruleengine.ErrRuleSetVersionGone)
}

func TestService_ListHistory(t *testing.T) {
	s, _ := newSvc(t)
	for v := int64(1); v <= 3; v++ {
		_, err := s.PutRuleSet(context.Background(), "a", v, []byte(`
rulesets:
  - name: a
    default_topic: t
`))
		require.NoError(t, err)
	}
	list := s.ListHistory(context.Background(), "a")
	assert.Len(t, list, 3)
}

func TestService_ListTenants(t *testing.T) {
	s, _ := newSvc(t)
	out := s.ListTenants(context.Background())
	require.Len(t, out, 1)
	assert.Equal(t, "app-business", out[0].Tenant)
}

func TestService_Stats(t *testing.T) {
	s, _ := newSvc(t)
	_, _ = s.PutRuleSet(context.Background(), "a", 1, []byte(`
rulesets:
  - name: a
    default_topic: t
`))
	stats := s.Stats(context.Background())
	assert.Equal(t, "a", stats.CurrentRuleSet) // PutRuleSet 切到了 "a"
	assert.Equal(t, int64(1), stats.CurrentVersion)
	assert.Equal(t, 1, stats.HistorySize)
}

// 验证 PutRuleSet 切换了 manager(多 ruleset 模式下 SetRuleSet 触达 manager)。
func TestService_PutRuleSet_SwitchesPipeline(t *testing.T) {
	s, _ := newSvc(t)
	_, err := s.PutRuleSet(context.Background(), "a", 1, []byte(`
rulesets:
  - name: a
    default_topic: t
`))
	require.NoError(t, err)
	rs := s.ruleMgr.Rules("a")
	require.NotNil(t, rs, "manager 应当有 a ruleset")
	assert.Equal(t, "a", rs.RuleSet.Name)
	assert.Equal(t, int64(1), rs.RuleSet.Version)
}

// 防止 _ 导入 atomic 报错(若以后用到)
var _ = atomic.Int32{}
