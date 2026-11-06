// ruleengine 集成测试:覆盖从 YAML 加载到 Pipeline 端到端跑通。
//
// 覆盖点:
//   - LoadFile/LoadBytes 解析 Config
//   - CompileConfig 编译出可执行 RuleSet
//   - 完整 pipeline(relabel → route → sample)对 sample 的实际改造
//   - 异常 YAML → 失败语义
//   - 同一 RuleSet 在 Pipeline 中可重入
package ruleengine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoadFile_SysidRoutingYAML(t *testing.T) {
	// 校验仓库根目录的 configs/rules/sysid-routing.yaml 合法可加载可编译
	// 不依赖 cwd:用相对于当前测试文件的路径
	repoRoot := findRepoRoot(t)
	path := filepath.Join(repoRoot, "configs", "rules", "sysid-routing.yaml")
	cfg, err := LoadFile(path)
	require.NoError(t, err, "LoadFile 失败: %v", err)
	require.NotNil(t, cfg)
	require.Len(t, cfg.Rulesets, 1, "应解析出 1 个 ruleset")
	rs := cfg.Rulesets[0]
	assert.Equal(t, "sysid-routing", rs.Name)
	assert.Equal(t, "s-systems", rs.Business)
	assert.Equal(t, "", rs.SourceTopic, "input_topic 已废弃,应为空")
	assert.Equal(t, "prom.local.routed.default", rs.DefaultTopic)
	assert.Equal(t, int64(1), rs.Version)
	// 3 个 stage:relabel / route / sample
	assert.Equal(t, []string{"relabel", "route", "sample"}, stageTypes(rs.Stages))

	compiled, err := Compile(&rs)
	require.NoError(t, err)
	assert.Equal(t, "sysid-routing", compiled.RuleSet.Name)
	assert.Equal(t, 3, len(compiled.Stages))
}

func TestLoadBytes_InvalidYAMLErrors(t *testing.T) {
	// 语法错 / 字段缺 → LoadBytes / Compile 都应给出可读错误
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "missing_default_topic",
			yaml: `
rulesets:
  - name: bad
    stages: []
    version: 1
`,
		},
		{
			name: "unknown_stage_type",
			yaml: `
rulesets:
  - name: bad
    default_topic: t
    stages:
      - type: totally_bogus
        config: {}
    version: 1
`,
		},
		{
			name: "duplicate_stage_type",
			yaml: `
rulesets:
  - name: bad
    default_topic: t
    stages:
      - type: route
        config: {}
      - type: route
        config: {}
    version: 1
`,
		},
		{
			name: "empty_name",
			yaml: `
rulesets:
  - default_topic: t
    stages: []
    version: 1
`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := LoadBytes([]byte(c.yaml))
			// 有些错 LoadBytes 阶段就拦;有些需 Compile 校验
			if err != nil {
				return
			}
			_, cerr := CompileConfig(cfg, "bad")
			assert.Error(t, cerr, "%s: 应编译失败", c.name)
		})
	}
}

func TestCompileConfig_NotFound(t *testing.T) {
	cfg, err := LoadBytes([]byte(`
rulesets:
  - name: a
    default_topic: t
    version: 1
`))
	require.NoError(t, err)
	_, err = CompileConfig(cfg, "missing")
	assert.Error(t, err, "找不到的 ruleset 应返回 error")
}

func TestPipeline_SysidRoutingE2E(t *testing.T) {
	// 加载 sysid-routing.yaml,跑完整流水线:
	//   - relabel 删 env/instance/pod
	//   - route 按 sysid 分流
	//   - sample rate=1.0 全量保留
	repoRoot := findRepoRoot(t)
	path := filepath.Join(repoRoot, "configs", "rules", "sysid-routing.yaml")
	cfg, err := LoadFile(path)
	require.NoError(t, err)

	rs, err := CompileConfig(cfg, "sysid-routing")
	require.NoError(t, err)
	t.Logf("ruleset stages: %d", len(rs.Stages))
	for i, s := range rs.Stages {
		t.Logf("  stage[%d]: type=%s config=%+v", i, s.Type, s.Config)
	}

	// 收集每条投递到 out 的消息
	var captured []sink.Message
	var mu sync.Mutex
	p := NewPipeline(zap.NewExample(), func(_ context.Context, m sink.Message) error {
		mu.Lock()
		defer mu.Unlock()
		captured = append(captured, m)
		return nil
	})
	p.SetRules(rs)

	// 构造样本:3 种 sysid,每种 20 条 = 60 条
	sysids := []string{"s001", "s002", "unknown"}
	var samples []parser.Sample
	for i := 0; i < 20; i++ {
		for _, sid := range sysids {
			samples = append(samples, parser.Sample{
				Metric: "app_requests_total",
				Labels: []parser.Label{
					{Name: "sysid", Value: sid},
					{Name: "job", Value: "api"},
					{Name: "env", Value: "prod"},      // 应被 relabel 删除
					{Name: "instance", Value: "host"}, // 应被 relabel 删除
				},
			})
		}
	}
	t.Logf("input samples: %d", len(samples))

	err = p.Process(context.Background(), samples, []byte("raw"), sink.Message{Topic: ""})
	require.NoError(t, err)

	// rate=1.0 全量保留,60 条应全部投递
	assert.Equal(t, 60, len(captured), "rate=1.0 × 60 sample,应全部投递")

	// 检查路由覆盖:s001/s002/default topic 应各出现 20 次
	topics := map[string]int{}
	for _, m := range captured {
		topics[m.Topic]++
	}
	t.Logf("captured topics distribution: %v", topics)

	assert.Equal(t, 20, topics["prom.local.routed.s001"], "s001 应路由到对应 topic")
	assert.Equal(t, 20, topics["prom.local.routed.s002"], "s002 应路由到对应 topic")
	assert.Equal(t, 20, topics["prom.local.routed.default"], "未知 sysid 应走 default topic")

	// 投递消息的 payload 必须等于入参 raw(原样传递)
	for _, m := range captured {
		assert.Equal(t, []byte("raw"), m.Payload, "payload 应为原 raw")
	}
}

func TestPipeline_MultipleRulesetsInConfig(t *testing.T) {
	// 同一 Config 含多个 ruleset,可通过 CompileConfig 选不同 name
	yaml := `
rulesets:
  - name: a
    default_topic: ta
    version: 1
  - name: b
    default_topic: tb
    stages:
      - type: sample
        config: { rate: 0.5 }
    version: 2
`
	cfg, err := LoadBytes([]byte(yaml))
	require.NoError(t, err)
	rsA, err := CompileConfig(cfg, "a")
	require.NoError(t, err)
	rsB, err := CompileConfig(cfg, "b")
	require.NoError(t, err)
	assert.Equal(t, "a", rsA.RuleSet.Name)
	assert.Equal(t, int64(1), rsA.RuleSet.Version)
	assert.Equal(t, 0, len(rsA.Stages))
	assert.Equal(t, 1, len(rsB.Stages))
}

func TestPipeline_RecompileAndSwap(t *testing.T) {
	// 模拟 v1 → v2 切换:同一 pipeline 切换 ruleset 后行为应改变
	var p *Pipeline
	p = NewPipeline(zap.NewNop(), func(_ context.Context, _ sink.Message) error { return nil })

	rs1, err := Compile(&RuleSet{Name: "v1", DefaultTopic: "t1", Version: 1})
	require.NoError(t, err)
	p.SetRules(rs1)
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version)

	rs2, err := Compile(&RuleSet{
		Name:         "v2",
		DefaultTopic: "t2",
		Stages:       []Stage{{Type: "sample", Config: map[string]interface{}{"rate": 0.0}}}, // 全丢
		Version:      2,
	})
	require.NoError(t, err)
	p.SetRules(rs2)
	assert.Equal(t, int64(2), p.Rules().RuleSet.Version)
	assert.Equal(t, "t2", p.Rules().RuleSet.DefaultTopic)

	// 全丢规则 → 0 次 out
	var called int
	p2 := NewPipeline(zap.NewNop(), func(_ context.Context, _ sink.Message) error {
		called++
		return nil
	})
	p2.SetRules(rs2)
	_ = p2.Process(context.Background(), []parser.Sample{{Metric: "m"}}, []byte("r"), sink.Message{Topic: "t"})
	assert.Equal(t, 0, called, "rate=0 应全丢,out 不被调")
}

// stageTypes 抽出 Stage.Type 列表,便于断言。
func stageTypes(stages []Stage) []string {
	out := make([]string, 0, len(stages))
	for _, s := range stages {
		out = append(out, s.Type)
	}
	return out
}

// findRepoRoot 向上找 go.mod,确保测试在不同 cwd 都能定位 configs/rules/。
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("找不到 go.mod,无法定位 repo root")
		}
		dir = parent
	}
}

// m_T 找到第一条 topic 匹配的消息(测试辅助)。
func m_T(t *testing.T, msgs []sink.Message, topic string) sink.Message {
	t.Helper()
	for _, m := range msgs {
		if m.Topic == topic {
			return m
		}
	}
	return sink.Message{}
}
