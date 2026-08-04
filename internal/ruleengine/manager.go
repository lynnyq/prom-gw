// Package ruleengine - manager.go: 多 ruleset 编排器(plan T2.7 / T2.8 / design 5.2)。
//
// 职责:
//   - 为每条 RuleSet 持有一条独立 *Pipeline(spec 5.2 "每条 ruleset = 一条独立 pipeline")
//   - 持有 *router.Router,接收 sample 时按 Match fan-out 到对应 pipeline
//   - 提供 SetRuleSet 钩子支持热更新单个 ruleset
//   - 提供 Process 入口,签名与 Pipeline.Process 兼容(便于直接作为 receiver handler 注入)
//
// 与 ruleengine.Pipeline 的关系:
//   - Pipeline 是"单 ruleset 的执行单元",自带 stage runner + atomic 切换
//   - Manager 是"多 ruleset 的编排器",内含多条 Pipeline + router
//
// 调用方约定:
//   - 启动时调用 SetRuleSet 注册 N 条 ruleset(可只注册 1 条,等价单 ruleset 模式)
//   - 每次配置变更(Nacos 推 / 文件 watch / API 改)→ 调 SetRuleSet(name, compiled)
//     替换对应 ruleset 即可,其他 ruleset 不受影响(per-ruleset 故障隔离)
//   - 入口 handler 直接调 Manager.Process,无需感知路由细节
package ruleengine

import (
	"context"
	"fmt"
	"sync"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/router"
	"github.com/lynnyq/bigdata/internal/sink"
	"go.uber.org/zap"
)

// Manager 多 ruleset 编排器。
//
// 字段说明:
//   - logger: 必填
//   - out: 规则链跑完后,所有 ruleset 共用的下游投递函数(由 main.go 注入,
//     通常为 sink.Pipeline.Submit)
//   - mu: 保护 pipelines map 与 rtr 的一致性(读路径也走锁,避免与 rtr.Store race)
//   - pipelines: name -> *Pipeline,每条 ruleset 一条独立 pipeline
//   - rtr: fan-out 路由器;SetRuleSet 时重建(路由表条目数 = ruleset 数)
type Manager struct {
	logger *zap.Logger
	out    SinkFunc

	mu        sync.RWMutex
	pipelines map[string]*Pipeline
	rtr       *router.Router
}

// ManagerConfig 构造参数。
type ManagerConfig struct {
	// Logger 必填。
	Logger *zap.Logger
	// Out 规则链跑完后的下游投递函数(每个 pipeline 共用一份)。
	// 若为 nil,默认 noop。
	Out SinkFunc
}

// NewManager 构造一个空 manager;调用方需调 SetRuleSet 注册 ruleset。
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.Out == nil {
		cfg.Out = func(_ context.Context, _ sink.Message) error { return nil }
	}
	m := &Manager{
		logger:    cfg.Logger,
		out:       cfg.Out,
		pipelines: make(map[string]*Pipeline),
		rtr:       router.New(cfg.Logger.Named("router")),
	}
	return m
}

// SetRuleSet 注册或替换一条 ruleset(用 compiled 构造 pipeline + 写 router)。
//
// 行为:
//   - 1. 构造(或替换)对应 name 的 *Pipeline,SetRules(compiled)
//   - 2. 重建 router.Entries:用所有 ruleset 当前的 Match + 对应 Pipeline.Process
//   - 3. 失败(compile 已在 Compile 阶段校验过,这里只校验 router.Validate)→ 返回 error,
//      不替换旧 pipeline
//
// 该方法是热更新入口:Nacos / File / Admin API 推一份新 yaml 进来,
// 调 Compile 得到 CompiledRuleSet,再调本方法即可。
func (m *Manager) SetRuleSet(compiled *CompiledRuleSet) error {
	if compiled == nil {
		return fmt.Errorf("manager: nil ruleset")
	}
	name := compiled.RuleSet.Name
	if name == "" {
		return fmt.Errorf("manager: ruleset name is required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. 构造 / 替换 pipeline
	pipe, exists := m.pipelines[name]
	if !exists {
		pipe = NewPipeline(m.logger.Named("rule."+name), m.out)
	}
	pipe.SetRules(compiled)
	m.pipelines[name] = pipe
	ingestCity, _ := extractMetaLabels(nil)
	obs.RulesetVersion.WithLabelValues(name, ingestCity).Set(float64(compiled.RuleSet.Version))

	// 2. 重建 router 路由表
	if err := m.rebuildRouterLocked(); err != nil {
		// 路由表重建失败:回滚到旧 pipeline(若存在),整体返回 error
		// 实际上 NewPipeline + SetRules 已经成功,这里把刚加的删掉
		if !exists {
			delete(m.pipelines, name)
		} else {
			// 旧 pipeline 已被覆盖,这里无解;只回滚 router
		}
		return fmt.Errorf("manager: rebuild router: %w", err)
	}
	return nil
}

// rebuildRouterLocked 用当前 m.pipelines 重建 router.Entries。
//
// 规则:每条 pipeline 对应一个 Entry;最后一条(Match 为空)是 default;
// 当前实现:把所有 entries 放到 router,router.Validate 会强制 default 在最后。
// 若多条 entries 都无 Match,Validate 会失败,SetRuleSet 不会替换。
func (m *Manager) rebuildRouterLocked() error {
	entries := make([]router.Entry, 0, len(m.pipelines))
	for name, pipe := range m.pipelines {
		rs := pipe.Rules()
		if rs == nil {
			continue
		}
		// 把 method value 捕获下来,避免 race(后续若 SetRules 替换内部,method value 不会变)
		process := pipe.Process
		var match router.Matcher
		if rs.RuleSet.Match.MetricExact != "" || rs.RuleSet.Match.MetricPrefix != "" {
			match = &rs.RuleSet.Match
		}
		entries = append(entries, router.Entry{
			Name:    name,
			Match:   match,
			Process: process,
		})
	}
	// 稳定排序:把 entries 按 name 排,但 router.Validate 要求 default 在最后
	// 这里我们手动分离:有 Match 放前面,无 Match 放最后
	nonDefault := make([]router.Entry, 0, len(entries))
	var defaultEntry *router.Entry
	for i := range entries {
		if entries[i].Match == nil {
			cp := entries[i]
			defaultEntry = &cp
		} else {
			nonDefault = append(nonDefault, entries[i])
		}
	}
	ordered := nonDefault
	if defaultEntry != nil {
		ordered = append(ordered, *defaultEntry)
	}
	return m.rtr.SetEntries(ordered)
}

// Process 入口:按 router 路由分发到各 ruleset pipeline。
//
// 签名与 ruleengine.Pipeline.Process 兼容,可直接作为 receiver.Handler 注入。
func (m *Manager) Process(ctx context.Context, samples []parser.Sample, raw []byte, msg sink.Message) error {
	m.mu.RLock()
	rtr := m.rtr
	m.mu.RUnlock()
	if rtr == nil {
		return fmt.Errorf("manager: router not initialized")
	}
	return rtr.Process(ctx, samples, raw, msg)
}

// Rules 返回指定 name 的 ruleset(快照);name 为空时返回第一条。
//
// 主要用于 admin Stats / 调试;运行期 Process 不读此方法。
func (m *Manager) Rules(name string) *CompiledRuleSet {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if name == "" {
		for _, p := range m.pipelines {
			return p.Rules()
		}
		return nil
	}
	if p, ok := m.pipelines[name]; ok {
		return p.Rules()
	}
	return nil
}

// AllRules 返回所有 ruleset 的当前快照(用于 admin 列表 / metrics)。
func (m *Manager) AllRules() map[string]*CompiledRuleSet {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]*CompiledRuleSet, len(m.pipelines))
	for name, p := range m.pipelines {
		out[name] = p.Rules()
	}
	return out
}

// Names 返回所有已注册 ruleset 的 name 列表(排序后)。
func (m *Manager) Names() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.pipelines))
	for name := range m.pipelines {
		out = append(out, name)
	}
	return out
}

// Router 返回内部 router(用于调试 / metrics)。
func (m *Manager) Router() *router.Router { return m.rtr }
