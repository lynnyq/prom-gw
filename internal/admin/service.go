// Package admin - service.go: Service 接口的默认实现(基于 config.Manager)。
//
// 构造方式:
//
//	svc := NewService(ManagerDeps{
//	    Manager:   mgr,
//	    RuleMgr:   ruleMgr,  // 多 ruleset 编排器(plan T2.7/T2.8)
//	    Auth:      localAuth,
//	    History:   hist,
//	})
package admin

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lynnyq/prom-gw/internal/auth"
	"github.com/lynnyq/prom-gw/internal/config"
	"github.com/lynnyq/prom-gw/internal/ruleengine"
	"go.uber.org/zap"
)

// ManagerService 默认 Service 实现。
type ManagerService struct {
	mu      sync.RWMutex
	manager *config.Manager
	ruleMgr *ruleengine.Manager
	auth    AuthProvider
	history *config.History
	dropIn  atomic.Uint64
	dropOut atomic.Uint64
	logger  *zap.Logger
}

// ManagerDeps 构造参数。
type ManagerDeps struct {
	Manager *config.Manager
	// RuleMgr 多 ruleset 编排器,接收 Put / Reload / Rollback 后的 SetRuleSet 调用;
	// 单 ruleset 测试场景下可传 nil,此时 Put 仍会写 history 但不切 pipeline。
	RuleMgr *ruleengine.Manager
	Auth    AuthProvider
	History *config.History
	Logger  *zap.Logger
}

// AuthProvider 提供 ListBusinesses 所需元数据(避免直接依赖具体 type)。
//
// LocalTokenAuthenticator 实现此接口。
type AuthProvider interface {
	ListBusinesses() []auth.Business
}

// NewManagerService 构造。
func NewManagerService(d ManagerDeps) *ManagerService {
	if d.Logger == nil {
		d.Logger = zap.NewNop()
	}
	return &ManagerService{
		manager: d.Manager,
		ruleMgr: d.RuleMgr,
		auth:    d.Auth,
		history: d.History,
		logger:  d.Logger,
	}
}

// --- Service impl ---

func (s *ManagerService) ListRuleSets(_ context.Context) []RuleSetSummary {
	// 从 history 列所有 name
	names := s.history.Names()
	sort.Strings(names)
	out := make([]RuleSetSummary, 0, len(names))
	for _, n := range names {
		// 找最新版本
		latest, err := s.history.Latest(n)
		if err != nil {
			continue
		}
		// 算 stage 数
		stages := 0
		if latest.Compiled != nil {
			stages = len(latest.Compiled.Stages)
		}
		out = append(out, RuleSetSummary{
			Name:    n,
			Version: latest.Version,
			Stages:  stages,
			Source:  latest.Source,
		})
	}
	return out
}

func (s *ManagerService) GetRuleSet(_ context.Context, name string) (RuleSetDetail, error) {
	latest, err := s.history.Latest(name)
	if err != nil {
		return RuleSetDetail{}, fmt.Errorf("ruleset %q: %w", name, ruleengine.ErrRuleSetNotFound)
	}
	stages := 0
	if latest.Compiled != nil {
		stages = len(latest.Compiled.Stages)
	}
	return RuleSetDetail{
		Name:         name,
		Version:      latest.Version,
		DefaultTopic: latest.Compiled.RuleSet.DefaultTopic,
		Stages:       stages,
		RawYAML:      string(latest.RawYAML),
		Source:       latest.Source,
	}, nil
}

func (s *ManagerService) PutRuleSet(_ context.Context, name string, version int64, rawYAML []byte) (int64, error) {
	if name == "" {
		return 0, ruleengine.ErrRuleSetInvalid
	}
	// version 校验:必须严格大于当前最新
	if latest, err := s.history.Latest(name); err == nil {
		if version <= latest.Version {
			return 0, fmt.Errorf("version %d <= current %d: %w", version, latest.Version, ruleengine.ErrRuleSetConflict)
		}
	}
	// 编译(先单独校验,不入库,免得失败时污染)
	rs, err := ruleengine.LoadBytes(rawYAML)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ruleengine.ErrRuleSetInvalid, err)
	}
	var found bool
	for i := range rs.Rulesets {
		if rs.Rulesets[i].Name != name {
			continue
		}
		// 强制 version 等于路径里的 version
		rs.Rulesets[i].Version = version
		compiled, err := ruleengine.Compile(&rs.Rulesets[i])
		if err != nil {
			return 0, fmt.Errorf("%w: %v", ruleengine.ErrRuleSetInvalid, err)
		}
		// 入 history
		if err := s.history.Save(config.HistoryRecord{
			Name:     name,
			Version:  version,
			Bytes:    len(rawYAML),
			RawYAML:  rawYAML,
			Source:   "api",
			Compiled: compiled,
		}); err != nil {
			return 0, fmt.Errorf("save history: %w", err)
		}
		// 切到 manager(多 ruleset 模式下,Manager 内部按 name 路由到对应 pipeline)
		if s.ruleMgr != nil {
			if err := s.ruleMgr.SetRuleSet(compiled); err != nil {
				return version, fmt.Errorf("%w: %v", ruleengine.ErrRuleSetApplyFailed, err)
			}
		}
		found = true
		break
	}
	if !found {
		return 0, fmt.Errorf("ruleset %q: %w", name, ruleengine.ErrRuleSetNotFound)
	}
	return version, nil
}

func (s *ManagerService) ReloadRuleSet(_ context.Context, name string) error {
	// Reload 语义: 优先从 source 重拉,失败 / 无 source 时退到 history 的 latest 版本。
	// 这样:
	//   1. 真实部署场景:Nacos/File source 推送后,可强制重拉(覆盖 PUT 写入的版本)
	//   2. 测试场景:无 source,PUT 入 history 后,reload 把 history latest 重新编译
	//      并切到 pipeline,确保 :reload 端点在两种部署下行为一致
	snap := s.manager.Current()
	if !snap.IsEmpty() {
		rs, err := s.manager.ApplySnapshot(snap)
		if err != nil {
			// 空 snapshot 是合法状态(空 pipeline),不报错
			if !strings.Contains(err.Error(), "no ruleset in snapshot") {
				return fmt.Errorf("%w: %v", ruleengine.ErrRuleSetApplyFailed, err)
			}
		} else if rs != nil && rs.RuleSet.Name == name {
			// 成功切到同名 ruleset
			return nil
		}
	}
	// 退到 history latest:重新编译 + 切到 manager
	latest, err := s.history.Latest(name)
	if err != nil {
		return fmt.Errorf("%w: %v", ruleengine.ErrRuleSetNotFound, err)
	}
	if latest.Compiled == nil {
		return ruleengine.ErrRuleSetApplyFailed
	}
	if s.ruleMgr != nil {
		if err := s.ruleMgr.SetRuleSet(latest.Compiled); err != nil {
			return fmt.Errorf("%w: %v", ruleengine.ErrRuleSetApplyFailed, err)
		}
	}
	return nil
}

func (s *ManagerService) RollbackRuleSet(_ context.Context, name string, toVersion int64) error {
	rec, err := s.history.Get(name, toVersion)
	if err != nil {
		return fmt.Errorf("%w: %v", ruleengine.ErrRuleSetVersionGone, err)
	}
	if rec.Compiled == nil {
		return ruleengine.ErrRuleSetApplyFailed
	}
	if s.ruleMgr != nil {
		if err := s.ruleMgr.SetRuleSet(rec.Compiled); err != nil {
			return fmt.Errorf("%w: %v", ruleengine.ErrRuleSetApplyFailed, err)
		}
	}
	return nil
}

func (s *ManagerService) ListHistory(_ context.Context, name string) []config.HistoryRecord {
	return s.history.List(name)
}

func (s *ManagerService) ListBusinesses(_ context.Context) []BusinessInfo {
	if s.auth == nil {
		return nil
	}
	src := s.auth.ListBusinesses()
	out := make([]BusinessInfo, 0, len(src))
	for _, t := range src {
		out = append(out, BusinessInfo{
			Business:     t.Name,
			BusinessID:   t.BusinessID,
			DefaultTopic: t.DefaultTopic,
			RateLimit:    t.RateLimit,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Business < out[j].Business })
	return out
}

func (s *ManagerService) Stats(_ context.Context) StatsResp {
	bySrc := map[string]int{}
	var curName string
	var curVer int64

	// 多 ruleset 模式:从 manager 拿所有 ruleset 的当前版本
	if s.ruleMgr != nil {
		all := s.ruleMgr.AllRules()
		// 选择字典序第一个作为"primary"展示
		names := make([]string, 0, len(all))
		for n := range all {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			rs := all[n]
			if rs == nil {
				continue
			}
			if curName == "" {
				curName = rs.RuleSet.Name
				curVer = rs.RuleSet.Version
			}
			// 拿 source
			if latest, err := s.history.Latest(rs.RuleSet.Name); err == nil {
				bySrc[latest.Source]++
			}
		}
	} else {
		// 单 ruleset 模式:从 history 拿任意一条
		names := s.history.Names()
		if len(names) > 0 {
			sort.Strings(names)
			latest, err := s.history.Latest(names[0])
			if err == nil {
				curName = names[0]
				curVer = latest.Version
				bySrc[latest.Source]++
			}
		}
	}

	dropIn := s.dropIn.Load()
	dropOut := s.dropOut.Load()
	rate := 0.0
	if dropIn > 0 {
		rate = float64(dropIn-dropOut) / float64(dropIn)
	}
	return StatsResp{
		CurrentRuleSet: curName,
		CurrentVersion: curVer,
		BySource:       bySrc,
		HistorySize:    s.history.Size(),
		DropRate:       rate,
	}
}
