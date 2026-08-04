// Package ruleengine - compiler.go: 规则加载与编译。
//
// 设计要点(plan T2.2):
//   - LoadFile 解析 YAML 顶层 Config
//   - Compile 把 RuleSet 转换为 CompiledRuleSet,绑定 stage 编译函数
//   - 编译失败返回详细错误(stage index / field name),便于运维定位
//   - stage 不在 registry 时返回 error,拒绝切换
package ruleengine

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadFile 读取 YAML 文件并解析为 Config。
//
// 错误:
//   - 文件读取失败(权限/不存在)
//   - YAML 解析失败
func LoadFile(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ruleengine: read %q: %w", path, err)
	}
	return LoadBytes(raw)
}

// LoadBytes 解析 YAML 字节为 Config(便于测试与 Nacos 等非文件源)。
func LoadBytes(raw []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("ruleengine: unmarshal yaml: %w", err)
	}
	return &cfg, nil
}

// Compile 把源 RuleSet 编译为可执行的 CompiledRuleSet。
//
// 步骤:
//  1. Validate 基础合法性(name/default_topic/stage type)
//  2. 对每个 stage,查 registry 找到对应 compiler,执行 Compile 生成 StageApplyFunc
//  3. 把 ResultStage 装入 CompiledRuleSet.Stages
//
// 失败语义:任一 stage 编译失败 → 整体返回 error,该 RuleSet 不可用。
//
// 编译期注入:
//   - route stage 的 default_topic 若 cfg 未配置,自动用 RuleSet.DefaultTopic 兜底
func Compile(rs *RuleSet) (*CompiledRuleSet, error) {
	if rs == nil {
		return nil, fmt.Errorf("ruleengine: nil RuleSet")
	}
	if err := rs.Validate(); err != nil {
		return nil, err
	}
	out := &CompiledRuleSet{
		RuleSet: *rs,
		Stages:  make([]CompiledStage, 0, len(rs.Stages)),
	}
	for i, s := range rs.Stages {
		compiler := LookupCompiler(s.Type)
		if compiler == nil {
			return nil, fmt.Errorf("ruleengine: ruleset %q stage[%d] type %q not registered",
				rs.Name, i, s.Type)
		}
		// 给 route stage 自动注入 default_topic(若 cfg 没配)
		if s.Type == "route" {
			if s.Config == nil {
				s.Config = map[string]interface{}{}
			}
			if _, has := s.Config["default_topic"]; !has {
				s.Config["default_topic"] = rs.DefaultTopic
			}
		}
		cs, err := compiler(Stage{Type: s.Type, Config: s.Config})
		if err != nil {
			return nil, fmt.Errorf("ruleengine: ruleset %q stage[%d] type %q compile: %w",
				rs.Name, i, s.Type, err)
		}
		out.Stages = append(out.Stages, cs)
	}
	return out, nil
}

// CompileConfig 从 Config 中挑出名为 name 的 RuleSet 并编译。
//
// 用于:多 ruleset 配置 → 启动时选一个;Phase 2 之后 Nacos 推多 ruleset,
// admin API 选哪个生效(plan T4.5)。
func CompileConfig(cfg *Config, name string) (*CompiledRuleSet, error) {
	if cfg == nil {
		return nil, fmt.Errorf("ruleengine: nil config")
	}
	for i := range cfg.Rulesets {
		if cfg.Rulesets[i].Name == name {
			return Compile(&cfg.Rulesets[i])
		}
	}
	return nil, fmt.Errorf("ruleengine: ruleset %q not found in config", name)
}

// MustCompile 与 Compile 类似,但失败时 panic(仅用于 main 启动期)。
func MustCompile(rs *RuleSet) *CompiledRuleSet {
	c, err := Compile(rs)
	if err != nil {
		panic(err)
	}
	return c
}
