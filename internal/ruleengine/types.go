// Package ruleengine 提供标签/路由/采样/下采样/死值等多维清洗能力。
//
// 设计要点(plan T2.x):
//   - 核心是 Pipeline + 多个无状态 Stage;状态型 Stage(Downsample/DeadValue)用 atomic 切换
//   - 规则热更新通过 atomic.Pointer[CompiledRuleSet] 实现
//   - T1.9 阶段: 提供空规则骨架,所有 sample 透传;为 Phase 2 留好接口
//   - T2.x 阶段: 实现 Relabel/Route/Sample 三类 stage,按 Stage.Type 调度
package ruleengine

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/lynnyq/bigdata/internal/parser"
)

// Sentinel 错误(便于 admin / Service 包装)。
var (
	// ErrRuleSetNotFound 找不到对应 name 的 ruleset。
	ErrRuleSetNotFound = errors.New("ruleengine: ruleset not found")
	// ErrRuleSetConflict 同名 version 已存在或 version 非递增。
	ErrRuleSetConflict = errors.New("ruleengine: ruleset version conflict")
	// ErrRuleSetInvalid YAML 或 stage 编译失败。
	ErrRuleSetInvalid = errors.New("ruleengine: ruleset invalid")
	// ErrRuleSetVersionGone rollback 目标 version 不在 history。
	ErrRuleSetVersionGone = errors.New("ruleengine: target version not in history")
	// ErrRuleSetApplyFailed 切换到 pipeline 失败(理论上不应发生)。
	ErrRuleSetApplyFailed = errors.New("ruleengine: ruleset apply failed")
)

// RuleSet 规则集源模型(对应 plan T2.1)。
//
// 字段说明:
//   - Name: 规则集唯一名
//   - Business: 适用业务(v1 单 ruleset 全局生效,字段保留为多业务预留)
//   - SourceTopic: 标记该 ruleset 处理哪类数据(仅 spec 文档,运行期不参与逻辑);
//     YAML tag 为 input_topic(与 design §5.1 对齐),Go 字段名保留 SourceTopic 以减少改动
//   - DefaultTopic: 没路由命中时的兜底 topic
//   - Match: metric/label 命中条件(v1 仅按 metric 前缀匹配);空则全量接收
//   - Stages: 按顺序执行的 stage 列表
//   - Version: 单调递增版本号,用于热更新 / 回滚 / 审计
type RuleSet struct {
	Name         string  `yaml:"name" json:"name"`
	Business     string  `yaml:"business,omitempty" json:"business,omitempty"`
	SourceTopic  string  `yaml:"input_topic,omitempty" json:"source_topic,omitempty"`
	DefaultTopic string  `yaml:"default_topic" json:"default_topic"`
	Match        Match   `yaml:"match,omitempty" json:"match,omitempty"`
	Stages       []Stage `yaml:"stages,omitempty" json:"stages,omitempty"`
	Version      int64   `yaml:"version" json:"version"`
}

// Match 规则集命中条件(空 Match 表示全量接收)。
type Match struct {
	// MetricPrefix 仅当 metric 以此前缀开头时,该 ruleset 接管处理;空 = 全量接收
	MetricPrefix string `yaml:"metric_prefix,omitempty" json:"metric_prefix,omitempty"`
	// MetricExact 仅当 metric 等于该值时接管;空 = 不限制
	MetricExact string `yaml:"metric_exact,omitempty" json:"metric_exact,omitempty"`
}

// Matches 判断 sample 是否被该 ruleset 接管。
func (m Match) Matches(s parser.Sample) bool {
	if m.MetricExact != "" {
		return s.Metric == m.MetricExact
	}
	if m.MetricPrefix != "" {
		return len(s.Metric) >= len(m.MetricPrefix) && s.Metric[:len(m.MetricPrefix)] == m.MetricPrefix
	}
	return true
}

// Stage 规则阶段源模型(对应 plan T2.1)。
//
// 字段说明:
//   - Type: 类型标识(relabel/route/sample)
//   - Config: 阶段私有配置,使用通用 map 在编译期由 Compile 强类型化
//
// YAML 反序列化支持两种写法(design §5.1 inline + 旧版 config: 嵌套):
//
//  1. inline(推荐,与 design doc 一致):
//     - type: relabel
//     drop_labels: [pod]
//     会被解析为 Config={"drop_labels":["pod"]}
//
//  2. config: 嵌套(向后兼容):
//     - type: relabel
//     config:
//     drop_labels: [pod]
//     会被解析为 Config={"drop_labels":["pod"]}
//
// 同时出现时,inline 字段优先(覆盖 config: 内同名字段)。
type Stage struct {
	Type   string                 `yaml:"type" json:"type"`
	Config map[string]interface{} `yaml:"config,omitempty" json:"config,omitempty"`
}

// UnmarshalYAML 实现 yaml.Unmarshaler,支持 design §5.1 的 inline 字段写法,
// 同时保留对旧版 config: 嵌套写法的向后兼容。
//
// 行为:
//   - 先把 type 解析出来
//   - 把整个 stage node 解析为 map[string]interface{}
//   - 若存在 config 键且值为 map,先把其内容合并进 Config(向后兼容)
//   - 再把除 type / config 之外的字段合并进 Config(inline,优先级高于 config:)
func (s *Stage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain struct {
		Type string `yaml:"type"`
	}
	var p plain
	if err := unmarshal(&p); err != nil {
		return err
	}
	s.Type = p.Type

	var raw map[string]interface{}
	if err := unmarshal(&raw); err != nil {
		return err
	}
	s.Config = make(map[string]interface{})

	// 第一遍:向后兼容 — 若存在 config 键且为 map,先合并其内容
	if cfgRaw, ok := raw["config"]; ok {
		if m, ok := cfgRaw.(map[string]interface{}); ok {
			for ck, cv := range m {
				s.Config[ck] = cv
			}
		}
	}
	// 第二遍:inline 字段(覆盖 config: 内同名字段)
	for k, v := range raw {
		if k == "type" || k == "config" {
			continue
		}
		s.Config[k] = v
	}
	return nil
}

// StageHandler 公共 Stage 接口(便于未来在 stages/ 子包中放更复杂实现)。
//
// 与源模型 Stage(配置数据结构)区分:StageHandler 表示可编译执行的 stage 类型。
type StageHandler interface {
	// Name 返回类型标识,与 Stage.Type 一致。
	Name() string
	// Compile 把源配置编译为可执行 Apply 函数。
	Compile(cfg map[string]interface{}) (StageApplyFunc, error)
}

// CompiledStage 编译后的 stage,持有预解析的强类型配置 + 执行函数。
type CompiledStage struct {
	Type   string
	Config map[string]interface{}
	Apply  StageApplyFunc
}

// StageApplyFunc stage 执行签名:
//
//   - ctx: request-scoped context,用于链路追踪
//   - in: 输入 samples
//   - prev: 上一阶段的输出(供 stage 做"原地修改"优化;可复用底层)
//
// 返回值:
//   - out: 输出 samples(允许复用 prev/in 的底层数组)
//   - dropped: 主动丢弃的 sample 数
//   - err: 阶段内部错误(返回后,Pipeline 仍继续后续 stage;err 仅用于 metric)
//
// 重要:必须返回 out slice,而不是修改 prev 的 len 字段,因为函数内修改 slice header
// 不会反映到 caller(append 可能 realloc,slice header 是值类型)。
type StageApplyFunc func(ctx context.Context, in, prev []parser.Sample) (out []parser.Sample, dropped int, err error)

// --- 顶层 Config 容器(用于多 ruleset 配置) ---

// Config 规则引擎顶层配置,对应配置文件根结构。
//
// 字段:
//   - Rulesets: 多 ruleset 列表(v1 启动时按 Name 去重,运行时只持一个全局 active)
//   - Global: 全局参数(channel 容量、限流)
type Config struct {
	Rulesets []RuleSet `yaml:"rulesets" json:"rulesets"`
	Global   Global    `yaml:"global" json:"global"`
}

// Global 全局参数。
type Global struct {
	RateLimitPerInstance int `yaml:"rate_limit_per_instance" json:"rate_limit_per_instance"`
	ChannelBuffer        int `yaml:"channel_buffer" json:"channel_buffer"`
}

// CompiledRuleSet 编译后的规则集,可被 atomic.Pointer 装载。
type CompiledRuleSet struct {
	RuleSet RuleSet
	Stages  []CompiledStage
}

// Clone 深拷贝(用于历史版本存储 T4.6)。
func (c *CompiledRuleSet) Clone() *CompiledRuleSet {
	if c == nil {
		return nil
	}
	out := &CompiledRuleSet{
		RuleSet: c.RuleSet,
		Stages:  make([]CompiledStage, len(c.Stages)),
	}
	for i, s := range c.Stages {
		out.Stages[i] = CompiledStage{
			Type:   s.Type,
			Config: cloneMap(s.Config),
			Apply:  s.Apply, // 函数指针,直接共享
		}
	}
	return out
}

// SortedStages 返回按 Type 排序的 stage 列表(便于测试稳定断言)。
func (c *CompiledRuleSet) SortedStages() []CompiledStage {
	out := make([]CompiledStage, len(c.Stages))
	copy(out, c.Stages)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// stageOrder 定义 design §5.2 要求的 stage 顺序:
//
//	relabel → enrich → route → sample → downsample → deadvalue
//
// 不要求所有类型都出现,但出现的必须保持相对顺序。
var stageOrder = map[string]int{
	"relabel":    0,
	"enrich":     1,
	"route":      2,
	"sample":     3,
	"downsample": 4,
	"deadvalue":  5,
}

// Validate 校验单个 RuleSet 的合法性(在 Compile 之前调用,提供更早的失败信息)。
//
// 校验项:
//   - name / default_topic 必填
//   - stage type 必须已知
//   - 除 relabel 外,同类型 stage 不允许重复(relabel 常见多步清洗,允许重复)
//   - stage 顺序必须符合 design §5.2:relabel → enrich → route → sample → downsample → deadvalue
func (rs *RuleSet) Validate() error {
	if rs.Name == "" {
		return fmt.Errorf("ruleset: name is required")
	}
	if rs.DefaultTopic == "" {
		return fmt.Errorf("ruleset %q: default_topic is required", rs.Name)
	}
	seen := make(map[string]int, len(rs.Stages)) // type → 出现次数
	lastOrder := -1
	for i, s := range rs.Stages {
		// 已知 type 检查
		order, known := stageOrder[s.Type]
		if !known {
			return fmt.Errorf("ruleset %q: stage[%d] unsupported type %q", rs.Name, i, s.Type)
		}
		// 重复 type 检查(relabel 允许重复,其他类型不允许)
		seen[s.Type]++
		if s.Type != "relabel" && seen[s.Type] > 1 {
			return fmt.Errorf("ruleset %q: stage[%d] duplicate type %q (only relabel may repeat)", rs.Name, i, s.Type)
		}
		// 顺序检查:当前 stage 的 order 必须 >= 上一个 stage 的 order
		if order < lastOrder {
			return fmt.Errorf("ruleset %q: stage[%d] type %q out of order (expected relabel→enrich→route→sample→downsample→deadvalue)",
				rs.Name, i, s.Type)
		}
		lastOrder = order
	}
	return nil
}

func cloneMap(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
