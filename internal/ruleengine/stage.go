// Package ruleengine - stage.go: Stage 接口与内置 stage 实现。
//
// 设计要点(plan T2.3 / T2.4-2.6):
//   - Stage 用函数式实现,Compile 时把 Config 强类型化并产出 StageApplyFunc
//   - 单一 registry 维护 type -> 编译函数;新 stage 仅需在 registry 中注册一行
//   - 单条 stage 失败不阻断整批:err 仅 metric 计数,继续跑下一 stage
//   - 数组复用:stage 内允许 in[:0] 原地复用底层数组,降低 GC
//   - StageApplyFunc **必须返回 out slice**(函数内修改 prev 的 len 字段不会
//     反映到 caller,因为 slice header 是值类型,append 可能 realloc)
package ruleengine

import (
	"context"
	"fmt"
	"math/rand/v2"
	"regexp"
	"sort"
	"sync"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.opentelemetry.io/otel/attribute"
)

// StageCompiler 把一个源 Stage 编译为 CompiledStage。
// 返回 error 时,Pipeline 拒绝切换该 RuleSet。
type StageCompiler func(s Stage) (CompiledStage, error)

// --- 内置 stage 实现 ---

// RelabelStage 实现 plan T2.4: drop_labels / keep_labels / label_map。
type RelabelStage struct{}

func (RelabelStage) Name() string { return "relabel" }

// Compile 解析 relabel 配置:
//
//	{
//	  "drop_labels": ["env", "instance"],  // 显式删除 label
//	  "keep_labels": ["job", "region"],   // 白名单:仅保留这些(与 drop 互斥)
//	  "label_map":   { "cluster": "kubernetes.io/cluster" }  // 重命名
//	}
//
// 注意:drop 与 keep 同时配置时,优先 keep(更严格的语义);不抛错,只记 warn metric。
func (RelabelStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	if len(cfg) == 0 {
		// 空配置 = 透传
		return passthroughApply, nil
	}
	drop := stringSetFromCfg(cfg, "drop_labels")
	keep := stringSetFromCfg(cfg, "keep_labels")
	labelMap := stringMapFromCfg(cfg, "label_map")

	if len(drop) > 0 && len(keep) > 0 {
		ingestCity, sourceDC := obs.MetaLabels()
		obs.ErrorsTotal.WithLabelValues("rule", "relabel_drop_keep_conflict", ingestCity, sourceDC).Inc()
	}

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "relabel")
		defer span.End()
		out := prev[:0]
		dropped := 0
		for _, s := range in {
			labels := applyRelabel(s.Labels, drop, keep, labelMap)
			if len(labels) == 0 && len(keep) > 0 {
				dropped++
				continue
			}
			cp := s.Clone()
			cp.Labels = labels
			out = append(out, cp)
		}
		span.SetAttributes(
			attribute.Int("in", len(in)),
			attribute.Int("out", len(out)),
			attribute.Int("dropped", dropped),
		)
		return out, dropped, nil
	}, nil
}

// passthroughApply 是空配置 relabel 用的透传函数。
//
// 行为:返回 in(不复制),仅为了让 Pipeline 拿到一致的 out slice。
func passthroughApply(_ context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
	_ = prev
	return in, 0, nil
}

// applyRelabel 实施 drop/keep/label_map,返回新 Labels slice(可能复用底层)。
func applyRelabel(in []parser.Label, drop, keep map[string]struct{}, rename map[string]string) []parser.Label {
	useKeep := len(keep) > 0
	out := in[:0]
	for _, l := range in {
		if useKeep {
			if _, ok := keep[l.Name]; !ok {
				continue
			}
		} else if _, ok := drop[l.Name]; ok {
			continue
		}
		if newName, ok := rename[l.Name]; ok {
			l.Name = newName
		}
		out = append(out, l)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Value < out[j].Value
	})
	return out
}

// --- route stage ---

// RouteStage 实现 plan T2.5: 按 match 字典路由到不同 topic。
type RouteStage struct{}

func (RouteStage) Name() string { return "route" }

type routeRule struct {
	match map[string]string
	topic string
}

func (RouteStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	rules := buildRouteRules(cfg)
	// 兜底:cfg.default_topic → 上层 ruleset.DefaultTopic(由 Compile 注入,见 compiler.go)
	dflt, _ := cfg["default_topic"].(string)
	if len(rules) == 0 {
		// 没 rules:直接把 default_topic 赋给每个 sample
		return func(_ context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
			out := prev[:0]
			for _, s := range in {
				if s.TargetTopic == "" {
					s.TargetTopic = dflt
				}
				out = append(out, s)
			}
			return out, 0, nil
		}, nil
	}

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "route")
		defer span.End()
		out := prev[:0]
		dropped := 0
		for _, s := range in {
			target := dflt
			for _, r := range rules {
				if matchAll(s.Labels, r.match) {
					target = r.topic
					break
				}
			}
			if target == "" {
				dropped++
				continue
			}
			cp := s.Clone()
			cp.TargetTopic = target
			out = append(out, cp)
		}
		span.SetAttributes(attribute.Int("routed", len(out)))
		return out, dropped, nil
	}, nil
}

// buildRouteRules 从 cfg 构造 routeRule 切片,同时支持两种写法:
//
//  1. design §5.1 单条 match/to_topic(inline):
//
//     match: { app: "payment" }
//     to_topic: prom.routed.payment
//     default_topic: prom.routed.default
//
//  2. 旧版 rules 数组(向后兼容):
//
//     rules:
//     - match: { team: core }
//     topic: prom.core
//
// to_topic(design)和 topic(旧版)都支持;to_topic 优先。
func buildRouteRules(cfg map[string]interface{}) []routeRule {
	// 优先:rules 数组(旧版,支持多条)
	rulesRaw, _ := cfg["rules"].([]interface{})
	if len(rulesRaw) > 0 {
		out := make([]routeRule, 0, len(rulesRaw))
		for _, raw := range rulesRaw {
			m, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			match, _ := m["match"].(map[string]interface{})
			topic := readString(m, "to_topic", "topic")
			if topic == "" {
				continue
			}
			out = append(out, routeRule{
				match: toStringMap(match),
				topic: topic,
			})
		}
		return out
	}

	// design §5.1:单条 match + to_topic
	matchRaw, hasMatch := cfg["match"].(map[string]interface{})
	if !hasMatch {
		return nil
	}
	topic := readString(cfg, "to_topic", "topic")
	if topic == "" {
		return nil
	}
	return []routeRule{{
		match: toStringMap(matchRaw),
		topic: topic,
	}}
}

// readString 按 keys 顺序读第一个非空字符串(用于 to_topic → topic 的 fallback)。
func readString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// toStringMap 把 map[string]interface{} 转为 map[string]string(忽略非字符串值)。
func toStringMap(in map[string]interface{}) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func matchAll(labels []parser.Label, m map[string]string) bool {
	if len(m) == 0 {
		return false
	}
	for k, v := range m {
		found := false
		for _, l := range labels {
			if l.Name == k && l.Value == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// --- sample stage ---

// SampleStage 实现 plan T2.6: 按概率 0.0~1.0 随机丢弃 sample。
//
// scope.metric_regex(design §5.1)可选:若配置,仅对 metric 名匹配正则的 sample
// 做采样;不匹配的 sample 原样透传。未配置 scope 时对所有 sample 生效(向后兼容)。
type SampleStage struct{}

func (SampleStage) Name() string { return "sample" }

func (SampleStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	rateVal, _ := cfg["rate"].(float64)
	if rateVal < 0 || rateVal > 1 {
		return nil, fmt.Errorf("sample: rate must be in [0, 1], got %v", rateVal)
	}
	scopeRe, err := compileScopeRegex(cfg, "sample")
	if err != nil {
		return nil, err
	}

	// 无 scope 且 rate=1 → 透传(快路径)
	if scopeRe == nil && rateVal == 1 {
		return passthroughApply, nil
	}
	// 无 scope 且 rate=0 → 全丢(快路径)
	if scopeRe == nil && rateVal == 0 {
		return func(_ context.Context, _, prev []parser.Sample) ([]parser.Sample, int, error) {
			return prev[:0], 0, nil
		}, nil
	}

	// 可选 seed:若用户传 seed,初始化独立 rand.Rand(用 sync.Pool 复用)
	var pool *sync.Pool
	if _, hasSeed := cfg["seed"]; hasSeed {
		seed := uint64FromCfg(cfg, "seed", 0)
		pool = &sync.Pool{
			New: func() interface{} {
				return rand.New(rand.NewPCG(seed, seed))
			},
		}
	}

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "sample")
		defer span.End()
		out := prev[:0]
		dropped := 0
		if pool != nil {
			r := pool.Get().(*rand.Rand)
			for _, s := range in {
				// scope 不匹配 → 透传(不参与采样)
				if scopeRe != nil && !scopeRe.MatchString(s.Metric) {
					out = append(out, s)
					continue
				}
				if r.Float64() > rateVal {
					dropped++
					continue
				}
				out = append(out, s)
			}
			pool.Put(r)
		} else {
			for _, s := range in {
				if scopeRe != nil && !scopeRe.MatchString(s.Metric) {
					out = append(out, s)
					continue
				}
				if rand.Float64() > rateVal {
					dropped++
					continue
				}
				out = append(out, s)
			}
		}
		span.SetAttributes(attribute.Int("kept", len(out)))
		return out, dropped, nil
	}, nil
}

// --- enrich stage ---

// EnrichStage 实现 plan T3.1: 给 sample 增加 / 覆盖 label。
//
// 字段值支持两种形式:
//
//   - 静态:    纯字符串,直接作为 label value
//   - 模板:    "${labels.<name>}",在 sample 已有 labels 中查 <name> 的值替换
//
// 配置示例(design §5.1 inline 写法):
//
//	{
//	  "add_labels": {
//	    "cluster":  "prod",                 // 静态
//	    "env_from": "${labels.env}",        // 引用 sample.labels.env
//	    "region":   "${labels.region}"      // 引用 sample.labels.region
//	  }
//	}
//
// 向后兼容:若 add_labels 缺失,回退读 labels(旧版字段名)。
//
// 行为:
//   - 新 label key 与 sample 已有 key 重名时,直接覆盖(下游 stage 仍按字母序排)
//   - 模板引用不存在的 label → 跳过该条 enrich,记 enrich_template_missing 计数
//   - 空 labels config → 透传(等价 noop)
type EnrichStage struct{}

func (EnrichStage) Name() string { return "enrich" }

func (EnrichStage) Compile(cfg map[string]interface{}) (StageApplyFunc, error) {
	// design §5.1 用 add_labels;向后兼容回退 labels
	labelsRaw, _ := cfg["add_labels"].(map[string]interface{})
	if labelsRaw == nil {
		labelsRaw, _ = cfg["labels"].(map[string]interface{})
	}
	if len(labelsRaw) == 0 {
		return passthroughApply, nil
	}
	// 编译期把每条 rule 解析为 (key, value 或 templatePath)
	type rule struct {
		key      string
		template string // 非空表示模板引用,值为 "labels.<name>"
		statik   string // 非空表示静态值
	}
	// 按 key 排序确保编译确定性(map 遍历顺序不确定)
	keys := make([]string, 0, len(labelsRaw))
	for k := range labelsRaw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rules := make([]rule, 0, len(keys))
	for _, k := range keys {
		v := labelsRaw[k]
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("enrich: labels.%s must be a string, got %T", k, v)
		}
		if len(s) >= 9 && s[:9] == "${labels." && s[len(s)-1] == '}' {
			rules = append(rules, rule{key: k, template: s[9 : len(s)-1]})
		} else {
			rules = append(rules, rule{key: k, statik: s})
		}
	}

	return func(ctx context.Context, in, prev []parser.Sample) ([]parser.Sample, int, error) {
		_, span := tracex.StartSpan(ctx, "rule", "enrich")
		defer span.End()
		out := prev[:0]
		dropped := 0
		for _, s := range in {
			// 先 clone,再在 labels 上 append(因为 enrich 不删 label,只加/覆盖)
			cp := s.Clone()
			for _, r := range rules {
				val := r.statik
				if r.template != "" {
					found := false
					for _, l := range s.Labels {
						if l.Name == r.template {
							val = l.Value
							found = true
							break
						}
					}
					if !found {
						ingestCity, sourceDC := obs.MetaLabels()
						obs.ErrorsTotal.WithLabelValues("rule", "enrich_template_missing", ingestCity, sourceDC).Inc()
						continue
					}
				}
				cp.Labels = upsertLabel(cp.Labels, r.key, val)
			}
			out = append(out, cp)
		}
		span.SetAttributes(attribute.Int("enriched", len(out)))
		return out, dropped, nil
	}, nil
}

// upsertLabel 在 labels 中设置或追加 label key=value,保持 labels 有序。
// spec §4.4: sorted_labels_hash 要求 labels 排序;enrich 新增 label 后必须重排。
func upsertLabel(labels []parser.Label, key, value string) []parser.Label {
	for i := range labels {
		if labels[i].Name == key {
			labels[i].Value = value
			return labels
		}
	}
	labels = append(labels, parser.Label{Name: key, Value: value})
	// 追加新 key 后重新排序,保证 SeriesKey() 产出稳定的 sorted_labels_hash
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})
	return labels
}

// --- registry ---

var (
	stageRegistryMu sync.RWMutex
	stageRegistry   = map[string]StageCompiler{}
)

func init() {
	RegisterStage(RelabelStage{})
	RegisterStage(RouteStage{})
	RegisterStage(SampleStage{})
	RegisterStage(EnrichStage{})
	RegisterStage(DownsampleStage{})
	RegisterStage(DeadValueStage{})
}

// RegisterStage 注册一个 stage 编译器;重复注册同类型返回 false(便于测试断言)。
func RegisterStage(s StageHandler) bool {
	stageRegistryMu.Lock()
	defer stageRegistryMu.Unlock()
	if _, ok := stageRegistry[s.Name()]; ok {
		return false
	}
	stageRegistry[s.Name()] = func(src Stage) (CompiledStage, error) {
		fn, err := s.Compile(src.Config)
		if err != nil {
			return CompiledStage{}, err
		}
		return CompiledStage{Type: src.Type, Config: src.Config, Apply: fn}, nil
	}
	return true
}

// LookupCompiler 取出 stage 编译器;返回 nil 表示 type 未注册。
func LookupCompiler(typ string) StageCompiler {
	stageRegistryMu.RLock()
	defer stageRegistryMu.RUnlock()
	return stageRegistry[typ]
}

// --- 工具函数 ---

func stringSetFromCfg(cfg map[string]interface{}, key string) map[string]struct{} {
	raw, ok := cfg[key]
	if !ok {
		return nil
	}
	arr, ok := raw.([]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]struct{}, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok && s != "" {
			out[s] = struct{}{}
		}
	}
	return out
}

func stringMapFromCfg(cfg map[string]interface{}, key string) map[string]string {
	raw, ok := cfg[key]
	if !ok {
		return nil
	}
	m, ok := raw.(map[string]interface{})
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func defaultTopicFromCfg(cfg map[string]interface{}) string {
	if v, ok := cfg["default_topic"].(string); ok {
		return v
	}
	return ""
}

func uint64FromCfg(cfg map[string]interface{}, key string, def uint64) uint64 {
	if v, ok := cfg[key]; ok {
		switch n := v.(type) {
		case int:
			return uint64(n)
		case int64:
			return uint64(n)
		case float64:
			return uint64(n)
		}
	}
	return def
}

// compileScopeRegex 从 cfg 中解析 scope.metric_regex(design §5.1)。
//
// 返回值:
//   - (nil, nil):未配置 scope 或 metric_regex 为空 → stage 对所有 sample 生效
//   - (re, nil):  编译成功 → stage 仅对 metric name 匹配 re 的 sample 生效
//   - (nil, err): 正则编译失败 → Compile 整体失败
//
// 用于 sample / downsample / deadvalue 三个 stage,统一"按 metric 名过滤"语义。
func compileScopeRegex(cfg map[string]interface{}, stageType string) (*regexp.Regexp, error) {
	scopeRaw, ok := cfg["scope"].(map[string]interface{})
	if !ok {
		return nil, nil
	}
	pattern, _ := scopeRaw["metric_regex"].(string)
	if pattern == "" {
		return nil, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid scope.metric_regex %q: %w", stageType, pattern, err)
	}
	return re, nil
}
