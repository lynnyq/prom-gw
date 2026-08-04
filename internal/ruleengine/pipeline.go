// Package ruleengine - pipeline.go: 规则引擎流水线(Phase 2 完整版)。
//
// 设计要点(plan T2.7 / T2.8):
//   - 单规则集,直接透传 sample 到下游 sink
//   - 提供 SetRules 钩子支持热更新(atomic.Pointer[CompiledRuleSet])
//   - Process(ctx, ...) 同步执行所有 stage,然后按 sample.TargetTopic 分发
//   - 切换瞬间 in-flight 的 Process 仍用旧规则跑完(per-batch 加载)
//   - 由于按 sample 路由,每批可能产生多个 topic → 一次 Process 内多次 out() 调用
package ruleengine

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"go.uber.org/zap"
)

// SinkFunc 规则链跑完后,把"处理后"的消息提交到下游(通常为 sink.Pipeline.Submit)。
//
// 参数:
//   - msg: 已带 topic/key/payload/headers 的待发送消息
//
// 返回:
//   - nil: 已入下游
//   - 其他: 下游背压/失败,接收端映射 503
type SinkFunc func(ctx context.Context, msg sink.Message) error

// Pipeline 规则引擎流水线。
//
// 字段说明:
//   - rules: 当前生效规则(atomic,支持热更新;在 Process 入口 Load,保证单批一致)
//   - out: 处理结果出口
//   - logger: 必填
type Pipeline struct {
	rules  atomic.Pointer[CompiledRuleSet]
	out    SinkFunc
	logger *zap.Logger
}

// NewPipeline 构造并装载一个默认空规则集。
//
// 默认规则集 Name="default" Version=1,Stages 为空 → Process 直接按 default_topic
// 投递(透传语义)。
func NewPipeline(logger *zap.Logger, out SinkFunc) *Pipeline {
	if logger == nil {
		logger = zap.NewNop()
	}
	if out == nil {
		out = func(_ context.Context, _ sink.Message) error { return nil }
	}
	p := &Pipeline{
		out:    out,
		logger: logger,
	}
	p.rules.Store(&CompiledRuleSet{
		RuleSet: RuleSet{
			Name:    "default",
			Version: 1,
		},
		Stages: nil,
	})
	return p
}

// Rules 读取当前生效规则(原子快照)。
func (p *Pipeline) Rules() *CompiledRuleSet { return p.rules.Load() }

// SetRules 原子切换规则。
//
// 入参 nil 直接返回(防御性);非 nil 时 Store 并打 info 日志。
// 运行中的 Process 仍用旧规则跑完当前批次,符合 plan T2.7 per-batch 加载语义。
func (p *Pipeline) SetRules(rs *CompiledRuleSet) {
	if rs == nil {
		return
	}
	old := p.rules.Load()
	p.rules.Store(rs)
	ingestCity, sourceDC := extractMetaLabels(nil)
	p.logger.Info("rule engine: rules swapped",
		zap.String("from_name", old.RuleSet.Name),
		zap.Int64("from_version", old.RuleSet.Version),
		zap.String("to_name", rs.RuleSet.Name),
		zap.Int64("to_version", rs.RuleSet.Version),
		zap.Int("stage_count", len(rs.Stages)),
	)
	obs.RulesetSwitchTotal.WithLabelValues(
		rs.RuleSet.Name,
		"v"+strconv.FormatInt(old.RuleSet.Version, 10),
		"v"+strconv.FormatInt(rs.RuleSet.Version, 10),
		ingestCity, sourceDC,
	).Inc()
	obs.RulesetVersion.WithLabelValues(rs.RuleSet.Name, ingestCity).Set(float64(rs.RuleSet.Version))
}

// extractMetaLabels 从 logger 提取 ingest_city/source_dc 标签(spec 7.1)。
//
// 委托给 obs.MetaLabels,统一在 obs 包维护实例级元数据(spec 7.1)。
// 这样其他包(sink / kafkasink / downsample) 也可以直接用 obs.MetaLabels
// 避免循环依赖(ruleengine 依赖 sink,sink 不能反向依赖 ruleengine)。
func extractMetaLabels(_ *zap.Logger) (string, string) {
	return obs.MetaLabels()
}

// Process 跑一批 sample:串行执行所有 stage,然后按 TargetTopic 分发到下游。
//
// 入参:
//   - samples: parser 解析后的 sample 列表
//   - raw: 原始 prompb.WriteRequest 字节(T1.10 集成测试要求字节级相等)
//   - msg: 由接收端构造好的 sink.Message 模板(已含 topic/key/headers,payload 等待填入)
//
// 行为:
//   - 单批入口 Load 当前 rules,保证本批内一致
//   - 依次执行 rs.Stages:sample 经 relabel/route/sample 清洗
//   - 末端按 sample.TargetTopic 切分(若同一 batch 内有多个 topic,产生多个 message)
//   - 任一 message 投递失败 → 整个 Process 返回 error(503 语义)
//
// 错误处理:
//   - 单条 stage 内部 error:记 metric,继续跑后续 stage(尽力保证 best-effort)
//   - sink.Send error:直接返回,receiver 映射 503
//
// buffer 复用约定:
//   - buf1/buf2 是两块预分配的 Sample slice,交替作为 "in" 和 "prev" 传给 stage
//   - stage.Apply 必须把结果写入 prev(允许 prev[:0] 复用底层),并把 prev slice 返回
//   - 当 stage 选择透传 in(如 SampleStage rate=1、Relabel 空配置)时,
//     Pipeline 检测 out 与 prev 不同底层,自动把数据从 in 搬到 prev,
//     保证下一轮 prev 永远指向"对面"那块 buffer,避免后续 stage 读到已污染数据
func (p *Pipeline) Process(ctx context.Context, samples []parser.Sample, raw []byte, msg sink.Message) error {
	start := time.Now()
	rs := p.Rules()

	ctx, span := tracex.StartSpan(ctx, "rule", "process")
	defer span.End()

	// spec 7.1: 实例级 ingest_city/source_dc 标签(由 main 在启动时调 SetMetaLabels 注入)
	ingestCity, sourceDC := extractMetaLabels(nil)

	p.logger.Debug("rule pipeline: processing",
		zap.Int("n_samples", len(samples)),
		zap.Int("n_stages", len(rs.Stages)),
	)

	// 1. 跑所有 stage(in-place 复用 buf1/buf2,避免中间分配)
	n := len(samples)
	buf1 := make([]parser.Sample, 0, n)
	buf1 = append(buf1, samples...)
	buf2 := make([]parser.Sample, 0, n)

	// cur 当前数据来源 slice, dst 当前 stage 的 prev(可写)
	cur := buf1
	dst := buf2[:0]
	totalDropped := 0
	for i, stage := range rs.Stages {
		stageStart := time.Now()
		out, dropped, err := runStage(ctx, stage, cur, dst)
		obs.StageDuration.WithLabelValues("rule_"+stage.Type, "ok", ingestCity).Observe(time.Since(stageStart).Seconds())
		if err != nil {
			obs.ErrorsTotal.WithLabelValues("rule", stage.Type, ingestCity, sourceDC).Inc()
			p.logger.Warn("stage apply error, continue",
				zap.String("stage", stage.Type),
				zap.Int("stage_index", i),
				zap.Error(err),
			)
		}
		totalDropped += dropped

		// 把数据稳定到 dst(若 stage 透传 in,cur 和 out 共享底层;否则 out 已写入 dst)
		if len(out) > 0 && &out[0] == &cur[0] {
			// stage 透传了 in,把 in 数据搬到 dst,避免下一轮 prev 仍指向 cur(已用)
			// 目的:无论 stage 行为,cur 与 dst 始终是"两块不同底层"slice
			moved := append(dst[:0], out...)
			out = moved
		}

		// 交换 cur / dst:dst 始终是 "cur 的对面" buffer(空 slice + 足够 cap)
		if len(out) > 0 && &out[0] == &buf1[0] {
			// 数据现在在 buf1,把 buf2 清空作为下一轮 prev
			cur = buf1
			dst = buf2[:0]
		} else {
			// 数据现在在 buf2(包括 out==0 情形),把 buf1 清空作为下一轮 prev
			cur = buf2[:0]
			if len(out) > 0 {
				cur = out
			}
			dst = buf1[:0]
		}
	}

	// 2. 按 TargetTopic 分发(同一 batch 可能有多个 topic → 多次 out)
	tenant := rs.RuleSet.Tenant
	if tenant == "" {
		tenant = "unknown"
	}
	defaultTopic := rs.RuleSet.DefaultTopic
	if defaultTopic == "" {
		defaultTopic = msg.Topic
	}

	// 统计:总 in / 路由后 / 丢弃
	nIn := int64(len(samples))
	nOut := int64(len(cur))
	obs.SamplesTotal.WithLabelValues("rule", tenant, "in", ingestCity, sourceDC).Add(float64(nIn))
	obs.SamplesTotal.WithLabelValues("rule", tenant, "drop", ingestCity, sourceDC).Add(float64(totalDropped))
	obs.SamplesTotal.WithLabelValues("rule", tenant, "out", ingestCity, sourceDC).Add(float64(nOut))

	if len(cur) == 0 {
		// 整批被采样/路由丢空,直接返回成功(不投递)
		obs.StageDuration.WithLabelValues("rule", "ok", ingestCity).Observe(time.Since(start).Seconds())
		return nil
	}

	p.logger.Debug("rule pipeline: dispatching",
		zap.Int("n_samples", len(cur)),
		zap.String("default_topic", defaultTopic),
	)

	// 每 sample 一次 out,key 各自用 seriesKey(同 series 落同 partition,顺序保留)
	for _, s := range cur {
		topic := s.TargetTopic
		if topic == "" {
			topic = defaultTopic
		}
		m := msg
		m.Topic = topic
		m.Key = []byte(strconv.FormatUint(s.SeriesKey(), 10))
		m.Payload = raw
		if err := p.out(ctx, m); err != nil {
			obs.ErrorsTotal.WithLabelValues("rule", "send", ingestCity, sourceDC).Inc()
			obs.SamplesTotal.WithLabelValues("rule", tenant, "error", ingestCity, sourceDC).Add(float64(nOut))
			obs.StageDuration.WithLabelValues("rule", "error", ingestCity).Observe(time.Since(start).Seconds())
			span.RecordError(err)
			return err
		}
		obs.SamplesTotal.WithLabelValues("rule", tenant, "ok", ingestCity, sourceDC).Add(1)
	}

	obs.StageDuration.WithLabelValues("rule", "ok", ingestCity).Observe(time.Since(start).Seconds())
	return nil
}

// groupByTopic 把 samples 按 TargetTopic 分组,返回 topic → 命中数(用于 metric 统计)。
//
// 注:不再用于"每 topic 一次 out"——out 已改为每 sample 一次(plan T2.8 + Kafka per-series
// 顺序保证),保留此函数仅为可能的 future 聚合模式,以及 metric 统计入口。
func groupByTopic(samples []parser.Sample, defaultTopic string) map[string]int {
	out := make(map[string]int, 4)
	for i := range samples {
		t := samples[i].TargetTopic
		if t == "" {
			t = defaultTopic
		}
		out[t]++
	}
	return out
}

// runStage 调用单个 stage 并 recover panic。
//
// 关键:返回 stage 输出的 slice(可能与 prev 复用底层数组);
// 丢入 panic 时返回 in(视为 stage 未生效,数据透传,避免误丢)。
func runStage(ctx context.Context, stage CompiledStage, in, prev []parser.Sample) (out []parser.Sample, dropped int, err error) {
	if stage.Apply == nil {
		return in, 0, nil
	}
	ingestCity, sourceDC := extractMetaLabels(nil)
	defer func() {
		if r := recover(); r != nil {
			obs.ErrorsTotal.WithLabelValues("rule", "panic_"+stage.Type, ingestCity, sourceDC).Inc()
			// panic 时返回 in(透传),并把 panic 信息包装为 error
			out = in
			dropped = 0
			err = fmt.Errorf("rule: stage %q panic: %v", stage.Type, r)
		}
	}()
	return stage.Apply(ctx, in, prev)
}
