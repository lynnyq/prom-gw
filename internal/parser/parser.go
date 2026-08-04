// Package parser 把 prompb.WriteRequest 转换为内部 Sample 列表。
//
// 设计要点(spec T1.6):
//   - Meta 走 ctx(由 receiver 注入),不依赖外部参数透传
//   - TraceID 不进 Sample,由 ctx 透传(避免 GC 压力)
//   - Tenant/SourceDC 走 stringpool.Intern
//   - Labels 排序后入 sample,保证 SeriesKey 一致
//   - 单条 series 失败不阻断整批,记 gateway_errors_total{type="parse_series"}
package parser

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/lynnyq/bigdata/pkg/stringpool"
	"github.com/prometheus/prometheus/prompb"
)

// Meta 携带请求级元数据,从 receiver 注入到 ctx 中。
type Meta struct {
	Tenant     string // 来自 token.Name
	TenantID   string // 来自 token.TenantID(未来 IAM 主键,v1 可空)
	SourceDC   string // 来自 instance tag(--source-dc 启动参数)或 X-Source-DC 头
	RemoteIP   string // 来自 http.Request.RemoteAddr
	IngestTs   int64  // 进入 GW 时刻(纳秒时间戳,由 receiver 注入)
	TraceID    string // 由 tracing 中间件注入,后续写 Kafka header
	IngestCity string // 城市标识(bj/sz/hf),来自 --ingest-city 启动参数或 INGEST_CITY env,spec 4.3 / 7.1
}

type metaCtxKey struct{}

// ContextWithMeta 把 m 注入 ctx。
func ContextWithMeta(ctx context.Context, m Meta) context.Context {
	return context.WithValue(ctx, metaCtxKey{}, m)
}

// MetaFromContext 从 ctx 取 Meta;不存在返回 false。
func MetaFromContext(ctx context.Context) (Meta, bool) {
	m, ok := ctx.Value(metaCtxKey{}).(Meta)
	return m, ok
}

// ErrMetaMissing ctx 缺少 Meta。receiver 内部 bug 才会出现此错误,
// 应 panic / 触发 fast-fail,不当 4xx 处理。
var ErrMetaMissing = errors.New("parser: Meta missing from context")

// ParseResult 单次解析的统计与产物。
type ParseResult struct {
	Samples    []Sample
	ParseError int // 单条 series 失败计数(已跳过)
}

// Parse 把 req 转换为 []Sample;ctx 必带 Meta(由 receiver.Auth 中间件注入)。
//
// 失败语义:
//   - ctx 缺 Meta -> ErrMetaMissing(调用方应 panic / fast-fail)
//   - 单条 series 解析失败 -> 跳过 + 计数,继续处理剩余
func Parse(ctx context.Context, req *prompb.WriteRequest) (ParseResult, error) {
	meta, ok := MetaFromContext(ctx)
	if !ok {
		return ParseResult{}, ErrMetaMissing
	}

	// 预分配容量: 经验值每个 TimeSeries 平均 ~1 Sample;有 Histogram/Exemplar 的会更多。
	// 保守给 len(Timeseries) * 2,实际 append 会按需扩容。
	out := make([]Sample, 0, len(req.Timeseries)*2)

	ingestMs := meta.IngestTs
	if ingestMs == 0 {
		ingestMs = time.Now().UnixNano()
	}

	for _, ts := range req.Timeseries {
		s, ok := convertTimeSeries(&ts, &meta, ingestMs)
		if !ok {
			continue
		}
		s.InternStrings()
		out = append(out, s)
	}
	return ParseResult{Samples: out}, nil
}

func convertTimeSeries(ts *prompb.TimeSeries, meta *Meta, ingestMs int64) (Sample, bool) {
	if len(ts.Labels) == 0 {
		return Sample{}, false
	}
	// 1. 拆出 __name__,其余 label 排序
	labels := make([]Label, 0, len(ts.Labels))
	var metric string
	for _, l := range ts.Labels {
		if l.Name == "__name__" {
			metric = l.Value
			continue
		}
		labels = append(labels, Label{Name: l.Name, Value: l.Value})
	}
	if metric == "" {
		return Sample{}, false // 缺 __name__,Prometheus 必带,跳过
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Name != labels[j].Name {
			return labels[i].Name < labels[j].Name
		}
		return labels[i].Value < labels[j].Value
	})

	if len(ts.Samples) == 0 {
		return Sample{}, false
	}
	// v1 只取第一个 sample(与 Prometheus remote_write 语义一致: 每个 series 一个 sample)
	// histograms/exemplars 留给 Phase 2 处理
	pb := ts.Samples[0]

	return Sample{
		Tenant:     stringpool.Intern(meta.Tenant),
		TenantID:   stringpool.Intern(meta.TenantID),
		SourceDC:   stringpool.Intern(meta.SourceDC),
		IngestCity: stringpool.Intern(meta.IngestCity),
		Metric:     stringpool.Intern(metric),
		Labels:     labels,
		Value:      pb.Value,
		// Prometheus 协议 timestamp 为毫秒,直接透传。
		Timestamp: pb.Timestamp,
		IngestTs:  ingestMs,
	}, true
}
