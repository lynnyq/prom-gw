// Package parser 把 prompb.WriteRequest 转换为内部 Sample 列表,
// 填入请求级 Meta(business / source_dc / trace_id 等)。
//
// Sample 内部表示设计与 spec 一致:
//   - 字段紧凑,整结构 ≤ 256 字节
//   - 不存 TraceID(走 request-scoped context)
//   - Business/SourceDC 走 stringpool.Intern 复用
//   - Labels 容量 4 起,扩容时复用底层数组
package parser

import (
	"hash/fnv"

	"github.com/lynnyq/prom-gw/pkg/stringpool"
)

// Label 内部标签表示(已排序,保证 hash 一致)。
type Label struct {
	Name  string
	Value string
}

// Sample 清洗后进入规则引擎的最小数据单元。
// 注意: TraceID 字段被刻意移除,由 ctx 透传(见 T1.12 OTel 接入)。
type Sample struct {
	Business   string // 业务标识,stringpool 复用
	SourceDC   string // 来自哪个机房,stringpool 复用
	IngestCity string // 城市标识(bj/sz/hf),stringpool 复用
	Metric     string
	Labels     []Label // 排序后,容量 4 起
	Value      float64
	// Timestamp 来自 prompb.Sample.Timestamp,毫秒。
	Timestamp int64
	// IngestTs 来自 receiver 注入的 Meta.IngestTs,纳秒。
	IngestTs int64
	// TargetTopic 路由目标(由 route stage 写入),空表示使用 default_topic。
	// 注意:TargetTopic 字段不进 Kafka message header,只影响下游投递。
	TargetTopic string
}

// SeriesKey 返回 series 的稳定 hash key,用于 Kafka 分区 + 状态型 stage 索引。
// 算法: FNV-1a 64,对 (business, metric, sorted labels) 拼接后计算。
// 拼接格式: business + "\x00" + metric + "\x00" + name + "=" + value + "\x00"
// (用 \x00 避免 a="x"b="y" 与 a="xb"="y" 碰撞)
func (s Sample) SeriesKey() uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s.Business))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(s.Metric))
	_, _ = h.Write([]byte{0})
	for _, l := range s.Labels {
		_, _ = h.Write([]byte(l.Name))
		_, _ = h.Write([]byte{'='})
		_, _ = h.Write([]byte(l.Value))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

// Clone 浅拷贝(Label slice 共享底层数组;parser 高频调用,避免深拷贝)。
// 适用于 stage 之间流转,以及死值丢弃后仍要保留原值的场景。
func (s Sample) Clone() Sample {
	cp := Sample{
		Business:   s.Business,
		SourceDC:   s.SourceDC,
		IngestCity: s.IngestCity,
		Metric:     s.Metric,
		Labels:     s.Labels, // 共享
		Value:      s.Value,
		Timestamp:  s.Timestamp,
		IngestTs:   s.IngestTs,
		TargetTopic: s.TargetTopic,
	}
	return cp
}

// InternStrings 把高频复用字符串入池(调用方在 Parse 之后调一次即可)。
// 注意: 不要对 Labels 里的 Value 全部入池,可能爆内存;只对 Name 与已知有限集入池。
func (s *Sample) InternStrings() {
	s.Business = stringpool.Intern(s.Business)
	s.SourceDC = stringpool.Intern(s.SourceDC)
	s.IngestCity = stringpool.Intern(s.IngestCity)
	s.Metric = stringpool.Intern(s.Metric)
	for i := range s.Labels {
		s.Labels[i].Name = stringpool.Intern(s.Labels[i].Name)
	}
}
