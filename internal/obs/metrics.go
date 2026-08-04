// Package obs 统一可观测: zap JSON 日志、prometheus self-export、OpenTelemetry trace。
//
// Phase 1 范围:
//   - Metrics: 阶段级 / 错误 / 资源 / WAL
//   - Tracing: Phase 1 末接入(本文件占位)
//   - Logging: 由 cmd/prom-gw 直接初始化,不在本包
package obs

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 全局注入的实例级 ingest_city / source_dc(spec 7.1)。
//
// 启动时 main 调用 SetMetaLabels 写入,所有 obs.*WithLabelValues 间接
// 通过这里获取,避免各包重复实现 atomic 读写。
var (
	metaIngestCity atomic.Pointer[string]
	metaSourceDC   atomic.Pointer[string]
)

// SetMetaLabels 全局注入实例级 ingest_city / source_dc 标签。
//
// 由 cmd/prom-gw 在启动时调用一次(spec 7.1);后续所有 obs.*WithLabelValues
// 间接通过 MetaLabels() 拿这两个值。
func SetMetaLabels(ingestCity, sourceDC string) {
	metaIngestCity.Store(&ingestCity)
	metaSourceDC.Store(&sourceDC)
}

// MetaLabels 返回当前注入的 (ingest_city, sourceDC) 元组。
//
// 用于补全 spec 7.1 要求的 ingest_city / source_dc 标签;
// 在 main 调 SetMetaLabels 之前返回两个空字符串,这是 spec 要求的"未配置"语义。
func MetaLabels() (ingestCity, sourceDC string) {
	if p := metaIngestCity.Load(); p != nil {
		ingestCity = *p
	}
	if p := metaSourceDC.Load(); p != nil {
		sourceDC = *p
	}
	return ingestCity, sourceDC
}

// metricsInit 标记 InitMetricsForTest 是否被调过(防止与 promauto 重复注册 panic)。
var metricsInit bool

// InitMetricsForTest 显式触发全局指标注册(供测试场景)。
//
// promauto.NewCounterVec 在 init() 已注册到 DefaultRegisterer;该函数对 vec 做
// 一次 WithLabelValues 触发 metric 行实例化,便于 DefaultGatherer().Gather()
// 返回本包全部指标(否则 vec 是空的,某些 metric 完全不会出现)。
func InitMetricsForTest() {
	if metricsInit {
		return
	}
	metricsInit = true
	city, dc := "test_city", "test_dc"
	// 触发各 vec 至少 +0,确保 DefaultGatherer 能看到 metric 行(label 集合确定)
	SamplesTotal.WithLabelValues("test", "test", "ok", city, dc).Add(0)
	StageDuration.WithLabelValues("test", "ok", city).Observe(0)
	RequestDuration.WithLabelValues("test", "ok", city).Observe(0)
	ErrorsTotal.WithLabelValues("test", "test", city, dc).Add(0)
	AuthFailTotal.WithLabelValues("test", city, dc).Add(0)
	BackpressureRejected.WithLabelValues("test", city, dc).Add(0)
	BytesIn.WithLabelValues("test", city, dc).Add(0)
	BytesOut.WithLabelValues("test", city).Add(0)
	RulesetSwitchTotal.WithLabelValues("test", "test", "test", city, dc).Add(0)
	RulesetVersion.WithLabelValues("test", city).Set(0)
	ConfigReloadTotal.WithLabelValues("test", "test", city, dc).Add(0)
	RulesetRoutedTotal.WithLabelValues("test", city, dc).Add(0)
	RulesetErrorsTotal.WithLabelValues("test", city, dc).Add(0)
	RateLimitRejected.WithLabelValues("test", city, dc).Add(0)
	ProduceErrorsTotal.WithLabelValues("test", city, dc).Add(0)
	AdminAuthFailTotal.WithLabelValues("test", city, dc).Add(0)
	RulesetProcessedTotal.WithLabelValues("test", "test", city, dc).Add(0)
	StateSeries.WithLabelValues("test", city).Set(0)
	RulesetHistorySize.WithLabelValues("test", city).Set(0)
	// GaugeVec 系列
	WalBytesVec.WithLabelValues(city).Set(0)
	WalOldestAgeVec.WithLabelValues(city).Set(0)
}

// 全局指标(进程级单例)。
// 命名规范: gateway_<scope>_<name>_<unit>(可选)
// label 规范:
//   - stage / tenant / status / type / reason
//   - ingest_city / source_dc(spec 7.1:所有指标必带,便于北京 Grafana 跨城聚合/切片)
var (
	// SamplesTotal 按阶段计 samples 处理量。
	// stage ∈ {receive, decode, parse, pipeline, kafka, wal}
	// tenant 来源 token;空表示未鉴权
	// status ∈ {ok, error, drop}
	// ingest_city / source_dc 标识数据来源(便于跨城聚合/切片)
	SamplesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_samples_total",
		Help: "Total number of samples processed, by stage/tenant/status/ingest_city/source_dc.",
	}, []string{"stage", "tenant", "status", "ingest_city", "source_dc"})

	// StageDuration 各阶段处理耗时(秒)。spec 7.1 要求带 ingest_city 标签。
	StageDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_stage_duration_seconds",
		Help:    "Stage processing duration in seconds.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14), // 0.1ms ~ 1.6s
	}, []string{"stage", "op", "ingest_city"})

	// RequestDuration HTTP 入口全链路耗时。spec 7.1 要求带 ingest_city 标签。
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_request_duration_seconds",
		Help:    "HTTP request total processing duration.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms ~ 8s
	}, []string{"endpoint", "status", "ingest_city"})

	// ErrorsTotal 错误计数。
	// stage ∈ {decode, parse, auth, kafka, wal}
	// type 细分类(snappy / protobuf / parse_series / kafka_produce / wal_full 等)
	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_errors_total",
		Help: "Total number of errors, by stage/type/ingest_city/source_dc.",
	}, []string{"stage", "type", "ingest_city", "source_dc"})

	// AuthFailTotal 鉴权失败计数(reason 分类用于告警分桶)。
	// reason ∈ {missing, invalid, expired, revoked, iam_unavailable}
	AuthFailTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_auth_fail_total",
		Help: "Total number of authentication failures, by reason/ingest_city/source_dc.",
	}, []string{"reason", "ingest_city", "source_dc"})

	// BackpressureRejected 背压拒绝(503)计数。
	// stage 标识哪个 channel 满
	BackpressureRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_backpressure_rejected_total",
		Help: "Total number of requests rejected due to backpressure, by stage/ingest_city/source_dc.",
	}, []string{"stage", "ingest_city", "source_dc"})

	// RateLimitRejected 限流拒绝(429)计数(plan T5.1)。
	// tenant 来源 token;空表示未鉴权或 default 限流
	RateLimitRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limit_rejected_total",
		Help: "Total number of requests rejected by rate limiter, by tenant/ingest_city/source_dc.",
	}, []string{"tenant", "ingest_city", "source_dc"})

	// WalBytesVec WAL 当前占用字节(Gauge)。spec 7.1 要求带 ingest_city 标签。
	WalBytesVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_wal_bytes",
		Help: "Current WAL disk usage in bytes, by ingest_city.",
	}, []string{"ingest_city"})

	// WalOldestAgeVec 最老未确认 segment 存活时长(秒,Gauge)。spec 7.1 要求带 ingest_city 标签。
	WalOldestAgeVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_wal_oldest_age_seconds",
		Help: "Age of the oldest unacknowledged WAL segment in seconds, by ingest_city.",
	}, []string{"ingest_city"})

	// WalHardReject WAL 满硬拒绝次数(spec 6.2 / plan T1.8 第三道防线)。
	WalHardReject = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_wal_hard_reject_total",
		Help: "Total number of hard rejections due to WAL capacity.",
	})

	// Goroutines 当前 goroutine 数(Gauge)。
	Goroutines = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gateway_goroutines",
		Help: "Current number of goroutines.",
	}, numGoroutines)

	// MemBytes 进程驻留内存字节数(Gauge,spec 7.1)。
	MemBytes = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gateway_mem_bytes",
		Help: "Resident memory bytes held by the process (HeapAlloc + HeapInuse).",
	}, memBytes)

	// CpuRatio 进程 CPU 使用率(0-1,Gauge,spec 7.1)。
	// 由 runtime cpu collector 提供最近 1s 窗口的 busy ratio。
	CpuRatio = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gateway_cpu_ratio",
		Help: "Process CPU busy ratio (0-1) over the recent sampling window.",
	}, readCPUBusyRatio)

	// BytesIn / BytesOut 网络流量。
	BytesIn = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_bytes_in_total",
		Help: "Total bytes received, by tenant/ingest_city/source_dc.",
	}, []string{"tenant", "ingest_city", "source_dc"})

	BytesOut = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_bytes_out_total",
		Help: "Total bytes sent to Kafka, by topic/ingest_city.",
	}, []string{"topic", "ingest_city"})

	// RulesetSwitchTotal 规则集热切换计数(plan T2.10 / T5.6)。
	// from_version="v0" 表示首次加载。
	RulesetSwitchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_ruleset_switch_total",
		Help: "Total number of ruleset hot-swaps, labeled by name/from_version/to_version/ingest_city/source_dc.",
	}, []string{"name", "from_version", "to_version", "ingest_city", "source_dc"})

	// RulesetVersion 当前生效 ruleset 版本(Gauge)。
	// spec 7.1 要求带 ingest_city 标签便于跨城切片。
	RulesetVersion = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_ruleset_version",
		Help: "Currently active ruleset version, by name/ingest_city.",
	}, []string{"name", "ingest_city"})

	// ConfigReloadTotal 配置热重载计数(plan T2.10 / T4.1)。
	// source ∈ {file, nacos, api} ;status ∈ {ok, error}
	ConfigReloadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_config_reload_total",
		Help: "Total number of config hot-reloads, by source/outcome/ingest_city/source_dc.",
	}, []string{"source", "status", "ingest_city", "source_dc"})

	// RulesetRoutedTotal router 路由到各 ruleset 的 sample 数(plan T2.8 / design 5.2)。
	// name 来自 router.Entry.Name;未命中 default 的样本不计入。
	RulesetRoutedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_ruleset_routed_total",
		Help: "Total number of samples routed to each ruleset by the fan-out router, with ingest_city/source_dc.",
	}, []string{"name", "ingest_city", "source_dc"})

	// RulesetErrorsTotal ruleset 内部处理错误计数(router 层聚合)。
	// name 来自 router.Entry.Name;用于识别某 ruleset 是否频繁失败。
	RulesetErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_ruleset_errors_total",
		Help: "Total number of per-ruleset processing errors surfaced through the router, with ingest_city/source_dc.",
	}, []string{"name", "ingest_city", "source_dc"})

	// RulesetProcessedTotal 各 ruleset + stage 处理的 sample 数(plan T2.7 / spec 7.1)。
	// spec 7.1: gateway_ruleset_processed_total{ruleset, stage, ingest_city}
	RulesetProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_ruleset_processed_total",
		Help: "Total number of samples processed by each ruleset+stage, by ingest_city/source_dc.",
	}, []string{"ruleset", "stage", "ingest_city", "source_dc"})

	// StateSeries 各 ruleset 中状态型 stage 当前跟踪的 series 数(plan T3.4)。
	// 当前在 downsample/deadvalue 等状态型 stage 内部填充。
	StateSeries = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_state_series",
		Help: "Current number of series tracked in stateful stages, by ruleset/ingest_city.",
	}, []string{"ruleset", "ingest_city"})

	// RulesetHistorySize 各 ruleset 在 history ring buffer 中的版本数(plan T4.6)。
	RulesetHistorySize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_ruleset_history_size",
		Help: "Current number of versions stored in ruleset history, by ruleset/ingest_city.",
	}, []string{"ruleset", "ingest_city"})

	// ProduceErrorsTotal Kafka produce 错误计数(plan T1.7)。
	// reason 细分:kafka_produce, kafka_timeout, kafka_backpressure 等。
	// 早期实现中用 ErrorsTotal{stage="kafka",type=...} 替代,本指标为独立计数便于告警分桶。
	ProduceErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_produce_errors_total",
		Help: "Total number of Kafka produce errors, by reason/ingest_city/source_dc.",
	}, []string{"reason", "ingest_city", "source_dc"})

	// AdminAuthFailTotal Admin API 来源 IP 白名单拒绝计数(plan T4.3)。
	// spec 要求便于告警分桶;同时 admin 包内部用 atomic.Int64 跟踪,
	// 这里只把同一份数值用 prometheus 暴露。
	AdminAuthFailTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_admin_auth_fail_total",
		Help: "Total number of admin API allowlist rejections, by reason/ingest_city/source_dc.",
	}, []string{"reason", "ingest_city", "source_dc"})

	// GatewayPanicRecovered 累计 panic 恢复次数(spec 6.6 / plan T0.7)已声明在下方 GaugeFunc。
)

// PanicRecovered 累计 panic 恢复次数(spec 6.6 / plan T0.7)。
//
// 由 safego 在 init() 中把 recordPanic 钩到该 Gauge 上,所有 stage worker
// / kafka flusher / config watcher / admin handler 的 panic 都会触发 Inc。
// 之所以用 GaugeFunc 而非 Counter,是因为 safego.Stats() 内部用 mutex 保护
// uint64 累加,跨包直接累加会破坏 safego 现有测试断言。GaugeFunc 在采集时
// 读一次原子值,无锁。
var PanicRecovered = promauto.NewGaugeFunc(prometheus.GaugeOpts{
	Name: "gateway_panic_recovered_total",
	Help: "Total number of recovered panics across all goroutines (safego.stats).",
}, panicCount)
