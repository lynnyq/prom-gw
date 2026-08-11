// Package obs - tracing.go: OpenTelemetry 链路追踪初始化与工具函数。
//
// 设计要点(plan T1.12):
//   - 启动时按 OTEL_EXPORTER_OTLP_ENDPOINT 环境变量初始化 OTLP gRPC exporter
//   - 不配置 endpoint 时使用 noop tracer(避免高吞吐下 SDK 自身 CPU 占用,见 plan Risks 表)
//   - 使用 BatchSpanProcessor(默认) + AlwaysSample(便于全量追踪,后续按需改 ProbabilitySampler)
//   - TraceID 通过 context.Context 在 6 阶段间传递;不存进 Sample(避免 string 分配压力)
//   - 终止时 Flush 上报 in-flight spans
package obs

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Tracer 全局 Tracer(本进程内单例),由 InitTracing 初始化。
// 未初始化时为 noop tracer,任意代码都能安全调用 obs.Tracer.Start。
var Tracer trace.Tracer = noop.NewTracerProvider().Tracer("prom-gw")

// tracerProvider 持有实际 provider 引用,用于 Shutdown 时 Flush。
var tracerProvider *sdktrace.TracerProvider

// TracingConfig 启动配置。
type TracingConfig struct {
	// ServiceName 服务名,写入 resource attributes。默认 "prom-gw"。
	ServiceName string
	// ServiceVersion 服务版本(由 main 注入,默认 version 变量)。
	ServiceVersion string
	// OTLPEndpoint gRPC endpoint(如 "otel-collector:4317");空 → 走 noop。
	OTLPEndpoint string
	// Insecure 不使用 TLS(默认 true,内部网络)。
	Insecure bool
	// SampleRatio 采样率 0.0-1.0。默认 1.0(全采样);高吞吐可降到 0.1。
	SampleRatio float64
	// IngestCity 城市标识(bj/sz/hf),写入 resource attributes(spec §7.2: 所有 span 必带)。
	IngestCity string
	// SourceDC 机房标识,写入 resource attributes(spec §7.2: 所有 span 必带)。
	SourceDC string
	// Logger 可选,初始化失败时打 warn。
	Logger *zap.Logger
}

// InitTracing 初始化全局 TracerProvider。
//
// 行为:
//   - OTLPEndpoint 为空 → 装载 noop tracer(无 SDK 开销),直接返回 nil
//   - 非空 → 建 OTLP gRPC exporter + BatchSpanProcessor + AlwaysSample
//   - 同时设置全局 TextMapPropagator(W3C traceparent + baggage,便于跨服务传递)
//   - 启动失败 → 降级到 noop + 返回 error(不阻断进程)
func InitTracing(cfg TracingConfig) error {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "prom-gw"
	}
	if cfg.ServiceVersion == "" {
		cfg.ServiceVersion = "dev"
	}

	// 无论是否启用 OTLP exporter,都先设置 W3C TraceContext + Baggage propagator。
	// 这是必须的:即便 tracer 走 noop(高吞吐省 CPU),也要让 traceparent 注入/解析
	// 正常运作,这样下游消费者(Kafka header / OTel collector)能拿到 traceID。
	// 不设置的话,otel.GetTextMapPropagator() 会返回默认 noop,Inject 不写任何东西。
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.OTLPEndpoint == "" {
		// 没配 endpoint → noop,避免 OTel SDK 在高吞吐下吃 CPU
		if cfg.Logger != nil {
			cfg.Logger.Info("tracing disabled: OTEL_EXPORTER_OTLP_ENDPOINT not set, using noop tracer")
		}
		Tracer = noop.NewTracerProvider().Tracer(cfg.ServiceName)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Warn("tracing init failed, falling back to noop", zap.Error(err))
		}
		Tracer = noop.NewTracerProvider().Tracer(cfg.ServiceName)
		return fmt.Errorf("init otlp exporter: %w", err)
	}

	// resource 描述本服务
	// spec §7.2: 所有 span 必带 ingest_city / source_dc attribute
	resAttrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
	}
	if cfg.IngestCity != "" {
		resAttrs = append(resAttrs, attribute.String("ingest_city", cfg.IngestCity))
	}
	if cfg.SourceDC != "" {
		resAttrs = append(resAttrs, attribute.String("source_dc", cfg.SourceDC))
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			resAttrs...,
		),
	)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Warn("tracing resource merge failed, falling back to noop", zap.Error(err))
		}
		Tracer = noop.NewTracerProvider().Tracer(cfg.ServiceName)
		return fmt.Errorf("init resource: %w", err)
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	}
	// 采样:默认全采样;按需降低
	if cfg.SampleRatio > 0 && cfg.SampleRatio < 1 {
		tpOpts = append(tpOpts, sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRatio)))
	} else {
		tpOpts = append(tpOpts, sdktrace.WithSampler(sdktrace.AlwaysSample()))
	}

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer(cfg.ServiceName)
	tracerProvider = tp

	if cfg.Logger != nil {
		cfg.Logger.Info("tracing initialized",
			zap.String("endpoint", cfg.OTLPEndpoint),
			zap.String("service", cfg.ServiceName),
			zap.Float64("sample_ratio", cfg.SampleRatio),
		)
	}
	return nil
}

// ShutdownTracing 关闭 TracerProvider,触发 BatchSpanProcessor Flush。
//
// 应在 main 退出前调用;ctx 超时则放弃未发送的 spans(避免 hang 住退出)。
func ShutdownTracing(ctx context.Context) error {
	if tracerProvider == nil {
		return nil
	}
	return tracerProvider.Shutdown(ctx)
}

// OTLPEndpointFromEnv 解析 OTEL_EXPORTER_OTLP_ENDPOINT 或 OTEL_EXPORTER_OTLP_TRACES_ENDPOINT。
//
// OTEL SDK 规范:完整 endpoint 通过 OTEL_EXPORTER_OTLP_ENDPOINT 给;TRACES 专用变量优先。
func OTLPEndpointFromEnv() string {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); v != "" {
		return v
	}
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
}
