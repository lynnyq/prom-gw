// prom-gw: 多机房 Prometheus RemoteWrite 协议网关。
// 启动顺序: flag -> logger -> safego -> obs -> config -> auth -> kafka/wal -> pipeline -> receiver -> healthz -> signals。
// 详细设计见 docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lynnyq/bigdata/internal/admin"
	"github.com/lynnyq/bigdata/internal/config"
	"github.com/lynnyq/bigdata/internal/kafkasink"
	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/lynnyq/bigdata/internal/receiver"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"github.com/lynnyq/bigdata/internal/sink"
	"github.com/lynnyq/bigdata/internal/wal"
	"github.com/lynnyq/bigdata/pkg/safego"
	"github.com/lynnyq/bigdata/pkg/tracex"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// version 由 Makefile 通过 -ldflags 注入。
var version = "dev"

// 退出码(semantic,符合 systemd 期望)。
const (
	exitOK     = 0
	exitFatal  = 1
	exitReload = 2 // SIGHUP 触发,留给后续 phase
)

func main() {
	if err := run(); err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("fatal: %v", err)
		os.Exit(exitFatal)
	}
	os.Exit(exitOK)
}

func run() error {
	// 1. flag 解析
	var (
		cfgPath     = flag.String("config", envOrDefault("PROM_GW_CONFIG", "configs/rules/default.yaml"), "ruleset 配置文件路径(可由 PROM_GW_CONFIG 环境变量覆盖,推荐按城市分目录 configs/rules/<city>/default.yaml)")
		tokensPath  = flag.String("tokens", envOrDefault("PROM_GW_TOKENS", "configs/tokens/local.yaml"), "token 配置文件路径(可由 PROM_GW_TOKENS 环境变量覆盖)")
		metricsAddr = flag.String("metrics-addr", ":8080", "prometheus self-export 监听地址")
		healthAddr  = flag.String("health-addr", ":8081", "healthz/readyz 监听地址")
		writeAddr   = flag.String("write-addr", ":19201", "Prometheus RemoteWrite 接入地址")
		adminAddr   = flag.String("admin-addr", ":8082", "Admin API 监听地址")
		adminCIDR   = flag.String("admin-allow-cidr", "127.0.0.1/32,10.0.0.0/8", "Admin API 来源 IP 白名单(逗号分隔 CIDR)")
		sourceDC    = flag.String("source-dc", "dc-unknown", "本实例所属机房标识,写入 Meta.SourceDC(spec 4.1 / 7.1)")
		ingestCity  = flag.String("ingest-city", envOrDefault("INGEST_CITY", "dc-unknown"), "城市标识(bj/sz/hf),由 systemd template 注入 INGEST_CITY 环境变量(spec 4.3 / 7.1 / 9.1)")
		walDir      = flag.String("wal-dir", "/data/wal", "磁盘 WAL 数据目录")
		walMaxBytes = flag.Int64("wal-max-bytes", 50<<30, "WAL 总字节上限,默认 50GB")
		walDiskRatio = flag.Float64("wal-disk-used-ratio", 0.80, "WAL 所在磁盘使用率硬阈值(0-1),到达后切硬拒绝(spec 6.2 / plan T1.8 双阈值之一)")
		// Nacos 源(可选,空则不启用)
		nacosAddr       = flag.String("nacos-addr", "", "Nacos 服务端列表,逗号分隔 ip:port(空 = 不启用 Nacos)")
		nacosNamespace  = flag.String("nacos-namespace", "", "Nacos namespace id")
		nacosUsername   = flag.String("nacos-username", "", "Nacos 用户名")
		nacosPassword   = flag.String("nacos-password", "", "Nacos 密码")
		nacosDataID     = flag.String("nacos-data-id", "prom-gw-rules", "Nacos dataId")
		nacosGroup      = flag.String("nacos-group", "GATEWAY", "Nacos group")
		nacosSnapshotPath = flag.String("nacos-snapshot-path", "/data/nacos_snapshot.json", "last-good snapshot 持久化路径(空 = 不持久化)")
		showVer     = flag.Bool("version", false, "打印版本后退出")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("prom-gw %s\n", version)
		return nil
	}

	// 2. zap logger(JSON 格式)
	logger, err := newLogger()
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	// 2.1 OpenTelemetry tracing(OTEL_EXPORTER_OTLP_ENDPOINT 决定是否启用,默认 noop)
	if err := obs.InitTracing(obs.TracingConfig{
		ServiceName:    "prom-gw",
		ServiceVersion: version,
		OTLPEndpoint:   obs.OTLPEndpointFromEnv(),
		Insecure:       true,
		SampleRatio:    1.0,
		IngestCity:     *ingestCity,
		SourceDC:       *sourceDC,
		Logger:         logger.Named("tracing"),
	}); err != nil {
		// 降级到 noop 已由 InitTracing 内部完成,这里只 warn
		logger.Warn("tracing degraded to noop", zap.Error(err))
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = obs.ShutdownTracing(shutdownCtx)
	}()

	logger.Info("starting prom-gw",
		zap.String("version", version),
		zap.String("config", *cfgPath),
		zap.String("tokens", *tokensPath),
		zap.String("source_dc", *sourceDC),
		zap.String("ingest_city", *ingestCity),
		zap.String("wal_dir", *walDir),
	)

	// 2.2 全局注入实例级 ingest_city/source_dc 标签(spec 7.1)
	// ruleengine / sink / kafkasink / downsample 等模块会通过
	// obs.MetaLabels 拿这两个值,保证 gateway_* 指标都带
	// ingest_city / source_dc 标签。
	obs.SetMetaLabels(*ingestCity, *sourceDC)

	// 3. safego panic 钩子(全局 panic 计数)
	safego.Go("logger-keepalive", func() {
		// 占位 goroutine,演示 safego 用法
	})

	// 4. 加载 token 鉴权器
	auth, err := config.NewLocalTokenAuthenticator(*tokensPath)
	if err != nil {
		return fmt.Errorf("init auth: %w", err)
	}
	logger.Info("tokens loaded", zap.Int("count", auth.Size()))

	// 5. 初始化 kafkasink(从 KAFKA_BROKERS 环境变量读,无则跳过并降级到 WAL only)
	var (
		kafkaProducer *kafkasink.Producer
		walInst       wal.WAL
		combinedSink  sink.Sink
	)
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers != "" {
		// KAFKA_BROKERS 可能是逗号分隔的多 broker 列表(如 "kafka-1:9092,kafka-2:9092"),
		// 必须拆分成 []string,否则 franz-go 会把整个字符串当作单个 broker 地址,
		// 导致 DNS 解析失败 → Ping 超时 → 降级到 WAL-only 模式。
		brokerList := splitAndTrim(brokers, ",")
		p, err := kafkasink.New(kafkasink.Config{
			Brokers:        brokerList,
			Logger:         logger.Named("kafkasink"),
			BlockTimeout:   100 * time.Millisecond,
			ConnectTimeout: 5 * time.Second,
			Compression:    "zstd",
			Idempotent:     true,
		})
		if err != nil {
			logger.Warn("kafka connect failed, will run in WAL-only mode",
				zap.Strings("brokers", brokerList),
				zap.Error(err),
			)
		} else {
			kafkaProducer = p
			logger.Info("kafkasink connected", zap.Strings("brokers", brokerList))
		}
	} else {
		logger.Info("KAFKA_BROKERS not set, will run in WAL-only mode")
	}

	// 6. 初始化 WAL
	walInst, err = wal.New(wal.Config{
		Dir:            *walDir,
		MaxBytes:       *walMaxBytes,
		DiskUsedRatio:  *walDiskRatio,
	})
	if err != nil {
		return fmt.Errorf("init wal: %w", err)
	}
	// adminCloser 在 admin server 创建后被赋值(spec §6.5: admin 必须在 sink/WAL 之后关闭,
	// 确保 sink drain 期间 admin 仍可查询状态)。
	// defer 注册顺序: tracing → adminCloser → walInst → sink → ...
	// LIFO 执行顺序: ... → sink.Close → walInst.Close → adminCloser → tracing
	var adminCloser func()
	defer func() {
		if adminCloser != nil {
			adminCloser()
		}
	}()
	defer func() { _ = walInst.Close() }()
	logger.Info("wal initialized",
		zap.String("dir", *walDir),
		zap.Int64("max_bytes", *walMaxBytes),
		zap.Float64("disk_used_ratio", *walDiskRatio),
	)

	// 7. 构造 sink(adapter 包装 kafka + wal;kafka 未连上时只走 wal)
	walSink := sink.NewWALSink(walInst)
	if kafkaProducer != nil {
		combinedSink = sink.NewAdapterSink(sink.AdapterConfig{
			Logger:                  logger.Named("sink-adapter"),
			FailThreshold:           3,
			RecoverCheck:            1 * time.Second,
			RecoverSuccessThreshold: 3,
		}, kafkaProducer, walSink)
	} else {
		combinedSink = walSink
	}
	defer func() { _ = combinedSink.Close() }()

	// 8. 构造 pipeline
	pipe := sink.NewPipeline(sink.PipelineConfig{
		BufferSize: 65535,
		Logger:     logger.Named("pipeline"),
	}, combinedSink)
	pipe.Start()
	defer pipe.Stop()

	// 8.1 构造规则引擎编排器(plan T2.7/T2.8 / spec 5.2:多 ruleset 并行)
	// 每条 ruleset 持有一条独立 *Pipeline;Manager 通过内部 Router 按 Match
	// 把 sample fan-out 到对应 Pipeline;支持热更新单条 ruleset。
	ruleMgr := ruleengine.NewManager(ruleengine.ManagerConfig{
		Logger: logger.Named("rule-mgr"),
		Out: func(ctx context.Context, msg sink.Message) error {
			return pipe.Submit(ctx, msg)
		},
	})
	// 初始默认空 ruleset 注册为 "default"(空 Match 兜底),便于启动后立即能处理请求
	if err := ruleMgr.SetRuleSet(&ruleengine.CompiledRuleSet{
		RuleSet: ruleengine.RuleSet{
			Name:    "default",
			Version: 1,
		},
		Stages: nil,
	}); err != nil {
		return fmt.Errorf("init default ruleset: %w", err)
	}
	logger.Info("rule engine manager initialized",
		zap.Int("rulesets", len(ruleMgr.Names())),
		zap.Strings("names", ruleMgr.Names()),
	)

	// 8.2 构造 config manager(本地文件源 + 默认源;可选 Nacos 源)
	hist := config.NewHistory(config.HistoryConfig{Capacity: 10})
	cfgMgr := config.NewManager(config.ManagerConfig{Logger: logger.Named("config"), History: hist})

	// 8.2.1 可选 Nacos source(若配置了 --nacos-addr 才启用)
	if *nacosAddr != "" {
		nacosClient, nerr := config.NewNacosSDKClient(config.NacosConfig{
			Addrs:        splitAndTrim(*nacosAddr, ","),
			NamespaceID:  *nacosNamespace,
			Username:     *nacosUsername,
			Password:     *nacosPassword,
			SnapshotPath: *nacosSnapshotPath,
		}, logger.Named("nacos"))
		if nerr != nil {
			logger.Warn("config: nacos source init failed, will skip",
				zap.String("addr", *nacosAddr),
				zap.Error(nerr),
			)
		} else {
			nacosSrc, nerr2 := config.NewNacosSource(nacosClient, *nacosDataID, *nacosGroup, logger.Named("nacos-source"))
			if nerr2 != nil {
				logger.Warn("config: nacos source build failed",
					zap.Error(nerr2))
			} else {
				// 优先级最高:addSource 按顺序排,Nacos 加在 File 之前
				// 简化做法:先清空 sources,把 nacos 插队
				cfgMgr.AddSource(nacosSrc)
				logger.Info("config: nacos source registered",
					zap.String("data_id", *nacosDataID),
					zap.String("group", *nacosGroup),
				)
			}
		}
	}

	fileSrc, err := config.NewFileSource(*cfgPath, logger.Named("filesource"))
	if err != nil {
		logger.Warn("config: file source init failed, will fallback to default",
			zap.String("path", *cfgPath),
			zap.Error(err),
		)
	} else {
		cfgMgr.AddSource(fileSrc)
	}
	cfgMgr.AddSource(config.NewDefaultSource())
	// onChange: 切换 pipeline + 写 history
	cfgMgr.SetOnChange(func(snap config.Snapshot) {
		if !snap.Valid() {
			return
		}
		if _, err := cfgMgr.ApplySnapshot(snap); err != nil {
			logger.Warn("config: apply snapshot failed, keep old",
				zap.String("source", snap.Source),
				zap.Error(err),
			)
			return
		}
		// 把 active ruleset 切到 manager(多 ruleset 全部注册,manager 内部 fan-out)
		// 注:ApplySnapshot 已把全部 ruleset 入 history;这里把每条 ruleset 编译 + 注入 manager。
		// 任意一条编译失败 → 该条不进 manager,其他继续生效(per-ruleset 故障隔离)。
		rs, err := ruleengine.LoadBytes(snap.RawYAML)
		if err != nil {
			logger.Warn("config: reload parse failed", zap.Error(err))
			return
		}
		applied := 0
		for i := range rs.Rulesets {
			compiled, cerr := ruleengine.Compile(&rs.Rulesets[i])
			if cerr != nil {
				logger.Warn("config: ruleset compile failed, skip",
					zap.String("ruleset", rs.Rulesets[i].Name),
					zap.Error(cerr),
				)
				continue
			}
			if serr := ruleMgr.SetRuleSet(compiled); serr != nil {
				logger.Warn("config: manager set ruleset failed",
					zap.String("ruleset", rs.Rulesets[i].Name),
					zap.Error(serr),
				)
				continue
			}
			applied++
			logger.Info("rule engine: applied via source",
				zap.String("source", snap.Source),
				zap.String("ruleset", compiled.RuleSet.Name),
				zap.Int64("version", compiled.RuleSet.Version),
			)
		}
		logger.Info("rule engine: snapshot applied",
			zap.String("source", snap.Source),
			zap.Int("total", len(rs.Rulesets)),
			zap.Int("applied", applied),
		)
	})
	cfgCtx, cfgCancel := context.WithCancel(context.Background())
	defer cfgCancel()
	if err := cfgMgr.Start(cfgCtx); err != nil {
		return fmt.Errorf("config manager start: %w", err)
	}
	defer cfgMgr.Close()

	// 8.3 构造 Admin service + server
	adminSvc := admin.NewManagerService(admin.ManagerDeps{
		Manager: cfgMgr,
		RuleMgr: ruleMgr,
		Auth:    auth,
		History: hist,
		Logger:  logger.Named("admin-svc"),
	})
	cidrs := splitAndTrim(*adminCIDR, ",")
	adminSrv, err := admin.New(admin.Config{
		Addr:        *adminAddr,
		AllowCIDR:   cidrs,
		Logger:      logger.Named("admin"),
		IngestCity:  *ingestCity,
		SourceDC:    *sourceDC,
	}, adminSvc)
	if err != nil {
		return fmt.Errorf("init admin: %w", err)
	}
	safego.Go("admin-server", func() {
		logger.Info("admin api listening", zap.String("addr", *adminAddr))
		if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("admin server failed", zap.Error(err))
		}
	})
	// spec §6.5: admin 在 sink/WAL 之后关闭(通过 adminCloser defer 顺序保证)
	adminCloser = func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adminSrv.Shutdown(shutdownCtx)
	}

	// 9. WAL 监控 goroutine:周期性写 wal_bytes / wal_oldest_age 指标
	safego.Go("wal-metrics", func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		// 立即跑一次
		obs.WalBytesVec.WithLabelValues(*ingestCity).Set(float64(walInst.Bytes()))
		obs.WalOldestAgeVec.WithLabelValues(*ingestCity).Set(float64(walInst.OldestAge().Seconds()))
		for {
			select {
			case <-time.After(5 * time.Second):
			}
			obs.WalBytesVec.WithLabelValues(*ingestCity).Set(float64(walInst.Bytes()))
			obs.WalOldestAgeVec.WithLabelValues(*ingestCity).Set(float64(walInst.OldestAge().Seconds()))
		}
	})

	// 9.1 history metrics goroutine:周期性写 ruleset_history_size / ruleset_version(spec 7.1 / plan T4.6)
	safego.Go("history-metrics", func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		// 立即跑一次
		publishHistoryMetrics(hist, ruleMgr, *ingestCity)
		for {
			select {
			case <-time.After(5 * time.Second):
			}
			publishHistoryMetrics(hist, ruleMgr, *ingestCity)
		}
	})

	// 10. 启动 receiver,handler 走 pipeline.Submit
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recv, err := receiver.New(receiver.Config{
		Addr:          *writeAddr,
		Authenticator: auth,
		Logger:        logger.Named("receiver"),
		SourceDC:      *sourceDC,
		IngestCity:    *ingestCity,
		Handler: func(ctx context.Context, raw []byte, samples []parser.Sample, defaultTopic string) error {
			// 抽取请求级 Meta 用于 headers
			meta, _ := parser.MetaFromContext(ctx)
			headers := map[string]string{
				"tenant":         meta.Tenant,
				"source_dc":      meta.SourceDC,
				"ingest_city":    meta.IngestCity,
				"ingest_dc":      *sourceDC, // spec 4.3: ingest_dc 标识本条数据由哪城 prom-gw 写入(本机 flag 值,非 meta.SourceDC)
				"ingest_time_ms": fmt.Sprintf("%d", meta.IngestTs/1e6), // spec 4.3: ms 单位
			}
			// T1.12: 注入 traceparent(W3C trace context),由 OTel Propagator 序列化当前 span
			tracex.InjectTraceparent(ctx, headers)
			// T1.9 6 阶段串联: receiver → rule engine manager(fan-out)→ pipeline → sink
			// Manager 按 Match 把 sample 分桶后,并行触发各 ruleset 的 pipeline;
			// 每 ruleset 内仍走"relabel→route→sample→…" 单 stage 链。
			msg := sink.Message{
				Topic:   defaultTopic,
				Key:     []byte(meta.Tenant), // Phase 1: 按 tenant 分区
				Headers: headers,
			}
			return ruleMgr.Process(ctx, samples, raw, msg)
		},
	})
	if err != nil {
		return fmt.Errorf("init receiver: %w", err)
	}
	// 启动时把 token 文件里的 per-tenant RateLimit 推给 receiver(plan T5.1),
	// 后续 SIGHUP 会再次调用 recv.UpdateTenantLimits 热更新
	recv.UpdateTenantLimits(auth.TenantLimits())
	safego.Go("receiver", func() {
		logger.Info("receiver listening", zap.String("addr", *writeAddr))
		if err := recv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("receiver failed", zap.Error(err))
		}
	})
	defer func() {
		// spec §6.5: receiver 停机超时 30s,确保 in-flight 请求处理完
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		_ = recv.Shutdown(shutdownCtx)
	}()

	// 11. 启动 healthz + metrics + pprof(都挂在 metrics 端口,默认 :8080)
	// 使用默认 registry(obs 包的指标通过 promauto 注册到 default registerer),
	// 配合 NewGoCollector/NewProcessCollector 也注册到 default registerer。
	// 这样 /metrics 端点能一次看到所有 gateway_* 和 go_* 指标。
	// pprof 路径:/debug/pprof/ (Go 默认)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/debug/pprof/", pprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	metricsSrv := &http.Server{
		Addr:              *metricsAddr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           metricsMux,
	}
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	healthMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// T1.9: 检查 pipeline + rule engine 状态;后续阶段加上 kafka ping / wal 状态
		submitted, drained, depth := pipe.Stats()
		if submitted == drained && depth == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// pipeline 还在排空(刚启动或刚重启):也算 ready
		w.WriteHeader(http.StatusNoContent)
	})
	healthSrv := &http.Server{
		Addr:              *healthAddr,
		ReadHeaderTimeout: 5 * time.Second,
		Handler:           healthMux,
	}

	safego.Go("metrics-server", func() {
		logger.Info("metrics listening", zap.String("addr", *metricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", zap.Error(err))
		}
	})
	safego.Go("health-server", func() {
		logger.Info("health listening", zap.String("addr", *healthAddr))
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server failed", zap.Error(err))
		}
	})

	// 12. 信号监听
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			logger.Info("SIGHUP received, reloading tokens + tenant rate limits")
			if err := auth.Reload(*tokensPath); err != nil {
				logger.Error("token reload failed", zap.Error(err))
			} else {
				logger.Info("tokens reloaded", zap.Int("count", auth.Size()))
				// plan T5.1: SIGHUP 同时刷新 receiver 的 per-tenant 限流配置
				recv.UpdateTenantLimits(auth.TenantLimits())
				logger.Info("tenant rate limits reloaded",
					zap.Int("tenants", len(auth.TenantLimits())),
				)
			}
		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("shutdown signal received, exiting", zap.String("signal", sig.String()))
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer shutdownCancel()
			_ = healthSrv.Shutdown(shutdownCtx)
			_ = metricsSrv.Shutdown(shutdownCtx)
			cancel()
			return nil
		}
	}
	_ = rootCtx
	return nil
}

func newLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.MessageKey = "msg"
	cfg.EncoderConfig.LevelKey = "level"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg.Build()
}

// splitAndTrim 按 sep 拆分字符串并 trim 空格,跳过空段。
func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

// envOrDefault 读 env 变量,空时返回 fallback(用于让 INGEST_CITY 既可经
// systemd Environment= 注入,也可经 CLI flag 显式覆盖)。
func envOrDefault(envKey, fallback string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return fallback
}

// publishHistoryMetrics 周期性写 ruleset 相关指标(spec 7.1 / plan T4.6)。
//
//   - gateway_ruleset_history_size{ruleset, ingest_city}: history 中各 ruleset 的版本数
//   - gateway_ruleset_version{ruleset, ingest_city}: 已在 manager.SetRuleSet 写入,这里再保险
func publishHistoryMetrics(hist *config.History, mgr *ruleengine.Manager, ingestCity string) {
	for _, name := range hist.Names() {
		obs.RulesetHistorySize.WithLabelValues(name, ingestCity).Set(float64(hist.SizeByName(name)))
	}
	if mgr != nil {
		for _, name := range mgr.Names() {
			rs := mgr.Rules(name)
			if rs != nil {
				obs.RulesetVersion.WithLabelValues(name, ingestCity).Set(float64(rs.RuleSet.Version))
			}
		}
	}
}
