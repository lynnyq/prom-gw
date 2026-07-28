// prom-gw: 多机房 Prometheus RemoteWrite 协议网关。
// 启动顺序: flag -> logger -> safego -> obs -> config -> healthz -> signals。
// 详细设计见 docs/superpowers/specs/2026-07-28-prometheus-multidc-remotewrite-gateway-design.md。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lynnyq/bigdata/pkg/safego"
	"github.com/prometheus/client_golang/prometheus"
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
		cfgPath     = flag.String("config", "configs/rules/default.yaml", "ruleset 配置文件路径")
		tokensPath  = flag.String("tokens", "configs/tokens/local.yaml", "token 配置文件路径")
		metricsAddr = flag.String("metrics-addr", ":8080", "prometheus self-export 监听地址")
		healthAddr  = flag.String("health-addr", ":8081", "healthz/readyz 监听地址")
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

	logger.Info("starting prom-gw",
		zap.String("version", version),
		zap.String("config", *cfgPath),
		zap.String("tokens", *tokensPath),
	)

	// 3. safego panic 钩子(全局 panic 计数)
	safego.Go("logger-keepalive", func() {
		// 占位 goroutine,演示 safego 用法;后续 phase 在此启动 kafka flusher / config watcher
	})

	// 4. 启动 healthz + metrics
	// rootCtx 在 Phase 1 会传给 receiver / kafkasink / wal 协程;
	// 当前 Phase 0 暂未启动业务 goroutine,先用占位。
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// prom registry: Phase 1 起 obs 模块会统一注册业务指标
	promReg := prometheus.NewRegistry()
	promReg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{}))
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
		// Phase 1 末实现: 检查 Kafka 可达 + WAL 状态 + ConfigManager 健康
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
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

	// 5. 信号监听
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			logger.Info("SIGHUP received, reload (TODO: 后续 phase 实现)")
			// 预留: configs.ReloadAll() -> exitReload
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
