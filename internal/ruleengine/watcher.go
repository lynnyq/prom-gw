// Package ruleengine - watcher.go: 本地规则文件热更新(fsnotify)。
//
// 设计要点(plan T2.10):
//   - 监听单个 ruleset 文件或整个 rules 目录
//   - 文件变更 → 重新 LoadFile + Compile
//   - 校验失败 → 保留旧版本,记 error metric + warn 日志
//   - 校验成功 → 通过回调把新 CompiledRuleSet 推给 Pipeline
//   - 同一文件在 fsnotify 中常被多次触发(Write + Rename + Create),
//     用 debounce(默认 200ms)合并
//   - 启动时主动 Load 一次(失败致命);后续热加载失败不致命
package ruleengine

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/lynnyq/prom-gw/internal/obs"
	"go.uber.org/zap"
)

// Watcher 监听一个 ruleset 文件并在变更时触发回调。
//
// 用法:
//
//	w, err := NewWatcher(WatcherConfig{Path: "/etc/prom-gw/app-business.yaml", Debounce: 200*time.Millisecond}, logger, func(rs *CompiledRuleSet) error {
//	    pipeline.SetRules(rs)
//	    return nil
//	})
//	if err != nil { log.Fatal(err) }
//	defer w.Close()
//	<-ctx.Done()
type Watcher struct {
	path    string
	dir     string // 监听目录(Path 的父目录;fsnotify 不支持单文件,只能监听父目录)
	logger  *zap.Logger
	apply   func(*CompiledRuleSet) error
	debounce time.Duration
	active  string // 当前 ruleset name(由 Path 推断为文件名,或 Config 显式指定)
	fs      *fsnotify.Watcher
	mu      sync.Mutex
	closed  bool
}

// WatcherConfig 监听配置。
type WatcherConfig struct {
	// Path 规则文件绝对或相对路径(必须)
	Path string
	// Debounce 文件变更后等待多久才触发 reload(默认 200ms)
	Debounce time.Duration
	// RulesetName 规则集名字(用于 CompileConfig 选择;
	// 留空时用文件 basename 去后缀,例如 app-business.yaml → app-business)
	RulesetName string
	// Apply 编译成功后调用,失败时回退旧版本(不调用)
	Apply func(*CompiledRuleSet) error
}

// NewWatcher 构造并启动 watcher;同时立即 Load 一次作为 warm-up。
//
// 启动失败(文件不存在/初始编译失败)→ 返回 error,调用方应 fast-fail。
// 后续运行中 reload 失败 → 记 metric + warn 日志,不返回 error。
func NewWatcher(cfg WatcherConfig, logger *zap.Logger) (*Watcher, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.Path == "" {
		return nil, errInvalidConfig("Path required")
	}
	if cfg.Apply == nil {
		cfg.Apply = func(_ *CompiledRuleSet) error { return nil }
	}
	if cfg.Debounce <= 0 {
		cfg.Debounce = 200 * time.Millisecond
	}
	if cfg.RulesetName == "" {
		base := filepath.Base(cfg.Path)
		cfg.RulesetName = trimExt(base)
	}

	w := &Watcher{
		path:     cfg.Path,
		dir:      filepath.Dir(cfg.Path),
		logger:   logger,
		apply:    cfg.Apply,
		debounce: cfg.Debounce,
		active:   cfg.RulesetName,
	}

	// 1. 立即 Load 一次(warm-up):失败 fatal
	if err := w.reload(); err != nil {
		return nil, err
	}

	// 2. 启动 fsnotify 监听父目录(单文件无法 watch,只能 watch 所在目录)
	fs, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fs.Add(w.dir); err != nil {
		_ = fs.Close()
		return nil, err
	}
	w.fs = fs

	go w.loop()
	return w, nil
}

// Close 停止监听,释放资源;幂等。
func (w *Watcher) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.fs != nil {
		return w.fs.Close()
	}
	return nil
}

// reload 主动 reload:LoadFile + CompileConfig + apply。
//
// 失败时:
//   - warm-up(NewWatcher 内):返回 error,NewWatcher 把它透传出去
//   - 热加载(loop 内):仅记 metric + warn,不返回 error(旧版本继续生效)
func (w *Watcher) reload() error {
	rawCfg, err := LoadFile(w.path)
	if err != nil {
		w.logger.Warn("watcher: reload LoadFile failed, keep old ruleset",
			zap.String("path", w.path),
			zap.Error(err),
		)
		obs.ErrorsTotal.WithLabelValues("config", "watcher_load", "", "").Inc()
		obs.ConfigReloadTotal.WithLabelValues("file", "error", "", "").Inc()
		return err
	}
	rs, err := CompileConfig(rawCfg, w.active)
	if err != nil {
		w.logger.Warn("watcher: reload CompileConfig failed, keep old ruleset",
			zap.String("path", w.path),
			zap.String("ruleset", w.active),
			zap.Error(err),
		)
		obs.ErrorsTotal.WithLabelValues("config", "watcher_compile", "", "").Inc()
		obs.ConfigReloadTotal.WithLabelValues("file", "error", "", "").Inc()
		return err
	}
	if err := w.apply(rs); err != nil {
		w.logger.Warn("watcher: apply new ruleset failed, keep old ruleset",
			zap.String("ruleset", w.active),
			zap.Int64("version", rs.RuleSet.Version),
			zap.Error(err),
		)
		obs.ErrorsTotal.WithLabelValues("config", "watcher_apply", "", "").Inc()
		obs.ConfigReloadTotal.WithLabelValues("file", "error", "", "").Inc()
		return err
	}
	obs.ConfigReloadTotal.WithLabelValues("file", "ok", "", "").Inc()
	w.logger.Info("watcher: ruleset reloaded",
		zap.String("path", w.path),
		zap.String("ruleset", w.active),
		zap.Int64("version", rs.RuleSet.Version),
		zap.Int("stage_count", len(rs.Stages)),
	)
	return nil
}

// loop 是 fsnotify 事件循环:
//   - 用 debounce 合并同文件多次事件(Write+Rename+Create)
//   - 收到事件且 debounce 已到 → 调 reload
//   - ctx 不需要,Close 关闭 fs 后 Events/Errors channel 自动关闭 → loop 退出
func (w *Watcher) loop() {
	var (
		timer     *time.Timer
		timerC    <-chan time.Time
	)
	for {
		select {
		case ev, ok := <-w.fs.Events:
			if !ok {
				return
			}
			// 只关心我们这个文件
			abs, _ := filepath.Abs(ev.Name)
			want, _ := filepath.Abs(w.path)
			if abs != want {
				continue
			}
			// 过滤不关心的事件
			if !isReloadEvent(ev.Op) {
				continue
			}
			// 启动 / 重置 debounce
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(w.debounce)
			timerC = timer.C
		case <-timerC:
			// debounce 触发 → 重新加载
			timer = nil
			timerC = nil
			if err := w.reload(); err != nil {
				// 已在 reload 内打 metric,这里不再重复
				_ = err
			}
		case err, ok := <-w.fs.Errors:
			if !ok {
				return
			}
			w.logger.Warn("watcher: fsnotify error",
				zap.String("path", w.path),
				zap.Error(err),
			)
			obs.ErrorsTotal.WithLabelValues("config", "watcher_fsnotify", "", "").Inc()
		}
	}
}

// isReloadEvent 哪些 fsnotify 操作算"需要 reload"。
//
// 经验值:Write + Create + Rename + Chmod 都可能触发文本编辑器的保存
// (vim 的 :w 实际是 RENAME+CREATE,IDE 是 WRITE+CHMOD,Rename 来自 mv 覆盖)。
// 实际产品中通常把 Write/Create/Rename/Chmod 都视为 reload 触发,
// debounce 合并后只跑一次。
func isReloadEvent(op fsnotify.Op) bool {
	return op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename|fsnotify.Chmod) != 0
}

// trimExt 去掉文件后缀(简单实现,够用;不考虑多 . 文件名)。
func trimExt(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return s[:i]
		}
	}
	return s
}

// errInvalidConfig 包装配置错误。
type errInvalid string

func (e errInvalid) Error() string { return "watcher: " + string(e) }

func errInvalidConfig(msg string) error { return errInvalid(msg) }

// 编译期断言:errInvalid 实现 error。
var _ error = errInvalid("")

// 防止 ctx 未被使用(目前 watcher 不用 ctx,但保留接口位置方便 future 改动)。
var _ = context.Background
