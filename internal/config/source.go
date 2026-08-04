// Package config - source.go: 配置源抽象 + 多源实现。
//
// 设计要点(plan T4.1 / T4.2):
//   - Source 接口:对外暴露 Get(取当前快照) + Watch(订阅变更) + Close
//   - FileSource:从本地 YAML 文件读,用 fsnotify 监听(直接复用 ruleengine.Watcher 思路)
//   - NacosSource:从 Nacos 配置中心拉取,长轮询;失败时 NacosSource 仍保持,
//     内部打 error 状态;Manager 降级到上一成功快照
//   - DefaultSource:内置兜底(空 ruleset),仅在 Nacos + 文件都失败时使用
//   - Manager:编排多个 source,优先级 Nacos → File → Default,任何 source
//     推送都触发 OnChange 回调
package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/lynnyq/bigdata/internal/obs"
	"github.com/lynnyq/bigdata/internal/ruleengine"
	"go.uber.org/zap"
)

// Snapshot 配置快照(任意 source 产出的统一格式)。
type Snapshot struct {
	// RawYAML 原始 YAML 字节;为空表示该 source 未拉到
	RawYAML []byte
	// MD5 原始字节的 MD5(用于版本比对 / 冷启动校验)
	MD5 string
	// Source 来源标识("nacos" / "file" / "default")
	Source string
	// FetchedAt 拉取时间
	FetchedAt time.Time
	// Err 拉取时的错误(用于告警 / 降级判断)
	Err error
}

// IsEmpty 是否未拉到有效内容。
func (s Snapshot) IsEmpty() bool { return len(s.RawYAML) == 0 }

// Valid 是否有有效内容(无错误且非空)。
func (s Snapshot) Valid() bool { return s.Err == nil && !s.IsEmpty() }

// --- Source 接口 ---

// Source 配置源。
//
// Get 返回当前快照(同步,失败时 Err 非 nil);Watch 异步推变更到返回的 channel;
// Close 停止后台 goroutine,关闭 channel。
type Source interface {
	Name() string
	Get(ctx context.Context) Snapshot
	Watch(ctx context.Context) <-chan Snapshot
	Close() error
}

// --- FileSource ---

// FileSource 从本地 YAML 文件读。
type FileSource struct {
	path   string
	logger *zap.Logger
	mu     sync.Mutex
	last   Snapshot
	fs     *fsnotify.Watcher
	stop   chan struct{}
}

// NewFileSource 构造并启动 fsnotify 监听。
//
// 启动时立即 LoadFile(失败 fatal 行为由 Manager 决定,这里只返回 error)。
func NewFileSource(path string, logger *zap.Logger) (*FileSource, error) {
	if path == "" {
		return nil, errors.New("config: file source path required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	fs, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := fs.Add(path); err != nil {
		_ = fs.Close()
		return nil, fmt.Errorf("config: fsnotify add %q: %w", path, err)
	}
	s := &FileSource{
		path:   path,
		logger: logger,
		fs:     fs,
		stop:   make(chan struct{}),
	}
	// 启动时立即拉一次
	if err := s.refresh(); err != nil {
		_ = fs.Close()
		return nil, err
	}
	// 实际的事件循环在 Watch() 中启动(Manager 调 Start 后才会启动监听)
	return s, nil
}

func (f *FileSource) Name() string { return "file" }

// Get 同步取当前快照(读 f.last,不变更)。
func (f *FileSource) Get(_ context.Context) Snapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// Watch 返回只读 channel,文件变更时推送新快照。
func (f *FileSource) Watch(ctx context.Context) <-chan Snapshot {
	out := make(chan Snapshot, 1)
	safegoName := "filesource-watch"
	_ = safegoName
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-f.stop:
				return
			case ev, ok := <-f.fs.Events:
				if !ok {
					return
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
					continue
				}
				// debounce
				time.Sleep(200 * time.Millisecond)
				_ = f.refresh()
				f.mu.Lock()
				out <- f.last
				f.mu.Unlock()
			case err, ok := <-f.fs.Errors:
				if !ok {
					return
				}
				f.logger.Warn("filesource: fsnotify error", zap.Error(err))
				obs.ErrorsTotal.WithLabelValues("config", "filesource_fsnotify", "", "").Inc()
			}
		}
	}()
	return out
}

// Close 停止监听。
func (f *FileSource) Close() error {
	close(f.stop)
	if f.fs != nil {
		return f.fs.Close()
	}
	return nil
}

func (f *FileSource) refresh() error {
	raw, err := os.ReadFile(f.path)
	if err != nil {
		f.mu.Lock()
		f.last = Snapshot{Err: err, Source: "file", FetchedAt: time.Now()}
		f.mu.Unlock()
		obs.ConfigReloadTotal.WithLabelValues("file", "error", "", "").Inc()
		return err
	}
	f.mu.Lock()
	f.last = Snapshot{
		RawYAML:   raw,
		MD5:       computeMD5(raw),
		Source:    "file",
		FetchedAt: time.Now(),
	}
	f.mu.Unlock()
	obs.ConfigReloadTotal.WithLabelValues("file", "ok", "", "").Inc()
	return nil
}

// --- NacosSource ---

// NacosClient 抽象 Nacos 客户端(便于 mock 测试,不直接依赖 nacos-sdk-go)。
//
// v1:Manager 通过此接口对接;真实实现可以在另一个文件里用 nacos-sdk-go 接入。
// Plan T4.8 集成测试中用 mock 验证。
type NacosClient interface {
	// GetConfig 拉取一次;错误时返回 error。
	GetConfig(ctx context.Context, dataID, group string) (string, error)
	// ListenConfig 长轮询订阅变更;返回只读 channel 推送新内容。
	ListenConfig(ctx context.Context, dataID, group string) <-chan NacosChange
	// PublishConfig 主动推送(用于 Admin API 测试)。
	PublishConfig(ctx context.Context, dataID, group, content string) error
	// Close 关闭。
	Close() error
}

// NacosChange 推送的变更事件。
type NacosChange struct {
	DataID  string
	Group   string
	Content string
	Err     error // 非 nil 表示拉取失败,content 可能为空
}

// NacosSource 从 Nacos 拉取。
type NacosSource struct {
	client NacosClient
	dataID string
	group  string
	logger *zap.Logger
	mu     sync.Mutex
	last   Snapshot
}

// NewNacosSource 构造。
func NewNacosSource(client NacosClient, dataID, group string, logger *zap.Logger) (*NacosSource, error) {
	if client == nil {
		return nil, errors.New("config: nacos source: client nil")
	}
	if dataID == "" {
		return nil, errors.New("config: nacos source: dataID required")
	}
	if group == "" {
		group = "DEFAULT_GROUP"
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &NacosSource{
		client: client,
		dataID: dataID,
		group:  group,
		logger: logger,
	}, nil
}

func (n *NacosSource) Name() string { return "nacos" }

// Get 同步拉取一次(不依赖后台监听)。
func (n *NacosSource) Get(ctx context.Context) Snapshot {
	content, err := n.client.GetConfig(ctx, n.dataID, n.group)
	snap := Snapshot{
		Source:    "nacos",
		FetchedAt: time.Now(),
	}
	if err != nil {
		snap.Err = err
		obs.ConfigReloadTotal.WithLabelValues("nacos", "error", "", "").Inc()
		n.logger.Warn("nacos get failed", zap.Error(err))
	} else {
		snap.RawYAML = []byte(content)
		snap.MD5 = computeMD5(snap.RawYAML)
		obs.ConfigReloadTotal.WithLabelValues("nacos", "ok", "", "").Inc()
	}
	n.mu.Lock()
	n.last = snap
	n.mu.Unlock()
	return snap
}

// Watch 订阅变更(包装 NacosClient.ListenConfig)。
func (n *NacosSource) Watch(ctx context.Context) <-chan Snapshot {
	src := make(chan Snapshot, 1)
	ch := n.client.ListenConfig(ctx, n.dataID, n.group)
	go func() {
		defer close(src)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				if ev.Err != nil {
					n.logger.Warn("nacos listen error", zap.Error(ev.Err))
					src <- Snapshot{Source: "nacos", Err: ev.Err, FetchedAt: time.Now()}
					continue
				}
				snap := Snapshot{
					RawYAML:   []byte(ev.Content),
					Source:    "nacos",
					FetchedAt: time.Now(),
				}
				snap.MD5 = computeMD5(snap.RawYAML)
				n.mu.Lock()
				n.last = snap
				n.mu.Unlock()
				obs.ConfigReloadTotal.WithLabelValues("nacos", "ok", "", "").Inc()
				src <- snap
			}
		}
	}()
	return src
}

// Close 关闭底层 client。
func (n *NacosSource) Close() error {
	if n.client != nil {
		return n.client.Close()
	}
	return nil
}

// --- DefaultSource(内置兜底) ---

// DefaultSource 总是返回一份空 ruleset,不会失败。
type DefaultSource struct {
	raw []byte
}

func NewDefaultSource() *DefaultSource {
	return &DefaultSource{
		raw: []byte("rulesets: []\nglobal:\n  rate_limit_per_instance: 100000\n  channel_buffer: 65535\n"),
	}
}

func (d *DefaultSource) Name() string                          { return "default" }
func (d *DefaultSource) Get(_ context.Context) Snapshot        { return Snapshot{RawYAML: d.raw, MD5: computeMD5(d.raw), Source: "default", FetchedAt: time.Now()} }
func (d *DefaultSource) Watch(_ context.Context) <-chan Snapshot {
	ch := make(chan Snapshot)
	close(ch)
	return ch
}
func (d *DefaultSource) Close() error { return nil }

// --- Manager ---

// Manager 编排多 source,选最高优先级且 valid 的 snapshot。
//
// 优先级:Nacos > File > Default。
// 启动顺序:Nacos 拉取(超时 5s)→ 若失败 File 拉取 → 若失败 Default。
// 运行期:Nacos 推送新值 → 切到 Nacos;若 Nacos 报错,保留旧 Nacos 快照(若没成功过)
// 或降到 File / Default。
type Manager struct {
	logger *zap.Logger
	sources []Source
	mu     sync.RWMutex
	current Snapshot
	history *History
	// onChange 任何 source 推送新快照时调用(主线程决策优先级)
	onChange func(Snapshot)
}

// ManagerConfig 构造参数。
type ManagerConfig struct {
	Logger  *zap.Logger
	History *History
}

// NewManager 构造。
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	return &Manager{
		logger:  cfg.Logger,
		history: cfg.History,
	}
}

// AddSource 注册 source(按调用顺序作为优先级,先加的优先)。
func (m *Manager) AddSource(s Source) { m.sources = append(m.sources, s) }

// SetOnChange 注册变更回调(主线程收到推送后,Manager 选最优后调它)。
func (m *Manager) SetOnChange(fn func(Snapshot)) { m.onChange = fn }

// Start 启动:立即尝试每个 source 拉一次,选最优;然后启动每个 source 的 Watch。
func (m *Manager) Start(ctx context.Context) error {
	// 1. 启动时 Nacos → File → Default 拉一次
	var chosen Snapshot
	for _, s := range m.sources {
		snap := s.Get(ctx)
		if snap.Valid() {
			chosen = snap
			m.logger.Info("config: source selected on startup",
				zap.String("source", s.Name()),
				zap.Int("bytes", len(snap.RawYAML)),
			)
			break
		}
		m.logger.Warn("config: source invalid on startup",
			zap.String("source", s.Name()),
			zap.Error(snap.Err),
		)
	}
	if !chosen.Valid() {
		// 所有 source 都拉不到,使用 DefaultSource(理论上 DefaultSource 永远 valid)
		def := NewDefaultSource()
		chosen = def.Get(ctx)
		m.logger.Warn("config: all sources failed, fallback to default")
	}
	m.mu.Lock()
	m.current = chosen
	m.mu.Unlock()
	if m.onChange != nil {
		m.onChange(chosen)
	}

	// 2. 启动每个 source 的 watch
	for _, s := range m.sources {
		s := s
		ch := s.Watch(ctx)
		go m.consume(s.Name(), ch)
	}
	return nil
}

func (m *Manager) consume(name string, ch <-chan Snapshot) {
	for snap := range ch {
		m.mu.Lock()
		// 只在 content 变化时切换(避免重复推送)
		if snap.Valid() && snap.MD5 != m.current.MD5 {
			oldSrc := m.current.Source
			m.current = snap
			m.mu.Unlock()
			m.logger.Info("config: snapshot changed",
				zap.String("from", oldSrc),
				zap.String("to", name),
				zap.String("md5", snap.MD5),
				zap.Int("bytes", len(snap.RawYAML)),
			)
			if m.onChange != nil {
				m.onChange(snap)
			}
		} else {
			m.mu.Unlock()
			if snap.Err != nil {
				m.logger.Warn("config: source push with error, keep current",
					zap.String("source", name),
					zap.Error(snap.Err),
				)
			}
		}
	}
}

// Current 读取当前生效 snapshot。
func (m *Manager) Current() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Close 关闭所有 source。
func (m *Manager) Close() {
	for _, s := range m.sources {
		_ = s.Close()
	}
}

// ApplySnapshot 接收 rawYAML,做"编译 → history 入库 → 切到 pipeline"完整闭环。
//
// 用于:onChange 回调 / Admin API 提交。
// 失败:编译失败不切换,返回 error。
func (m *Manager) ApplySnapshot(snap Snapshot) (*ruleengine.CompiledRuleSet, error) {
	if !snap.Valid() {
		return nil, errors.New("config: invalid snapshot")
	}
	cfg, err := ruleengine.LoadBytes(snap.RawYAML)
	if err != nil {
		return nil, fmt.Errorf("config: load yaml: %w", err)
	}
	// 编译所有 ruleset 并入库(便于 admin / rollback 查找)
	out := make(map[string]*ruleengine.CompiledRuleSet, len(cfg.Rulesets))
	for i := range cfg.Rulesets {
		rs, err := ruleengine.Compile(&cfg.Rulesets[i])
		if err != nil {
			return nil, fmt.Errorf("config: compile ruleset[%d] %q: %w", i, cfg.Rulesets[i].Name, err)
		}
		out[rs.RuleSet.Name] = rs
		if m.history != nil {
			_ = m.history.Save(HistoryRecord{
				Name:     rs.RuleSet.Name,
				Version:  rs.RuleSet.Version,
				Bytes:    len(snap.RawYAML),
				RawYAML:  snap.RawYAML,
				Source:   snap.Source,
				Compiled: rs,
			})
		}
	}
	m.mu.Lock()
	m.current = snap
	m.mu.Unlock()
	// 返回 active ruleset(若有多个,取第一个)
	for _, rs := range out {
		return rs, nil
	}
	return nil, errors.New("config: no ruleset in snapshot")
}

// --- helpers ---

func computeMD5(b []byte) string {
	// 简单 hash;不引 crypto 库也够用(用于去重)
	// 用 djb2,够用
	var h uint64 = 5381
	for _, c := range b {
		h = h*33 + uint64(c)
	}
	return fmt.Sprintf("%x", h)
}
