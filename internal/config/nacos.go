// Package config - nacos.go: Nacos 真实客户端实现(基于 nacos-sdk-go)。
//
// Plan T4.1:启动时拉取 dataId+group,长轮询订阅变更;失败保留最后成功版本;
// 持久化 last_good_snapshot 到 /data/nacos_snapshot.json,下次启动时优先恢复。
//
// 本文件实现 source.go 中定义的 NacosClient 接口(便于 mock 测试)。
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"
)

// NacosConfig 启动参数(对应 NacosSource 需要的 NacosClient 实现)。
type NacosConfig struct {
	// Addrs Nacos 服务端列表(ip:port,无 http:// 前缀),至少一项
	Addrs []string
	// NamespaceID Nacos namespace id(空 = public)
	NamespaceID string
	// Username / Password 鉴权(空 = 匿名)
	Username string
	Password string
	// ContextPath 默认为 /nacos
	ContextPath string
	// TimeoutMs 单次请求超时,默认 5000ms
	TimeoutMs uint64
	// LogDir / CacheDir SDK 本地缓存目录
	LogDir   string
	CacheDir string
	// SnapshotPath last-good-snapshot 持久化文件路径(空 = 不持久化)
	// 启动时优先加载,运行期每次成功拉取都写一次。
	SnapshotPath string
	// ListenCacheMs 长轮询间隔,默认 10000ms
	ListenCacheMs uint64
}

// NacosClientAdapter 把 nacos-sdk-go IConfigClient 适配为本包 NacosClient 接口。
//
// 这样源码 / 测试都面向统一的 config.NacosClient(便于 mock + 替换)。
type NacosClientAdapter struct {
	inner   IConfigClient
	logger  *zap.Logger
	mu      sync.Mutex
	cancels map[string]context.CancelFunc // key: dataID/group → ctx cancel for close
	// listeners 跟踪每个 ListenConfig 启动的清理 goroutine,确保 Close
	// 在所有 listener goroutine 退出后再返回,避免 Close 后还有 goroutine
	// 写 cancels map 触发 race / use-after-close。
	listeners sync.WaitGroup
}

// IConfigClient 是 nacos-sdk-go IConfigClient 的最小子集,只暴露本包需要的方法。
//
// 真实 SDK 类型实现此接口;测试里可用 mock 实现。
type IConfigClient interface {
	GetConfig(param vo.ConfigParam) (string, error)
	PublishConfig(param vo.ConfigParam) (bool, error)
	ListenConfig(param vo.ConfigParam) error
	CancelListenConfig(param vo.ConfigParam) error
}

// NewNacosSDKClient 构造 SDK 客户端;若未配置 Addrs 返回 nil,调用方降级到 file/default。
func NewNacosSDKClient(cfg NacosConfig, logger *zap.Logger) (*NacosClientAdapter, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("config: nacos: addrs required")
	}
	if cfg.ContextPath == "" {
		cfg.ContextPath = "/nacos"
	}
	if cfg.TimeoutMs == 0 {
		cfg.TimeoutMs = 5000
	}
	if cfg.ListenCacheMs == 0 {
		cfg.ListenCacheMs = 10000
	}
	if cfg.LogDir == "" {
		cfg.LogDir = "/tmp/nacos/log"
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = "/tmp/nacos/cache"
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	// 1) server config
	sc := make([]constant.ServerConfig, 0, len(cfg.Addrs))
	for _, a := range cfg.Addrs {
		host, port, err := net.SplitHostPort(a)
		if err != nil {
			return nil, fmt.Errorf("config: nacos: bad addr %q: %w", a, err)
		}
		var p uint64 = 8848
		if _, err := fmt.Sscanf(port, "%d", &p); err != nil {
			return nil, fmt.Errorf("config: nacos: bad port %q: %w", port, err)
		}
		sc = append(sc, *constant.NewServerConfig(host, p, constant.WithContextPath(cfg.ContextPath)))
	}

	// 2) client config
	clientCfg := *constant.NewClientConfig(
		constant.WithNamespaceId(cfg.NamespaceID),
		constant.WithTimeoutMs(cfg.TimeoutMs),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir(cfg.LogDir),
		constant.WithCacheDir(cfg.CacheDir),
		constant.WithLogLevel("warn"),
	)
	if cfg.Username != "" {
		clientCfg.Username = cfg.Username
		clientCfg.Password = cfg.Password
	}

	// 3) SDK client
	c, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &clientCfg,
		ServerConfigs: sc,
	})
	if err != nil {
		return nil, fmt.Errorf("config: nacos: create client: %w", err)
	}

	return &NacosClientAdapter{
		inner:   c,
		logger:  logger,
		cancels: make(map[string]context.CancelFunc),
	}, nil
}

// recordListener 在启动新清理 goroutine 之前调用,Close 等待所有清理 goroutine 退出。
func (a *NacosClientAdapter) recordListener() {
	a.listeners.Add(1)
}

// doneListener 与 recordListener 配对,在清理 goroutine 退出前调用。
func (a *NacosClientAdapter) doneListener() {
	a.listeners.Done()
}

// GetConfig 拉取一次(对应 config.NacosClient.GetConfig)。
func (a *NacosClientAdapter) GetConfig(_ context.Context, dataID, group string) (string, error) {
	return a.inner.GetConfig(vo.ConfigParam{DataId: dataID, Group: group})
}

// PublishConfig 主动推送(对应 config.NacosClient.PublishConfig)。
func (a *NacosClientAdapter) PublishConfig(_ context.Context, dataID, group, content string) error {
	ok, err := a.inner.PublishConfig(vo.ConfigParam{
		DataId: dataID, Group: group, Content: content,
	})
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("config: nacos: publish returned false")
	}
	return nil
}

// ListenConfig 把 SDK OnChange callback 包装成 channel(对应 config.NacosClient.ListenConfig)。
//
// 行为:
//   - 启动时立即 GetConfig 一次作为 warm-up
//   - 之后 SDK 每次推送,送一次 NacosChange
//   - ctx 取消时,CancelListenConfig + close channel
func (a *NacosClientAdapter) ListenConfig(ctx context.Context, dataID, group string) <-chan NacosChange {
	out := make(chan NacosChange, 4)
	listenCtx, cancel := context.WithCancel(ctx)

	key := dataID + "/" + group
	a.mu.Lock()
	a.cancels[key] = cancel
	a.mu.Unlock()

	// 1) warm-up
	a.recordListener()
	go func() {
		defer a.doneListener()
		content, err := a.inner.GetConfig(vo.ConfigParam{DataId: dataID, Group: group})
		ev := NacosChange{DataID: dataID, Group: group, Content: content, Err: err}
		if err != nil && a.logger != nil {
			a.logger.Warn("nacos: warmup get failed",
				zap.String("dataId", dataID), zap.Error(err))
		}
		select {
		case out <- ev:
		case <-listenCtx.Done():
		}
	}()

	// 2) 注册 SDK 长轮询
	if err := a.inner.ListenConfig(vo.ConfigParam{
		DataId: dataID,
		Group:  group,
		OnChange: func(_, g, d, data string) {
			ev := NacosChange{DataID: d, Group: g, Content: data}
			select {
			case out <- ev:
			case <-listenCtx.Done():
			}
		},
	}); err != nil {
		if a.logger != nil {
			a.logger.Warn("nacos: listen config failed",
				zap.String("dataId", dataID), zap.Error(err))
		}
		// 立刻 push 一个 error 事件,让上层知道监听失败
		go func() {
			select {
			case out <- NacosChange{DataID: dataID, Group: group, Err: err}:
			case <-listenCtx.Done():
			}
		}()
	}

	// 3) ctx 取消时取消监听 + 从 cancels 表删除
	//
	// 注意:不 close(out),避免 SDK OnChange callback 或 warmup goroutine 还在
	// 持有 out 引用时发生 send-on-closed  panic;调用方用 ctx 取消表达"我不再读了",
	// 写侧 goroutine 在 select 命中 <-listenCtx.Done() 时自然退出,out 引用归零后被 GC。
	a.recordListener()
	go func() {
		defer a.doneListener()
		<-listenCtx.Done()
		_ = a.inner.CancelListenConfig(vo.ConfigParam{DataId: dataID, Group: group})
		a.mu.Lock()
		delete(a.cancels, key)
		a.mu.Unlock()
	}()
	return out
}

// Close 关闭 SDK client(SDK 无显式 Close,只取消所有监听)。
//
// 先取消所有 listener 等待其清理 goroutine 退出,再返回,保证 Close 之后
// 没有 goroutine 还在写 cancels map / 关闭 out channel。
func (a *NacosClientAdapter) Close() error {
	a.mu.Lock()
	for k, cancel := range a.cancels {
		cancel()
		delete(a.cancels, k)
	}
	a.mu.Unlock()
	a.listeners.Wait()
	return nil
}

// --- last_good_snapshot 持久化(Plan T4.1 + Risks) ---

// PersistedSnapshot 冷启动优先恢复的快照。
type PersistedSnapshot struct {
	Source      string    `json:"source"`
	DataID      string    `json:"data_id"`
	Group       string    `json:"group"`
	Content     string    `json:"content"`
	MD5         string    `json:"md5"`
	FetchedAt   time.Time `json:"fetched_at"`
	PersistedAt time.Time `json:"persisted_at"`
}

// LoadPersistedSnapshot 读快照文件;不存在或读取失败返回 nil(允许降级)。
func LoadPersistedSnapshot(path string) *PersistedSnapshot {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var s PersistedSnapshot
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil
	}
	return &s
}

// SavePersistedSnapshot 写快照文件(atomic rename,失败仅 warn 不致命)。
func SavePersistedSnapshot(path string, snap PersistedSnapshot) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	snap.PersistedAt = time.Now()
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
