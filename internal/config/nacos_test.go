package config

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeConfigClient 测试用 mock(实现 IConfigClient 接口)。
//
// 真实 nacos-sdk-go 在 test/integration/nacos_test.go 集成测试中验证;
// 这里只验证 adapter 行为,不依赖真实 Nacos。
type fakeConfigClient struct {
	mu         sync.Mutex
	getContent string
	getErr     error
	pubOK      bool
	pubErr     error
	// listenFns key: dataID/group → OnChange(签名与 SDK 一致:ns, group, dataId, data)
	listenFns map[string]func(namespace, group, dataID, data string)
}

func newFakeConfigClient() *fakeConfigClient {
	return &fakeConfigClient{
		listenFns: make(map[string]func(string, string, string, string)),
	}
}

func (f *fakeConfigClient) GetConfig(_ vo.ConfigParam) (string, error) {
	return f.getContent, f.getErr
}
func (f *fakeConfigClient) PublishConfig(_ vo.ConfigParam) (bool, error) {
	return f.pubOK, f.pubErr
}
func (f *fakeConfigClient) ListenConfig(p vo.ConfigParam) error {
	f.mu.Lock()
	f.listenFns[p.DataId+"/"+p.Group] = p.OnChange
	f.mu.Unlock()
	return nil
}
func (f *fakeConfigClient) CancelListenConfig(_ vo.ConfigParam) error { return nil }

// push 模拟 SDK 推送(测试用,直接调 OnChange)。
func (f *fakeConfigClient) push(dataID, group, content string) {
	f.mu.Lock()
	fn := f.listenFns[dataID+"/"+group]
	f.mu.Unlock()
	if fn != nil {
		fn("", group, dataID, content)
	}
}

// TestNacosClientAdapter_GetPublish 验证 SDK 包装的 GetConfig/PublishConfig。
func TestNacosClientAdapter_GetPublish(t *testing.T) {
	fake := newFakeConfigClient()
	fake.getContent = "v1"
	fake.pubOK = true

	a := &NacosClientAdapter{
		inner:   fake,
		cancels: make(map[string]context.CancelFunc),
	}

	got, err := a.GetConfig(context.Background(), "prom-gw-rules", "GATEWAY")
	require.NoError(t, err)
	assert.Equal(t, "v1", got)

	require.NoError(t, a.PublishConfig(context.Background(), "prom-gw-rules", "GATEWAY", "v2"))
}

// TestNacosClientAdapter_ListenChannel 验证 ListenConfig 把 SDK OnChange 转 channel。
func TestNacosClientAdapter_ListenChannel(t *testing.T) {
	fake := newFakeConfigClient()
	fake.getContent = "warmup-content"

	a := &NacosClientAdapter{
		inner:   fake,
		cancels: make(map[string]context.CancelFunc),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := a.ListenConfig(ctx, "prom-gw-rules", "GATEWAY")

	// 1) warm-up
	select {
	case ev := <-ch:
		assert.NoError(t, ev.Err)
		assert.Equal(t, "warmup-content", ev.Content)
		assert.Equal(t, "prom-gw-rules", ev.DataID)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("warmup not received")
	}

	// 2) 模拟 SDK 推送
	fake.push("prom-gw-rules", "GATEWAY", "v2")
	select {
	case ev := <-ch:
		assert.NoError(t, ev.Err)
		assert.Equal(t, "v2", ev.Content)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("change not received")
	}

	// 3) ctx 取消后,SDK 不应再投递新事件(真实 SDK 通过 CancelListenConfig
	//    注销;fake mock 直接调 OnChange 不受该信号控制,故此处只能验证
	//    adapter 的 CancelListenConfig 调用 + cancels 表清空,不直接断言 channel 行为)。
	cancel()
	// 等待 cleanup goroutine 完成
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		_, ok := a.cancels["prom-gw-rules/GATEWAY"]
		return !ok
	}, time.Second, 10*time.Millisecond, "cancels 表应在 ctx 取消后被清理")
}

// TestNacosClientAdapter_ListenErrorFallback warm-up 失败时,channel 立即收到 Err 事件。
func TestNacosClientAdapter_ListenErrorFallback(t *testing.T) {
	fake := newFakeConfigClient()
	fake.getErr = fmt.Errorf("nacos down")

	a := &NacosClientAdapter{
		inner:   fake,
		cancels: make(map[string]context.CancelFunc),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := a.ListenConfig(ctx, "prom-gw-rules", "GATEWAY")

	select {
	case ev := <-ch:
		assert.Error(t, ev.Err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("error event not received")
	}
}

// TestNacosClientAdapter_Close 验证 Close 取消所有监听。
func TestNacosClientAdapter_Close(t *testing.T) {
	fake := newFakeConfigClient()
	fake.getContent = "v1"

	a := &NacosClientAdapter{
		inner:   fake,
		cancels: make(map[string]context.CancelFunc),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = a.ListenConfig(ctx, "d1", "g1")
	_ = a.ListenConfig(ctx, "d2", "g2")

	require.Len(t, a.cancels, 2)
	require.NoError(t, a.Close())
	assert.Empty(t, a.cancels)
}

// TestLoadSavePersistedSnapshot 验证快照持久化 + 冷启动恢复。
func TestLoadSavePersistedSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/nacos-snapshot.json"

	in := PersistedSnapshot{
		Source:    "nacos",
		DataID:    "prom-gw-rules",
		Group:     "GATEWAY",
		Content:   "rulesets: []",
		MD5:       "abc123",
		FetchedAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, SavePersistedSnapshot(path, in))

	out := LoadPersistedSnapshot(path)
	require.NotNil(t, out)
	assert.Equal(t, in.Content, out.Content)
	assert.Equal(t, in.MD5, out.MD5)
	assert.Equal(t, in.DataID, out.DataID)
	assert.False(t, out.PersistedAt.IsZero(), "PersistedAt should be set on save")
}

// TestLoadPersistedSnapshot_Missing 缺失时返回 nil(不报错)。
func TestLoadPersistedSnapshot_Missing(t *testing.T) {
	assert.Nil(t, LoadPersistedSnapshot("/does/not/exist/snapshot.json"))
	assert.Nil(t, LoadPersistedSnapshot(""))
}
