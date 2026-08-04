// Package config - source_test.go: Source / Manager 单元测试。
//
// 测试范围:
//   - DefaultSource:总是 valid
//   - NacosSource:Get + Watch(用 mock client)
//   - FileSource:Get + Watch(用临时文件 + fsnotify)
//   - Manager:优先级 / 启动 fallback / 变更通知
package config

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mock NacosClient ---

type mockNacos struct {
	mu      sync.Mutex
	content string
	err     error
	push    chan NacosChange
	closed  bool
}

func newMockNacos(initial string) *mockNacos {
	return &mockNacos{
		content: initial,
		push:    make(chan NacosChange, 4),
	}
}

func (m *mockNacos) GetConfig(_ context.Context, _, _ string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	return m.content, nil
}

func (m *mockNacos) ListenConfig(_ context.Context, _, _ string) <-chan NacosChange {
	return m.push
}

func (m *mockNacos) PublishConfig(_ context.Context, _, _, content string) error {
	m.mu.Lock()
	m.content = content
	m.mu.Unlock()
	return nil
}

func (m *mockNacos) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	close(m.push)
	return nil
}

func (m *mockNacos) pushChange(c NacosChange) {
	defer func() { recover() }()
	m.push <- c
}

// --- DefaultSource ---

func TestDefaultSource_AlwaysValid(t *testing.T) {
	s := NewDefaultSource()
	snap := s.Get(context.Background())
	assert.True(t, snap.Valid())
	assert.Equal(t, "default", snap.Source)
}

// --- NacosSource ---

func TestNacosSource_Get_Success(t *testing.T) {
	mock := newMockNacos("rulesets: []\n")
	s, err := NewNacosSource(mock, "prom-gw-rules", "GATEWAY", zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	snap := s.Get(context.Background())
	assert.True(t, snap.Valid())
	assert.Equal(t, "nacos", snap.Source)
}

func TestNacosSource_Get_Failure(t *testing.T) {
	mock := newMockNacos("")
	mock.err = errors.New("nacos down")
	s, err := NewNacosSource(mock, "x", "G", zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	snap := s.Get(context.Background())
	assert.False(t, snap.Valid())
	assert.Error(t, snap.Err)
}

func TestNacosSource_Watch_PushesNewSnapshot(t *testing.T) {
	mock := newMockNacos("")
	s, err := NewNacosSource(mock, "x", "G", zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx)
	mock.pushChange(NacosChange{DataID: "x", Group: "G", Content: "rulesets: []\n"})

	select {
	case snap := <-ch:
		assert.True(t, snap.Valid())
		assert.Equal(t, "nacos", snap.Source)
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive push")
	}
}

// --- FileSource ---

func TestFileSource_Get(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rulesets: []\n"), 0o600))

	s, err := NewFileSource(path, zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	snap := s.Get(context.Background())
	assert.True(t, snap.Valid())
	assert.Equal(t, "file", snap.Source)
}

func TestFileSource_Watch_DetectsChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rulesets: []\n"), 0o600))

	s, err := NewFileSource(path, zap.NewNop())
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := s.Watch(ctx)

	// 改文件
	require.NoError(t, os.WriteFile(path, []byte("rulesets: []\n# changed\n"), 0o600))

	select {
	case snap := <-ch:
		assert.True(t, snap.Valid())
		assert.Contains(t, string(snap.RawYAML), "changed")
	case <-time.After(3 * time.Second):
		t.Fatal("file source did not push on change")
	}
}

// --- Manager ---

func TestManager_Start_NacosPriority(t *testing.T) {
	nacos := newMockNacos("rulesets: []\n")

	ns, err := NewNacosSource(nacos, "x", "G", zap.NewNop())
	require.NoError(t, err)
	defer ns.Close()

	m := NewManager(ManagerConfig{Logger: zap.NewNop(), History: NewHistory(HistoryConfig{})})
	m.AddSource(ns)
	m.AddSource(NewDefaultSource())

	var onChangeCount int32
	m.SetOnChange(func(_ Snapshot) { atomic.AddInt32(&onChangeCount, 1) })

	require.NoError(t, m.Start(context.Background()))
	assert.Equal(t, "nacos", m.Current().Source)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&onChangeCount), int32(1))
}

func TestManager_Start_NacosFails_FallbackToDefault(t *testing.T) {
	nacos := newMockNacos("")
	nacos.err = errors.New("nacos down")

	ns, err := NewNacosSource(nacos, "x", "G", zap.NewNop())
	require.NoError(t, err)
	defer ns.Close()

	m := NewManager(ManagerConfig{Logger: zap.NewNop(), History: NewHistory(HistoryConfig{})})
	m.AddSource(ns)
	m.AddSource(NewDefaultSource())

	require.NoError(t, m.Start(context.Background()))
	assert.Equal(t, "default", m.Current().Source)
}

func TestManager_Start_NoNacos_UseFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	require.NoError(t, os.WriteFile(path, []byte("rulesets: []\n"), 0o600))

	fs, err := NewFileSource(path, zap.NewNop())
	require.NoError(t, err)
	defer fs.Close()

	m := NewManager(ManagerConfig{Logger: zap.NewNop(), History: NewHistory(HistoryConfig{})})
	m.AddSource(fs)
	m.AddSource(NewDefaultSource())

	require.NoError(t, m.Start(context.Background()))
	assert.Equal(t, "file", m.Current().Source)
}

func TestManager_ApplySnapshot_CompileAndStore(t *testing.T) {
	m := NewManager(ManagerConfig{Logger: zap.NewNop(), History: NewHistory(HistoryConfig{Capacity: 5})})

	yaml := `
rulesets:
  - name: e2e
    default_topic: prom.e2e
    version: 7
    stages:
      - type: relabel
        config: { drop_labels: [pod] }
`
	rs, err := m.ApplySnapshot(Snapshot{RawYAML: []byte(yaml), Source: "api"})
	require.NoError(t, err)
	require.NotNil(t, rs)
	assert.Equal(t, "e2e", rs.RuleSet.Name)
	assert.Equal(t, int64(7), rs.RuleSet.Version)

	// history 应已入库
	got, err := m.history.Get("e2e", 7)
	require.NoError(t, err)
	assert.Equal(t, "api", got.Source)
}

func TestManager_ApplySnapshot_InvalidYAML(t *testing.T) {
	m := NewManager(ManagerConfig{Logger: zap.NewNop()})
	_, err := m.ApplySnapshot(Snapshot{RawYAML: []byte("rulesets: : not yaml"), Source: "api"})
	assert.Error(t, err)
}

func TestManager_ApplySnapshot_InvalidRules(t *testing.T) {
	m := NewManager(ManagerConfig{Logger: zap.NewNop()})
	yaml := `
rulesets:
  - name: bad
    version: 1
    stages:
      - type: nope
        config: {}
`
	_, err := m.ApplySnapshot(Snapshot{RawYAML: []byte(yaml), Source: "api"})
	assert.Error(t, err)
}
