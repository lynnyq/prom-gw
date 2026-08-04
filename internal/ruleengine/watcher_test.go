// ruleengine/watcher 单测:覆盖 plan T2.10 本地文件热更新行为。
//
// 覆盖点:
//   - 启动时 warm-up load(文件不存在/非法 → 返回 error)
//   - 正常启动 → Pipeline 立即拿到 v1 ruleset
//   - 修改文件 → debounce 触发 reload → Pipeline 拿到新 version
//   - 写入非法文件 → Pipeline 保留旧版本,不切换
//   - 写回合法文件 → Pipeline 重新切换
//   - Close 幂等 + 释放 fs
package ruleengine

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWatcher_StartupLoadsImmediately(t *testing.T) {
	// 写一个最小可用 yaml,warm-up 必须成功
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t
    version: 1
`)

	p := NewPipeline(zap.NewNop(), nil)
	w, err := NewWatcher(WatcherConfig{
		Path:  path,
		Apply: func(rs *CompiledRuleSet) error { p.SetRules(rs); return nil },
	}, zap.NewNop())
	require.NoError(t, err)
	defer w.Close()

	// 立即生效
	assert.Equal(t, "app", p.Rules().RuleSet.Name)
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version)
}

func TestWatcher_FileNotExistFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.yaml")
	_, err := NewWatcher(WatcherConfig{Path: path}, zap.NewNop())
	assert.Error(t, err, "文件不存在应让 NewWatcher 失败")
}

func TestWatcher_InvalidYAMLAtStartupFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	mustWrite(t, path, `this is not yaml: [`)

	_, err := NewWatcher(WatcherConfig{Path: path}, zap.NewNop())
	assert.Error(t, err, "YAML 解析失败应让 NewWatcher 失败")
}

func TestWatcher_ReloadOnFileChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t1
    version: 1
`)

	p := NewPipeline(zap.NewNop(), nil)
	w, err := NewWatcher(WatcherConfig{
		Path:     path,
		Debounce: 50 * time.Millisecond,
		Apply:    func(rs *CompiledRuleSet) error { p.SetRules(rs); return nil },
	}, zap.NewNop())
	require.NoError(t, err)
	defer w.Close()
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version)

	// 写 v2(加一个 sample stage)
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t2
    version: 2
    stages:
      - type: sample
        config: { rate: 0.5 }
`)

	// 等待 debounce + fsnotify 触发(留 1s 上限避免 flake)
	require.Eventually(t, func() bool {
		return p.Rules().RuleSet.Version == 2
	}, 1*time.Second, 20*time.Millisecond, "应切换到 v2")
	assert.Equal(t, "t2", p.Rules().RuleSet.DefaultTopic)
	assert.Equal(t, 1, len(p.Rules().Stages))
}

func TestWatcher_InvalidReloadKeepsOldVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t1
    version: 1
`)

	p := NewPipeline(zap.NewNop(), nil)
	w, err := NewWatcher(WatcherConfig{
		Path:     path,
		Debounce: 50 * time.Millisecond,
		Apply:    func(rs *CompiledRuleSet) error { p.SetRules(rs); return nil },
	}, zap.NewNop())
	require.NoError(t, err)
	defer w.Close()
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version)

	// 写一个非法 yaml(语法错)
	mustWrite(t, path, `rulesets: [`)
	time.Sleep(300 * time.Millisecond)

	// 旧版本必须保留
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version, "非法 yaml 不应切换")
	assert.Equal(t, "t1", p.Rules().RuleSet.DefaultTopic)

	// 写回合法 v2
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t2
    version: 2
`)
	require.Eventually(t, func() bool {
		return p.Rules().RuleSet.Version == 2
	}, 1*time.Second, 20*time.Millisecond, "合法 v2 应最终切换")
}

func TestWatcher_ApplyErrorKeepsOldVersion(t *testing.T) {
	// apply 回调返回 error 时,旧版本必须保留
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t1
    version: 1
`)

	p := NewPipeline(zap.NewNop(), nil)
	var applyErr atomic.Bool // 跨 goroutine 读写,必须 atomic
	var w *Watcher
	w, err := NewWatcher(WatcherConfig{
		Path:     path,
		Debounce: 50 * time.Millisecond,
		Apply: func(rs *CompiledRuleSet) error {
			if applyErr.Load() {
				return os.ErrInvalid
			}
			p.SetRules(rs)
			return nil
		},
	}, zap.NewNop())
	require.NoError(t, err)
	defer w.Close()
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version)

	// 让 apply 失败
	applyErr.Store(true)
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t2
    version: 2
`)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, int64(1), p.Rules().RuleSet.Version, "apply 失败时旧版本必须保留")

	// 恢复 apply
	applyErr.Store(false)
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t3
    version: 3
`)
	require.Eventually(t, func() bool {
		return p.Rules().RuleSet.Version == 3
	}, 1*time.Second, 20*time.Millisecond, "apply 恢复后应能切换")
}

func TestWatcher_CloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	mustWrite(t, path, `
rulesets:
  - name: app
    default_topic: t
    version: 1
`)

	w, err := NewWatcher(WatcherConfig{Path: path}, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, w.Close(), "Close 必须幂等")
}

func TestWatcher_ExplicitRulesetName(t *testing.T) {
	// 同一文件含多个 ruleset 时,显式指定要哪个
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.yaml")
	mustWrite(t, path, `
rulesets:
  - name: a
    default_topic: ta
    version: 1
  - name: b
    default_topic: tb
    version: 1
`)

	p := NewPipeline(zap.NewNop(), nil)
	w, err := NewWatcher(WatcherConfig{
		Path:        path,
		RulesetName: "b",
		Apply:       func(rs *CompiledRuleSet) error { p.SetRules(rs); return nil },
	}, zap.NewNop())
	require.NoError(t, err)
	defer w.Close()

	assert.Equal(t, "b", p.Rules().RuleSet.Name, "应选 b")
	assert.Equal(t, "tb", p.Rules().RuleSet.DefaultTopic)
}

// mustWrite 写入文件,失败立即 t.Fatal。
func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// 防止 context / atomic 未被使用的告警
var (
	_ = context.TODO
	_ = atomic.LoadInt32
)
