// Package router 单测:覆盖 fan-out / match / no-match / 多 ruleset / 错误聚合 / 热更新。
package router

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lynnyq/prom-gw/internal/parser"
	"github.com/lynnyq/prom-gw/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// --- mocks ---

// fixedMatcher 固定 prefix / exact 命中。
type fixedMatcher struct {
	prefix string
	exact  string
}

func (f *fixedMatcher) Matches(s parser.Sample) bool {
	if f.exact != "" {
		return s.Metric == f.exact
	}
	if f.prefix != "" {
		return len(s.Metric) >= len(f.prefix) && s.Metric[:len(f.prefix)] == f.prefix
	}
	return true // default
}

// captureFunc 记录每次 Process 的入参样本数 + topic。
type captureFunc struct {
	mu     sync.Mutex
	calls  [][]parser.Sample
	topics []string
	err    error
	count  atomic.Int32
}

func (c *captureFunc) Process(_ context.Context, samples []parser.Sample, _ []byte, msg sink.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make([]parser.Sample, len(samples))
	copy(cp, samples)
	c.calls = append(c.calls, cp)
	c.topics = append(c.topics, msg.Topic)
	c.count.Add(1)
	return c.err
}

func (c *captureFunc) Calls() int { return int(c.count.Load()) }

func (c *captureFunc) SampleCount(idx int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx >= len(c.calls) {
		return -1
	}
	return len(c.calls[idx])
}

// --- helpers ---

func mkSample(metric string) parser.Sample {
	return parser.Sample{Metric: metric, Labels: []parser.Label{{Name: "job", Value: "x"}}}
}

// --- Validate ---

func TestValidate_Empty(t *testing.T) {
	err := Validate(nil)
	require.Error(t, err)

	err = Validate([]Entry{})
	require.Error(t, err)
}

func TestValidate_NilProcess(t *testing.T) {
	err := Validate([]Entry{{Name: "x", Match: &fixedMatcher{}}})
	require.Error(t, err)
}

func TestValidate_DefaultNotLast(t *testing.T) {
	err := Validate([]Entry{
		{Name: "d", Match: nil, Process: func(_ context.Context, _ []parser.Sample, _ []byte, _ sink.Message) error { return nil }},
		{Name: "x", Match: &fixedMatcher{prefix: "x"}, Process: func(_ context.Context, _ []parser.Sample, _ []byte, _ sink.Message) error { return nil }},
	})
	require.Error(t, err, "default 必须放在最后")
}

func TestValidate_OK(t *testing.T) {
	pf := func(_ context.Context, _ []parser.Sample, _ []byte, _ sink.Message) error { return nil }
	err := Validate([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a"}, Process: pf},
		{Name: "d", Match: nil, Process: pf},
	})
	require.NoError(t, err)
}

// --- Process:基本 fan-out ---

func TestProcess_SingleRulesetAllMatched(t *testing.T) {
	cap := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
	}))
	samples := []parser.Sample{mkSample("a"), mkSample("b"), mkSample("c")}
	err := r.Process(context.Background(), samples, []byte("raw"), sink.Message{Topic: "t"})
	require.NoError(t, err)
	assert.Equal(t, 1, cap.Calls())
	assert.Equal(t, 3, cap.SampleCount(0))
}

func TestProcess_NoEntries(t *testing.T) {
	r := New(zap.NewNop())
	err := r.Process(context.Background(), []parser.Sample{mkSample("a")}, nil, sink.Message{Topic: "t"})
	require.Error(t, err, "空 entries 必须报错")
}

func TestProcess_DefaultOnlyNoMatch(t *testing.T) {
	// 只有 default,任何 sample 都应落到 default
	cap := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
	}))
	samples := []parser.Sample{mkSample("kube_pod_info"), mkSample("http_requests")}
	require.NoError(t, r.Process(context.Background(), samples, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 1, cap.Calls())
	assert.Equal(t, 2, cap.SampleCount(0))
}

// --- Process:多 ruleset + Match prefix 路由 ---

func TestProcess_MultiRulesetPrefix(t *testing.T) {
	capA := &captureFunc{}
	capB := &captureFunc{}
	capD := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: capA.Process},
		{Name: "b", Match: &fixedMatcher{prefix: "b_"}, Process: capB.Process},
		{Name: "d", Match: nil, Process: capD.Process},
	}))
	samples := []parser.Sample{
		mkSample("a_x"),
		mkSample("a_y"),
		mkSample("b_z"),
		mkSample("c_"),
	}
	require.NoError(t, r.Process(context.Background(), samples, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 1, capA.Calls())
	assert.Equal(t, 2, capA.SampleCount(0))
	assert.Equal(t, 1, capB.Calls())
	assert.Equal(t, 1, capB.SampleCount(0))
	assert.Equal(t, 1, capD.Calls())
	assert.Equal(t, 1, capD.SampleCount(0))
}

func TestProcess_MultiRulesetExact(t *testing.T) {
	capE := &captureFunc{}
	capD := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "e", Match: &fixedMatcher{exact: "exact_metric"}, Process: capE.Process},
		{Name: "d", Match: nil, Process: capD.Process},
	}))
	samples := []parser.Sample{
		mkSample("exact_metric"),
		mkSample("other"),
	}
	require.NoError(t, r.Process(context.Background(), samples, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 1, capE.Calls())
	assert.Equal(t, 1, capE.SampleCount(0))
	assert.Equal(t, 1, capD.Calls())
	assert.Equal(t, 1, capD.SampleCount(0))
}

// --- Process:错误聚合 ---

func TestProcess_ErrorAggregates(t *testing.T) {
	cap := &captureFunc{err: errors.New("downstream failed")}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
	}))
	err := r.Process(context.Background(), []parser.Sample{mkSample("a")}, nil, sink.Message{Topic: "t"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "downstream failed")
}

func TestProcess_FirstErrorStopsRemaining(t *testing.T) {
	// 第一个 ruleset 出错 → 第二个 ruleset 不应被调用
	cap1 := &captureFunc{err: errors.New("boom")}
	cap2 := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: cap1.Process},
		{Name: "b", Match: &fixedMatcher{prefix: "b_"}, Process: cap2.Process},
		{Name: "d", Match: nil, Process: func(_ context.Context, _ []parser.Sample, _ []byte, _ sink.Message) error { return nil }},
	}))
	samples := []parser.Sample{
		mkSample("a_x"),
		mkSample("b_z"),
		mkSample("c"),
	}
	err := r.Process(context.Background(), samples, nil, sink.Message{Topic: "t"})
	require.Error(t, err)
	assert.Equal(t, 1, cap1.Calls(), "a 应被调用")
	assert.Equal(t, 0, cap2.Calls(), "b 不应被调用(已 error)")
}

// --- Process:无 default 也没命中 ---

func TestProcess_NoMatchNoDefault(t *testing.T) {
	// 没有 default 兜底,某 sample 命中不到 → drop 计数
	capA := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: capA.Process},
		// 无 default
	}))
	samples := []parser.Sample{
		mkSample("a_x"),
		mkSample("b_x"), // 不命中,且无 default → drop
	}
	err := r.Process(context.Background(), samples, nil, sink.Message{Topic: "t"})
	require.NoError(t, err, "无 default 也没命中不应整体失败")
	assert.Equal(t, 1, capA.Calls())
	assert.Equal(t, 1, capA.SampleCount(0), "只有 a_x 进了 a 桶")
}

// --- SetEntries:热更新 ---

func TestSetEntries_HotSwap(t *testing.T) {
	cap1 := &captureFunc{}
	cap2 := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: cap1.Process},
		{Name: "d", Match: nil, Process: cap1.Process},
	}))
	// 热切换:把 a 桶换到 cap2(模拟 pipeline 重建)
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: cap2.Process},
		{Name: "d", Match: nil, Process: cap1.Process},
	}))
	// a_x 命中 a 桶,default 桶空;cap2 应被调,cap1 不应被调
	require.NoError(t, r.Process(context.Background(), []parser.Sample{mkSample("a_x")}, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 0, cap1.Calls(), "default 桶空,cap1 不应被调")
	assert.Equal(t, 1, cap2.Calls(), "a 桶新 Process 应接收")

	// 切换不命中的 sample,验证 default 桶仍走 cap1
	require.NoError(t, r.Process(context.Background(), []parser.Sample{mkSample("b_y")}, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 1, cap1.Calls(), "b_y 不命中 a,应走 default(cap1)")
	assert.Equal(t, 1, cap2.Calls(), "cap2 不应被第二次调用")
}

// --- SetEntries:并发安全 ---

func TestSetEntries_ConcurrentSafe(t *testing.T) {
	cap := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
	}))
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 8 个 writer 反复切 entries
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = r.SetEntries([]Entry{{Name: "default", Match: nil, Process: cap.Process}})
			}
		}()
	}
	// 4 个 reader 反复调 Process,同样响应 stop,避免与 writer 互相阻塞
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = r.Process(context.Background(), []parser.Sample{mkSample("a")}, nil, sink.Message{Topic: "t"})
			}
		}()
	}
	// 跑 200ms 后通过 close(stop) 通知所有 goroutine 退出
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	assert.Greater(t, cap.Calls(), 0, "reader 应至少跑过一次")
}

// --- Entries:快照 ---

func TestEntries_Snapshot(t *testing.T) {
	pf := func(_ context.Context, _ []parser.Sample, _ []byte, _ sink.Message) error { return nil }
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "a", Match: &fixedMatcher{prefix: "a_"}, Process: pf},
		{Name: "b", Match: &fixedMatcher{prefix: "b_"}, Process: pf},
		{Name: "d", Match: nil, Process: pf},
	}))
	got := r.Entries()
	require.Len(t, got, 3)
	// 顺序: a, b, d(default 在最后)
	names := []string{got[0].Name, got[1].Name, got[2].Name}
	sort.Strings(names) // 与具体顺序无关,只验证都存在
	assert.Equal(t, []string{"a", "b", "d"}, names)
}

// --- SetEntries:验证失败时,旧 entries 继续生效 ---

func TestSetEntries_RejectsInvalid(t *testing.T) {
	cap := &captureFunc{}
	r := New(zap.NewNop())
	require.NoError(t, r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
	}))
	// 非法:default 不在最后
	err := r.SetEntries([]Entry{
		{Name: "default", Match: nil, Process: cap.Process},
		{Name: "x", Match: &fixedMatcher{prefix: "x"}, Process: cap.Process},
	})
	require.Error(t, err, "非法 entries 必须拒绝")
	// 旧 entries 仍生效
	require.NoError(t, r.Process(context.Background(), []parser.Sample{mkSample("a")}, nil, sink.Message{Topic: "t"}))
	assert.Equal(t, 1, cap.Calls())
}
