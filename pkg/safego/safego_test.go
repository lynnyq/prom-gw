package safego

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGo_DoesNotCrashOnPanic(t *testing.T) {
	before := Stats()
	done := make(chan struct{})
	Go("panic-test", func() {
		defer close(done)
		panic("boom")
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not return after panic")
	}

	// close(done) 是 fn() 的 defer,recordPanic 是 runSafe 外层 defer,
	// LIFO 顺序下 close 先于 recordPanic 触发,故需要轮询等待计数刷新。
	require.Eventually(t, func() bool { return Stats() >= before+1 }, time.Second, 10*time.Millisecond, "panic should be recorded")
}

func TestGoWithRecover_CallbackInvoked(t *testing.T) {
	var (
		called atomic.Bool
		mu     sync.Mutex
		gotVal any
		gotLen int
	)
	done := make(chan struct{})

	GoWithRecover("with-recover", func() {
		defer close(done)
		panic("custom-value")
	}, func(v any, stack []byte) {
		mu.Lock()
		gotVal = v
		gotLen = len(stack)
		mu.Unlock()
		called.Store(true)
	})

	<-done
	// 给回调一点时间
	require.Eventually(t, called.Load, time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, "custom-value", gotVal)
	assert.Greater(t, gotLen, 0, "stack should not be empty")
}

func TestGoWithRecover_OnPanicPanicIsSwallowed(t *testing.T) {
	// 验证 onPanic 自身 panic 不会逃逸
	done := make(chan struct{})
	GoWithRecover("nested-panic", func() {
		defer close(done)
		panic("inner")
	}, func(value any, stack []byte) {
		panic("callback-panic")
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("goroutine did not return after nested panic")
	}
}

func TestGo_NormalCompletion(t *testing.T) {
	done := make(chan struct{})
	Go("normal", func() {
		defer close(done)
		// 不 panic
	})
	<-done
}

func TestStats_Accumulates(t *testing.T) {
	before := Stats()
	const n = 5
	for i := 0; i < n; i++ {
		Go("stats-test", func() { panic("x") })
	}
	// 等待异步 goroutine 跑完
	require.Eventually(t, func() bool {
		return Stats() >= before+n
	}, time.Second, 10*time.Millisecond)
}
