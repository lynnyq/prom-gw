package safego

import (
	"fmt"
	"runtime/debug"
	"sync"
)

// 统计信息
var (
	panicsRecovered uint64
	mu              sync.Mutex
)

// Stats 返回累计恢复的 panic 数量。
// 用于单元测试验证 / 上报 obs 模块。
func Stats() uint64 {
	mu.Lock()
	defer mu.Unlock()
	return panicsRecovered
}

func recordPanic() {
	mu.Lock()
	defer mu.Unlock()
	panicsRecovered++
}

// ReportPanic 手动上报一次 panic(用于 http.Handler 等非 goroutine 场景)。
//
// 调用方应先 recover() 再调用本函数;本函数会递增 safego 内部计数器,
// 使 gateway_panic_recovered_total 指标正确反映所有 panic(含 admin handler)。
// spec §6.6: 每个 goroutine(包括 admin server handler)统一通过 safego 包裹。
func ReportPanic(name string, value any, stack []byte) {
	recordPanic()
}

// Go 启动一个带 panic 恢复的 goroutine。
// 适用于 fire-and-forget 任务;fn 内部 panic 不会让进程崩溃。
//
//	name 用于日志/指标的标识,推荐简短有意义(如 "kafka-flusher")。
func Go(name string, fn func()) {
	go runSafe(name, fn, nil)
}

// GoWithRecover 与 Go 类似,但允许自定义 panic 回调(用于记录堆栈到 logger / metric)。
// onPanic 接收 panic value 与堆栈字节切片;onPanic 自身 panic 会被吞掉以防二次崩溃。
func GoWithRecover(name string, fn func(), onPanic func(value any, stack []byte)) {
	go runSafe(name, fn, onPanic)
}

func runSafe(name string, fn func(), onPanic func(any, []byte)) {
	defer func() {
		if r := recover(); r != nil {
			recordPanic()
			stack := debug.Stack()
			if onPanic != nil {
				// 防止 onPanic 自身 panic
				func() {
					defer func() {
						if r2 := recover(); r2 != nil {
							fmt.Printf("[safego] onPanic callback itself panicked for %s: %v\n", name, r2)
						}
					}()
					onPanic(r, stack)
				}()
			}
		}
	}()
	fn()
}
