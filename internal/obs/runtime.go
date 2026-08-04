package obs

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lynnyq/bigdata/pkg/safego"
)

// numGoroutines 单独函数便于 GaugeFunc 引用与单测。
func numGoroutines() float64 {
	return float64(runtime.NumGoroutine())
}

// memBytes 进程当前驻留内存字节数(spec 7.1 gateway_mem_bytes)。
//
// 使用 HeapAlloc + HeapInuse 估算(只读 Sys 会被 cache/buffer 干扰);
// 详细分布由默认 registry 中的 go_memstats_* 指标覆盖。
func memBytes() float64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc + ms.HeapInuse)
}

// panicCount 返回 safego 累计恢复的 panic 数(spec 6.6 / plan T0.7)。
//
// 通过 GaugeFunc 暴露为 gateway_panic_recovered_total。
// safego 内部用 mutex 保护的 uint64,这里直接调用 Stats();无锁路径由
// promauto 在采集时串行调用保证一致性。
func panicCount() float64 {
	return float64(safego.Stats())
}

// CPU busy ratio 计算(0-1,spec 7.1 gateway_cpu_ratio)。
//
// 基于两次采样间的 wall time vs CPU time;首次调用仅记录基线返回 0,
// 后续调用返回差分。readCPUTimeNS 由 runtime_unix.go / runtime_windows.go
// 分别提供(unix 走 syscall.Getrusage,windows 走 NtQuerySystemInformation,
// 这里为最小化跨平台代码,只实现 unix,其他返回 0)。
var (
	cpuLastNS    atomic.Int64
	cpuLastValue atomic.Uint64
	cpuInitOnce  atomic.Bool
)

// ReadCPUBusyRatio 公开函数便于单测。
func ReadCPUBusyRatio() float64 { return readCPUBusyRatio() }

// readCPUBusyRatio 实际计算。
func readCPUBusyRatio() float64 {
	now := time.Now().UnixNano()
	cur := readCPUTimeNS()
	if !cpuInitOnce.CompareAndSwap(false, true) {
		// 后续差分
	} else {
		// 首次:仅写基线
		cpuLastNS.Store(now)
		cpuLastValue.Store(cur)
		return 0
	}
	lastNS := cpuLastNS.Load()
	lastVal := cpuLastValue.Load()
	wall := now - lastNS
	if wall <= 0 {
		return 0
	}
	cpuDelta := int64(cur) - int64(lastVal)
	if cpuDelta < 0 {
		cpuDelta = 0
	}
	cpuLastNS.Store(now)
	cpuLastValue.Store(cur)
	ratio := float64(cpuDelta) / float64(wall)
	if ratio > 1 {
		ratio = 1
	}
	return ratio
}
