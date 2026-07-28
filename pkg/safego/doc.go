// Package safego 包裹所有 goroutine,统一捕获 panic,
// 防止 panic 逃逸导致进程崩溃;panic 计数 + 堆栈记录由 obs 处理。
package safego
