// Package kafkasink 封装 franz-go 异步批量 producer。
// 写入语义: 入内部 channel 即返回 nil error;同步等待用 Flush(timeout)。
package kafkasink
