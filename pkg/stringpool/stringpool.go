// Package stringpool 提供字符串 intern 池,降低 1.5M samples/s
// 持续场景下 tenant / source_dc / metric 等高复用字符串的 GC 压力。
//
// 设计: 简单的 sync.Map,key/value 都是 string;读多写少场景友好。
// 限制: 不主动 GC,只增长;如需回收,Phase 5 末做内存 profile 再决定。
package stringpool

import "sync"

// pool 默认实例(进程级单例)。
// 如需多租户隔离可改为带租户前缀的实例,但 1.5M samples/s 下 string 复用收益远大于按租户区分。
var pool sync.Map

// Intern 返回 s 在池中的复用副本。
// 首次调用时存入,后续调用直接返回池中引用;读多写少,几乎无锁竞争。
func Intern(s string) string {
	if s == "" {
		return ""
	}
	if v, ok := pool.Load(s); ok {
		return v.(string)
	}
	actual, _ := pool.LoadOrStore(s, s)
	return actual.(string)
}

// Size 返回当前池中字符串数量(测试 / 监控用)。
func Size() int {
	n := 0
	pool.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
