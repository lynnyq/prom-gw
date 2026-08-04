//go:build !windows

package obs

import (
	"syscall"
	"time"
)

// readCPUTimeNS 返回进程累计用户+系统 CPU 时间(纳秒)。
//
// 基于 syscall.Getrusage(RUSAGE_SELF) 的 ru_utime + ru_stime;
// unix 通用(linux / darwin / freebsd 等),windows 走 fallback。
// 失败时返回 0(不会因为 sys 错误让指标采集 panic)。
func readCPUTimeNS() uint64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	utime := time.Duration(ru.Utime.Sec)*time.Second + time.Duration(ru.Utime.Usec)*time.Microsecond
	stime := time.Duration(ru.Stime.Sec)*time.Second + time.Duration(ru.Stime.Usec)*time.Microsecond
	return uint64((utime + stime).Nanoseconds())
}
