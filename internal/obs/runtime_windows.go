//go:build windows

package obs

// readCPUTimeNS 在 windows 平台返回 0(spec 7.1 cpu_ratio 在 windows 不可用)。
//
// 本项目主要部署在 linux(VM + systemd),windows 仅为开发/测试便利;
// 若未来需要 windows 支持,可改用 NtQuerySystemInformation + 自定义 syscall。
func readCPUTimeNS() uint64 { return 0 }
