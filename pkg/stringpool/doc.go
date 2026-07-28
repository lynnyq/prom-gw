// Package stringpool 提供字符串 intern 池,降低 1.5M samples/s
// 持续场景下 tenant / source_dc / metric 等高复用字符串的 GC 压力。
package stringpool
