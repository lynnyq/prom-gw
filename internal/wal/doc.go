// Package wal 提供磁盘 WAL(Write-Ahead Log),作为 Kafka 不可用时的第三道防线。
// 写入格式: [4B length][8B ts][1B flags][topic][key][payload][headers];段尾追加 CRC32 校验。
package wal
