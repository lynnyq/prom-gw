// Package router 根据 sample.TargetTopic 决定最终写入的 Kafka topic。
// 本包有意保持轻量;路由规则由 ruleengine.Route stage 决策,本包只负责 fan-out。
package router
