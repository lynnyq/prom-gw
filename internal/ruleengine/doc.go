// Package ruleengine 提供标签/路由/采样/下采样/死值等多维清洗能力。
// 核心是 Pipeline + 多个无状态 Stage;状态型 Stage(Downsample/DeadValue)用 atomic 切换。
package ruleengine
