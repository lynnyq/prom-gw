// ruleengine/types 单测: 覆盖 Clone 等基础工具行为。
package ruleengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompiledRuleSet_Clone_NilSafe(t *testing.T) {
	var c *CompiledRuleSet
	assert.Nil(t, c.Clone(), "nil Clone 应返回 nil,不 panic")
}

func TestCompiledRuleSet_Clone_DeepCopies(t *testing.T) {
	src := &CompiledRuleSet{
		RuleSet: RuleSet{
			Name:    "app",
			Version: 5,
		},
		Stages: []CompiledStage{
			{Type: "relabel", Config: map[string]interface{}{"drop": []interface{}{"a"}}},
		},
	}
	dst := src.Clone()
	require.NotNil(t, dst)

	assert.Equal(t, src.RuleSet, dst.RuleSet)
	require.Equal(t, len(src.Stages), len(dst.Stages))
	assert.Equal(t, src.Stages[0].Type, dst.Stages[0].Type)
	require.NotNil(t, dst.Stages[0].Config)

	// 改 dst 内部 map 不会影响 src
	dst.Stages[0].Config["drop"] = "changed"
	assert.NotEqual(t, src.Stages[0].Config["drop"], dst.Stages[0].Config["drop"],
		"Clone 后修改 dst map 必须不影响 src")
}

func TestCompiledRuleSet_Clone_NilConfigPreserved(t *testing.T) {
	src := &CompiledRuleSet{
		RuleSet: RuleSet{Name: "x"},
		Stages:  []CompiledStage{{Type: "noop"}},
	}
	dst := src.Clone()
	assert.Nil(t, dst.Stages[0].Config, "src.Config 为 nil 时,dst 也必须为 nil")
}
