// Package config - history_test.go: 历史版本 ring buffer 单测。
package config

import (
	"testing"

	"github.com/lynnyq/prom-gw/internal/ruleengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkRecord(name string, v int64) HistoryRecord {
	return HistoryRecord{
		Name:     name,
		Version:  v,
		Bytes:    10,
		RawYAML:  []byte("yaml:" + name),
		Source:   "test",
		Compiled: &ruleengine.CompiledRuleSet{
			RuleSet: ruleengine.RuleSet{Name: name, Version: v},
		},
	}
}

func TestHistory_SaveAndGet(t *testing.T) {
	h := NewHistory(HistoryConfig{Capacity: 5})
	require.NoError(t, h.Save(mkRecord("a", 1)))
	require.NoError(t, h.Save(mkRecord("a", 2)))

	got, err := h.Get("a", 2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Version)

	got, err = h.Get("a", 1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), got.Version)
}

func TestHistory_GetNotFound(t *testing.T) {
	h := NewHistory(HistoryConfig{})
	_, err := h.Get("missing", 1)
	assert.ErrorIs(t, err, ErrVersionNotFound)
}

func TestHistory_Latest(t *testing.T) {
	h := NewHistory(HistoryConfig{})
	require.NoError(t, h.Save(mkRecord("a", 1)))
	require.NoError(t, h.Save(mkRecord("a", 5)))
	require.NoError(t, h.Save(mkRecord("a", 3)))
	// Save 顺序:1, 5, 3 → buf head=[3, 5, 1];Latest 应取 pos 最小(即 buf 头)3
	// 实际 Latest 选 bestPos 最小,Save 头部插入 → 3 应该是 buf[0]
	got, err := h.Latest("a")
	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Version)
}

func TestHistory_CapacityEviction(t *testing.T) {
	var evicted []HistoryRecord
	h := NewHistory(HistoryConfig{
		Capacity: 3,
		OnEvict: func(r HistoryRecord) { evicted = append(evicted, r) },
	})
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, h.Save(mkRecord("a", i)))
	}
	assert.Equal(t, 3, h.Size())
	assert.Len(t, evicted, 2, "evicted 2 oldest")

	// 剩下的应该是 v5 v4 v3
	list := h.List("a")
	require.Len(t, list, 3)
	assert.Equal(t, int64(5), list[0].Version)
	assert.Equal(t, int64(4), list[1].Version)
	assert.Equal(t, int64(3), list[2].Version)
}

func TestHistory_SaveSameVersion_Overwrites(t *testing.T) {
	h := NewHistory(HistoryConfig{Capacity: 3})
	require.NoError(t, h.Save(mkRecord("a", 1)))
	r := mkRecord("a", 1)
	r.RawYAML = []byte("new-yaml")
	require.NoError(t, h.Save(r))
	assert.Equal(t, 1, h.Size())
	got, _ := h.Get("a", 1)
	assert.Equal(t, []byte("new-yaml"), got.RawYAML)
}

func TestHistory_MaxBytesRejected(t *testing.T) {
	h := NewHistory(HistoryConfig{MaxBytesPerVersion: 10})
	r := mkRecord("a", 1)
	r.RawYAML = make([]byte, 100)
	err := h.Save(r)
	assert.Error(t, err)
}

func TestHistory_NilCompiledRejected(t *testing.T) {
	h := NewHistory(HistoryConfig{})
	r := mkRecord("a", 1)
	r.Compiled = nil
	assert.Error(t, h.Save(r))
}

func TestHistory_MultipleNamesIndependent(t *testing.T) {
	h := NewHistory(HistoryConfig{Capacity: 5})
	require.NoError(t, h.Save(mkRecord("a", 1)))
	require.NoError(t, h.Save(mkRecord("b", 1)))
	require.NoError(t, h.Save(mkRecord("a", 2)))
	assert.Equal(t, 3, h.Size())
	assert.Equal(t, []string{"a", "b"}, sorted(h.Names()))
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
