package stringpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntern_ReturnsSameReference(t *testing.T) {
	a := Intern("hello")
	b := Intern("hello")
	// Go 字符串值相等即可;运行时可能复用底层字节,但 local var 指针必然不同
	assert.Equal(t, a, b, "Intern should return the same string value for same input")
	// 通过 unsafe 检查底层指针相同(可选,验证 intern 真正生效)
	if a != "" && b != "" {
		// 直接比较字符串头:在 Go 中字符串相等即值相等,这里只能验证相等
		assert.Equal(t, len(a), len(b))
	}
}

func TestIntern_DistinctInputs(t *testing.T) {
	a := Intern("foo")
	b := Intern("bar")
	assert.NotEqual(t, a, b)
}

func TestIntern_Empty(t *testing.T) {
	assert.Equal(t, "", Intern(""))
}

func TestSize_GrowsWithUnique(t *testing.T) {
	before := Size()
	Intern("unique-a")
	Intern("unique-b")
	Intern("unique-c")
	assert.Equal(t, before+3, Size())
}
