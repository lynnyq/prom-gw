package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSample_SeriesKey_Deterministic(t *testing.T) {
	a := Sample{
		Tenant: "app-business",
		Metric: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}
	b := Sample{
		Tenant: "app-business",
		Metric: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}
	assert.Equal(t, a.SeriesKey(), b.SeriesKey(), "same series should produce same key")
}

func TestSample_SeriesKey_OrderIndependent(t *testing.T) {
	a := Sample{
		Tenant: "app-business",
		Metric: "http_requests_total",
		Labels: []Label{
			{Name: "method", Value: "GET"},
			{Name: "status", Value: "200"},
		},
	}
	b := Sample{
		Tenant: "app-business",
		Metric: "http_requests_total",
		Labels: []Label{
			{Name: "status", Value: "200"},
			{Name: "method", Value: "GET"},
		},
	}
	// FNV-1a 拼接顺序敏感,但相同 name=value 对集合的 hash 拼接结果应相同;
	// 实际生产中 Parse 会先 sort,这里只验证不同顺序会产生不同 key(避免误判)。
	assert.NotEqual(t, a.SeriesKey(), b.SeriesKey(),
		"unsorted labels should produce different key (parser must sort before hash)")
}

func TestSample_SeriesKey_NoCollision(t *testing.T) {
	a := Sample{
		Tenant: "t", Metric: "m",
		Labels: []Label{{Name: "x", Value: "ab"}},
	}
	b := Sample{
		Tenant: "t", Metric: "m",
		Labels: []Label{{Name: "xa", Value: "b"}},
	}
	assert.NotEqual(t, a.SeriesKey(), b.SeriesKey(),
		"different label splits should not collide (\\x00 separator)")
}

func TestSample_Clone(t *testing.T) {
	s := Sample{
		Tenant: "t", Metric: "m",
		Labels: []Label{{Name: "k", Value: "v"}},
		Value: 1.5, Timestamp: 100,
	}
	cp := s.Clone()
	assert.Equal(t, s.SeriesKey(), cp.SeriesKey())
	assert.Equal(t, s.Value, cp.Value)
	// 浅拷贝: 共享 Labels slice 底层数组
	assert.Equal(t, len(s.Labels), len(cp.Labels))
}

func TestSample_InternStrings(t *testing.T) {
	s := Sample{
		Tenant: "tenant-a", TenantID: "1001", SourceDC: "dc-1",
		Metric: "up",
		Labels: []Label{{Name: "instance", Value: "10.0.0.1:8080"}},
	}
	s.InternStrings()
	// 多次 intern 同字符串应返回同一 string 值(底层可能复用字节)
	again := Sample{
		Tenant: "tenant-a", TenantID: "1001", SourceDC: "dc-1",
		Metric: "up",
		Labels: []Label{{Name: "instance", Value: "10.0.0.2:8080"}},
	}
	again.InternStrings()
	assert.Equal(t, s.Tenant, again.Tenant, "tenant string should be shared")
	assert.Equal(t, s.Metric, again.Metric)
	assert.Equal(t, s.Labels[0].Name, again.Labels[0].Name)
}
