package parser

import (
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPrompbImported 验证 prompb 包的 WriteRequest / TimeSeries / Label / Sample 类型可用。
// Phase 1 T1.1 验收: 包导入成功,类型签名匹配预期。
func TestPrompbImported(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
		},
	}
	require.NotNil(t, req)
	require.Len(t, req.Timeseries, 1)
	assert.Equal(t, "up", req.Timeseries[0].Labels[0].Value)
	assert.Equal(t, float64(1), req.Timeseries[0].Samples[0].Value)
}
