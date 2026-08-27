package parser

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ctxWithMeta(t *testing.T) context.Context {
	t.Helper()
	return ContextWithMeta(context.Background(), Meta{
		Business:   "app-business",
		SourceDC:   "dc-1",
		IngestCity: "bj",
		IngestTs:   1000000,
	})
}

func TestParse_HappyPath(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "http_requests_total"},
					{Name: "status", Value: "200"},
					{Name: "method", Value: "GET"},
				},
				Samples: []prompb.Sample{{Value: 42, Timestamp: 1000}},
			},
		},
	}
	res, err := Parse(ctxWithMeta(t), req)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)

	s := res.Samples[0]
	assert.Equal(t, "app-business", s.Business)
	assert.Equal(t, "dc-1", s.SourceDC)
	assert.Equal(t, "bj", s.IngestCity)
	assert.Equal(t, "http_requests_total", s.Metric)
	assert.Equal(t, float64(42), s.Value)
	assert.Equal(t, int64(1000), s.Timestamp)
	// labels 已排序
	require.Len(t, s.Labels, 2)
	assert.Equal(t, "method", s.Labels[0].Name)
	assert.Equal(t, "status", s.Labels[1].Name)
}

func TestParse_MissingMeta(t *testing.T) {
	req := &prompb.WriteRequest{}
	_, err := Parse(context.Background(), req)
	assert.ErrorIs(t, err, ErrMetaMissing)
}

func TestParse_NoNameLabel_Skipped(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "foo", Value: "bar"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			},
		},
	}
	res, err := Parse(ctxWithMeta(t), req)
	require.NoError(t, err)
	assert.Empty(t, res.Samples, "no __name__ should be skipped")
}

func TestParse_EmptySamples_Skipped(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}},
				Samples: nil,
			},
		},
	}
	res, _ := Parse(ctxWithMeta(t), req)
	assert.Empty(t, res.Samples)
}

func TestParse_MultipleTimeSeries(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "a"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			},
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "b"}},
				Samples: []prompb.Sample{{Value: 2, Timestamp: 2}},
			},
			{
				// 缺 __name__,应被跳过
				Labels:  []prompb.Label{{Name: "x", Value: "y"}},
				Samples: []prompb.Sample{{Value: 0, Timestamp: 0}},
			},
		},
	}
	res, err := Parse(ctxWithMeta(t), req)
	require.NoError(t, err)
	assert.Len(t, res.Samples, 2)
	assert.Equal(t, "a", res.Samples[0].Metric)
	assert.Equal(t, "b", res.Samples[1].Metric)
}

func TestParse_SeriesKeyStableAcrossOrders(t *testing.T) {
	// 同一 series,labels 顺序不同 -> 排序后 SeriesKey 一致
	mk := func(labels []prompb.Label) *prompb.WriteRequest {
		return &prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{Labels: labels, Samples: []prompb.Sample{{Value: 1, Timestamp: 1}}},
			},
		}
	}
	a := mk([]prompb.Label{
		{Name: "__name__", Value: "m"},
		{Name: "a", Value: "1"},
		{Name: "b", Value: "2"},
	})
	b := mk([]prompb.Label{
		{Name: "__name__", Value: "m"},
		{Name: "b", Value: "2"},
		{Name: "a", Value: "1"},
	})
	ra, _ := Parse(ctxWithMeta(t), a)
	rb, _ := Parse(ctxWithMeta(t), b)
	require.Len(t, ra.Samples, 1)
	require.Len(t, rb.Samples, 1)
	assert.Equal(t, ra.Samples[0].SeriesKey(), rb.Samples[0].SeriesKey())
}

func TestContextWithMeta_RoundTrip(t *testing.T) {
	m := Meta{Business: "t1", SourceDC: "dc-x", IngestCity: "bj", RemoteIP: "1.2.3.4", TraceID: "trace-abc"}
	ctx := ContextWithMeta(context.Background(), m)
	got, ok := MetaFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, m, got)
}
