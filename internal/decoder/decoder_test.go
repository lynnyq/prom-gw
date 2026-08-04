package decoder

import (
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encode(t *testing.T, req *prompb.WriteRequest) []byte {
	t.Helper()
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	return snappy.Encode(nil, raw)
}

func TestDecode_HappyPath(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "up"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1000}},
			},
		},
	}
	got, err := Decode(encode(t, req))
	require.NoError(t, err)
	assert.Equal(t, "up", got.Timeseries[0].Labels[0].Value)
}

func TestDecode_EmptyBody(t *testing.T) {
	_, err := Decode(nil)
	require.Error(t, err)
	var de *Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, errTypeEmpty, de.Type)
}

func TestDecode_BadSnappy(t *testing.T) {
	_, err := Decode([]byte("not-snappy-data"))
	require.Error(t, err)
	var de *Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, errTypeSnappy, de.Type)
}

func TestDecode_BadProtobuf(t *testing.T) {
	// snappy 编码一段非 protobuf 数据
	bad := snappy.Encode(nil, []byte("hello world"))
	_, err := Decode(bad)
	require.Error(t, err)
	var de *Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, errTypeProtobuf, de.Type)
}

func TestDecode_SnappyEmpty(t *testing.T) {
	// snappy 编码空 buffer → 解码出空 raw → protobuf 解码出空 WriteRequest(合法)
	// Prometheus 客户端会发这种 no-op 请求,Gateway 应接受并返回 204
	empty := snappy.Encode(nil, nil)
	got, err := Decode(empty)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Equal(t, 0, len(got.Timeseries))
}

func TestError_Format(t *testing.T) {
	e := &Error{Stage: "decode", Type: "snappy", Cause: errors.New("boom")}
	assert.Contains(t, e.Error(), "stage=decode")
	assert.Contains(t, e.Error(), "type=snappy")
	assert.True(t, errors.Is(e, errors.New("boom")) == false) // 不会匹配,只 Unwrap
	assert.Equal(t, errors.Unwrap(e).Error(), "boom")
}
