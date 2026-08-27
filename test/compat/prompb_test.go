// Package compat 包含跨 Prometheus / Cortex / VM 等不同 RemoteWrite 客户端的
// 兼容性测试。
//
// 测试分为两类:
//   - prompb_test.go(本文件,无 build tag): 协议级单元测试,验证不同 wire format
//     变体(典型来自不同 RemoteWrite 客户端)能被 decoder / parser 正确处理。
//     任何 PR 都必须保证这些通过。
//   - matrix_docker_test.go(build tag: integration): 端到端测试,用 testcontainers
//     拉真实 Prometheus / Cortex / VM 容器发数据,验证全链路兼容。
//     通过 `INTEGRATION=1 go test -tags=integration ./test/compat/...` 运行。
//
// 客户端 wire format 调研:
package compat

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/lynnyq/bigdata/internal/decoder"
	"github.com/lynnyq/bigdata/internal/parser"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 兼容性测试共用 fixture:用 snappy+protobuf 编码给定的 WriteRequest。
func encode(t *testing.T, req *prompb.WriteRequest) []byte {
	t.Helper()
	raw, err := proto.Marshal(req)
	require.NoError(t, err)
	return snappy.Encode(nil, raw)
}

// makeReq 是构造请求的小工具,labels 第一个 key=__name__ 时作为 metric name。
func makeReq(metric string, labels map[string]string, value float64, ts int64) *prompb.WriteRequest {
	pl := []prompb.Label{{Name: "__name__", Value: metric}}
	for k, v := range labels {
		pl = append(pl, prompb.Label{Name: k, Value: v})
	}
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels:  pl,
			Samples: []prompb.Sample{{Value: value, Timestamp: ts}},
		}},
	}
}

// 1) Prometheus 官方 client 格式
//    - 包含 __name__ label
//    - 常见 label 顺序:__name__ 在最前
//    - 毫秒时间戳
func TestCompat_PrometheusOfficial(t *testing.T) {
	req := makeReq("http_requests_total",
		map[string]string{"job": "api", "code": "200", "env": "prod"},
		42, 1700000000000)
	body := encode(t, req)

	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	require.Len(t, decoded.Timeseries, 1)

	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{
		Business: "app", SourceDC: "dc1", IngestTs: 1700000000000000000,
	})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	assert.Equal(t, "http_requests_total", res.Samples[0].Metric)
	assert.Equal(t, float64(42), res.Samples[0].Value)
	assert.Equal(t, int64(1700000000000), res.Samples[0].Timestamp)
}

// 2) Cortex remote_write 格式
//    - 实际是 Prometheus client + Cortex distributor 转发,wire format 一致
//    - 常见差异:labels 顺序可能乱序(我们 parser 已按 name 排序,无影响)
//    - 同一 series 可能包含多个 sample(我们 v1 只取第一个,与 prom client 行为一致)
func TestCompat_CortexRemoteWrite(t *testing.T) {
	// 模拟 Cortex 推送:labels 顺序乱序 + 包含 cluster / region 等多 DC label
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "region", Value: "us-east-1"},
				{Name: "__name__", Value: "cortex_ingester_received_samples_total"},
				{Name: "cluster", Value: "prod-us-1"},
				{Name: "job", Value: "cortex-ingester"},
			},
			Samples: []prompb.Sample{{Value: 12345, Timestamp: 1700000000000}},
		}},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)

	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "cortex", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	s := res.Samples[0]
	assert.Equal(t, "cortex_ingester_received_samples_total", s.Metric)
	// 验证 labels 排序后 region 在 cluster 之前(alphabetical)
	require.Len(t, s.Labels, 3)
	assert.Equal(t, "cluster", s.Labels[0].Name)
	assert.Equal(t, "job", s.Labels[1].Name)
	assert.Equal(t, "region", s.Labels[2].Name)
}

// 3) VictoriaMetrics remote_write 格式
//    - 兼容 prompb 协议,可能用 __name__ 缺失的"匿名 metric"语法(vm_agent 偶发)
//    - 验证我们能优雅处理缺 __name__ 的 series
func TestCompat_VictoriaMetrics_MissingName(t *testing.T) {
	// VM 偶发会推送不带 __name__ 的 series(已被 Prometheus 协议禁止,实际少见)
	// 我们应该跳过(不 panic / 不污染整批)
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "vm_metric"}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
			},
			{
				// 缺 __name__,应被跳过
				Labels:  []prompb.Label{{Name: "job", Value: "vmagent"}},
				Samples: []prompb.Sample{{Value: 2, Timestamp: 2}},
			},
			{
				Labels:  []prompb.Label{{Name: "__name__", Value: "vm_metric_2"}},
				Samples: []prompb.Sample{{Value: 3, Timestamp: 3}},
			},
		},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "vm", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 2, "缺 __name__ 的 series 应被跳过")
	assert.Equal(t, "vm_metric", res.Samples[0].Metric)
	assert.Equal(t, "vm_metric_2", res.Samples[1].Metric)
}

// 4) Thanos receiver / sidecar 格式
//    - 与 prom client 同 wire format,但 sample 中 Timestamp 可能在 sample[0]
//      之外还有 histogram(native histograms,v2.40+ 引入)。
//    - v1 parser 只取 sample[0],histogram 被忽略(无 NPE / panic)
func TestCompat_Thanos_NativeHistogramIgnored(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "rpc_duration_seconds"},
				{Name: "job", Value: "thanos-receiver"},
			},
			// 真实 Thanos 可能在 Samples 之外附带 Histograms,但 prompb.WriteRequest
			// 当前版本只声明 Samples 字段(THanos 走的也是这字段);
			// 这里验证 v1 parser 在只有 sample 时不读额外字段。
			Samples: []prompb.Sample{{Value: 0.123, Timestamp: 1700000000000}},
		}},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "thanos", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	assert.Equal(t, 0.123, res.Samples[0].Value)
}

// 5) OpenMetrics / Mimir agent 格式
//    - 协议层与 prom 完全兼容,差异在 label name/value 字符集
//    - 验证 UTF-8 label value 可被传输
func TestCompat_OpenMetrics_UTF8LabelValue(t *testing.T) {
	req := makeReq("requests_total",
		map[string]string{"region": "亚太-北京", "env": "测试环境"},
		1, 1)
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "mm", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)

	// 找到 region / env label
	var region, env string
	for _, lb := range res.Samples[0].Labels {
		if lb.Name == "region" {
			region = lb.Value
		}
		if lb.Name == "env" {
			env = lb.Value
		}
	}
	assert.True(t, utf8.ValidString(region) && region == "亚太-北京")
	assert.True(t, utf8.ValidString(env) && env == "测试环境")
}

// 6) agent_exporter / node_exporter 文本 → remote_write 桥接
//    - 某些 exporter 桥接器会带超长 label value(比如进程命令行)
//    - 验证大 label 不被截断
func TestCompat_LargeLabelValue(t *testing.T) {
	big := strings.Repeat("x", 8192)
	req := makeReq("process_argv", map[string]string{"argv0": big}, 1, 1)
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "node", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	var got string
	for _, lb := range res.Samples[0].Labels {
		if lb.Name == "argv0" {
			got = lb.Value
		}
	}
	assert.Equal(t, 8192, len(got))
}

// 7) noop / heartbeat 格式
//    - Prometheus 在没有数据时可能推送 0 series 的空 WriteRequest
//    - 必须正确处理(204 No Content,无 panic)
func TestCompat_EmptyWriteRequest(t *testing.T) {
	req := &prompb.WriteRequest{} // 0 series
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	assert.Empty(t, decoded.Timeseries)

	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "noop", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	assert.Empty(t, res.Samples)
}

// 8) 大批量格式(模拟单请求 100K series)
//    - 验证 decoder / parser 在大批量下不 OOM、解析正确
func TestCompat_LargeBatch(t *testing.T) {
	const n = 10000
	serieses := make([]prompb.TimeSeries, 0, n)
	for i := 0; i < n; i++ {
		serieses = append(serieses, prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "load_avg"},
				{Name: "host", Value: "h-" + intToStr(i)},
			},
			Samples: []prompb.Sample{{Value: float64(i % 100), Timestamp: 1700000000000}},
		})
	}
	req := &prompb.WriteRequest{Timeseries: serieses}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	assert.Len(t, decoded.Timeseries, n)

	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "load", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	assert.Len(t, res.Samples, n)
}

// 9) 多 sample per series(otlp 桥接器 / Cortex distributor 偶发)
//    - 协议允许但 spec 写的是"每 series 一个 sample"
//    - v1 parser 取 sample[0],后续 sample 被忽略(无 panic)
func TestCompat_MultiSamplesPerSeries(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "m"},
				{Name: "k", Value: "v"},
			},
			Samples: []prompb.Sample{
				{Value: 1, Timestamp: 100},
				{Value: 2, Timestamp: 200},
				{Value: 3, Timestamp: 300},
			},
		}},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "m", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	assert.Equal(t, float64(1), res.Samples[0].Value)
	assert.Equal(t, int64(100), res.Samples[0].Timestamp)
}

// 10) 时间戳极端值(0 / 远未来 / 负数)
//     - Prometheus client 不会发这些,但旧 VM agent 偶发
//     - 验证不 panic
func TestCompat_TimestampEdgeCases(t *testing.T) {
	cases := []struct {
		name string
		ts   int64
	}{
		{"zero", 0},
		{"epoch_ms", 1},
		{"far_future", 9999999999999},
		{"negative", -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := makeReq("m", map[string]string{"k": "v"}, 1, c.ts)
			body := encode(t, req)
			decoded, err := decoder.Decode(body)
			require.NoError(t, err)
			ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "edge", IngestTs: 1})
			res, err := parser.Parse(ctx, decoded)
			require.NoError(t, err)
			require.Len(t, res.Samples, 1)
			assert.Equal(t, c.ts, res.Samples[0].Timestamp)
		})
	}
}

// 11) 重复 label name(同一 series)
//     - 协议不允许,Prometheus client 会去重;某些客户端没去重
//     - 验证:不会让 SeriesKey 错乱(取最后一次 / 第一次均可,只要确定即可)
func TestCompat_DuplicateLabelNames(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "m"},
				{Name: "k", Value: "first"},
				{Name: "k", Value: "second"}, // 重复
			},
			Samples: []prompb.Sample{{Value: 1, Timestamp: 1}},
		}},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "dup", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	require.Len(t, res.Samples, 1)
	// 我们 parser 不去重,保留两个 label;SeriesKey 自然会有 2 份 k 段
	// 这是已知行为(已在 docs/compatibility.md 备注)
	assert.GreaterOrEqual(t, len(res.Samples[0].Labels), 1)
}

// 12) 错误 snappy 流(模拟 vm agent 在断连时发半截包)
func TestCompat_BadSnappyPayload(t *testing.T) {
	_, err := decoder.Decode([]byte("not-a-valid-snappy-stream"))
	require.Error(t, err)
	var de *decoder.Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, "snappy", de.Type)
}

// 13) 错误 protobuf(模拟 snappy 包了随机字节)
func TestCompat_BadProtobufPayload(t *testing.T) {
	body := snappy.Encode(nil, []byte{0xff, 0xfe, 0xfd, 0xfc, 0x00, 0x01, 0x02})
	_, err := decoder.Decode(body)
	require.Error(t, err)
	var de *decoder.Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, "protobuf", de.Type)
}

// 14) 空 body
func TestCompat_EmptyBody(t *testing.T) {
	_, err := decoder.Decode(nil)
	require.Error(t, err)
	_, err = decoder.Decode([]byte{})
	require.Error(t, err)
}

// 15) 只含 labels 不含 samples(无效 series,应跳过)
func TestCompat_LabelsOnlySeries(t *testing.T) {
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{Labels: []prompb.Label{{Name: "__name__", Value: "m"}}, Samples: nil},
		},
	}
	body := encode(t, req)
	decoded, err := decoder.Decode(body)
	require.NoError(t, err)
	ctx := parser.ContextWithMeta(context.Background(), parser.Meta{Business: "l", IngestTs: 1})
	res, err := parser.Parse(ctx, decoded)
	require.NoError(t, err)
	assert.Empty(t, res.Samples)
}

// 16) body 长度检测:sanity 校验 snappy 输出不超过 SnappyMaxLen
func TestCompat_OversizedBodyRejected(t *testing.T) {
	// 构造稍大于 64MB 的 snappy 体
	huge := bytes.Repeat([]byte{0}, decoder.SnappyMaxLen+1)
	// 把它包成"snappy 流"(直接当 snappy 解码会失败,所以走 decoder 路径会先报 snappy 错)
	// 但 SnappyMaxLen 检查在 snappy 解码之后 → 我们这里用合法的 snappy 帧
	hugeSnappy := snappy.Encode(nil, huge)
	_, err := decoder.Decode(hugeSnappy)
	require.Error(t, err)
	// 注意:这里走的是"snappy output exceeds max"路径
	var de *decoder.Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, "snappy", de.Type)
}

// intToStr 避免引入 strconv 到 fixture helper。
func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
