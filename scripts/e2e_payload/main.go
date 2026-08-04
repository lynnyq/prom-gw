// e2e_payload 工具: 构造一条最小可用的 Prometheus RemoteWrite 字节。
//
// 用法:
//
//	RUN_ID=my-run go run ./scripts/e2e_payload > /tmp/payload.bin
//
// 输出到 stdout: snappy 编码后的 WriteRequest 字节,直接 POST 到 /api/v1/write。
package main

import (
	"io"
	"os"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	runID := os.Getenv("RUN_ID")
	if runID == "" {
		runID = "manual"
	}
	now := time.Now().UnixMilli()
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "e2e_test_metric"},
					{Name: "e2e_label", Value: "hello"},
					{Name: "e2e_run", Value: "run-" + runID},
				},
				Samples: []prompb.Sample{{Value: 42, Timestamp: now}},
			},
		},
	}
	raw, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	encoded := snappy.Encode(nil, raw)
	if _, err := io.Copy(os.Stdout, &byteReader{b: encoded}); err != nil {
		panic(err)
	}
}

type byteReader struct {
	b []byte
	i int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
