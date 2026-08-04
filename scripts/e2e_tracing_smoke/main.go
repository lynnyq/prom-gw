// e2e_tracing_smoke 端到端 smoke 测试:用真实运行的 prom-gw 验证 traceparent 串联。
//
// 用法:
//   go run ./scripts/e2e_tracing_smoke -addr=http://127.0.0.1:19201 -token=tk_app_business_dev
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:19201", "prom-gw 接收端地址")
	token := flag.String("token", "tk_app_business_dev", "认证 token")
	flag.Parse()

	// 1. 生成 traceparent(模拟客户端 OTel SDK inject 的产物)
	tp := makeTraceparent()

	// 2. 构造 prompb.WriteRequest(snappy + protobuf)
	req := &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "e2e_test_metric"},
					{Name: "instance", Value: "127.0.0.1:9100"},
				},
				Samples: []prompb.Sample{
					{Value: 42.0, Timestamp: time.Now().UnixMilli()},
				},
			},
		},
	}
	raw, err := proto.Marshal(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	body := snappy.Encode(nil, raw)

	// 3. 发出请求
	httpReq, err := http.NewRequest(http.MethodPost, *addr+"/api/v1/write", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "build req: %v\n", err)
		os.Exit(1)
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("Content-Encoding", "snappy")
	httpReq.Header.Set("Authorization", "Bearer "+*token)
	httpReq.Header.Set("traceparent", tp)

	fmt.Printf("[smoke] sending with traceparent=%s\n", tp)

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "do: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("[smoke] response: %d %s\n", resp.StatusCode, http.StatusText(resp.StatusCode))
	if len(respBody) > 0 {
		fmt.Printf("[smoke] body: %s\n", string(respBody))
	}
	if resp.StatusCode != http.StatusNoContent {
		fmt.Fprintf(os.Stderr, "[smoke] FAIL: expected 204\n")
		os.Exit(2)
	}
	fmt.Println("[smoke] PASS")
}

// makeTraceparent 生成一个 W3C trace context 字符串:
//
//	00-{32 hex traceID}-{16 hex spanID}-01
func makeTraceparent() string {
	traceID := randHex(16) // 16 bytes = 32 hex
	spanID := randHex(8)   // 8 bytes = 16 hex
	return fmt.Sprintf("00-%s-%s-01", traceID, spanID)
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
