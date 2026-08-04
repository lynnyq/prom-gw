// Package decoder 提供 Snappy + Protobuf 解码,对应 Prometheus RemoteWrite v1。
//
// 协议要点(spec 4.1):
//   - Content-Type: application/x-protobuf
//   - Content-Encoding: snappy
//   - Body: prompb.WriteRequest
//
// 错误统一返回 decoder.Error 类型,携带阶段(stage)与具体原因,
// 方便 receiver 映射 HTTP 400/415 + gateway_errors_total{stage,type}。
package decoder

import (
	"errors"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/prompb"
)

// 错误分类(用于 metric type 标签)。
const (
	errTypeSnappy   = "snappy"
	errTypeProtobuf = "protobuf"
	errTypeEmpty    = "empty"
)

// Error 解码错误,带 stage 与 type 用于 metric 分类。
type Error struct {
	Stage string // "decode"
	Type  string // snappy / protobuf / empty
	Cause error
}

func (e *Error) Error() string {
	return fmt.Sprintf("decode error (stage=%s type=%s): %v", e.Stage, e.Type, e.Cause)
}

func (e *Error) Unwrap() error { return e.Cause }

// SnappyMaxLen 限制最大解码后长度,防止恶意大包耗内存。
// Prometheus 官方推荐 64MB,这里沿用。
const SnappyMaxLen = 64 * 1024 * 1024

// Decode 解码 body 为 prompb.WriteRequest。
//
// 调用方需在调用前已校验 Content-Encoding / Content-Type(由 receiver 中间件处理);
// 这里只负责 snappy + protobuf 解码。
func Decode(body []byte) (*prompb.WriteRequest, error) {
	if len(body) == 0 {
		return nil, &Error{Stage: "decode", Type: errTypeEmpty, Cause: errors.New("empty body")}
	}

	// 1. snappy 解码
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, &Error{Stage: "decode", Type: errTypeSnappy, Cause: err}
	}
	if len(raw) > SnappyMaxLen {
		return nil, &Error{
			Stage: "decode",
			Type:  errTypeSnappy,
			Cause: fmt.Errorf("snappy output %d exceeds max %d", len(raw), SnappyMaxLen),
		}
	}
	// 注: 允许空 raw(Prometheus 客户端可能发 no-op WriteRequest,合法)。

	// 2. protobuf 解码
	req := &prompb.WriteRequest{}
	if err := proto.Unmarshal(raw, req); err != nil {
		return nil, &Error{Stage: "decode", Type: errTypeProtobuf, Cause: err}
	}
	return req, nil
}
