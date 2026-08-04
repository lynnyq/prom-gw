package receiver

import (
	"encoding/json"
	"io"
)

// encodeJSON 序列化并写入 w;出错由调用方记日志(响应已写完,无法再改 header)。
func encodeJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}
