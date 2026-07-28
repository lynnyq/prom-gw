// Package parser 把 prompb.WriteRequest 转换为内部 Sample 列表,
// 填入请求级 Meta(t tenant / source_dc / trace_id 等)。
package parser
