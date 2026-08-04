// Package admin - helpers.go: 请求解析、IP/CIDR 工具。
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
)

// parseCIDRs 把字符串列表编译为 []*net.IPNet;任一 CIDR 非法 → error。
func parseCIDRs(in []string) ([]*net.IPNet, error) {
	out := make([]*net.IPNet, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// "1.2.3.4" 简写也接受(自动加 /32)
		if !strings.Contains(s, "/") {
			s += "/32"
		}
		_, ipnet, err := net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf("invalid cidr %q: %w", s, err)
		}
		out = append(out, ipnet)
	}
	return out, nil
}

// parseClientIP 取 RemoteAddr 第一段。
func parseClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ipInNets 判断 ip 是否在任一网段内。
//
// 入参 ip 必须为 net.ParseIP 合法形式;否则视为不命中。
func ipInNets(ip string, nets []*net.IPNet) bool {
	if ip == "" {
		return false
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

// parseInt64Query 取 ?key=int64;缺失/非法 → error。
func parseInt64Query(r *http.Request, key string) (int64, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return 0, errors.New(key + " required")
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s invalid: %w", key, err)
	}
	return v, nil
}

// parsePutBody 解析 PUT 请求体:
//
//	{"version": 7, "raw_yaml": "rulesets:\n  - ..."}
//
// 或纯 YAML(Content-Type: application/yaml)作为 raw。
func parsePutBody(r *http.Request) (version int64, rawYAML []byte, err error) {
	ct := r.Header.Get("Content-Type")
	// 限制大小 4MB
	const max = 4 << 20
	r.Body = http.MaxBytesReader(nil, r.Body, max)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("read body: %w", err)
	}

	// 1) JSON 形式(优先)
	if strings.HasPrefix(ct, "application/json") || (len(body) > 0 && body[0] == '{') {
		var req struct {
			Version int64  `json:"version"`
			RawYAML string `json:"raw_yaml"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			return 0, nil, fmt.Errorf("invalid json: %w", err)
		}
		if req.Version <= 0 {
			return 0, nil, errors.New("version must be > 0")
		}
		if req.RawYAML == "" {
			return 0, nil, errors.New("raw_yaml required")
		}
		return req.Version, []byte(req.RawYAML), nil
	}

	// 2) YAML 形式:version 走 query,body 作为 yaml
	if strings.HasPrefix(ct, "application/yaml") || strings.HasPrefix(ct, "text/yaml") {
		v, err := parseInt64Query(r, "version")
		if err != nil {
			return 0, nil, err
		}
		return v, body, nil
	}

	return 0, nil, errors.New("content-type must be application/json or application/yaml")
}
