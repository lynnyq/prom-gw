// validate-yaml 校验 api/openapi/*.yaml 是合法 OpenAPI 3 文档(结构 + 必填字段)。
//
// 用法:
//
//	go run ./scripts/validate-yaml -spec api/openapi/admin.yaml
//
// 校验项:
//   - 合法 YAML
//   - 包含 openapi: 3.x.x
//   - info.title / info.version 必填
//   - paths.* 有 method 节点
//   - components.responses / schemas 引用闭合($ref 全部能解析)
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type openAPIDoc struct {
	OpenAPI    string                 `yaml:"openapi"`
	Info       map[string]any         `yaml:"info"`
	Paths      map[string]any         `yaml:"paths"`
	Components map[string]any         `yaml:"components"`
}

type openAPIPathItem map[string]any

func main() {
	spec := flag.String("spec", "api/openapi/admin.yaml", "OpenAPI spec file path")
	flag.Parse()

	raw, err := os.ReadFile(*spec)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	var doc openAPIDoc
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		log.Fatalf("yaml invalid: %v", err)
	}
	if !strings.HasPrefix(doc.OpenAPI, "3.") {
		log.Fatalf("openapi version must be 3.x.x, got %q", doc.OpenAPI)
	}
	if _, ok := doc.Info["title"]; !ok {
		log.Fatalf("info.title required")
	}
	if _, ok := doc.Info["version"]; !ok {
		log.Fatalf("info.version required")
	}
	if len(doc.Paths) == 0 {
		log.Fatalf("paths is empty")
	}
	// 收集所有 path,校验 method
	allowedMethods := map[string]bool{
		"get": true, "post": true, "put": true, "delete": true,
		"options": true, "head": true, "patch": true, "trace": true,
	}
	refs := regexp.MustCompile(`#/components/(\w+)/([\w./-]+)`)
	seenRefs := map[string][]string{}
	for path, rawItem := range doc.Paths {
		item, ok := rawItem.(map[string]any)
		if !ok {
			log.Fatalf("path %q: not a map", path)
		}
		hasMethod := false
		for k := range item {
			if allowedMethods[strings.ToLower(k)] {
				hasMethod = true
			}
		}
		if !hasMethod {
			log.Fatalf("path %q: no HTTP method", path)
		}
		// 收集 $ref
		collectRefs(item, refs, seenRefs)
	}
	// 校验 $ref 闭合
	for ref, where := range seenRefs {
		if !refExists(ref, doc) {
			log.Fatalf("unresolved $ref %q (referenced from %v)", ref, where)
		}
	}

	// 报告
	paths := make([]string, 0, len(doc.Paths))
	for p := range doc.Paths {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	fmt.Printf("OK: %d paths, %d component groups, %d $refs\n",
		len(doc.Paths),
		len(doc.Components),
		len(seenRefs),
	)
	for _, p := range paths {
		fmt.Println("  -", p)
	}
}

func collectRefs(v any, re *regexp.Regexp, out map[string][]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, vv := range t {
			if k == "$ref" {
				if s, ok := vv.(string); ok {
					out[s] = append(out[s], "any")
				}
				continue
			}
			collectRefs(vv, re, out)
		}
	case []any:
		for _, vv := range t {
			collectRefs(vv, re, out)
		}
	case string:
		for _, m := range re.FindAllString(t, -1) {
			out[m] = append(out[m], "any")
		}
	}
}

func refExists(ref string, doc openAPIDoc) bool {
	// 形如 #/components/responses/Forbidden
	parts := strings.Split(strings.TrimPrefix(ref, "#/components/"), "/")
	if len(parts) < 2 {
		return false
	}
	group, ok := doc.Components[parts[0]]
	if !ok {
		return false
	}
	m, ok := group.(map[string]any)
	if !ok {
		return false
	}
	_, ok = m[parts[1]]
	return ok
}
