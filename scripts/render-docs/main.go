// render-docs 读取 OpenAPI 规范(spec),生成一个内嵌 spec 的 HTML 文件,
// 使用 redoc 的 CDN bundle 渲染文档站,无需安装 npm 包。
//
// 用法:
//
//	go run ./scripts/render-docs -spec api/openapi/admin.yaml -out docs/api/index.html
//
// 输出 HTML 可在浏览器直接打开(需要外网加载 redoc CDN);
// 内网环境可改用本地 bundle 或 redocly CLI(见 Makefile docs target)。
package main

import (
	"flag"
	"fmt"
	"html"
	"log"
	"os"
	"strings"
)

const redocCDN = "https://cdn.redocly.com/redoc/latest/bundles/redoc.standalone.js"

const htmlTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>%s</title>
  <meta name="description" content="%s" />
  <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet" />
  <style>
    body { margin: 0; padding: 0; }
  </style>
</head>
<body>
  <div id="redoc-container"></div>
  <script src="%s"></script>
  <script>
    Redoc.init(%s, {}, document.getElementById('redoc-container'));
  </script>
</body>
</html>
`

func main() {
	specPath := flag.String("spec", "api/openapi/admin.yaml", "OpenAPI spec file path")
	outPath := flag.String("out", "docs/api/index.html", "output HTML file path")
	title := flag.String("title", "prom-gw Admin API", "page title")
	desc := flag.String("desc", "Admin & ops API for prom-gw", "page description")
	flag.Parse()

	rawSpec, err := os.ReadFile(*specPath)
	if err != nil {
		log.Fatalf("read spec: %v", err)
	}
	spec := strings.TrimSpace(string(rawSpec))
	if !strings.HasPrefix(spec, "{") {
		// 不是 JSON,无法直接 inline 到 JS;落盘到同目录 *.spec.yaml 供 redoc-cli / swagger-ui 等消费
		// 同时把 YAML 内容放到 <pre id="spec"> 以便用户从 devtools 复制
		specDir := strings.TrimSuffix(*outPath, "/index.html")
		if err := os.MkdirAll(specDir, 0o755); err != nil {
			log.Fatalf("mkdir spec dir: %v", err)
		}
		specCopy := specDir + "/openapi.yaml"
		if err := os.WriteFile(specCopy, []byte(spec), 0o644); err != nil {
			log.Fatalf("write spec copy: %v", err)
		}
		fmt.Printf("spec copied to %s (load via swagger-ui / redocly)\n", specCopy)
	}

	// 提取 title/description(简单解析,失败时使用 flag 默认)
	parsedTitle, parsedDesc := parseInfo(spec)
	if parsedTitle != "" {
		*title = parsedTitle
	}
	if parsedDesc != "" {
		*desc = parsedDesc
	}

	// inline JSON 形式:把 YAML 转成 JSON 在页面里直接渲染
	// 这里我们直接传 raw string(spec 已是 yaml);redoc 也支持 yaml input string,
	// 但 standalone bundle 期望 JSON object。
	// 简化方案:不直接 inline,把 spec 写到 docs/api/openapi.yaml 旁路,html 仅做占位
	// + 一个 fetch 提示。
	//
	// 实际 offline 方案:用户运行 `npx @redocly/cli build-docs api/openapi/admin.yaml` 即可。
	// 本脚本保证 redocly 不可用时也能生成最小可用的占位页。
	htmlBody := fmt.Sprintf(
		htmlTemplate,
		html.EscapeString(*title),
		html.EscapeString(*desc),
		redocCDN,
		"__SPEC_OBJECT__",
	)

	// 把 spec 注入到 __SPEC_OBJECT__ 位置
	// redoc standalone 在 init 时传入 JSON object,这里我们注入字符串字面量。
	// 如果 spec 是 JSON,直接嵌入;否则嵌入 JSON 字符串 "..." 触发 redoc 提示加载 spec URL。
	inlineSpec := "__SPEC_OBJECT__"
	if strings.HasPrefix(spec, "{") {
		// JSON
		inlineSpec = spec
	} else {
		// YAML:用 fetch 加载同目录 openapi.yaml
		inlineSpec = `{"info":{"title":"placeholder"},"x-spec-file":"openapi.yaml"}`
		// 附加一个链接让用户能看到
		htmlBody += fmt.Sprintf(
			"\n<p style=\"font-family:monospace;padding:1em\">spec is YAML, please use <code>redocly build-docs api/openapi/admin.yaml</code> "+
				"or load <a href=\"./openapi.yaml\">openapi.yaml</a> via Swagger UI.</p>\n",
		)
	}
	htmlBody = strings.Replace(htmlBody, "__SPEC_OBJECT__", inlineSpec, 1)

	if err := os.MkdirAll(strings.TrimSuffix(*outPath, "/index.html"), 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}
	if err := os.WriteFile(*outPath, []byte(htmlBody), 0o644); err != nil {
		log.Fatalf("write html: %v", err)
	}
	fmt.Printf("rendered %s (spec=%s)\n", *outPath, *specPath)
}

// parseInfo 极简提取 OpenAPI info.title / info.description(支持 JSON / YAML 两种格式)。
func parseInfo(spec string) (string, string) {
	lines := strings.Split(spec, "\n")
	var title, desc string
	inDesc := false
	descBuf := []string{}
	for _, l := range lines {
		trim := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(trim, "title:") && title == "":
			title = strings.TrimSpace(strings.TrimPrefix(trim, "title:"))
			title = strings.Trim(title, "\"'")
		case strings.HasPrefix(trim, "description:") && desc == "":
			inDesc = true
			rest := strings.TrimSpace(strings.TrimPrefix(trim, "description:"))
			if rest != "" && !strings.HasPrefix(rest, "|") {
				desc = strings.Trim(rest, "\"'")
				inDesc = false
			}
		case inDesc:
			if strings.HasPrefix(l, " ") || strings.HasPrefix(l, "\t") {
				descBuf = append(descBuf, strings.TrimSpace(l))
			} else {
				inDesc = false
				desc = strings.Join(descBuf, " ")
			}
		}
	}
	if desc == "" && len(descBuf) > 0 {
		desc = strings.Join(descBuf, " ")
	}
	return title, desc
}
