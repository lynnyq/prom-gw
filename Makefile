SHELL := /bin/bash
GO    ?= /usr/local/go/bin/go
PKG   := github.com/lynnyq/bigdata
BIN   := bin/prom-gw
PKGS  := ./...

# 在 sandbox/受限环境(goenv GOCACHE 不可写)下,强制把缓存重定向到 /tmp。
# 本机正常使用可注释这两行。
export GOCACHE    ?= /tmp/gocache-prom-gw
export GOMODCACHE ?= /tmp/gomodcache

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -X main.version=$(VERSION) -s -w

.PHONY: all build run test test-integration test-loadgen lint fmt clean proto codegen docs perf chaos release help

all: lint test build

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

build: ## 编译到 bin/prom-gw
	@mkdir -p bin
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BIN) ./cmd/prom-gw

run: build ## 启动(默认配置 configs/rules/default.yaml)
	./$(BIN) --config=configs/rules/default.yaml --tokens=configs/tokens/local.yaml

test: ## 跑单元测试 + 覆盖率
	$(GO) test -race -count=1 -coverprofile=coverage.out $(PKGS)
	@$(GO) tool cover -func=coverage.out | tail -1

test-integration: ## 跑集成测试(testcontainers,需要 Docker)
	INTEGRATION=1 $(GO) test -race -count=1 -tags=integration ./test/integration/...

test-loadgen: build ## 启动自研 loadgen 压测
	$(GO) run ./test/loadgen --rate=50000 --duration=30s

lint: ## golangci-lint
	golangci-lint run $(PKGS)

fmt: ## gofmt + goimports
	gofmt -w -s .
	goimports -w -local $(PKG) .

proto: ## 生成 protobuf
	@if command -v buf >/dev/null 2>&1; then \
		buf generate api/proto; \
	else \
		echo "buf 未安装,跳过(后续 T1.1 实施时再装)"; \
	fi

codegen: ## OpenAPI -> chi router/types
	@echo "(T4.7 实施时启用)"

docs: ## 渲染 API 文档站
	@echo "(T4.7 实施时启用)"

perf: build ## 性能压测(1.5M samples/s × 1h,默认 30s 冒烟)
	$(GO) run ./test/perf --duration=30s

chaos: ## 混沌测试
	bash test/chaos/run.sh

release: lint test build ## 打包发布
	@mkdir -p dist
	@tar -czf dist/prom-gw-$(VERSION).tar.gz -C bin prom-gw
	@echo "release: dist/prom-gw-$(VERSION).tar.gz"

clean: ## 清理构建产物
	rm -rf bin dist coverage.out
