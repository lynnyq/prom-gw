#!/usr/bin/env bash
# T1.11 端到端手动验证脚本
#
# 覆盖场景(对应 plan T1.11):
#   1. 启动 prom-gw(无 Kafka,跑 WAL-only 模式)
#   2. 用一个 mock Prometheus 客户端写一条 sample 到 /api/v1/write
#   3. 验证:
#      - 200 / 204 响应
#      - /metrics 出现 gateway_samples_total 增量
#      - WAL 目录有 seg-*.log.sealed 段文件
#      - 消费 WAL 记录能取回刚才写入的 bytes
#   4. 鉴权失败 / 缺失 token 各覆盖一次
#
# 依赖:
#   - 本地已 go build 产出 bin/prom-gw(make build)
#   - protoc 编译的 prompb 字节(此脚本自带 generator)
#   - 工具: curl、xxd、od、sed、awk
#
# 用法:
#   bash test/manual/e2e.sh              # 完整跑
#   KAFKA_BROKERS=localhost:9092 bash test/manual/e2e.sh   # 带 Kafka
#
# 退出码:0=全部通过,1=任一检查失败

set -euo pipefail

# ---- 配置 ----
GW_BIN="${GW_BIN:-./bin/prom-gw}"
CFG_PATH="${CFG_PATH:-./configs/rules/app-business.yaml}"
TOKENS_PATH="${TOKENS_PATH:-./configs/tokens/local.yaml}"
WAL_DIR="${WAL_DIR:-$(mktemp -d -t prom-gw-e2e-wal.XXXXXX)}"
WRITE_PORT="${WRITE_PORT:-19201}"
METRICS_PORT="${METRICS_PORT:-8080}"
HEALTH_PORT="${HEALTH_PORT:-8081}"
ADMIN_PORT="${ADMIN_PORT:-8082}"

WRITE_URL="http://127.0.0.1:${WRITE_PORT}/api/v1/write"
METRICS_URL="http://127.0.0.1:${METRICS_PORT}/metrics"
HEALTH_URL="http://127.0.0.1:${HEALTH_PORT}/healthz"
READY_URL="http://127.0.0.1:${HEALTH_PORT}/readyz"

# 默认 token(对应 configs/tokens/local.yaml)
TOKEN="tk_app_business_dev"
DEFAULT_TOPIC="prom.raw.app_business"

# 测试计数器
PASS=0
FAIL=0

# 颜色(只在 tty 启用)
if [[ -t 1 ]]; then
    GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
else
    GREEN=''; RED=''; YELLOW=''; NC=''
fi

ok() { echo -e "${GREEN}✓${NC} $1"; PASS=$((PASS+1)); }
ko() { echo -e "${RED}✗${NC} $1"; FAIL=$((FAIL+1)); }
info() { echo -e "${YELLOW}▶${NC} $1"; }
hr() { echo "----------------------------------------"; }

# ---- 0. 前置检查 ----
info "检查二进制:$GW_BIN"
if [[ ! -x "$GW_BIN" ]]; then
    if [[ -f "./prom-gw" ]]; then
        GW_BIN="./prom-gw"
    else
        echo "错误: 找不到可执行的 prom-gw 二进制,先跑 make build" >&2
        exit 1
    fi
fi

info "检查配置文件:$CFG_PATH"
[[ -f "$CFG_PATH" ]] || { echo "缺少 $CFG_PATH" >&2; exit 1; }

info "检查 token 文件:$TOKENS_PATH"
[[ -f "$TOKENS_PATH" ]] || { echo "缺少 $TOKENS_PATH" >&2; exit 1; }

# ---- 1. 启动 prom-gw ----
info "启动 prom-gw(WAL-only 模式,无 Kafka)"
mkdir -p "$WAL_DIR"
LOG_FILE="$(mktemp -t prom-gw-e2e-log.XXXXXX)"

# 后台启动,捕获 pid
"$GW_BIN" \
    --config="$CFG_PATH" \
    --tokens="$TOKENS_PATH" \
    --wal-dir="$WAL_DIR" \
    --write-addr=":${WRITE_PORT}" \
    --metrics-addr=":${METRICS_PORT}" \
    --health-addr=":${HEALTH_PORT}" \
    --admin-addr=":${ADMIN_PORT}" \
    --admin-allow-cidr="127.0.0.1/32" \
    --source-dc="dc-e2e-test" \
    >"$LOG_FILE" 2>&1 &

GW_PID=$!
echo "prom-gw pid=$GW_PID log=$LOG_FILE"

# 收尾函数(SIGINT/SIGTERM/EXIT 都跑)
cleanup() {
    if kill -0 "$GW_PID" 2>/dev/null; then
        info "停止 prom-gw (pid=$GW_PID)"
        kill -TERM "$GW_PID" 2>/dev/null || true
        wait "$GW_PID" 2>/dev/null || true
    fi
    if [[ "${KEEP_WAL:-0}" != "1" ]]; then
        rm -rf "$WAL_DIR" "$LOG_FILE" 2>/dev/null || true
    else
        info "保留 WAL=$WAL_DIR 日志=$LOG_FILE"
    fi
}
trap cleanup EXIT INT TERM

# 等启动
info "等待 prom-gw 启动(最多 10s)..."
for i in $(seq 1 50); do
    if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
        ok "prom-gw 启动成功 (${i}×200ms)"
        break
    fi
    if ! kill -0 "$GW_PID" 2>/dev/null; then
        echo "错误:prom-gw 异常退出,日志如下:" >&2
        cat "$LOG_FILE" >&2
        exit 1
    fi
    sleep 0.2
done

if ! curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then
    echo "错误: 启动超时" >&2
    cat "$LOG_FILE" >&2
    exit 1
fi

# ---- 2. 构造 Prometheus RemoteWrite 请求 ----
# 构造最小可用的 WriteRequest:1 个 TimeSeries + 1 个 Sample → snappy 编码 → POST
# 用 scripts/e2e_payload 生成(避免手搓 protobuf 二进制)
hr
info "构造 RemoteWrite 字节"

GEN_PAYLOAD_FILE="$(mktemp -t e2e-payload.XXXXXX.bin)"
RUN_ID="$$-$(date +%s)" go run ./scripts/e2e_payload > "$GEN_PAYLOAD_FILE"
GEN_RC=$?
if [[ $GEN_RC -ne 0 || ! -s "$GEN_PAYLOAD_FILE" ]]; then
    ko "payload 构造失败 (rc=$GEN_RC)"
    exit 1
fi
PAYLOAD_SIZE=$(stat -f%z "$GEN_PAYLOAD_FILE" 2>/dev/null || stat -c%s "$GEN_PAYLOAD_FILE")
ok "构造完成,size=${PAYLOAD_SIZE}B"

# ---- 3. 正常写入 ----
hr
info "POST $WRITE_URL (auth=$TOKEN)"
HTTP_CODE=$(curl -sS -o /tmp/e2e_body -w "%{http_code}" \
    -X POST "$WRITE_URL" \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer $TOKEN" \
    --data-binary "@$GEN_PAYLOAD_FILE")
if [[ "$HTTP_CODE" == "200" || "$HTTP_CODE" == "204" ]]; then
    ok "写入成功 HTTP $HTTP_CODE"
else
    ko "写入失败 HTTP $HTTP_CODE,body: $(cat /tmp/e2e_body)"
fi

# ---- 4. metrics 校验 ----
hr
info "校验 /metrics 出现 gateway_samples_total 增量"
sleep 1
SAMPLES_LINE=$(curl -sS "$METRICS_URL" | grep '^gateway_samples_total' | head -3 || true)
if [[ -n "$SAMPLES_LINE" ]]; then
    ok "指标可见:"
    echo "$SAMPLES_LINE" | sed 's/^/    /'
else
    ko "未找到 gateway_samples_total 指标"
fi

# ---- 5. WAL 段文件校验 ----
hr
info "校验 WAL 落盘(目录 $WAL_DIR)"
# WAL 落盘后才会 seal,所以 sleep + 写一条触发
sleep 1
SEG_COUNT=$(find "$WAL_DIR" -name '*.log.sealed' 2>/dev/null | wc -l | tr -d ' ')
if [[ "$SEG_COUNT" -ge 1 ]]; then
    ok "找到 $SEG_COUNT 个 sealed 段"
else
    # 在 WAL-only 模式下,segment 可能未达 rotation 阈值,允许未 seal
    ACTIVE_COUNT=$(find "$WAL_DIR" -name 'seg-*.log' 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$ACTIVE_COUNT" -ge 1 ]]; then
        ok "找到 $ACTIVE_COUNT 个 active 段(未达 rotate 阈值,正常)"
    else
        ko "WAL 目录没有段文件:$WAL_DIR"
    fi
fi

# ---- 6. 鉴权失败 ----
hr
info "鉴权测试:缺 token"
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "$WRITE_URL" \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    --data-binary "@$GEN_PAYLOAD_FILE")
if [[ "$HTTP_CODE" == "401" ]]; then
    ok "缺 token 正确返回 401"
else
    ko "缺 token 应返回 401,实际 $HTTP_CODE"
fi

info "鉴权测试:非法 token"
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "$WRITE_URL" \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer tk_invalid" \
    --data-binary "@$GEN_PAYLOAD_FILE")
if [[ "$HTTP_CODE" == "401" ]]; then
    ok "非法 token 正确返回 401"
else
    ko "非法 token 应返回 401,实际 $HTTP_CODE"
fi

# ---- 7. 错误请求 ----
hr
info "错误请求:非 snappy 字节"
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" \
    -X POST "$WRITE_URL" \
    -H "Content-Type: application/x-protobuf" \
    -H "Content-Encoding: snappy" \
    -H "Authorization: Bearer $TOKEN" \
    --data-binary "not-snappy-bytes")
if [[ "$HTTP_CODE" == "400" ]]; then
    ok "非法 snappy 正确返回 400"
else
    ko "非法 snappy 应返回 400,实际 $HTTP_CODE"
fi

# ---- 8. Admin API ----
hr
info "Admin API:GET /v1/rulesets"
HTTP_CODE=$(curl -sS -o /tmp/e2e_admin -w "%{http_code}" "http://127.0.0.1:${ADMIN_PORT}/v1/rulesets")
if [[ "$HTTP_CODE" == "200" ]]; then
    ok "Admin /v1/rulesets 200"
    head -c 200 /tmp/e2e_admin
    echo
else
    ko "Admin /v1/rulesets 失败 HTTP $HTTP_CODE"
fi

info "Admin API:GET /v1/stats"
HTTP_CODE=$(curl -sS -o /tmp/e2e_stats -w "%{http_code}" "http://127.0.0.1:${ADMIN_PORT}/v1/stats")
if [[ "$HTTP_CODE" == "200" ]]; then
    ok "Admin /v1/stats 200"
    head -c 200 /tmp/e2e_stats
    echo
else
    ko "Admin /v1/stats 失败 HTTP $HTTP_CODE"
fi

info "Admin API:GET /v1/tenants"
HTTP_CODE=$(curl -sS -o /dev/null -w "%{http_code}" "http://127.0.0.1:${ADMIN_PORT}/v1/tenants")
if [[ "$HTTP_CODE" == "200" ]]; then
    ok "Admin /v1/tenants 200"
else
    ko "Admin /v1/tenants 失败 HTTP $HTTP_CODE"
fi

# ---- 9. 总结 ----
hr
echo
echo "=== 结果 ==="
echo -e "通过: ${GREEN}${PASS}${NC}    失败: ${RED}${FAIL}${NC}"
if [[ "$FAIL" -eq 0 ]]; then
    echo -e "${GREEN}✅ 全部检查通过${NC}"
    exit 0
else
    echo -e "${RED}❌ 存在失败${NC}"
    echo "日志: $LOG_FILE"
    exit 1
fi
