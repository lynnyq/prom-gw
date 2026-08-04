#!/usr/bin/env bash
# T5.4 性能压测一键执行 + profile 采集脚本
#
# 流程:
#   1. 构建 prom-gw(开启 pprof)
#   2. 后台启动 prom-gw(WAL-only 模式,无 Kafka)
#   3. 启动 CPU + heap profile 采集
#   4. 跑 loadgen(--rate 默认 500K samples/s,--duration 默认 60s,远低于 1.5M 目标但 CI 友好)
#   5. 抓取 final profile + 解析 /metrics
#   6. 收尾
#
# 用法:
#   bash test/perf/profile.sh
#   RATE=1500000 DURATION=5m bash test/perf/profile.sh   # 1.5M samples/s × 5m
#   KAFKA_BROKERS=localhost:9092 bash test/perf/profile.sh   # 带 Kafka

set -euo pipefail

# ---- 配置 ----
RATE="${RATE:-500000}"
DURATION="${DURATION:-60s}"
CONCURRENCY="${CONCURRENCY:-4}"
BATCH="${BATCH:-500}"
SERIES="${SERIES:-10000}"
GW_BIN="${GW_BIN:-./bin/prom-gw}"
CFG="${CFG:-./configs/rules/app-business.yaml}"
TOKENS="${TOKENS:-./configs/tokens/local.yaml}"
WAL_DIR="${WAL_DIR:-$(mktemp -d -t prom-gw-perf-wal.XXXXXX)}"
OUT_DIR="${OUT_DIR:-./perf-out/$(date +%Y%m%d-%H%M%S)}"
WRITE_PORT="${WRITE_PORT:-19201}"
METRICS_PORT="${METRICS_PORT:-8080}"
HEALTH_PORT="${HEALTH_PORT:-8081}"
ADMIN_PORT="${ADMIN_PORT:-8082}"
PPROF_PORT="${PPROF_PORT:-9090}"

mkdir -p "$OUT_DIR"
echo "OUT_DIR=$OUT_DIR"

# ---- 0. 前置 ----
echo "▶ 构建 prom-gw (带 pprof)"
if [[ ! -x "$GW_BIN" ]]; then
    go build -o "$GW_BIN" ./cmd/prom-gw
fi

# ---- 1. 启动 prom-gw ----
echo "▶ 启动 prom-gw (WAL=$WAL_DIR)"
LOG="$OUT_DIR/prom-gw.log"
"$GW_BIN" \
    --config="$CFG" \
    --tokens="$TOKENS" \
    --wal-dir="$WAL_DIR" \
    --write-addr=":${WRITE_PORT}" \
    --metrics-addr=":${METRICS_PORT}" \
    --health-addr=":${HEALTH_PORT}" \
    --admin-addr=":${ADMIN_PORT}" \
    --admin-allow-cidr="127.0.0.1/32" \
    --source-dc="dc-perf" \
    >"$LOG" 2>&1 &

GW_PID=$!
echo "prom-gw pid=$GW_PID"

cleanup() {
    if kill -0 "$GW_PID" 2>/dev/null; then
        echo "▶ 停止 prom-gw"
        kill -TERM "$GW_PID" 2>/dev/null || true
        wait "$GW_PID" 2>/dev/null || true
    fi
    if [[ "${KEEP_WAL:-0}" != "1" ]]; then
        rm -rf "$WAL_DIR"
    fi
}
trap cleanup EXIT INT TERM

# 等启动
echo "▶ 等待启动..."
for i in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:${HEALTH_PORT}/healthz" >/dev/null 2>&1; then
        echo "  ✓ 启动成功"
        break
    fi
    if ! kill -0 "$GW_PID" 2>/dev/null; then
        echo "✗ prom-gw 异常退出" >&2
        cat "$LOG" >&2
        exit 1
    fi
    sleep 0.2
done

# ---- 2. CPU profile 后台采集 ----
echo "▶ 启动 CPU profile 采集(30s CPU 采样)"
CPU_PROFILE="$OUT_DIR/cpu.pprof"
go tool pprof -proto -seconds="$DURATION" "http://127.0.0.1:${METRICS_PORT}/debug/pprof/profile?seconds=$DURATION" >"$CPU_PROFILE" 2>"$OUT_DIR/cpu.pprof.log" &
PPROF_CPU_PID=$!

# ---- 3. 跑 loadgen ----
echo "▶ 跑 loadgen: rate=$RATE samples/s duration=$DURATION"
LOADGEN_LOG="$OUT_DIR/loadgen.log"
go run ./test/loadgen \
    --url="http://127.0.0.1:${WRITE_PORT}/api/v1/write" \
    --token="tk_app_business_dev" \
    --rate="$RATE" \
    --samples-per-batch="$BATCH" \
    --duration="$DURATION" \
    --concurrency="$CONCURRENCY" \
    --series-count="$SERIES" \
    --metrics-url="http://127.0.0.1:${METRICS_PORT}/metrics" \
    2>&1 | tee "$LOADGEN_LOG"

# ---- 4. Heap profile ----
echo "▶ Heap profile 采集"
HEAP_PROFILE="$OUT_DIR/heap.pprof"
curl -fsS "http://127.0.0.1:${METRICS_PORT}/debug/pprof/heap" -o "$HEAP_PROFILE" 2>/dev/null || echo "  (heap profile 不可用,跳过)"

# ---- 5. 抓 /metrics 全量 ----
echo "▶ 抓 /metrics 全量"
curl -fsS "http://127.0.0.1:${METRICS_PORT}/metrics" -o "$OUT_DIR/metrics.txt"

# 抓 GW stats(admin API)
curl -fsS "http://127.0.0.1:${ADMIN_PORT}/v1/stats" -o "$OUT_DIR/admin-stats.json" || true

# ---- 6. 汇总 ----
wait "$PPROF_CPU_PID" 2>/dev/null || true

echo
echo "=== Perf 报告 ==="
echo "GW log       : $LOG"
echo "loadgen log  : $LOADGEN_LOG"
echo "CPU profile  : $CPU_PROFILE"
echo "Heap profile : $HEAP_PROFILE"
echo "metrics      : $OUT_DIR/metrics.txt"

if [[ -f "$CPU_PROFILE" && -s "$CPU_PROFILE" ]]; then
    echo
    echo "--- CPU profile top 20 ---"
    go tool pprof -top -cum -nodecount=20 "$CPU_PROFILE" 2>/dev/null | head -40 || true
fi
if [[ -f "$HEAP_PROFILE" && -s "$HEAP_PROFILE" ]]; then
    echo
    echo "--- Heap profile top 20 ---"
    go tool pprof -top -nodecount=20 -sample_index=alloc_space "$HEAP_PROFILE" 2>/dev/null | head -40 || true
fi
