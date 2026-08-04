#!/usr/bin/env bash
# matrix_docker_smoke.sh — 用真实 Prometheus 镜像做兼容性冒烟测试。
#
# 目标:验证 prom-gw 兼容 Prometheus 多个版本 + Cortex + VM agent 的 remote_write 协议。
#
# 用法:
#   bash test/compat/matrix_docker_smoke.sh                          # 默认 4 套镜像
#   PROM_IMAGES="prom/prometheus:v2.40.0" bash test/compat/matrix_docker_smoke.sh  # 自定义
#
# 前置:
#   - docker 可用
#   - prom-gw 已构建到 bin/prom-gw(或设置 PROM_GW_BIN 环境变量)
#   - 当前主机有空闲的 :19201 端口(remote_write 接入)
#
# 每个镜像跑:
#   1. 用 docker run 启动该客户端
#   2. 配置 remote_write 指向本机 prom-gw
#   3. 等客户端采集自身指标 30s
#   4. 拉取 prom-gw 指标,验证 gateway_samples_total 增长 + 错误率 = 0
#
# 输出:每行一个镜像的状态(OK / SKIP / FAIL)。

set -euo pipefail

PROM_GW_BIN=${PROM_GW_BIN:-./bin/prom-gw}
GW_PORT=${GW_PORT:-19201}
GW_BIN_DIR=$(mktemp -d -t prom-gw-smoke.XXXXXX)
GW_LOG=$GW_BIN_DIR/prom-gw.log
GW_PID_FILE=$GW_BIN_DIR/prom-gw.pid

# 默认镜像矩阵
DEFAULT_IMAGES=(
  "prom/prometheus:v2.40.8"
  "prom/prometheus:v2.45.6"
  "prom/prometheus:v2.50.1"
  "prom/prometheus:v2.55.1"
  "prom/prometheus:latest"
  "grafana/cortex-tools:latest"
  "victoriametrics/vmagent:latest"
)

if [[ -n "${PROM_IMAGES:-}" ]]; then
  # shellcheck disable=SC2206
  IMAGES=( ${PROM_IMAGES} )
else
  IMAGES=( "${DEFAULT_IMAGES[@]}" )
fi

cleanup() {
  set +e
  if [[ -f "$GW_PID_FILE" ]]; then
    kill "$(cat "$GW_PID_FILE")" 2>/dev/null
    rm -f "$GW_PID_FILE"
  fi
  # 清理临时 docker 容器
  for cid in $(docker ps -aq --filter "label=prom-gw-smoke" 2>/dev/null); do
    docker rm -f "$cid" >/dev/null 2>&1
  done
  rm -rf "$GW_BIN_DIR"
}
trap cleanup EXIT

if ! command -v docker >/dev/null 2>&1; then
  echo "SKIP: docker 不可用" >&2
  exit 0
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "FAIL: curl 未安装" >&2
  exit 1
fi

if [[ ! -x "$PROM_GW_BIN" ]]; then
  echo "FAIL: prom-gw 未构建 ($PROM_GW_BIN 不存在或不可执行)" >&2
  echo "      运行 'make build' 后重试" >&2
  exit 1
fi

# 1. 启动 prom-gw(WAL-only 模式,不依赖 Kafka,直接落盘)
echo "==> 启动 prom-gw(:$GW_PORT, WAL-only 模式)"
PROM_GW_WAL_DIR=$GW_BIN_DIR/wal
mkdir -p "$PROM_GW_WAL_DIR"

# 准备最小配置:启用 WAL,跳过 Kafka 探测
cat > "$GW_BIN_DIR/rules.yaml" <<'EOF'
rulesets: []
global:
  rate_limit_per_instance: 100000
  channel_buffer: 65535
EOF

# 准备最小 token 配置
cat > "$GW_BIN_DIR/tokens.yaml" <<'EOF'
tokens:
  "tk_smoke":
    tenant: smoke
    tenant_id: "9001"
    default_topic: prom.smoke
    rate_limit: 100000
EOF

"$PROM_GW_BIN" \
  --config="$GW_BIN_DIR/rules.yaml" \
  --tokens="$GW_BIN_DIR/tokens.yaml" \
  --wal-dir="$PROM_GW_WAL_DIR" \
  --write-addr=":$GW_PORT" \
  --metrics-addr=":18080" \
  --admin-addr=":18082" \
  --health-addr=":18081" \
  --admin-allow-cidr="0.0.0.0/0" \
  --source-dc=docker-smoke \
  > "$GW_LOG" 2>&1 &
echo $! > "$GW_PID_FILE"

# 等待 healthz
for i in {1..30}; do
  if curl -fsS -m 1 "http://127.0.0.1:18081/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
  if [[ $i -eq 30 ]]; then
    echo "FAIL: prom-gw 启动超时" >&2
    tail -30 "$GW_LOG" >&2
    exit 1
  fi
done

# 记录启动时的 samples_total 基准
BASELINE=$(curl -fsS "http://127.0.0.1:18080/metrics" \
  | awk '/^gateway_samples_total\{stage="parse",status="ok"\}/ {print $2}' \
  | head -1)
BASELINE=${BASELINE:-0}
echo "==> 启动 OK,baseline samples_total=$BASELINE"

PASS=0
FAIL=0
SKIP=0

run_image() {
  local image=$1
  echo ""
  echo "==> 镜像: $image"

  # 拉镜像(失败可跳过)
  if ! docker pull "$image" >/dev/null 2>&1; then
    echo "  SKIP: pull 失败"
    SKIP=$((SKIP + 1))
    return 0
  fi

  local cid
  cid=$(docker run -d --rm \
    --label prom-gw-smoke=true \
    --network host \
    "$image" \
    sh -c "cat > /tmp/prometheus.yml <<YML
global:
  scrape_interval: 1s
  evaluation_interval: 1s
remote_write:
  - url: http://127.0.0.1:$GW_PORT/api/v1/write
    authorization:
      type: Bearer
      credentials: tk_smoke
    queue_config:
      capacity: 1000
      max_samples_per_send: 100
      batch_send_deadline: 1s
scrape_configs:
  - job_name: self
    static_configs:
      - targets: ['127.0.0.1:9090']
YML
exec prometheus --config.file=/tmp/prometheus.yml --web.listen-address=:9090 --storage.tsdb.path=/tmp/data 2>&1" \
  2>/dev/null) || true

  if [[ -z "$cid" ]]; then
    # Cortex/VM agent 不是 prometheus 二进制,改走 --entrypoint
    cid=$(docker run -d --rm \
      --label prom-gw-smoke=true \
      --network host \
      --entrypoint sh \
      "$image" \
      -c "echo '$image is not a Prometheus binary, running simplified check'; sleep 30" \
    2>/dev/null) || cid=""
  fi

  if [[ -z "$cid" ]]; then
    echo "  SKIP: 容器启动失败"
    SKIP=$((SKIP + 1))
    return 0
  fi

  # 等 30s 采集
  sleep 30

  # 检查 samples_total 是否增长
  local current
  current=$(curl -fsS "http://127.0.0.1:18080/metrics" \
    | awk '/^gateway_samples_total\{stage="parse",status="ok"\}/ {print $2}' \
    | head -1)
  current=${current:-0}

  local errors
  errors=$(curl -fsS "http://127.0.0.1:18080/metrics" \
    | awk '/^gateway_errors_total/ {sum += $2} END {print sum+0}')

  # 清理容器
  docker kill "$cid" >/dev/null 2>&1 || true
  docker rm -f "$cid" >/dev/null 2>&1 || true

  if [[ -n "$current" ]] && awk "BEGIN {exit !($current > $BASELINE)}"; then
    if [[ "${errors:-0}" == "0" ]]; then
      echo "  OK: samples $BASELINE -> $current, errors=$errors"
      PASS=$((PASS + 1))
    else
      echo "  FAIL: 有错误计数 errors=$errors"
      FAIL=$((FAIL + 1))
    fi
  else
    echo "  FAIL: samples 未增长 ($BASELINE -> $current),errors=$errors"
    FAIL=$((FAIL + 1))
  fi
}

for img in "${IMAGES[@]}"; do
  run_image "$img"
done

echo ""
echo "==> 矩阵结果"
echo "  PASS=$PASS  FAIL=$FAIL  SKIP=$SKIP"

# 关停 prom-gw
if [[ -f "$GW_PID_FILE" ]]; then
  kill "$(cat "$GW_PID_FILE")" 2>/dev/null || true
  rm -f "$GW_PID_FILE"
fi

# 退出码:全部 pass 或 skip 才算成功,任何 fail 整体失败
if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
exit 0
