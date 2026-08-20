#!/usr/bin/env bash
# matrix_docker_smoke.sh — 用真实 Prometheus 镜像做兼容性冒烟测试。
#
# 目标:验证 prom-gw 兼容 Prometheus 多个版本 + Cortex + VM agent 的 remote_write 协议。
#
# 用法:
#   bash test/compat/matrix_docker_smoke.sh                          # 默认镜像矩阵
#   PROM_IMAGES="prom/prometheus:v2.40.0" bash test/compat/matrix_docker_smoke.sh  # 自定义
#
# 前置:
#   - docker 可用
#   - prom-gw 已构建到 bin/prom-gw(或设置 PROM_GW_BIN 环境变量)
#   - 当前主机有空闲的 :19201 端口(remote_write 接入)和 :9090 端口(prometheus)
#
# 环境变量:
#   PROM_GW_BIN          prom-gw 二进制路径(默认 ./bin/prom-gw)
#   GW_PORT              prom-gw remote_write 端口(默认 19201)
#   WAL_DISK_USED_RATIO  WAL 磁盘使用率阈值(默认 0.95,冒烟测试用)
#
# 平台支持:
#   - Linux  : --network host,容器通过 127.0.0.1 访问宿主机
#   - macOS  : Docker Desktop 桥接网络 + host.docker.internal,容器通过宿主机端口映射访问
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
WAL_DISK_USED_RATIO=${WAL_DISK_USED_RATIO:-0.95}
GW_BIN_DIR=$(mktemp -d -t prom-gw-smoke.XXXXXX)
GW_LOG=$GW_BIN_DIR/prom-gw.log
GW_PID_FILE=$GW_BIN_DIR/prom-gw.pid

# 平台检测:macOS Docker Desktop 不支持 --network host,需用桥接 + host.docker.internal
OS_TYPE=$(uname -s)
if [[ "$OS_TYPE" == "Darwin" ]]; then
  HOST_GW="host.docker.internal"
  DOCKER_NET_OPTS=(-p 9090:9090)
else
  HOST_GW="127.0.0.1"
  DOCKER_NET_OPTS=(--network host)
fi

# 默认镜像矩阵
DEFAULT_IMAGES=(
  "prom/prometheus:v2.36.1"
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
echo "==> 启动 prom-gw(:$GW_PORT, WAL-only 模式, wal-disk-used-ratio=$WAL_DISK_USED_RATIO)"
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
  --wal-disk-used-ratio="$WAL_DISK_USED_RATIO" \
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
# 注意:awk 模式需匹配任意 label 顺序(Prometheus 按 label 名字母序输出)
BASELINE=$(curl -fsS "http://127.0.0.1:18080/metrics" \
  | awk '/^gateway_samples_total\{/ && /stage="parse"/ && /status="ok"/ {print $2; exit}')
BASELINE=${BASELINE:-0}
echo "==> 启动 OK,baseline samples_total=$BASELINE (platform=$OS_TYPE, host_gw=$HOST_GW)"

PASS=0
FAIL=0
SKIP=0

# ensure_image 确保镜像可用:本地有则跳过,本地无则拉取
# 避免镜像源不可达时,本地已有镜像仍被误判为 SKIP
ensure_image() {
  local image=$1
  if docker image inspect "$image" >/dev/null 2>&1; then
    return 0
  fi
  docker pull "$image" >/dev/null 2>&1
}

# generate_prom_config 生成 prometheus.yml,remote_write 指向宿主机 prom-gw
generate_prom_config() {
  local config_file=$1
  cat > "$config_file" <<EOF
global:
  scrape_interval: 1s
  evaluation_interval: 1s
remote_write:
  - url: http://$HOST_GW:$GW_PORT/api/v1/write
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
EOF
}

run_image() {
  local image=$1
  echo ""
  echo "==> 镜像: $image"

  # 确保镜像可用:本地已有则跳过拉取,避免镜像源不可达时误 SKIP
  if ! ensure_image "$image"; then
    echo "  SKIP: 镜像不可用(本地无缓存且 pull 失败)"
    SKIP=$((SKIP + 1))
    return 0
  fi

  # 判断是否为 Prometheus 官方镜像(cortex-tools / vmagent 不是 prometheus 二进制)
  if [[ "$image" != prom/prometheus:* ]]; then
    echo "  SKIP: 非 Prometheus 官方镜像,走单元测试覆盖"
    SKIP=$((SKIP + 1))
    return 0
  fi

  # 生成 prometheus 配置文件(挂载进容器,避免 heredoc 转义问题)
  local prom_config="$GW_BIN_DIR/prometheus-$(echo "$image" | tr '/:' '__').yml"
  generate_prom_config "$prom_config"

  local cid
  cid=$(docker run -d --rm \
    --label prom-gw-smoke=true \
    "${DOCKER_NET_OPTS[@]}" \
    -v "$prom_config:/etc/prometheus/prometheus.yml:ro" \
    "$image" \
    --config.file=/etc/prometheus/prometheus.yml \
    --web.listen-address=:9090 \
    --storage.tsdb.path=/tmp/data \
    2>/dev/null) || true

  if [[ -z "$cid" ]]; then
    echo "  SKIP: 容器启动失败"
    SKIP=$((SKIP + 1))
    return 0
  fi

  # 等待 prometheus 就绪(最多 15s)
  local ready=false
  for i in {1..30}; do
    if curl -fsS -m 1 "http://127.0.0.1:9090/-/healthy" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 0.5
  done

  if [[ "$ready" != "true" ]]; then
    echo "  FAIL: prometheus 启动超时"
    docker logs --tail 10 "$cid" 2>&1 | sed 's/^/    /'
    docker rm -f "$cid" >/dev/null 2>&1 || true
    FAIL=$((FAIL + 1))
    return 0
  fi

  # 等 30s 采集 + remote_write
  sleep 30

  # 检查 samples_total 是否增长(awk 模式匹配任意 label 顺序)
  local current
  current=$(curl -fsS "http://127.0.0.1:18080/metrics" \
    | awk '/^gateway_samples_total\{/ && /stage="parse"/ && /status="ok"/ {print $2; exit}')
  current=${current:-0}

  local errors
  errors=$(curl -fsS "http://127.0.0.1:18080/metrics" \
    | awk '/^gateway_errors_total\{/ {sum += $2} END {print sum+0}')

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
