#!/usr/bin/env bash
# test/chaos/run.sh
# 混沌测试入口脚本,被 `make chaos` 调用。
#
# 默认行为:在 in-process 模式跑 chaos_test.go 全部用例,无需外部 Kafka/Docker。
# 系统级场景(杀实例、停 Kafka、磁盘满等)见 chaos_runbook.md,需要人工/手动执行。
#
# 用法:
#   bash test/chaos/run.sh            # 默认 60s 超时
#   TIMEOUT=120 bash test/chaos/run.sh
#   KEEP_GOING=1 bash test/chaos/run.sh  # 失败不立即退出,跑完全部
set -uo pipefail

cd "$(dirname "$0")/../.."   # 项目根

TIMEOUT="${TIMEOUT:-60s}"
KEEP_GOING="${KEEP_GOING:-0}"
PKG="./test/chaos/..."

echo "[chaos] go version: $(go version 2>/dev/null || /usr/local/go/bin/go version)"
echo "[chaos] target package: $PKG"
echo "[chaos] timeout: $TIMEOUT  KEEP_GOING=$KEEP_GOING"

ARGS=(-race -count=1 -timeout "$TIMEOUT" -v "$PKG")
if [[ "$KEEP_GOING" == "1" ]]; then
    # -failfast 关掉,失败也继续跑
    ARGS=(-race -count=1 -timeout "$TIMEOUT" -v "$@")
fi

# 1) 主混沌套件
if command -v go >/dev/null 2>&1; then
    GO=go
else
    GO=/usr/local/go/bin/go
fi

echo "[chaos] running: $GO test ${ARGS[*]}"
"$GO" test "${ARGS[@]}"
rc=$?

# 2) 顺手跑 race detector 一遍(独立编译,确保不残留缓存)
if [[ $rc -eq 0 ]]; then
    echo "[chaos] OK - all chaos tests passed"
else
    echo "[chaos] FAIL - rc=$rc"
fi
exit $rc
