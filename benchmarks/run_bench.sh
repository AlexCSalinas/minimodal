#!/usr/bin/env bash
# Phase 3 cold-start bench harness. Starts the orchestrator once, then cycles
# through each strategy, restarting only the worker between runs. Results are
# written to benchmarks/results/ for diffing.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

mkdir -p benchmarks/results

PY=${PYTHON:-".venv/bin/python"}
ORCH_LOG=/tmp/minimodal-orch.log
WORKER_LOG=/tmp/minimodal-worker.log

pkill -f orchestrator/orchestrator 2>/dev/null || true
pkill -f "worker.worker"            2>/dev/null || true
sleep 0.3

rm -f minimodal.db
./orchestrator/orchestrator > "$ORCH_LOG" 2>&1 &
ORCH_PID=$!
sleep 1
trap "kill $ORCH_PID 2>/dev/null || true" EXIT

STRATEGIES=${STRATEGIES:-"inproc fork naive"}
N=${MINIMODAL_BENCH_N:-50}

for strategy in $STRATEGIES; do
    echo "==================================================="
    echo "strategy=$strategy"
    echo "==================================================="
    pkill -f "worker.worker" 2>/dev/null || true
    sleep 0.5

    # Forward MINIMODAL_BENCH_MODULE + MINIMODAL_PREIMPORT to the worker so
    # the fork strategy pre-imports the right heavy module.
    MINIMODAL_COLD_START=$strategy \
    MINIMODAL_PREIMPORT="${MINIMODAL_PREIMPORT:-}" \
    MINIMODAL_BENCH_MODULE="${MINIMODAL_BENCH_MODULE:-numpy}" \
    PYTHONPATH="$ROOT/worker:$ROOT/sdk" \
    ORCH_ADDR=127.0.0.1:50051 \
    WORKER_ADVERTISE_HOST=127.0.0.1 \
    WORKER_ID="bench-$strategy" \
        $PY -m worker.worker > "$WORKER_LOG.$strategy" 2>&1 &
    WORKER_PID=$!
    # torch takes a second or two to import on first warm-pool start
    sleep 3.0

    PYTHONPATH="$ROOT/sdk" \
    MINIMODAL_ORCH_ADDR=127.0.0.1:50051 \
    MINIMODAL_COLD_START_LABEL=$strategy \
    MINIMODAL_BENCH_N=$N \
    MINIMODAL_BENCH_MODULE="${MINIMODAL_BENCH_MODULE:-numpy}" \
        $PY benchmarks/cold_start_bench.py | tee "benchmarks/results/$strategy.txt"

    kill $WORKER_PID 2>/dev/null || true
    sleep 0.3
done

echo
echo "==================================================="
echo "summary"
echo "==================================================="
for strategy in $STRATEGIES; do
    if [ -f "benchmarks/results/$strategy.txt" ]; then
        echo "--- $strategy ---"
        cat "benchmarks/results/$strategy.txt"
        echo
    fi
done
