"""Phase 3 cold-start benchmark.

Submits N invocations of a deliberately import-heavy function and reports
p50 / p95 / p99 of the cold_start_ms reported by the orchestrator for each.
Run with the worker started under each strategy in turn:

    # in shell 1
    ./orchestrator/orchestrator

    # in shell 2 (one of these)
    MINIMODAL_COLD_START=inproc .venv/bin/python -m worker.worker
    MINIMODAL_COLD_START=fork   .venv/bin/python -m worker.worker
    MINIMODAL_COLD_START=naive  .venv/bin/python -m worker.worker

    # in shell 3
    .venv/bin/python benchmarks/cold_start_bench.py

Or use benchmarks/run_bench.sh which automates the rotation.
"""

from __future__ import annotations

import os
import statistics
import sys
import time

from minimodal import App


def heavy_noop() -> int:
    """User function under test.

    Imports the configured heavy module (numpy or torch) and does trivial
    work. The *strategy* decides how often the import cost is paid:

      - inproc: paid once at worker startup. Per-call cost ≈ 0.
      - fork:   paid once when the warm pool starts (children inherit the
                module via copy-on-write). Per-call cost ≈ socket round trip.
      - naive:  paid every call (fresh interpreter + import).

    Module choice is read from MINIMODAL_BENCH_MODULE in the worker's env
    (via warm-pool preimport) and the user function's import line below.
    """
    import os as _os
    mod = _os.environ.get("MINIMODAL_BENCH_MODULE", "numpy")
    if mod == "torch":
        import torch
        return int(torch.arange(10).sum().item())
    import numpy as np
    return int(np.sum(np.arange(10)))


app = App("cold-start-bench")
fn = app.function(heavy_noop)


def percentile(xs: list[int], p: float) -> int:
    xs = sorted(xs)
    if not xs:
        return 0
    k = int(round((p / 100) * (len(xs) - 1)))
    return xs[k]


def main() -> int:
    strategy = os.environ.get("MINIMODAL_COLD_START_LABEL") or os.environ.get("MINIMODAL_COLD_START", "unknown")
    n = int(os.environ.get("MINIMODAL_BENCH_N", "50"))
    warmup = int(os.environ.get("MINIMODAL_BENCH_WARMUP", "5"))

    print(f"strategy={strategy} n={n} warmup={warmup}")

    # Warmup so the first naive-strategy run doesn't get penalized by, e.g.,
    # the orchestrator's first BoltDB transaction allocation.
    for _ in range(warmup):
        fn.spawn().get()

    cold_starts: list[int] = []
    execution: list[int] = []
    wall_clock: list[int] = []

    t0 = time.monotonic()
    for i in range(n):
        invoke_start = time.monotonic()
        f = fn.spawn()
        result = f.get()
        wall_ms = int((time.monotonic() - invoke_start) * 1000)
        # 0+1+...+9 = 45 for both numpy.arange and torch.arange paths.
        assert result == 45, f"unexpected result {result}"
        cold_starts.append(f.cold_start_ms)
        execution.append(f.execution_ms)
        wall_clock.append(wall_ms)
    elapsed = time.monotonic() - t0

    print()
    print(f"  invocations: {n}  total time: {elapsed:.2f}s  throughput: {n / elapsed:.1f} req/s")
    print()
    print(f"  cold_start_ms — p50={percentile(cold_starts, 50)}  p95={percentile(cold_starts, 95)}  p99={percentile(cold_starts, 99)}  mean={statistics.mean(cold_starts):.1f}  max={max(cold_starts)}")
    print(f"  execution_ms  — p50={percentile(execution, 50)}  p95={percentile(execution, 95)}  p99={percentile(execution, 99)}  mean={statistics.mean(execution):.1f}")
    print(f"  client_wall_ms— p50={percentile(wall_clock, 50)}  p95={percentile(wall_clock, 95)}  p99={percentile(wall_clock, 99)}  mean={statistics.mean(wall_clock):.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
