# minimodal

[![ci](https://github.com/AlexCSalinas/minimodal/actions/workflows/ci.yml/badge.svg)](https://github.com/AlexCSalinas/minimodal/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> A from-scratch reimplementation of [Modal's](https://modal.com) serverless
> function runtime. Go orchestrator + Python worker pool, ~3K lines.
>
> **Headline:** 11× throughput speedup on torch workloads (1.4 → 15.9 req/s)
> by replacing naive subprocess cold-start with copy-on-write fork from a
> pre-warmed parent.

## What is this

If you've used Modal: you decorate a Python function with `@app.function` and
it runs in the cloud in ~100ms, fresh container included. That's not clever
caching — it's a stack of systems tricks: process snapshotting, fork-based
pre-warming, lazy page faulting, custom container runtime.

This rebuilds the core of that stack on one box, in a form you can read in
an afternoon.

## The benchmark

30 invocations of a function whose body imports `torch` and does a trivial reduce. Apple Silicon, Python 3.13.

| strategy             | cold p50 | cold p99 | throughput |
|----------------------|---------:|---------:|-----------:|
| naive (subprocess)   |  164 ms  |  231 ms  |  1.4 req/s |
| **fork (warm pool)** |   **0**  |    **1** | **15.9 req/s** |

**Why fork wins:** the warm-pool parent has `torch` already resident. Children inherit it via copy-on-write, so `import torch` inside the user function becomes a `sys.modules` lookup — not 480 ms of reading from disk. This is the actual reason serverless ML platforms can't just "spawn a container": Python's import cost dominates, and no container runtime can outrun it.

Full table (including numpy) in `benchmarks/`.

## Architecture

Three tiers, one box. A call flows: **SDK → orchestrator → worker → back**.

- **Python SDK** — `@app.function` decorator cloudpickle-serializes the call and sends it over gRPC. Returns a `Future`.
- **Go orchestrator** (`:50051` gRPC, `:8080` HTTP) — async dispatch queue, job state machine (PENDING → RUNNING → DONE/FAILED), heartbeat-based worker reaper, BoltDB WAL so jobs survive crashes, live metrics dashboard.
- **Python worker pool** — pluggable cold-start strategy (`inproc` / `fork`-from-warm-pool / `naive` subprocess), cloudpickle deserialize + execute, CRIU checkpoint/restore hooks stubbed for v2.

## What's in here

| Layer | What | Where |
|---|---|---|
| Cold-start | inproc, copy-on-write fork from warm pool, naive subprocess, stubbed CRIU | `worker/` |
| Fault tolerance | At-least-once delivery, WAL replay across orchestrator crashes, heartbeat-based worker reaping, CAS state transitions | `orchestrator/job_store.go` |
| Idempotency | Caller-supplied keys → effective exactly-once, result bytes persisted across restarts | `orchestrator/server.go` |
| Observability | `/metrics` JSON, dashboard with live p50/p95/p99 cold-start chart | `dashboard/`, `orchestrator/metrics.go` |
| Tests | SIGKILL mid-execution + reaper recovery, restart with in-flight job, idempotency across restart | `tests/` |

## Quickstart

```bash
make tools && python3 -m venv .venv
.venv/bin/pip install grpcio grpcio-tools cloudpickle protobuf
PYTHON=.venv/bin/python make proto
make run   # docker-compose: 1 orch + 4 workers; dashboard at :8080
```

```python
from minimodal import App
app = App("hello")

@app.function
def greet(name: str) -> str:
    return f"hi {name}"

print(greet.remote("alex"))   # → "hi alex"
```

More examples in `examples/` (parallel map, numpy matmul).

## Why fork isn't actually enough

Copy-on-write fork is fast but breaks on three real-world things:

1. **CUDA contexts can't be forked.** Kernel-side driver handles aren't inheritable. PyTorch and TensorFlow both die.
2. **File descriptors leak.** The parent's sockets and DB connections corrupt the child. This worker forks *before* opening anything.
3. **Multithreaded parents deadlock.** Only the calling thread is copied; held mutexes stay locked forever. `WarmPool.start()` must precede the gRPC server.

The real Modal trick is **CRIU + `userfaultfd`**: snapshot a warm process to disk, restore it via lazy page faults — restoring a 500 MB Python process takes ~10–15 ms wall time and amortizes the rest of the page faults across the first request. v1 stubs this in `worker/snapshot.py`; nothing in this stack runs on macOS yet.

## Stretch goals

- Real CRIU integration on Linux — target <50 ms cold start on torch + a 200 MB model
- Multi-tenant isolation (cgroups + seccomp filters per function)
- Distributed orchestrator (Raft consensus on the job store)
- Streaming results over gRPC
