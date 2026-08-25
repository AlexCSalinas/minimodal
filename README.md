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
| Autoscaling | demand-driven worker launch (queue depth / saturation), idle scale-down, min/max bounds | `orchestrator/autoscaler.go` |
| Streaming | live `print()` relay worker → client, push-based result delivery (`WatchJob`), no polling | `orchestrator/watch.go`, `worker/logstream.py` |
| Consensus | Raft-replicated job store: majority commits, leader failover with zero job loss, versioned CAS transitions | `orchestrator/raft_store.go`, `orchestrator/raft_fsm.go` |
| Observability | `/metrics` JSON, dashboard with live p50/p95/p99 cold-start chart | `dashboard/`, `orchestrator/metrics.go` |
| Tests | Go unit tests; Python unit tests for the SDK + every cold-start strategy; integration tests covering SIGKILL mid-execution + reaper recovery, restart with in-flight job, idempotency across restart | `orchestrator/*_test.go`, `tests/unit/`, `tests/` |

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

```bash
make ci                # ruff + pytest + gofmt + go vet + go test -race + build
make test-integration  # boots a real orchestrator + worker
```

## Autoscaling

By default the worker pool is static (whatever you point at the orchestrator).
Set `MINIMODAL_AUTOSCALE=1` and the orchestrator manages its own fleet: it
execs worker processes on demand and reaps them when idle.

- **Scale-up** fires when jobs are waiting with nowhere to go — nonzero queue
  depth (including a job stuck in the dispatcher's retry loop), or every alive
  worker at its declared capacity. One launch per policy tick, with a cooldown
  so a burst doesn't over-spawn while workers are still booting.
- **Scale-down** reclaims one worker at a time once it has sat idle past
  `MINIMODAL_AUTOSCALE_IDLE_TIMEOUT` with an empty queue. Only workers the
  autoscaler launched are ever stopped — externally started workers (e.g.
  docker-compose replicas) count toward capacity but are never touched, so
  a static base pool composes with burst scaling on top.
- **Scale-to-zero** is the default (`MINIMODAL_AUTOSCALE_MIN=0`): an idle
  deployment runs no workers at all; the first invocation pays one worker
  boot and everything after rides the warm pool.

```bash
MINIMODAL_AUTOSCALE=1 \
MINIMODAL_WORKER_CMD=".venv/bin/python -m worker.worker" \
go run ./orchestrator
```

Knobs (env): `MINIMODAL_AUTOSCALE_MIN` / `_MAX` (0 / 4),
`_IDLE_TIMEOUT` (30s), `_COOLDOWN` (3s), `_REGISTER_TIMEOUT` (15s),
`_TICK` (500ms), plus `MINIMODAL_WORKER_CMD`, `MINIMODAL_WORKER_DIR`,
`MINIMODAL_WORKER_BASE_PORT` for how workers get exec'd.

## Live logs & streamed results

A `print()` inside your function shows up on your terminal while the function
is still running on the worker — and the result arrives as a push, not a poll:

- **Worker → orchestrator**: the worker tees each task's stdout/stderr through
  a thread-local router and ships complete lines over a client-streaming
  `StreamTaskLogs` RPC (lazily — silent functions pay nothing).
- **Orchestrator → client**: `Future.get()` consumes a server-streaming
  `WatchJob` RPC carrying status transitions, log lines, and finally the
  terminal result. The old GetJobStatus polling loop is gone.
- Logs are live-only: bounded in-memory buffers while the job runs, replayed
  to watchers that join mid-run, discarded once the job is terminal. Results
  still persist in the WAL as before.
- Executors that can't stream mid-run (naive subprocess) return captured
  output with the final report; it reaches the watcher as tail lines just
  before the result. Fork-child output currently lands in the worker's own
  log only.

Try it: `python examples/streaming_logs.py`.

## Distributed orchestrator (Raft)

Set `MINIMODAL_RAFT=1` and the job store becomes a replicated state machine:
every write commits on a majority of orchestrator nodes before it counts, so
job state — pending payloads, results, the idempotency index — survives the
loss of any minority, **including the leader**.

```bash
# Node A bootstraps a new cluster; B and C join through A's HTTP endpoint.
MINIMODAL_RAFT=1 MINIMODAL_RAFT_ID=node-a MINIMODAL_RAFT_BIND=10.0.0.1:7000 \
  MINIMODAL_RAFT_DIR=/var/minimodal/raft ./orchestrator
MINIMODAL_RAFT=1 MINIMODAL_RAFT_ID=node-b MINIMODAL_RAFT_BIND=10.0.0.2:7000 \
  MINIMODAL_RAFT_JOIN=http://10.0.0.1:8080 ./orchestrator
# GET /raft/status on any node shows state, leader, and membership.
```

Design notes:

- **Closures over consensus.** `Store.Transition` takes an arbitrary mutator
  function, which can't ride a Raft log. The leader runs the mutator against
  its local copy and replicates a compare-and-swap keyed on a per-record
  version; if another command won the race, the CAS conflicts at apply time
  and the leader re-reads and re-runs the mutator. The Store interface — and
  every caller in the scheduler/reaper/RPC layer — is unchanged.
- **Deterministic FSM.** Timestamps are stamped once by the leader at propose
  time; every node applies commands verbatim to its own embedded BoltStore,
  so replicas converge byte-for-byte. Snapshots serialize the full job table
  + idempotency index; a node joining late catches up from snapshot + log.
- **Leader-only writes.** A write on a follower returns `FailedPrecondition`
  with the leader's address in the message. Dispatch, recovery, and the
  reaper run on the leader; on failover the new leader replays unfinished
  jobs from the replicated WAL.
- **Scope.** v1 replicates the store and elects who schedules. Pointing SDK
  clients and workers at the new leader after failover is the deployment's
  job (VIP / DNS / proxy) — connection-level failover in the SDK is future
  work.

## Why fork isn't actually enough

Copy-on-write fork is fast but breaks on three real-world things:

1. **CUDA contexts can't be forked.** Kernel-side driver handles aren't inheritable. PyTorch and TensorFlow both die.
2. **File descriptors leak.** The parent's sockets and DB connections corrupt the child. This worker forks *before* opening anything.
3. **Multithreaded parents deadlock.** Only the calling thread is copied; held mutexes stay locked forever. `WarmPool.start()` must precede the gRPC server.

Which also means the worker can't refill its own pool: by the time a warm child
dies, the worker is multithreaded and can no longer fork safely. So `start()`
forks one single-threaded *spawner* process that holds the preimported pages and
forks every warm child on demand — a crashed or wedged child is replaced with an
equally warm one instead of shrinking the pool toward zero.

The real Modal trick is **CRIU + `userfaultfd`**: snapshot a warm process to disk, restore it via lazy page faults — restoring a 500 MB Python process takes ~10–15 ms wall time and amortizes the rest of the page faults across the first request. v1 stubs this in `worker/snapshot.py`; nothing in this stack runs on macOS yet.

## Stretch goals

- Real CRIU integration on Linux — target <50 ms cold start on torch + a 200 MB model
- Multi-tenant isolation (cgroups + seccomp filters per function)
