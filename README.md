# minimodal

> A from-scratch Modal-style serverless function runtime, in ~3K lines of Go + Python.
> **v1 shipped.** 5 phases, end-to-end, with real benchmarks.

## Why this exists

If you've used [Modal](https://modal.com), you know the magic trick: slap
`@app.function` on a Python function and it runs in the cloud in ~100ms,
fresh container included. That's not just clever caching. It's a stack of
systems tricks: process snapshotting, memory mapping, lazy page faulting,
fork-based pre-warming.

This project rebuilds the core of that stack from scratch, on one box,
in a form you can read end-to-end in an afternoon.

## Contents

- [Architecture](#architecture)
- [Quickstart](#quickstart)
- [Roadmap](#roadmap)
- [The headline: 11× cold-start speedup](#the-headline-11-cold-start-speedup)
- [How it works](#how-it-works)
- [Fault tolerance](#fault-tolerance)
- [Observability](#observability)
- [Idempotency](#idempotency)
- [CRIU on Linux: the spec](#criu-on-linux-the-spec)
- [Stretch goals](#stretch-goals)

---

## Architecture

```
┌─────────────────────────────────────────────┐
│  Python SDK (@app.function decorator)       │
│  - cloudpickle serializes function + args   │
│  - InvokeFunction gRPC → returns Future     │
└────────────────────┬────────────────────────┘
                     │ gRPC :50051
┌────────────────────▼────────────────────────┐
│  Go Orchestrator                            │
│  - Async dispatch queue + scheduler         │
│  - Job state machine (PENDING→RUNNING→DONE) │
│  - Heartbeat fault detection + reaper       │
│  - BoltDB WAL for at-least-once delivery    │
│  - HTTP /metrics + dashboard on :8080       │
└────────────────────┬────────────────────────┘
                     │ gRPC ExecuteTask
┌────────────────────▼────────────────────────┐
│  Python Worker Pool                         │
│  - Strategies: inproc / fork warm / naive   │
│  - cloudpickle deserialize + execute        │
│  - CRIU checkpoint/restore hooks (stubbed)  │
└─────────────────────────────────────────────┘
```

Three components, one box:

- **Python SDK** — `@app.function` decorator, cloudpickle-serializes the call, returns a `Future` over gRPC.
- **Go orchestrator** — async dispatch queue, job state machine, heartbeat fault detection, BoltDB WAL, live dashboard.
- **Python worker pool** — three cold-start strategies (`inproc`, `fork`, `naive`), CRIU snapshot/restore hooks stubbed.

---

## Quickstart

**One-time setup** (installs protoc, Go plugins, project-local Python venv):

```bash
make tools
python3 -m venv .venv
.venv/bin/pip install grpcio grpcio-tools cloudpickle protobuf
```

**Build and run:**

```bash
# generate stubs + build orchestrator
PYTHON=.venv/bin/python make proto
make orchestrator
./orchestrator/orchestrator &              # gRPC :50051 + HTTP :8080

# start a worker (in another shell)
PYTHONPATH=worker:sdk \
  ORCH_ADDR=127.0.0.1:50051 \
  WORKER_ADVERTISE_HOST=127.0.0.1 \
  MINIMODAL_COLD_START=fork \
  .venv/bin/python -m worker.worker

# invoke (in a third shell)
PYTHONPATH=sdk MINIMODAL_ORCH_ADDR=127.0.0.1:50051 \
  .venv/bin/python examples/hello.py       # → "hi alex"
```

Or run the full local cluster with `make run` (docker-compose: 1 orchestrator + 4 workers).

- Dashboard: http://localhost:8080
- JSON metrics: `curl localhost:8080/metrics`

**Try a function:**

```python
# examples/hello.py
from minimodal import App
app = App("hello")

@app.function
def greet(name: str) -> str:
    return f"hi {name}"

if __name__ == "__main__":
    print(greet.remote("alex"))   # → "hi alex"
```

---

## Roadmap

| Phase | Status | What |
|-------|--------|------|
| 1 | done | gRPC plumbing, worker registration, heartbeat |
| 2 | done | End-to-end happy path (invoke → execute → result) |
| 3 | done | Cold start: warm fork pool + naive subproc + CRIU stubs |
| 4 | done | Fault tolerance: heartbeat timeout + WAL replay |
| 5 | done | Observability: /metrics endpoint + live dashboard |

---

## The headline: 11× cold-start speedup

Each row is 30 invocations of a function whose body imports a module and does a trivial reduce. Apple Silicon, Python 3.13, via `benchmarks/run_bench.sh`.

- `cold_start_ms` — wall time *outside* the user fn (subprocess spawn, fork dispatch, deserialization)
- `execution_ms` — `fn(*args)` including any imports *inside* the body

**numpy** (~30 MB on disk, ~80 ms cold import):

| strategy | cold p50 | p95 | p99 | exec p50 | throughput |
|----------|---------:|----:|----:|---------:|-----------:|
| inproc   |   0 ms   |  0  |  0  |   0 ms   | 14.7 req/s |
| fork     |   0 ms   |  0  |  1  |   0 ms   | 14.7 req/s |
| naive    |  40 ms   | 51  | 69  |  26 ms   | 8.3 req/s  |

**torch** (~750 MB on disk, ~500 ms cold import):

| strategy | cold p50 | p95 | p99 | exec p50 | throughput |
|----------|---------:|----:|----:|---------:|-----------:|
| inproc   |   0 ms   |  0  |  0  |   0 ms   | 15.9 req/s |
| fork     |   0 ms   |  1  |  1  |   0 ms   | 15.9 req/s |
| naive    | 164 ms   | 229 | 231 | 480 ms   | 1.4 req/s  |

### What this means

A torch-importing function under **naive** cold start takes ~700 ms per call: 164 ms interpreter + cloudpickle prelude, then 480 ms of `import torch` inside the body.

The same function under **fork** takes <1 ms per call. The warm-pool parent already has torch resident; children inherit it via copy-on-write. `import torch` inside the user fn is just a `sys.modules` lookup.

The throughput collapse is **11×** (1.4 → 15.9 req/s). At a 1-second SLA, the naive worker is already missing every request. The fork worker has 990 ms of slack.

This is why serverless ML platforms can't get away with "just spawn a container." Even a perfectly tuned container runtime can't beat the unavoidable cost of populating Python's symbol table from disk on every invocation.

---

## How it works

### Cold starts in Python are an import problem

The dominant cost isn't the interpreter — it's imports.

| Workload | Cold time |
|----------|----------:|
| Fresh Python interpreter | ~20 ms |
| `+ import numpy` | ~80 ms |
| `+ import torch` | 1–2 s |

Real ML serving workloads aren't bottlenecked on Python. They're bottlenecked on the **symbol table being populated**. That's why "serverless ML" platforms need something smarter than naive container spawning.

### Fork is fast, but it isn't enough

`fork()` is incredible because copy-on-write means the child only pays for pages it actually mutates. But three things bite you:

1. **CUDA contexts can't be forked.** The CUDA driver opens kernel connections that aren't inheritable. PyTorch and TensorFlow both blow up if you try. Real GPU workloads need process snapshotting (CRIU).
2. **File descriptors leak.** Anything the parent opened — gRPC channels, tempfiles, DB connections — is inherited. The child sees stale state and can wedge. This worker forks *before* opening any sockets to keep children clean.
3. **Multithreaded parents deadlock.** `fork()` in a process with threads is undefined behavior: only the calling thread is copied, leaving held mutexes locked forever. This is why `WarmPool.start()` must run before the gRPC server.

### CRIU is the real answer

CRIU (Checkpoint/Restore In Userspace) snapshots a live process — memory pages, file descriptors, registers, signal state — to disk, and restores it later by mapping pages back in. Restored processes wake up on the line *after* the checkpoint call.

Combined with `userfaultfd`, you don't even pay for the memory copy at restore. Pages fault in lazily as the restored process touches them.

**Realistic numbers** on Linux, 500 MB Python with torch + transformers + a 200 MB model:

| Approach | Cold start |
|----------|-----------:|
| Naive (import everything) | 3–4 s |
| Fork from pre-warmed parent | ~50 ms |
| **CRIU + lazy pages** | **10–15 ms** + amortized faults |

That last number is why Modal-style sub-100ms cold starts are achievable on big ML workloads. The kernel still does the work — just *after* you've already started serving the request.

v1 stubs CRIU in `worker/snapshot.py`. The full Linux integration is specced [below](#criu-on-linux-the-spec).

---

## Fault tolerance

minimodal provides **at-least-once delivery**.

- A PENDING or RUNNING job survives orchestrator crashes (persisted to BoltDB with its cloudpickle payload).
- Worker death is detected within `MINIMODAL_WORKER_TIMEOUT` (default 6s) via missed heartbeats.
- The reaper requeues RUNNING jobs owned by dead workers with an incremented retry counter.
- Exceeding `MINIMODAL_MAX_RETRIES` (default 3) marks the job FAILED.

**The duplicate-execution corner case:** if a worker reports a successful result and *then* dies before its message reaches the orchestrator, the job re-executes. The client sees *some* result, but the user function may have run more than once. For effective exactly-once, see [Idempotency](#idempotency).

### Atomic state transitions

`JobStore.Transition(jobID, mutator)` does read-modify-write inside a single BoltDB transaction and returns `(committed bool, err error)` so callers can detect lost CAS races against concurrent terminal transitions.

### Tested scenarios

`tests/test_fault_tolerance.py`:

1. Worker SIGKILL mid-execution → reaper re-dispatches → client gets result.
2. Orchestrator restart with PENDING job in WAL → workers re-register → job completes.
3. Two consecutive worker deaths exceed `MAX_RETRIES=1` → job marked FAILED.

---

## Observability

The orchestrator runs an HTTP server on `:8080`:

- `GET /metrics` — JSON snapshot of orchestrator state. Polled every second by the dashboard.
- `GET /healthz` — liveness check.
- `GET /` — live dashboard (served from `dashboard/index.html`).

Trimmed `/metrics` after a 20-invocation `naive` bench run:

```json
{
  "uptime_s": 38,
  "active_workers": 1,
  "queued_jobs": 0,
  "total_invocations": 22,
  "total_completed": 22,
  "total_failed": 0,
  "in_flight_jobs": 0,
  "error_rate": 0,
  "cold_start_samples": 22,
  "p50_cold_start_ms": 41,
  "p95_cold_start_ms": 45,
  "p99_cold_start_ms": 46,
  "mean_cold_start_ms": 41.59,
  "workers": [
    {
      "id": "naive-w",
      "address": "127.0.0.1:50100",
      "alive": true,
      "active_tasks": 0,
      "capacity": 4,
      "utilization": 0,
      "last_heartbeat_seconds_ago": 0
    }
  ]
}
```

**Implementation:**

- Cold-start samples kept in a 1024-entry ring buffer.
- Percentiles computed on demand via sort (cheap at this scale).
- Counters use `sync/atomic` — lock-free on the hot path.

**Dashboard:**

- Four big-number cards (active workers, queued, in-flight, total invocations).
- Four cards for cold-start percentiles + error rate.
- Live line chart of p50/p95/p99 over the last 60 seconds.
- Worker grid with per-worker utilization bars (red if dead, green→yellow by load).

---

## Idempotency

A caller-supplied `idempotency_key` deduplicates submissions. The orchestrator stores `(key → job_id)` in a dedicated BoltDB bucket; check-and-set runs in a single transaction, so concurrent submits with the same key are linearizable.

```python
from minimodal import App
app = App("idem")

@app.function
def charge_user(user_id: int, cents: int) -> str:
    # Must run at most once even if the client retries.
    return process_payment(user_id, cents)

# Retry-safe — second call returns same job_id and result, no re-execution.
result = charge_user.remote(42, 100, _idempotency_key="payment-2024-03-15-42")
```

Result bytes are persisted on terminal transition (alongside status), so a client retrying with the same key *after an orchestrator restart* still gets the original result back — not just "DONE with empty payload."

`tests/test_idempotency.py` covers same-lifetime and across-restart cases. The user function uses a side-channel tempfile to prove it executed exactly once.

This is the upgrade path from baseline at-least-once → effective exactly-once for cooperating clients.

---

## CRIU on Linux: the spec

A concrete deployment spec. **Nothing here runs on macOS.**

### Kernel prerequisites

Verify with `zcat /proc/config.gz | grep -E 'CHECKPOINT_RESTORE|USERFAULTFD'`:

```
CONFIG_CHECKPOINT_RESTORE=y
CONFIG_USERFAULTFD=y
CONFIG_HAVE_ARCH_USERFAULTFD_WP=y
```

Plus `criu --version >= 3.18` for userfaultfd lazy-restore support.

### Per-function snapshot lifecycle

**First invocation:**

1. Worker spawns a normal Python child and exec's a *prelude* script that:
   - imports every module declared in the function's preimport hint
   - calls a `__warmup__` hook on the user fn (model load, JIT compile)
   - blocks on a pipe waiting for the snapshot command
2. Worker runs `criu dump --tree <pid> -D /var/lib/minimodal/snapshots/<func_hash> --leave-running --shell-job`. The child stays alive; CRIU writes its full memory image + open-fd state to the snapshot dir.
3. Worker stores `(func_hash → snapshot_dir)` in a local LRU keyed by the cloudpickle payload hash.

**Per-invocation restore:**

1. Look up the snapshot dir for the function's hash. Miss → fall back to fork/naive.
2. `criu restore -D <dir> --restore-detached --shell-job --lazy-pages --tcp-established`. The `--lazy-pages` flag is the trick — CRIU returns control as soon as the address space is mapped (~1–10 ms); a `userfaultfd` listener serves pages on demand from the on-disk image.
3. The restored process inherits a pipe with `(function_bytes, args_bytes)`, executes, writes the result back, exits.

### Hook points in this codebase

Most stubs already exist.

`worker/snapshot.py`:

```python
def checkpoint_process(pid, cfg):
    subprocess.run(['criu', 'dump', '--tree', str(pid),
                    '-D', str(cfg.image_dir),
                    '--leave-running', '--shell-job', '--track-mem',
                    '--manage-cgroups', 'soft'], check=True)

def restore_process(cfg):
    p = subprocess.Popen(['criu', 'restore', '-D', str(cfg.image_dir),
                          '--restore-detached', '--shell-job',
                          '--lazy-pages', '--tcp-established',
                          '--pidfile', str(cfg.image_dir / 'restored.pid')])
    p.wait(timeout=5.0)
    return int((cfg.image_dir / 'restored.pid').read_text())
```

`worker/executor.py` — add `CRIUExecutor`:

```python
class CRIUExecutor:
    name = "criu"
    def execute(self, fn_bytes, args_bytes):
        snap = self._snapshot_for(fn_bytes)  # LRU hit or warm-and-snapshot
        pid = snapshot.restore_process(snap)
        send_payload_to(pid, fn_bytes, args_bytes)   # via inherited pipe
        result = read_result_from(pid)
        os.waitpid(pid, 0)
        return result
```

`worker/worker.py` — `make_executor("criu")` returns `CRIUExecutor` instead of `NotImplementedError`.

### What makes this hard in practice

- **CUDA contexts.** `--ext-cuda` is experimental. Production users freeze/thaw the CUDA driver via NVIDIA's `cuda-checkpoint` utility orchestrated alongside CRIU. The naive approach hangs on restore.
- **TCP connections.** A restored process believes it still has its old sockets. `--tcp-established` makes CRIU rebuild them, but the orchestrator on the other end doesn't know. Snapshot *before* opening any orchestrator-bound socket; re-establish fresh post-restore.
- **`pthread` state.** CRIU restores threads, but Python's GIL state and `threading` primitives get weird if you snapshot mid-call. Always snapshot at a quiescent point (the prelude block).
- **Mounts and namespaces.** A restored process expects the same view of `/proc`, `/sys`, and namespaces it had at dump time. Run snapshot + restore in the same cgroup + mount namespace.
- **Snapshot validity.** Invalidated by Python/glibc/kernel upgrades. Keep a `(python_version, glibc_version, kernel_version)` tuple in metadata; mismatch → fall back to fork and re-warm.

---

## Stretch goals

- **Real CRIU integration on Linux** — spec [above](#criu-on-linux-the-spec). Target: <50 ms cold start on torch + a 200 MB model.
- **Multi-tenant isolation** — cgroups for memory limits, seccomp filters per function.
- **Billing-grade metering** — per-invocation CPU + memory accounting via cgroup v2 stats.
- **Distributed orchestrator** — Raft consensus for the job store.
- **Worker affinity / sticky routing** — for stateful functions.
- **Streaming results** — generators over gRPC streaming RPCs.
- **Snapshot validity tracking** — invalidate on Python/glibc/kernel upgrades.
