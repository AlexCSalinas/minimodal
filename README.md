# minimodal

A from-scratch implementation of a Modal-like distributed function execution
runtime, built to demonstrate the systems engineering behind serverless cold
start optimization.

> **Status:** v1 complete — 5 phases shipped end-to-end. ~3K lines of Go +
> Python. Roadmap and benchmarks below.

## What this is

If you've used [Modal](https://modal.com), you know the magic trick: you slap
`@app.function` on a Python function and it runs in the cloud in ~100ms,
including the time to start a fresh container. That's not just clever caching
— it's a stack of carefully composed systems tricks: process snapshotting,
memory mapping, lazy page faulting, fork-based pre-warming, and a custom
container runtime.

This project rebuilds the core of that stack from scratch, on one box, in a
form you can read end-to-end in an afternoon. The point isn't to compete with
Modal — it's to make every layer legible.

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

## Quickstart

One-time setup (installs protoc, Go protoc plugins, and a project-local
Python venv with grpcio + cloudpickle):

```bash
make tools                                  # brew install protobuf + go install plugins
python3 -m venv .venv                       # macOS Python is PEP 668 externally-managed
.venv/bin/pip install grpcio grpcio-tools cloudpickle protobuf
```

Build + run:

```bash
PYTHON=.venv/bin/python make proto          # generate Go + Python gRPC stubs
make orchestrator                           # build the Go binary
./orchestrator/orchestrator &               # gRPC :50051 + HTTP :8080

# in another shell — start one (or more) workers
PYTHONPATH=worker:sdk \
  ORCH_ADDR=127.0.0.1:50051 \
  WORKER_ADVERTISE_HOST=127.0.0.1 \
  MINIMODAL_COLD_START=fork \
  .venv/bin/python -m worker.worker

# in a third shell — hit the system
PYTHONPATH=sdk MINIMODAL_ORCH_ADDR=127.0.0.1:50051 \
  .venv/bin/python examples/hello.py        # prints "hi alex"
```

Open http://localhost:8080 for the live dashboard, or `curl localhost:8080/metrics`
for the JSON form.

Full local cluster:

```bash
make run            # docker-compose up: orchestrator + 4 workers
```

Try a function:

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

## Roadmap

| Phase | Status | What |
|-------|--------|------|
| 1 | done | gRPC plumbing, worker registration, heartbeat |
| 2 | done | End-to-end happy path (invoke → execute → result) |
| 3 | done | Cold start: warm fork pool + naive subproc + CRIU stubs |
| 4 | done | Fault tolerance: heartbeat timeout + WAL replay |
| 5 | done | Observability: /metrics endpoint + live dashboard |

## Cold start benchmark

Measured on Apple Silicon, Python 3.13, via `benchmarks/run_bench.sh`. Each
row is 30 invocations of a function whose body imports the named module and
does a trivial reduce. `cold_start_ms` is wall time *outside* the user fn
call (subprocess spawn, fork dispatch, deserialization); `execution_ms` is
`fn(*args)` including any imports inside the body.

**numpy** (~30 MB on disk, ~80 ms cold import):

| strategy | cold p50 | p95 | p99 | exec p50 | throughput |
|----------|---------:|----:|----:|---------:|-----------:|
| inproc   | 0 ms     | 0   | 0   | 0 ms     | 14.7 req/s |
| fork     | 0 ms     | 0   | 1   | 0 ms     | 14.7 req/s |
| naive    | 40 ms    | 51  | 69  | 26 ms    | 8.3 req/s  |

**torch** (~750 MB on disk, ~500 ms cold import):

| strategy | cold p50 | p95 | p99 | exec p50 | throughput |
|----------|---------:|----:|----:|---------:|-----------:|
| inproc   | 0 ms     | 0   | 0   | 0 ms     | 15.9 req/s |
| fork     | 0 ms     | 1   | 1   | 0 ms     | 15.9 req/s |
| naive    | 164 ms   | 229 | 231 | 480 ms   | 1.4 req/s  |

This is the headline result. Under naive cold start, a torch-importing
function takes ~700 ms per call (164 ms interpreter+cloudpickle prelude in
the subprocess + 480 ms `import torch` inside the function body). The same
function under fork takes <1 ms per call because the warm-pool parent
already has torch resident — children inherit it via copy-on-write, and
`import torch` inside the user fn is just a `sys.modules` lookup.

The throughput collapse is **11×** (1.4 req/s vs 15.9). At a 1-second SLA,
the naive worker would already be missing every request; the fork worker
has 990 ms of slack.

This is the actual reason serverless ML platforms can't get away with
"just spawn a container" — even a perfectly tuned container runtime can't
beat the unavoidable cost of populating Python's symbol table from disk
on every invocation.

## Design notes

### Why cold starts are hard in Python

The dominant cost isn't the interpreter — it's imports. A fresh Python
interpreter starts in ~20ms. Adding `import numpy` doubles that. Adding
`import torch` adds 1–2 seconds. Real ML serving workloads aren't bottlenecked
on Python; they're bottlenecked on the symbol table being populated. This
is why a "serverless ML" platform can't get away with naive container
spawning: even with a perfectly tuned container runtime, the Python prelude
dominates.

### Why fork isn't always enough

`fork()` is incredible because copy-on-write means the child only pays for
pages it actually mutates. But three things bite you:

1. **CUDA contexts can't be forked.** The CUDA driver opens connections to
   the kernel that aren't inheritable. PyTorch and TensorFlow both blow up
   if you try. To pre-warm GPU workloads you need a real process snapshot,
   which is what CRIU does.
2. **File descriptors leak.** Anything the parent opened (gRPC channels,
   tempfiles, DB connections) is inherited. The child sees stale state and
   can wedge if it tries to use them. This worker forks before opening
   *any* sockets to keep children clean — see the ordering in `worker/worker.py`.
3. **Multithreaded parents deadlock.** `fork()` in a process that has any
   threads is undefined behavior because only the calling thread is copied,
   leaving held mutexes locked forever in the child. This is why
   `WarmPool.start()` must be called before the gRPC server (or any thread).

### How CRIU solves it

CRIU (Checkpoint/Restore In Userspace) snapshots a live process — all memory
pages, file descriptor table, registers, signal state — to disk, and
restores it later by mapping the pages back in. Restored processes wake up
on the line after the checkpoint call.

Combined with `userfaultfd`, you don't even pay for the memory copy at
restore time: pages are faulted in lazily as the restored process touches
them. So restoring a 500MB Python process with all its imports loaded takes
~10ms wall time and only resident-set-size grows over the first few seconds
as code paths exercise different modules.

This is the "real" Modal cold-start optimization. v1 stubs it in
`worker/snapshot.py` — implementing it requires Linux and is sketched in the
next section.

### CRIU on Linux: what would actually change

Concrete spec for a Linux deployment. Nothing here runs on macOS.

**Kernel prerequisites.** Verify with `zcat /proc/config.gz | grep -E 'CHECKPOINT_RESTORE|USERFAULTFD'`:
```
CONFIG_CHECKPOINT_RESTORE=y
CONFIG_USERFAULTFD=y
CONFIG_HAVE_ARCH_USERFAULTFD_WP=y
```
Plus `criu --version >= 3.18` for userfaultfd lazy-restore support.

**Per-function snapshot lifecycle.** First time a function is invoked:

1. Worker spawns a normal Python child, exec's a tiny *prelude* script that:
   - `import`s every module declared in the function's preimport hint
   - calls a `__warmup__` hook on the user's function (model load, JIT compile)
   - blocks on a pipe waiting for the snapshot command
2. Worker calls `criu dump --tree <pid> -D /var/lib/minimodal/snapshots/<func_hash> --leave-running --shell-job`. The child stays alive; CRIU writes its full memory image + open-fd state to the snapshot dir.
3. Worker stores `(func_hash → snapshot_dir)` in a local LRU keyed by content hash of the cloudpickle payload. Later invocations of the same function key into this map.

**Per-invocation restore.** On `ExecuteTask`:

1. Look up the snapshot dir for the function's hash. If miss → fall back to fork or naive strategy.
2. `criu restore -D <dir> --restore-detached --shell-job --lazy-pages --tcp-established`. The `--lazy-pages` flag is the trick: CRIU returns control as soon as the process's address space is mapped (~1–10 ms), and a `userfaultfd` listener serves pages on demand from the on-disk image as the restored process touches them.
3. The restored process inherits a pipe with the (function_bytes, args_bytes) payload, executes, writes the result back, exits. Worker tears down the lazy-pages daemon for that PID.

**Hook points in this codebase.** Lines mostly already exist as TODO stubs.

```
worker/snapshot.py
  checkpoint_process(pid, cfg):
    subprocess.run(['criu', 'dump', '--tree', str(pid), '-D', str(cfg.image_dir),
                    '--leave-running', '--shell-job', '--track-mem',
                    '--manage-cgroups', 'soft'], check=True)

  restore_process(cfg):
    p = subprocess.Popen(['criu', 'restore', '-D', str(cfg.image_dir),
                          '--restore-detached', '--shell-job',
                          '--lazy-pages', '--tcp-established',
                          '--pidfile', str(cfg.image_dir / 'restored.pid')])
    p.wait(timeout=5.0)
    return int((cfg.image_dir / 'restored.pid').read_text())

worker/executor.py — add CRIUExecutor with:
  name = "criu"
  def execute(self, fn_bytes, args_bytes):
      snap = self._snapshot_for(fn_bytes)  # LRU hit or warm-and-snapshot
      pid = snapshot.restore_process(snap)
      send_payload_to(pid, fn_bytes, args_bytes)   # via inherited pipe
      result = read_result_from(pid)
      os.waitpid(pid, 0)
      return result

worker/worker.py — make_executor("criu") returns CRIUExecutor instead of NotImplementedError.
```

**Caveats — what makes this hard in practice.**

- **CUDA contexts.** CRIU has a `--ext-cuda` flag but it's experimental.
  Most production users freeze + thaw the CUDA driver via NVIDIA's
  `cuda-checkpoint` utility orchestrated alongside CRIU. The naive
  approach hangs on restore.
- **TCP connections.** A restored process believes it still has open
  sockets. `--tcp-established` makes CRIU rebuild them, but the *other
  end* (the orchestrator) doesn't know. In practice you snapshot
  *before* opening any orchestrator-bound socket and re-establish
  fresh in the restored process.
- **`pthread` state.** CRIU restores threads, but Python's GIL state and
  `threading` primitives can get into weird shapes if you snapshot
  mid-call. Always snapshot at a known quiescent point (the prelude
  block).
- **Mounts and namespaces.** A restored process expects the same view
  of `/proc`, `/sys`, and namespace memberships it had at dump time.
  Run snapshot and restore inside the same cgroup + mount namespace.
- **Snapshot validity.** A snapshot is invalidated by Python interpreter
  upgrades, glibc upgrades, kernel ABI breaks. Keep a `(python_version,
  glibc_version, kernel_version)` tuple in the snapshot metadata; mismatch
  → fall back to the fork strategy and re-warm.

**Realistic numbers.** On a modern Linux box with a 500MB Python process
holding torch + transformers + a 200MB model:

- `import torch + import transformers + AutoModel.from_pretrained` cold: ~3-4 s
- fork from pre-warmed parent: ~50 ms (page table copy is the floor)
- CRIU restore with lazy pages: 10–15 ms wall time, then a few hundred ms of
  amortized page faults during the first request

That last number is why Modal-style sub-100ms cold starts on big ML
workloads are achievable at all. It's not magic — it's making the kernel
do the work it was always going to do, just *after* you've already started
serving the request.

### Fault tolerance model

minimodal provides **at-least-once** delivery. The contract:

- A job that's PENDING or RUNNING is durable across orchestrator crashes
  (persisted to BoltDB with its cloudpickle payload).
- Worker death is detected within `MINIMODAL_WORKER_TIMEOUT` (default 6s) via
  missed heartbeats. The reaper finds RUNNING jobs owned by the dead worker
  and requeues them with an incremented retry counter.
- Exceeding `MINIMODAL_MAX_RETRIES` (default 3) marks the job FAILED.
- If a worker reports a successful result and then dies before its message
  reaches the orchestrator, the job will be re-executed by another worker.
  The client sees the result of *some* run, but the user function may have
  run more than once.

To upgrade to **exactly-once** the client would need to supply an
idempotency key, and the orchestrator would dedupe by `(idempotency_key,
function_hash)`. Documented as a TODO; not built.

The integration tests under `tests/test_fault_tolerance.py` exercise three
scenarios:

1. Worker SIGKILL mid-execution → reaper re-dispatches → client gets result.
2. Orchestrator restart with PENDING job in WAL → workers re-register → job
   completes from the recovered state.
3. Two consecutive worker deaths exceed `MAX_RETRIES=1` → job marked FAILED.

Atomic state transitions are mediated by `JobStore.Transition(jobID, mutator)`
which does read-modify-write inside a single BoltDB transaction and returns
`(committed bool, err error)` so callers can detect lost CAS races against
concurrent terminal transitions.

### Observability

The orchestrator exposes an HTTP server on `:8080` with three endpoints:

- `GET /metrics` — JSON snapshot of orchestrator state. Polled by the
  dashboard every second.
- `GET /healthz` — liveness check (returns "ok").
- `GET /` — the dashboard HTML (served from `dashboard/index.html`,
  resolved via `MINIMODAL_DASHBOARD_PATH` env or `../dashboard` relative
  to the binary).

A trimmed `/metrics` payload after a 20-invocation `naive` bench run:

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

The cold-start samples are kept in a 1024-entry ring buffer; percentiles
are computed on demand via a sort over the buffer (cheap at this scale).
Counters use `sync/atomic` so they're lock-free on the hot path.

The dashboard at `/` renders this as:
- four big-number cards (active workers, queued, in-flight, total invocations)
- four cards for cold-start percentiles + error rate
- a live line chart of p50/p95/p99 over the last 60 seconds
- a worker grid with per-worker utilization bars (red if dead, green→yellow
  shaded by load if alive)

### Idempotency keys (exactly-once for cooperating clients)

A caller-supplied `idempotency_key` on `InvokeFunction` deduplicates
submissions. The orchestrator stores a `(key → job_id)` mapping in a
dedicated BoltDB bucket; the check-and-set runs in a single transaction so
two concurrent submits with the same key are linearizable.

```python
from minimodal import App
app = App("idem")

@app.function
def charge_user(user_id: int, cents: int) -> str:
    # Critical: must run at most once even if the client retries.
    return process_payment(user_id, cents)

# Retry-safe — second call returns the same job_id and result, no
# re-execution.
result = charge_user.remote(42, 100, _idempotency_key="payment-2024-03-15-42")
```

The result bytes are persisted on terminal transition (alongside the
status), so a client retrying with the same key *after an orchestrator
restart* still gets the original result back — not just "DONE with empty
payload."

`tests/test_idempotency.py` covers both the same-lifetime and
across-restart cases. The user function uses a side-channel tempfile to
prove it executed exactly once.

This is the upgrade path from the system's baseline at-least-once
delivery to effective exactly-once for cooperating clients.

### What I'd build next

- Real CRIU integration on Linux (spec above)
- Multi-tenant isolation: cgroups for memory limits, seccomp filters per function
- Billing-grade metering: per-invocation CPU + memory accounting via cgroup v2 stats
- Distributed orchestrator with Raft consensus for the job store
- Worker affinity / sticky routing for stateful functions
- Streaming results (generators) via gRPC streaming RPCs
- Snapshot validity tracking: invalidate on Python/glibc/kernel upgrades
