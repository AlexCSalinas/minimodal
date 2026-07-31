"""Phase 4 fault-tolerance integration tests.

Runs against real orchestrator + worker subprocesses. Each test owns its own
BoltDB file, log files, and ports, so individual tests are independent.

Run with:

    .venv/bin/python tests/test_fault_tolerance.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PY = os.path.join(ROOT, ".venv/bin/python")
ORCH_BIN = os.path.join(ROOT, "orchestrator/orchestrator")

sys.path.insert(0, os.path.join(ROOT, "sdk"))


def _boot_orchestrator(db_path: str, log_path: str, *, heartbeat="500ms", timeout="2s"):
    env = os.environ.copy()
    env["MINIMODAL_HEARTBEAT_INTERVAL"] = heartbeat
    env["MINIMODAL_WORKER_TIMEOUT"] = timeout
    env["MINIMODAL_DB_PATH"] = db_path
    log = open(log_path, "w")
    proc = subprocess.Popen([ORCH_BIN], env=env, stdout=log, stderr=subprocess.STDOUT)
    # Wait for it to be ready (port open).
    _await_port("127.0.0.1", 50051, timeout=5.0)
    return proc, log


def _boot_worker(worker_id: str, port: int, log_path: str):
    env = os.environ.copy()
    env["ORCH_ADDR"] = "127.0.0.1:50051"
    env["WORKER_ADVERTISE_HOST"] = "127.0.0.1"
    env["WORKER_PORT"] = str(port)
    env["WORKER_ID"] = worker_id
    env["WORKER_CAPACITY"] = "2"
    env["PYTHONPATH"] = f"{ROOT}/worker:{ROOT}/sdk"
    log = open(log_path, "w")
    proc = subprocess.Popen(
        [PY, "-m", "worker.worker"], env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    _await_port("127.0.0.1", port, timeout=5.0)
    # Also wait until the orchestrator has registered the worker.
    time.sleep(0.5)
    return proc, log


def _await_port(host: str, port: int, timeout: float) -> None:
    import socket as _socket
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with _socket.create_connection((host, port), timeout=0.2):
                return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    raise TimeoutError(f"{host}:{port} did not open within {timeout}s")


def _kill(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGINT)
    except Exception:
        pass
    try:
        proc.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        proc.kill()


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------
def _cleanup(*procs):
    for p in procs:
        _kill(p)


def _fresh_db(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Test 1 — kill a worker mid-flight, verify re-dispatch
# ---------------------------------------------------------------------------
def test_worker_death_redispatch() -> None:
    print("\n=== test_worker_death_redispatch ===")
    db = "/tmp/minimodal-fault-1.db"
    _fresh_db(db)
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)

    orch, _ = _boot_orchestrator(db, "/tmp/orch-fault-1.log")
    w1, _ = _boot_worker("victim", 50101, "/tmp/w-victim.log")
    w2, _ = _boot_worker("survivor", 50102, "/tmp/w-survivor.log")

    try:
        os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"
        from minimodal import App
        from minimodal.pb import minimodal_pb2

        app = App("fault-test-1")

        @app.function
        def slow(secs: float) -> str:
            import time as _t
            _t.sleep(secs)
            return f"completed after {secs}s"

        f = slow.spawn(2.5)
        print(f"  submitted job_id={f.job_id}")

        # Wait until the job is RUNNING so we know which worker has it.
        victim_id = None
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            status = app.client.get_job_status(f.job_id)
            if status.status == minimodal_pb2.JOB_STATUS_RUNNING and status.worker_id:
                victim_id = status.worker_id
                break
            time.sleep(0.05)
        assert victim_id, "job never reached RUNNING"
        print(f"  job picked up by worker={victim_id}")

        victim_proc = w1 if victim_id == "victim" else w2
        survivor_id = "survivor" if victim_id == "victim" else "victim"
        print(f"  SIGKILL worker={victim_id} (pid={victim_proc.pid})")
        victim_proc.kill()

        result = f.get(timeout_s=15.0)
        print(f"  result: {result!r}  (took total {f.total_duration_ms}ms)")
        assert "completed" in result

        status = app.client.get_job_status(f.job_id)
        print(f"  final: status={status.status}  worker={status.worker_id}  retry={status.retry_count}")
        assert status.retry_count >= 1, f"expected retry_count>=1, got {status.retry_count}"
        assert status.worker_id == survivor_id, f"expected completion on {survivor_id}, got {status.worker_id}"
        print("  PASS")
    finally:
        _cleanup(w1, w2, orch)


# ---------------------------------------------------------------------------
# Test 2 — orchestrator restart with PENDING job in WAL
# ---------------------------------------------------------------------------
def test_orchestrator_restart_replays_wal() -> None:
    print("\n=== test_orchestrator_restart_replays_wal ===")
    db = "/tmp/minimodal-fault-2.db"
    _fresh_db(db)
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)

    # Phase A: orchestrator up, NO workers. Submit a job → it queues, then we
    # stop the orchestrator. Job survives in BoltDB as PENDING.
    orch, _ = _boot_orchestrator(db, "/tmp/orch-fault-2a.log")
    try:
        os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"
        # Re-import App to get a fresh client (in case a stale one is cached
        # by the previous test).
        for mod in list(sys.modules):
            if mod.startswith("minimodal"):
                del sys.modules[mod]
        from minimodal import App
        from minimodal.pb import minimodal_pb2

        app = App("fault-test-2")

        @app.function
        def adder(a: int, b: int) -> int:
            return a + b

        f = adder.spawn(7, 8)
        print(f"  submitted job_id={f.job_id}  (no workers running)")
        time.sleep(0.3)
        status = app.client.get_job_status(f.job_id)
        print(f"  pre-restart status={status.status} (expect PENDING={minimodal_pb2.JOB_STATUS_PENDING})")
        assert status.status == minimodal_pb2.JOB_STATUS_PENDING
        saved_job_id = f.job_id
    finally:
        _kill(orch)

    # Phase B: relaunch orchestrator pointing at the same DB. It should
    # recover the PENDING job from the WAL.
    orch, _ = _boot_orchestrator(db, "/tmp/orch-fault-2b.log")
    w, _ = _boot_worker("post-restart", 50104, "/tmp/w-restart.log")

    try:
        from minimodal import App as App2
        from minimodal.future import Future
        app2 = App2("fault-test-2")
        f2 = Future(saved_job_id, app2.client)
        result = f2.get(timeout_s=10.0)
        print(f"  result after restart: {result!r}")
        assert result == 15, f"expected 15, got {result}"

        with open("/tmp/orch-fault-2b.log") as fh:
            log = fh.read()
        assert 'msg="WAL replay completed" recovered=1' in log, f"WAL replay log line missing\n{log}"
        print("  PASS (WAL replay confirmed in orchestrator log)")
    finally:
        _cleanup(w, orch)


# ---------------------------------------------------------------------------
# Test 3 — exceed max retries
# ---------------------------------------------------------------------------
def test_max_retries_exhausted() -> None:
    print("\n=== test_max_retries_exhausted ===")
    db = "/tmp/minimodal-fault-3.db"
    _fresh_db(db)
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)

    # MAX_RETRIES=1 so the second worker death marks the job FAILED.
    env = os.environ.copy()
    env["MINIMODAL_HEARTBEAT_INTERVAL"] = "500ms"
    env["MINIMODAL_WORKER_TIMEOUT"] = "2s"
    env["MINIMODAL_DB_PATH"] = db
    env["MINIMODAL_MAX_RETRIES"] = "1"
    log = open("/tmp/orch-fault-3.log", "w")
    orch = subprocess.Popen([ORCH_BIN], env=env, stdout=log, stderr=subprocess.STDOUT)
    _await_port("127.0.0.1", 50051, 5.0)
    w1, _ = _boot_worker("v1", 50105, "/tmp/w-v1.log")
    w2, _ = _boot_worker("v2", 50106, "/tmp/w-v2.log")

    try:
        for mod in list(sys.modules):
            if mod.startswith("minimodal"):
                del sys.modules[mod]
        os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"
        from minimodal import App
        from minimodal.future import FutureError
        from minimodal.pb import minimodal_pb2

        app = App("fault-test-3")

        @app.function
        def forever() -> int:
            import time as _t
            _t.sleep(10)
            return 1

        f = forever.spawn()

        # Wait for RUNNING on either worker, then kill it.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            status = app.client.get_job_status(f.job_id)
            if status.status == minimodal_pb2.JOB_STATUS_RUNNING:
                break
            time.sleep(0.05)
        first_victim = status.worker_id
        print(f"  killing first worker={first_victim}")
        (w1 if first_victim == "v1" else w2).kill()

        # Wait for the redispatch.
        deadline = time.monotonic() + 5.0
        second_worker = None
        while time.monotonic() < deadline:
            status = app.client.get_job_status(f.job_id)
            if status.status == minimodal_pb2.JOB_STATUS_RUNNING and status.worker_id != first_victim:
                second_worker = status.worker_id
                break
            time.sleep(0.1)
        assert second_worker, "redispatch never happened"
        print(f"  redispatched to worker={second_worker}")

        # Kill the second worker too.
        (w1 if second_worker == "v1" else w2).kill()

        # With MAX_RETRIES=1, the second death should push retry_count to 2,
        # which is > 1, so the job is marked FAILED.
        try:
            f.get(timeout_s=10.0)
            print("  FAIL: expected FutureError")
            sys.exit(1)
        except FutureError as e:
            print(f"  got expected FutureError: {e}")
            assert "max retries" in str(e), f"unexpected error: {e}"
        print("  PASS")
    finally:
        _cleanup(w1, w2, orch)


if __name__ == "__main__":
    test_worker_death_redispatch()
    test_orchestrator_restart_replays_wal()
    test_max_retries_exhausted()
    print("\nall fault-tolerance tests passed ✓")
