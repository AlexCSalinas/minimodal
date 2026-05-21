"""Idempotency integration test.

Submits the same function twice with the same _idempotency_key and verifies:
  1. Both calls return the same job_id.
  2. The function body executes only once (proven via a tmpfile side channel
     that the user fn writes to — if it runs twice the file has two lines).
  3. A second key still works.

Run with:
    .venv/bin/python tests/test_idempotency.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PY = os.path.join(ROOT, ".venv/bin/python")
ORCH_BIN = os.path.join(ROOT, "orchestrator/orchestrator")

sys.path.insert(0, os.path.join(ROOT, "sdk"))


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


def _boot_orch(db_path: str, log_path: str):
    env = os.environ.copy()
    env["MINIMODAL_DB_PATH"] = db_path
    log = open(log_path, "w")
    proc = subprocess.Popen([ORCH_BIN], env=env, stdout=log, stderr=subprocess.STDOUT)
    _await_port("127.0.0.1", 50051, 5.0)
    return proc


def _boot_worker(worker_id: str, port: int, log_path: str):
    env = os.environ.copy()
    env["ORCH_ADDR"] = "127.0.0.1:50051"
    env["WORKER_ADVERTISE_HOST"] = "127.0.0.1"
    env["WORKER_PORT"] = str(port)
    env["WORKER_ID"] = worker_id
    env["PYTHONPATH"] = f"{ROOT}/worker:{ROOT}/sdk"
    log = open(log_path, "w")
    proc = subprocess.Popen([PY, "-m", "worker.worker"], env=env, stdout=log, stderr=subprocess.STDOUT)
    _await_port("127.0.0.1", port, 5.0)
    time.sleep(0.5)
    return proc


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


def test_idempotent_submission() -> None:
    print("\n=== test_idempotent_submission ===")
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)

    db = "/tmp/minimodal-idem.db"
    try:
        os.unlink(db)
    except FileNotFoundError:
        pass

    counter_file = tempfile.NamedTemporaryFile(delete=False, suffix=".log").name
    open(counter_file, "w").close()  # truncate

    orch = _boot_orch(db, "/tmp/orch-idem.log")
    worker = _boot_worker("idem-w", 50110, "/tmp/w-idem.log")

    try:
        for mod in list(sys.modules):
            if mod.startswith("minimodal"):
                del sys.modules[mod]
        os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"
        from minimodal import App

        app = App("idem-test")
        cf = counter_file  # closure capture

        @app.function
        def write_once(token: str) -> str:
            # Side channel: every execution appends to the file. If the
            # function runs twice, the file has two lines.
            with open(cf, "a") as fh:
                fh.write(token + "\n")
            return token

        # First submit with key "k1"
        f1 = write_once.spawn("alpha", _idempotency_key="k1")
        r1 = f1.get(timeout_s=10.0)
        print(f"  first submit:  job_id={f1.job_id}  result={r1!r}")

        # Second submit with same key — should NOT execute again
        f2 = write_once.spawn("alpha", _idempotency_key="k1")
        r2 = f2.get(timeout_s=10.0)
        print(f"  second submit: job_id={f2.job_id}  result={r2!r}")

        assert f1.job_id == f2.job_id, f"expected same job_id, got {f1.job_id} vs {f2.job_id}"
        assert r1 == r2 == "alpha"

        # Side channel: file should have exactly one line
        with open(counter_file) as fh:
            lines = fh.readlines()
        print(f"  side-channel lines: {lines!r}")
        assert len(lines) == 1, f"expected 1 execution, got {len(lines)} lines: {lines!r}"

        # Different key → new job
        f3 = write_once.spawn("beta", _idempotency_key="k2")
        r3 = f3.get(timeout_s=10.0)
        print(f"  third submit (new key): job_id={f3.job_id} result={r3!r}")
        assert f3.job_id != f1.job_id
        assert r3 == "beta"

        with open(counter_file) as fh:
            lines = fh.readlines()
        assert len(lines) == 2, f"expected 2 total executions, got {len(lines)}"
        print("  PASS")
    finally:
        _kill(worker)
        _kill(orch)
        try:
            os.unlink(counter_file)
        except FileNotFoundError:
            pass


def test_idempotency_persists_across_restart() -> None:
    """An idempotency key honored after orchestrator restart (BoltDB persistence)."""
    print("\n=== test_idempotency_persists_across_restart ===")
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)

    db = "/tmp/minimodal-idem-restart.db"
    try:
        os.unlink(db)
    except FileNotFoundError:
        pass

    counter_file = tempfile.NamedTemporaryFile(delete=False, suffix=".log").name
    open(counter_file, "w").close()

    orch = _boot_orch(db, "/tmp/orch-idem-r-a.log")
    worker = _boot_worker("idem-r-w", 50111, "/tmp/w-idem-r.log")

    try:
        for mod in list(sys.modules):
            if mod.startswith("minimodal"):
                del sys.modules[mod]
        os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"
        from minimodal import App
        cf = counter_file

        app = App("idem-restart")

        @app.function
        def write_once(token: str) -> str:
            with open(cf, "a") as fh:
                fh.write(token + "\n")
            return token

        f1 = write_once.spawn("xyz", _idempotency_key="kpersist")
        r1 = f1.get(timeout_s=10.0)
        print(f"  before restart: job_id={f1.job_id} result={r1!r}")
        saved_id = f1.job_id

        _kill(worker)
        _kill(orch)
        time.sleep(0.3)

        # Restart with same DB.
        orch = _boot_orch(db, "/tmp/orch-idem-r-b.log")
        worker = _boot_worker("idem-r-w2", 50112, "/tmp/w-idem-r2.log")

        for mod in list(sys.modules):
            if mod.startswith("minimodal"):
                del sys.modules[mod]
        from minimodal import App as App2

        app2 = App2("idem-restart")

        @app2.function
        def write_once2(token: str) -> str:
            with open(cf, "a") as fh:
                fh.write(token + "\n")
            return token

        f2 = write_once2.spawn("xyz", _idempotency_key="kpersist")
        r2 = f2.get(timeout_s=10.0)
        print(f"  after restart:  job_id={f2.job_id} result={r2!r}")

        assert f2.job_id == saved_id, "idempotency key not honored across restart"

        with open(counter_file) as fh:
            lines = fh.readlines()
        assert len(lines) == 1, f"expected exactly 1 execution, got {len(lines)}"
        print("  PASS")
    finally:
        _kill(worker)
        _kill(orch)
        try:
            os.unlink(counter_file)
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    test_idempotent_submission()
    test_idempotency_persists_across_restart()
    print("\nall idempotency tests passed ✓")
