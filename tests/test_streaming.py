"""Streaming integration test: live logs + pushed results over gRPC.

Boots the real orchestrator binary + a Python worker and verifies:

  1. print() inside an inproc function reaches the client WHILE the function
     is still running (live streaming) — in order, with stdout/stderr flags —
     and the result arrives as a pushed event, not a poll.
  2. A naive-subprocess function's prints arrive as completion-time tail
     lines, and no longer corrupt the framed result protocol.

Run with:
    .venv/bin/python tests/test_streaming.py
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


def _boot_worker(worker_id: str, port: int, log_path: str, strategy: str = "inproc"):
    env = os.environ.copy()
    env["ORCH_ADDR"] = "127.0.0.1:50051"
    env["WORKER_ADVERTISE_HOST"] = "127.0.0.1"
    env["WORKER_PORT"] = str(port)
    env["WORKER_ID"] = worker_id
    env["MINIMODAL_COLD_START"] = strategy
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


def _reset():
    subprocess.run(["pkill", "-f", "orchestrator/orchestrator"], check=False)
    subprocess.run(["pkill", "-f", "worker.worker"], check=False)
    time.sleep(0.3)
    for mod in list(sys.modules):
        if mod.startswith("minimodal"):
            del sys.modules[mod]
    os.environ["MINIMODAL_ORCH_ADDR"] = "127.0.0.1:50051"


def test_live_log_streaming() -> None:
    print("\n=== test_live_log_streaming ===")
    _reset()
    db = "/tmp/minimodal-stream.db"
    try:
        os.unlink(db)
    except FileNotFoundError:
        pass

    orch = _boot_orch(db, "/tmp/orch-stream.log")
    worker = _boot_worker("stream-w", 50120, "/tmp/w-stream.log", strategy="inproc")

    try:
        from minimodal import App
        from minimodal.serialization import deserialize_result

        app = App("stream-test")

        @app.function
        def chatty(n: int) -> str:
            import sys
            import time as _t

            for i in range(n):
                print(f"tick {i}")
                _t.sleep(0.2)
            print("warning line", file=sys.stderr)
            return "finished"

        fut = chatty.spawn(5)

        # Consume the raw event stream with arrival timestamps so we can
        # prove logs arrived DURING execution, not batched at completion.
        stamped = []
        for event in app.client.watch_job(fut.job_id, timeout_s=30.0):
            kind = event.WhichOneof("event")
            stamped.append((time.monotonic(), kind, event))
            if kind == "result":
                break

        logs = [(e.log.line, e.log.is_stderr) for _, k, e in stamped if k == "log"]
        stdout_lines = [line for line, is_err in logs if not is_err]
        print(f"  received {len(logs)} log lines")
        assert stdout_lines == [f"tick {i}" for i in range(5)], stdout_lines
        assert ("warning line", True) in logs, logs

        t_first_log = next(t for t, k, _ in stamped if k == "log")
        t_result = next(t for t, k, _ in stamped if k == "result")
        gap = t_result - t_first_log
        print(f"  first log arrived {gap:.2f}s before the result")
        assert gap > 0.5, (
            f"logs should stream during the ~1s run, not arrive with the "
            f"result (gap={gap:.3f}s)"
        )

        result_event = next(e for _, k, e in stamped if k == "result").result
        assert deserialize_result(result_event.result_bytes) == "finished"

        # A second get() on the Future exercises the late-watcher path: the
        # job is terminal, so the result must come back immediately from the
        # WAL — and still deserialize identically.
        assert fut.get(timeout_s=10.0, print_logs=False) == "finished"
        print("  PASS")
    finally:
        _kill(worker)
        _kill(orch)


def test_naive_subprocess_tail_lines() -> None:
    print("\n=== test_naive_subprocess_tail_lines ===")
    _reset()
    db = "/tmp/minimodal-stream-naive.db"
    try:
        os.unlink(db)
    except FileNotFoundError:
        pass

    orch = _boot_orch(db, "/tmp/orch-stream-naive.log")
    worker = _boot_worker("stream-nw", 50121, "/tmp/w-stream-naive.log", strategy="naive")

    try:
        from minimodal import App

        app = App("stream-naive-test")

        @app.function
        def chatty_child() -> int:
            print("hello from the subprocess")
            return 21 * 2

        fut = chatty_child.spawn()
        result = fut.get(timeout_s=30.0, print_logs=False)

        # Before this change, the print() landed in front of the framed
        # response envelope on the child's stdout and corrupted the result.
        assert result == 42, result
        # Exactly the user's output — no runtime noise (gRPC fork-handler
        # chatter etc.) may leak into the log stream.
        assert fut.logs == [("hello from the subprocess", False)], fut.logs
        print(f"  result={result} logs={fut.logs}")
        print("  PASS")
    finally:
        _kill(worker)
        _kill(orch)


if __name__ == "__main__":
    test_live_log_streaming()
    test_naive_subprocess_tail_lines()
    print("\nall streaming tests passed ✓")
