"""Fork-based warm worker pool — the second cold-start strategy.

DESIGN
------
On worker startup (before any thread or socket is opened) the pool:
  1. Imports every module in WarmPoolConfig.preimport. These imports become
     resident pages that all forked children share via copy-on-write.
  2. Forks N child processes. Each child binds a Unix domain socket and loops
     on accept(), reading a (fn_bytes, args_bytes) request and writing back
     an envelope with the result + execution_ms.
  3. The parent pushes each (pid, socket_path) into a thread-safe queue of
     idle children.

On ExecuteTask the worker thread calls ForkWarmPoolExecutor.execute(), which:
  - Pops an idle child from the queue (blocking until one is free).
  - Connects to the child's socket, sends the framed request, reads the
    framed response.
  - Returns the child to the idle queue (so it can serve the next task).

The win vs `naive`: the child doesn't pay Python startup OR `import cloudpickle`
OR any preimport cost. It already has all of that resident. Cold-start cost
drops from ~50–150ms to sub-millisecond.

CAVEATS
-------
  - fork() in a multithreaded program is unsafe (deadlocks via held mutexes
    in the parent). We fork BEFORE the gRPC server or any thread starts —
    enforced by worker/worker.py calling start() first.
  - Children inherit open file descriptors. We open no sockets / grpc
    channels in the parent before forking; the gRPC connection to the
    orchestrator is established afterwards.
  - If a child dies (e.g. user fn segfaults via a C extension), we lose
    that slot. v1 just logs and shrinks the pool; a real impl would respawn.
  - Long-lived children accumulate state from previous tasks (module-level
    mutations, leaked file handles). v1 reuses children indefinitely. A
    "true" Modal-like build would cycle children after K tasks or M bytes
    of leaked memory.
"""

from __future__ import annotations

import logging
import os
import queue
import signal
import socket
import struct
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass, field
from typing import Iterable

import cloudpickle

from worker.executor import ExecutionResult

log = logging.getLogger("worker.warm_pool")


# =============================================================================
# Wire protocol over the Unix socket
# =============================================================================
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError(f"got {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


def _send_message(sock: socket.socket, payload: bytes) -> None:
    sock.sendall(struct.pack("!I", len(payload)))
    sock.sendall(payload)


def _recv_message(sock: socket.socket) -> bytes:
    (n,) = struct.unpack("!I", _recv_exact(sock, 4))
    return _recv_exact(sock, n)


# =============================================================================
# Child process main loop
# =============================================================================
def _child_main(sock_path: str) -> None:
    """Bind, listen, serve forever. Never returns under normal operation."""
    # Children should die promptly if the parent goes away. SIGTERM cleanup
    # below ensures the socket file is unlinked.
    def _cleanup_and_exit(signum, _frame):
        try:
            os.unlink(sock_path)
        except FileNotFoundError:
            pass
        os._exit(0)

    signal.signal(signal.SIGTERM, _cleanup_and_exit)
    signal.signal(signal.SIGINT, _cleanup_and_exit)

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        srv.bind(sock_path)
    except OSError as e:
        sys.stderr.write(f"warm-child bind {sock_path}: {e}\n")
        os._exit(1)
    srv.listen(1)

    while True:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        try:
            request_bytes = _recv_message(conn)
            request = cloudpickle.loads(request_bytes)
            fn_bytes = request["fn"]
            args_bytes = request["args"]

            t0 = time.monotonic()
            try:
                fn = cloudpickle.loads(fn_bytes)
                args, kwargs = cloudpickle.loads(args_bytes)
                result = fn(*args, **kwargs)
                envelope = {
                    "ok": True,
                    "result_bytes": cloudpickle.dumps(result),
                    "execution_ms": int((time.monotonic() - t0) * 1000),
                }
            except Exception:
                envelope = {
                    "ok": False,
                    "error": traceback.format_exc(),
                    "execution_ms": int((time.monotonic() - t0) * 1000),
                }
            _send_message(conn, cloudpickle.dumps(envelope))
        except EOFError:
            pass
        except Exception:
            sys.stderr.write(f"warm-child unexpected error:\n{traceback.format_exc()}\n")
        finally:
            try:
                conn.close()
            except Exception:
                pass


# =============================================================================
# Parent-side warm pool
# =============================================================================
@dataclass
class WarmPoolConfig:
    size: int = 4
    preimport: Iterable[str] = field(default_factory=tuple)
    socket_dir: str | None = None


@dataclass
class _ChildSlot:
    pid: int
    sock_path: str


class ForkWarmPoolExecutor:
    """Executor backed by a pool of forked, pre-warmed Python children."""

    name = "fork"

    def __init__(self, config: WarmPoolConfig) -> None:
        self._config = config
        self._idle: queue.Queue[_ChildSlot] = queue.Queue()
        self._slots: list[_ChildSlot] = []
        self._socket_dir = config.socket_dir or tempfile.mkdtemp(prefix="minimodal-warm-")
        self._started = False

    # -- lifecycle ----------------------------------------------------------
    def start(self) -> None:
        """Pre-import deps, then fork the pool. MUST be called before any
        thread is started or any socket is opened in the parent."""
        if self._started:
            return

        for mod in self._config.preimport:
            mod = mod.strip()
            if not mod:
                continue
            try:
                __import__(mod)
                log.info("preimported %s", mod)
            except Exception as e:
                log.warning("preimport %s failed: %s", mod, e)

        # Also pre-import cloudpickle (we already imported it at module load
        # time, but be explicit so the comment is true).
        import cloudpickle  # noqa: F401

        for i in range(self._config.size):
            sock_path = os.path.join(self._socket_dir, f"child-{i}.sock")
            if os.path.exists(sock_path):
                os.unlink(sock_path)
            pid = os.fork()
            if pid == 0:
                # ---- child ----
                try:
                    _child_main(sock_path)
                finally:
                    os._exit(0)
            # ---- parent ----
            slot = _ChildSlot(pid=pid, sock_path=sock_path)
            self._slots.append(slot)
            self._await_socket(sock_path, timeout_s=5.0)
            self._idle.put(slot)
            log.info("warm child %d ready (pid=%d sock=%s)", i, pid, sock_path)

        self._started = True
        log.info("warm pool started: %d children", len(self._slots))

    def shutdown(self) -> None:
        for slot in self._slots:
            try:
                os.kill(slot.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        # Don't wait for reap — parent is shutting down anyway. The OS will
        # reap orphans. Just clean up socket files.
        for slot in self._slots:
            try:
                os.unlink(slot.sock_path)
            except FileNotFoundError:
                pass
        self._slots.clear()
        self._started = False

    # -- Executor protocol --------------------------------------------------
    def execute(self, function_bytes: bytes, args_bytes: bytes) -> ExecutionResult:
        if not self._started:
            raise RuntimeError("WarmPool not started")

        wall_start = time.monotonic()
        slot = self._idle.get()  # blocks until a child is free
        request = cloudpickle.dumps({"fn": function_bytes, "args": args_bytes})

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.connect(slot.sock_path)
                _send_message(sock, request)
                response_bytes = _recv_message(sock)
            finally:
                sock.close()
            envelope = cloudpickle.loads(response_bytes)
            wall_ms = int((time.monotonic() - wall_start) * 1000)
            execution_ms = int(envelope.get("execution_ms", 0))
            cold_start_ms = max(0, wall_ms - execution_ms)

            if envelope.get("ok"):
                return ExecutionResult(
                    success=True,
                    result_bytes=envelope["result_bytes"],
                    cold_start_ms=cold_start_ms,
                    execution_ms=execution_ms,
                )
            return ExecutionResult(
                success=False,
                error=envelope.get("error", "unknown error"),
                cold_start_ms=cold_start_ms,
                execution_ms=execution_ms,
            )
        except (ConnectionError, EOFError, OSError) as e:
            log.error("warm child %d failed: %s — dropping from pool", slot.pid, e)
            try:
                os.kill(slot.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            return ExecutionResult(
                success=False,
                error=f"warm child connection error: {e}",
                cold_start_ms=int((time.monotonic() - wall_start) * 1000),
            )
        finally:
            if self._is_alive(slot.pid):
                self._idle.put(slot)

    # -- internals ----------------------------------------------------------
    def _await_socket(self, path: str, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if os.path.exists(path):
                return
            time.sleep(0.005)
        raise RuntimeError(f"warm child socket {path} never appeared")

    @staticmethod
    def _is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
