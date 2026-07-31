"""Fork-based warm worker pool — the second cold-start strategy.

DESIGN
------
On worker startup (before any thread or socket is opened) the pool:
  1. Imports every module in WarmPoolConfig.preimport. These imports become
     resident pages that all forked children share via copy-on-write.
  2. Forks a single-threaded *spawner* process. Every warm child is forked by
     the spawner rather than by the worker, so children can be replaced at any
     time — forking from the worker itself is only safe before it starts
     threads. The spawner inherits the preimported pages, so respawned
     children are just as warm as the originals.
  3. Asks the spawner for N children. Each child binds a Unix domain socket
     and loops on accept(), reading a (fn_bytes, args_bytes) request and
     writing back an envelope with the result + execution_ms.
  4. The parent pushes each (pid, socket_path) into a thread-safe queue of
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
  - If a child dies (e.g. user fn segfaults via a C extension) or wedges past
    the task timeout, the pool kills it and asks the spawner for a fresh one,
    so the pool cannot shrink to zero and strand every later task.
  - Long-lived children accumulate state from previous tasks (module-level
    mutations, leaked file handles). v1 reuses children indefinitely. A
    "true" Modal-like build would cycle children after K tasks or M bytes
    of leaked memory.
"""

from __future__ import annotations

import ctypes
import json
import logging
import os
import queue
import signal
import socket
import struct
import sys
import tempfile
import threading
import time
import traceback
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field

import cloudpickle

from worker.executor import ExecutionResult

log = logging.getLogger("worker.warm_pool")

_PR_SET_PDEATHSIG = 1


def _die_with_parent() -> None:
    """Ask the kernel to SIGTERM this process when its parent exits.

    Linux-only (prctl); a no-op everywhere else. Without it an orphaned
    spawner or warm child would linger after the worker is killed.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(_PR_SET_PDEATHSIG, signal.SIGTERM)
    except OSError:
        pass


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
    _die_with_parent()

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
# Spawner process
# =============================================================================
def _spawner_main(control_path: str) -> None:
    """Fork a warm child per request on `control_path`. Never returns.

    Lives so the pool can be refilled after the worker has started threads,
    at which point the worker can no longer fork safely itself. The spawner
    is single-threaded for its whole life, so fork() here stays safe, and it
    carries the same preimported pages the worker had at start() time.
    """
    _die_with_parent()
    children: list[int] = []

    def _cleanup_and_exit(signum, _frame):
        for pid in children:
            _signal_pid(pid, signal.SIGTERM)
        try:
            os.unlink(control_path)
        except FileNotFoundError:
            pass
        os._exit(0)

    signal.signal(signal.SIGTERM, _cleanup_and_exit)
    signal.signal(signal.SIGINT, _cleanup_and_exit)

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        srv.bind(control_path)
    except OSError as e:
        sys.stderr.write(f"warm-spawner bind {control_path}: {e}\n")
        os._exit(1)
    srv.listen(8)

    while True:
        try:
            conn, _ = srv.accept()
        except OSError:
            break
        try:
            request = json.loads(_recv_message(conn))
            sock_path = request["sock_path"]
            try:
                os.unlink(sock_path)
            except FileNotFoundError:
                pass

            pid = os.fork()
            if pid == 0:
                # ---- warm child ---- close inherited spawner fds first.
                srv.close()
                conn.close()
                try:
                    _child_main(sock_path)
                finally:
                    os._exit(0)

            children.append(pid)
            _send_message(conn, json.dumps({"pid": pid}).encode())
        except Exception:
            sys.stderr.write(f"warm-spawner error:\n{traceback.format_exc()}\n")
            try:
                _send_message(conn, json.dumps({"error": "spawn failed"}).encode())
            except OSError:
                pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
            children = [pid for pid in children if not _reap(pid)]


def _reap(pid: int) -> bool:
    """waitpid(WNOHANG) the child. True once it has exited and been reaped."""
    try:
        reaped, _ = os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return True
    except OSError:
        return False
    return reaped != 0


def _signal_pid(pid: int, sig: int) -> None:
    try:
        os.kill(pid, sig)
    except (ProcessLookupError, PermissionError):
        pass


def _await_socket(path: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return
        time.sleep(0.005)
    raise RuntimeError(f"unix socket {path} never appeared")


# =============================================================================
# Parent-side warm pool
# =============================================================================
@dataclass
class WarmPoolConfig:
    size: int = 4
    preimport: Iterable[str] = field(default_factory=tuple)
    socket_dir: str | None = None
    # Longest a single task may occupy a warm child before it is killed and
    # replaced. Bounds the damage from a user function that never returns.
    task_timeout_s: float = 60.0
    # Longest execute() waits for a free child before giving up. Without a
    # bound, a saturated pool turns into an unbounded hang.
    acquire_timeout_s: float = 30.0


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
        self._slots_lock = threading.Lock()
        self._socket_dir = config.socket_dir or tempfile.mkdtemp(prefix="minimodal-warm-")
        self._control_path = os.path.join(self._socket_dir, "spawner.sock")
        self._spawner_pid: int | None = None
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

        self._start_spawner()

        for _ in range(self._config.size):
            slot = self._spawn_child()
            with self._slots_lock:
                self._slots.append(slot)
            self._idle.put(slot)

        self._started = True
        log.info("warm pool started: %d children", len(self._slots))

    def shutdown(self) -> None:
        # The spawner SIGTERMs and reaps its children, so killing it tears the
        # whole pool down. Kill the children too in case it already died.
        for slot in self._slots:
            _signal_pid(slot.pid, signal.SIGTERM)
        if self._spawner_pid is not None:
            _signal_pid(self._spawner_pid, signal.SIGTERM)
            try:
                os.waitpid(self._spawner_pid, 0)
            except (ChildProcessError, OSError):
                pass
            self._spawner_pid = None

        for path in [slot.sock_path for slot in self._slots] + [self._control_path]:
            try:
                os.unlink(path)
            except OSError:
                pass
        with self._slots_lock:
            self._slots.clear()
        self._started = False

    # -- Executor protocol --------------------------------------------------
    def execute(self, function_bytes: bytes, args_bytes: bytes) -> ExecutionResult:
        if not self._started:
            raise RuntimeError("WarmPool not started")

        wall_start = time.monotonic()
        try:
            slot = self._idle.get(timeout=self._config.acquire_timeout_s)
        except queue.Empty:
            return ExecutionResult(
                success=False,
                error=(
                    "no warm child available within "
                    f"{self._config.acquire_timeout_s}s"
                ),
                cold_start_ms=int((time.monotonic() - wall_start) * 1000),
            )
        request = cloudpickle.dumps({"fn": function_bytes, "args": args_bytes})
        healthy = True

        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self._config.task_timeout_s)
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
        except (TimeoutError, ConnectionError, EOFError, OSError) as e:
            healthy = False
            log.error("warm child %d failed: %s — replacing it", slot.pid, e)
            return ExecutionResult(
                success=False,
                error=f"warm child connection error: {e}",
                cold_start_ms=int((time.monotonic() - wall_start) * 1000),
            )
        finally:
            if healthy:
                self._idle.put(slot)
            else:
                self._replace(slot)

    # -- internals ----------------------------------------------------------
    def _start_spawner(self) -> None:
        """Fork the single-threaded process that forks every warm child.

        MUST run while the worker is still single-threaded: this is the last
        fork() the worker itself performs.
        """
        try:
            os.unlink(self._control_path)
        except FileNotFoundError:
            pass

        pid = os.fork()
        if pid == 0:
            try:
                _spawner_main(self._control_path)
            finally:
                os._exit(0)

        self._spawner_pid = pid
        try:
            _await_socket(self._control_path, timeout_s=5.0)
        except RuntimeError:
            _signal_pid(pid, signal.SIGKILL)
            self._spawner_pid = None
            raise
        log.info("warm-pool spawner ready (pid=%d)", pid)

    def _spawn_child(self) -> _ChildSlot:
        """Ask the spawner for one fresh warm child."""
        sock_path = os.path.join(self._socket_dir, f"child-{uuid.uuid4().hex[:8]}.sock")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(5.0)
        try:
            sock.connect(self._control_path)
            _send_message(sock, json.dumps({"sock_path": sock_path}).encode())
            reply = json.loads(_recv_message(sock))
        finally:
            sock.close()

        if "pid" not in reply:
            raise RuntimeError(f"spawner refused to fork a child: {reply.get('error')}")

        _await_socket(sock_path, timeout_s=5.0)
        slot = _ChildSlot(pid=int(reply["pid"]), sock_path=sock_path)
        log.info("warm child ready (pid=%d sock=%s)", slot.pid, slot.sock_path)
        return slot

    def _replace(self, slot: _ChildSlot) -> None:
        """Kill a broken child and put a fresh one in its place.

        A pool that only ever shrinks eventually strands every task on an
        empty idle queue, so replacement — not removal — is the invariant.
        """
        _signal_pid(slot.pid, signal.SIGKILL)
        try:
            os.unlink(slot.sock_path)
        except OSError:
            pass
        with self._slots_lock:
            if slot in self._slots:
                self._slots.remove(slot)

        try:
            fresh = self._spawn_child()
        except Exception as e:
            log.error("could not respawn warm child: %s — pool is now %d", e, len(self._slots))
            return
        with self._slots_lock:
            self._slots.append(fresh)
        self._idle.put(fresh)
