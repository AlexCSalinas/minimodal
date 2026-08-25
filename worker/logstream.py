"""Live log streaming: user-function output → orchestrator → watching client.

Two pieces:

  StdIORouter — a process-wide sys.stdout / sys.stderr replacement that tees
  every write to the original stream and, when the *current thread* has a
  sink registered, to that sink. Tasks run one-per-thread on the worker's
  task pool, so a thread-local sink cleanly separates concurrent tasks'
  output. (The previous redirect_stdout approach swapped the process-global
  stream, so two concurrent inproc tasks captured each other's prints.)

  TaskLogStreamer — batches complete lines for one job and ships them to the
  orchestrator over the client-streaming StreamTaskLogs RPC. The RPC and its
  thread start lazily on the first line, so functions that print nothing pay
  zero overhead — important for cold-start benchmarks.
"""

from __future__ import annotations

import io
import logging
import queue
import sys
import threading
import time
from collections.abc import Callable

import grpc

from worker.pb import minimodal_pb2

log = logging.getLogger("worker.logstream")

# sink(text, is_stderr) receives raw write() chunks, not necessarily lines.
Sink = Callable[[str, bool], None]


class StdIORouter(io.TextIOBase):
    def __init__(self, fallback, is_stderr: bool) -> None:
        self._fallback = fallback
        self._is_stderr = is_stderr
        self._local = threading.local()

    def set_sink(self, sink: Sink) -> None:
        self._local.sink = sink

    def clear_sink(self) -> None:
        self._local.sink = None

    def write(self, s: str) -> int:
        sink = getattr(self._local, "sink", None)
        if sink is not None:
            sink(s, self._is_stderr)
        try:
            return self._fallback.write(s)
        except ValueError:
            # Fallback was closed under us (test harness teardown, interpreter
            # exit). The sink already got the data; losing the tee is fine.
            return len(s)

    def flush(self) -> None:
        try:
            self._fallback.flush()
        except ValueError:
            pass

    def writable(self) -> bool:
        return True


_install_lock = threading.Lock()
_routers: tuple[StdIORouter, StdIORouter] | None = None


def install_routers() -> tuple[StdIORouter, StdIORouter]:
    """Swap sys.stdout/sys.stderr for routers, once per process. Idempotent.

    If something replaced sys.stdout since the last call (a test harness's
    capture, a logging wrapper), re-attach on top of the replacement — the
    router must be the live stream for sinks to see the writes.
    """
    global _routers
    with _install_lock:
        if _routers is None:
            _routers = (
                StdIORouter(sys.stdout, is_stderr=False),
                StdIORouter(sys.stderr, is_stderr=True),
            )
        out, err = _routers
        if sys.stdout is not out:
            out._fallback = sys.stdout
            sys.stdout = out
        if sys.stderr is not err:
            err._fallback = sys.stderr
            sys.stderr = err
        return _routers


class LineBuffer:
    """Accumulates raw write() chunks into complete lines for one stream.

    Only ever written from a single thread (the task's thread, via the
    router's thread-local sink), so no locking is needed.
    """

    def __init__(self, emit: Callable[[str, bool], None], is_stderr: bool) -> None:
        self._emit = emit
        self._is_stderr = is_stderr
        self._buf = ""

    def write(self, s: str) -> None:
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            self._emit(line, self._is_stderr)

    def flush_partial(self) -> None:
        """Emit any trailing output that never got a newline."""
        if self._buf:
            self._emit(self._buf, self._is_stderr)
            self._buf = ""


class TaskLogStreamer:
    """Ships one job's log lines to the orchestrator via StreamTaskLogs."""

    _CLOSE = object()
    _MAX_BATCH = 256

    def __init__(self, stub, job_id: str, worker_id: str) -> None:
        self._stub = stub
        self._job_id = job_id
        self._worker_id = worker_id
        self._q: queue.SimpleQueue = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._out = LineBuffer(self._enqueue, is_stderr=False)
        self._err = LineBuffer(self._enqueue, is_stderr=True)

    # The router's Sink: raw text chunks in, complete lines out.
    def sink(self, text: str, is_stderr: bool) -> None:
        (self._err if is_stderr else self._out).write(text)

    def _enqueue(self, line: str, is_stderr: bool) -> None:
        self._q.put(minimodal_pb2.LogLine(
            line=line,
            is_stderr=is_stderr,
            ts_unix_ms=int(time.time() * 1000),
        ))
        self._ensure_thread()

    def _ensure_thread(self) -> None:
        with self._thread_lock:
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run, daemon=True,
                    name=f"logstream-{self._job_id[:8]}",
                )
                self._thread.start()

    def _chunks(self):
        while True:
            item = self._q.get()
            if item is self._CLOSE:
                return
            lines = [item]
            closing = False
            try:
                while len(lines) < self._MAX_BATCH:
                    nxt = self._q.get_nowait()
                    if nxt is self._CLOSE:
                        closing = True
                        break
                    lines.append(nxt)
            except queue.Empty:
                pass
            yield minimodal_pb2.TaskLogChunk(
                job_id=self._job_id, worker_id=self._worker_id, lines=lines,
            )
            if closing:
                return

    def _run(self) -> None:
        try:
            self._stub.StreamTaskLogs(self._chunks())
        except grpc.RpcError as e:
            # Logs are best-effort: a broken stream must never fail the task.
            log.warning("log stream failed job=%s: %s", self._job_id, e.code())

    def close(self, timeout_s: float = 5.0) -> None:
        """Flush partial lines, end the stream, and wait for delivery."""
        self._out.flush_partial()
        self._err.flush_partial()
        if self._thread is None:
            return
        self._q.put(self._CLOSE)
        self._thread.join(timeout_s)
