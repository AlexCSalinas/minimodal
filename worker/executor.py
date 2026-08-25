"""Executor abstraction + the three cold-start strategies' implementations.

An Executor takes (function_bytes, args_bytes) and returns an ExecutionResult.
The three implementations are:

  InprocExecutor          — run in the worker's own interpreter (Phase 2 default)
  NaiveSubprocessExecutor — fresh `python -m worker.subprocess_runner` per task
  ForkWarmPoolExecutor    — pre-forked warm children (see worker/warm_pool.py)

The worker picks one at startup based on MINIMODAL_COLD_START. cold_start_ms
is the overhead *outside* the user function call (subprocess spawn / fork
dispatch / serialization); execution_ms is just fn(*args, **kwargs).
"""

from __future__ import annotations

import io
import logging
import os
import struct
import subprocess
import sys
import time
import traceback
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from typing import Protocol

from minimodal.serialization import (
    deserialize_args,
    deserialize_callable,
    serialize_result,
)

log = logging.getLogger("worker.executor")


@dataclass
class ExecutionResult:
    success: bool
    result_bytes: bytes = b""
    error: str = ""
    cold_start_ms: int = 0
    execution_ms: int = 0
    stdout: str = ""
    stderr: str = ""


class Executor(Protocol):
    """Interface every cold-start strategy implements.

    output_sink, when given, receives the user function's raw stdout/stderr
    writes as they happen — sink(text, is_stderr) — for live log streaming.
    Strategies that can only capture output at completion ignore the sink and
    return the buffered text in ExecutionResult.stdout/.stderr instead; the
    worker ships those as tail lines with the final report.
    """

    name: str

    def execute(
        self, function_bytes: bytes, args_bytes: bytes, output_sink=None
    ) -> ExecutionResult: ...

    def shutdown(self) -> None: ...


# =============================================================================
# Strategy 1: InprocExecutor
# Run the function in the worker's own interpreter. Phase 2 default. Fast,
# but unrealistic for serverless — once numpy is imported, every subsequent
# task gets it for free.
# =============================================================================
@dataclass
class InprocExecutor:
    name: str = "inproc"

    def execute(
        self, function_bytes: bytes, args_bytes: bytes, output_sink=None
    ) -> ExecutionResult:
        if output_sink is not None:
            return self._execute_streaming(function_bytes, args_bytes, output_sink)
        stdout_buf, stderr_buf = io.StringIO(), io.StringIO()
        cold_start = time.monotonic()
        try:
            fn = deserialize_callable(function_bytes)
            args, kwargs = deserialize_args(args_bytes)
            exec_start = time.monotonic()
            with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
                result = fn(*args, **kwargs)
            exec_end = time.monotonic()
            return ExecutionResult(
                success=True,
                result_bytes=serialize_result(result),
                cold_start_ms=int((exec_start - cold_start) * 1000),
                execution_ms=int((exec_end - exec_start) * 1000),
                stdout=stdout_buf.getvalue(),
                stderr=stderr_buf.getvalue(),
            )
        except Exception:
            return ExecutionResult(
                success=False,
                error=traceback.format_exc(),
                cold_start_ms=int((time.monotonic() - cold_start) * 1000),
                execution_ms=0,
                stdout=stdout_buf.getvalue(),
                stderr=stderr_buf.getvalue(),
            )

    def _execute_streaming(
        self, function_bytes: bytes, args_bytes: bytes, output_sink
    ) -> ExecutionResult:
        # Thread-local routing instead of redirect_stdout: the redirect
        # approach swaps the process-global stream, so two concurrent tasks
        # would capture each other's prints. The router tees each thread's
        # writes to its own sink, live. Output goes to the sink as it
        # happens, so ExecutionResult.stdout/.stderr stay empty — nothing is
        # double-delivered as tail lines.
        from worker.logstream import install_routers

        out_router, err_router = install_routers()
        cold_start = time.monotonic()
        try:
            fn = deserialize_callable(function_bytes)
            args, kwargs = deserialize_args(args_bytes)
            exec_start = time.monotonic()
            out_router.set_sink(output_sink)
            err_router.set_sink(output_sink)
            try:
                result = fn(*args, **kwargs)
            finally:
                out_router.clear_sink()
                err_router.clear_sink()
            exec_end = time.monotonic()
            return ExecutionResult(
                success=True,
                result_bytes=serialize_result(result),
                cold_start_ms=int((exec_start - cold_start) * 1000),
                execution_ms=int((exec_end - exec_start) * 1000),
            )
        except Exception:
            return ExecutionResult(
                success=False,
                error=traceback.format_exc(),
                cold_start_ms=int((time.monotonic() - cold_start) * 1000),
                execution_ms=0,
            )

    def shutdown(self) -> None:
        pass


# =============================================================================
# Strategy 2: NaiveSubprocessExecutor
# Spawn a fresh Python interpreter per task. Pays the full import-overhead
# cost every invocation. This is the worst-case baseline we compare against.
#
# Wire protocol (stdin → child, stdout → parent):
#   request:  [u32 fn_len][fn_bytes][u32 args_len][args_bytes]
#   response: [u32 envelope_len][cloudpickle({ok, result|err, execution_ms})]
# =============================================================================
@dataclass
class NaiveSubprocessExecutor:
    python_path: str = field(default_factory=lambda: sys.executable)
    name: str = "naive"

    def execute(
        self, function_bytes: bytes, args_bytes: bytes, output_sink=None
    ) -> ExecutionResult:
        # output_sink unused: the child buffers user output and returns it in
        # the envelope, so it reaches the client as tail lines at completion.
        wall_start = time.monotonic()
        env = os.environ.copy()
        # Ensure the subprocess can import worker.subprocess_runner + minimodal.
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        env["PYTHONPATH"] = f"{repo_root}:{repo_root}/sdk:{env.get('PYTHONPATH', '')}"
        try:
            proc = subprocess.Popen(
                [self.python_path, "-m", "worker.subprocess_runner"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
            )
        except Exception:
            return ExecutionResult(
                success=False,
                error=f"spawn subprocess: {traceback.format_exc()}",
                cold_start_ms=int((time.monotonic() - wall_start) * 1000),
            )

        request = (
            struct.pack("!I", len(function_bytes)) + function_bytes
            + struct.pack("!I", len(args_bytes)) + args_bytes
        )
        try:
            stdout_data, stderr_data = proc.communicate(request, timeout=60.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout_data, stderr_data = proc.communicate()
            return ExecutionResult(
                success=False,
                error="subprocess timed out",
                cold_start_ms=int((time.monotonic() - wall_start) * 1000),
                stderr=stderr_data.decode(errors="replace"),
            )

        wall_ms = int((time.monotonic() - wall_start) * 1000)

        if proc.returncode != 0 or len(stdout_data) < 4:
            return ExecutionResult(
                success=False,
                error=f"subprocess exit={proc.returncode}: {stderr_data.decode(errors='replace')[:500]}",
                cold_start_ms=wall_ms,
                stderr=stderr_data.decode(errors="replace"),
            )

        envelope_len = struct.unpack("!I", stdout_data[:4])[0]
        envelope_bytes = stdout_data[4 : 4 + envelope_len]
        from minimodal.serialization import deserialize_result as _loads  # cloudpickle.loads
        envelope = _loads(envelope_bytes)

        execution_ms = int(envelope.get("execution_ms", 0))
        cold_start_ms = max(0, wall_ms - execution_ms)

        # User-function output is captured inside the child (see
        # subprocess_runner) and travels back in the envelope — it must NOT
        # go to the child's real stdout, which carries the framed response.
        # Only the envelope's capture counts as user output: the process's
        # raw stderr carries runtime noise (e.g. gRPC fork-handler
        # diagnostics inherited from the worker) and is used for error
        # diagnostics only.
        if envelope.get("ok"):
            return ExecutionResult(
                success=True,
                result_bytes=envelope["result_bytes"],
                cold_start_ms=cold_start_ms,
                execution_ms=execution_ms,
                stdout=envelope.get("stdout", ""),
                stderr=envelope.get("stderr", ""),
            )
        return ExecutionResult(
            success=False,
            error=envelope.get("error", "unknown error"),
            cold_start_ms=cold_start_ms,
            execution_ms=execution_ms,
            stdout=envelope.get("stdout", ""),
            stderr=envelope.get("stderr", ""),
        )

    def shutdown(self) -> None:
        pass


# =============================================================================
# Strategy 3: ForkWarmPoolExecutor — implemented in worker/warm_pool.py
# (lives there because the fork() must happen before any thread or socket in
# the worker process — calling sites need that ordering invariant.)
# =============================================================================


def make_executor(strategy: str, capacity: int) -> Executor:
    """Construct an executor based on MINIMODAL_COLD_START."""
    strategy = strategy.lower()
    if strategy in ("", "inproc"):
        return InprocExecutor()
    if strategy == "naive":
        return NaiveSubprocessExecutor()
    if strategy == "fork":
        from worker.warm_pool import ForkWarmPoolExecutor, WarmPoolConfig
        preimport = tuple(filter(None, os.environ.get("MINIMODAL_PREIMPORT", "").split(",")))
        return ForkWarmPoolExecutor(WarmPoolConfig(
            size=capacity,
            preimport=preimport,
            task_timeout_s=float(os.environ.get("MINIMODAL_TASK_TIMEOUT_S", "60")),
            acquire_timeout_s=float(os.environ.get("MINIMODAL_ACQUIRE_TIMEOUT_S", "30")),
        ))
    if strategy == "criu":
        raise NotImplementedError("CRIU is Linux-only and stubbed in v1 (see worker/snapshot.py)")
    raise ValueError(f"unknown MINIMODAL_COLD_START={strategy!r}")
