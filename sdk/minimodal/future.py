"""Future returned by Func.spawn() — consumes the WatchJob event stream.

The orchestrator pushes status transitions, live log lines, and finally the
terminal result over a single server-streaming RPC. get() relays remote
stdout/stderr to the local terminal as it arrives, so a `print()` inside the
user function shows up on the caller's screen while the function is still
running — and the result lands with no polling loop.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import grpc

from minimodal.pb import minimodal_pb2
from minimodal.serialization import deserialize_result

if TYPE_CHECKING:
    from minimodal.client import OrchestratorClient


class FutureError(RuntimeError):
    pass


class Future:
    def __init__(self, job_id: str, client: OrchestratorClient) -> None:
        self.job_id = job_id
        self._client = client
        self._cached_result: Any = None
        self._resolved = False
        # Every log line received, as (line, is_stderr), in arrival order.
        self.logs: list[tuple[str, bool]] = []
        # Populated when get() resolves (DONE or FAILED).
        self.cold_start_ms: int = 0
        self.total_duration_ms: int = 0
        self.execution_ms: int = 0
        self.worker_id: str = ""

    def get(
        self,
        timeout_s: float = 60.0,
        poll_interval_s: float | None = None,
        print_logs: bool = True,
    ) -> Any:
        """Block until the job finishes. Re-raises remote errors.

        Remote output is relayed to the local stdout/stderr as it streams in
        (pass print_logs=False to collect it silently on .logs instead).
        poll_interval_s is accepted for backward compatibility and ignored —
        completion is pushed over the WatchJob stream, not polled.
        """
        if self._resolved:
            return self._cached_result

        try:
            for event in self._client.watch_job(self.job_id, timeout_s=timeout_s):
                kind = event.WhichOneof("event")
                if kind == "log":
                    self._on_log(event.log, print_logs)
                elif kind == "result":
                    return self._finish(event.result)
                # "status" events are informational; nothing to do yet.
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise TimeoutError(
                    f"job {self.job_id} did not complete within {timeout_s}s"
                ) from None
            raise

        raise FutureError(f"job {self.job_id}: watch stream ended without a result")

    def _on_log(self, log: minimodal_pb2.LogLine, print_logs: bool) -> None:
        self.logs.append((log.line, log.is_stderr))
        if print_logs:
            stream = sys.stderr if log.is_stderr else sys.stdout
            print(log.line, file=stream, flush=True)

    def _finish(self, result: minimodal_pb2.JobResult) -> Any:
        self.cold_start_ms = result.cold_start_ms
        self.total_duration_ms = result.total_duration_ms
        self.execution_ms = max(0, result.total_duration_ms - result.cold_start_ms)
        self.worker_id = result.worker_id
        if result.status == minimodal_pb2.JOB_STATUS_DONE:
            self._cached_result = deserialize_result(result.result_bytes)
            self._resolved = True
            return self._cached_result
        raise FutureError(f"job {self.job_id} failed: {result.error}")
