"""Future returned by Func.spawn() — polls GetJobStatus until DONE/FAILED."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

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
        # Populated when get() resolves (DONE or FAILED).
        self.cold_start_ms: int = 0
        self.total_duration_ms: int = 0
        self.execution_ms: int = 0
        self.worker_id: str = ""

    def get(self, timeout_s: float = 60.0, poll_interval_s: float = 0.05) -> Any:
        """Block until the job finishes (or timeout). Re-raises remote errors."""
        if self._resolved:
            return self._cached_result

        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            status = self._client.get_job_status(self.job_id)
            if status.status == minimodal_pb2.JOB_STATUS_DONE:
                self._absorb_metrics(status)
                self._cached_result = deserialize_result(status.result_bytes)
                self._resolved = True
                return self._cached_result
            if status.status == minimodal_pb2.JOB_STATUS_FAILED:
                self._absorb_metrics(status)
                raise FutureError(f"job {self.job_id} failed: {status.error}")
            time.sleep(poll_interval_s)

        raise TimeoutError(f"job {self.job_id} did not complete within {timeout_s}s")

    def _absorb_metrics(self, status) -> None:
        self.cold_start_ms = status.cold_start_ms
        self.total_duration_ms = status.total_duration_ms
        self.execution_ms = max(0, status.total_duration_ms - status.cold_start_ms)
        self.worker_id = status.worker_id
