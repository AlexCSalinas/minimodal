"""Thin wrapper over the generated gRPC stub for the Orchestrator service."""

from __future__ import annotations

import grpc

from minimodal.pb import minimodal_pb2, minimodal_pb2_grpc


class OrchestratorClient:
    def __init__(self, address: str) -> None:
        self.address = address
        self._channel = grpc.insecure_channel(address)
        self._stub = minimodal_pb2_grpc.OrchestratorStub(self._channel)

    def invoke(
        self,
        function_bytes: bytes,
        args_bytes: bytes,
        function_name: str = "",
        idempotency_key: str = "",
    ) -> str:
        req = minimodal_pb2.InvokeFunctionRequest(
            function_bytes=function_bytes,
            args_bytes=args_bytes,
            function_name=function_name,
            idempotency_key=idempotency_key,
        )
        resp = self._stub.InvokeFunction(req)
        return resp.job_id

    def get_job_status(self, job_id: str) -> minimodal_pb2.JobStatusResponse:
        return self._stub.GetJobStatus(
            minimodal_pb2.GetJobStatusRequest(job_id=job_id)
        )

    def watch_job(self, job_id: str, timeout_s: float | None = None):
        """Server-streaming subscription to a job's lifecycle.

        Yields JobEvent messages — status transitions, live log lines, and
        finally the terminal result, after which the stream ends. timeout_s
        is a gRPC deadline on the whole stream.
        """
        return self._stub.WatchJob(
            minimodal_pb2.WatchJobRequest(job_id=job_id), timeout=timeout_s
        )

    def close(self) -> None:
        self._channel.close()
