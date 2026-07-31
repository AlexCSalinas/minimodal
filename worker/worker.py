"""Worker entrypoint.

Startup order matters because some cold-start strategies (fork) require
forking BEFORE the gRPC server, the orchestrator channel, or any thread is
opened. We do all that in this order:

  1. Decide strategy from MINIMODAL_COLD_START. If `fork`: build the warm
     pool now — this is the only window where fork() is safe.
  2. Open the orchestrator gRPC channel (needed for register, heartbeat,
     and the worker's ReportTaskResult callback).
  3. Start the task thread pool.
  4. Start the worker gRPC server (which spawns gRPC handler threads).
  5. Register with the orchestrator.
  6. Run the heartbeat loop until shutdown.

ExecuteTask acks immediately, runs the task on the worker-local thread pool
(which dispatches to the chosen Executor), then calls
Orchestrator.ReportTaskResult with the outcome.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import threading
import time
import uuid
from concurrent import futures

import grpc

from worker.executor import Executor, make_executor
from worker.pb import minimodal_pb2, minimodal_pb2_grpc

log = logging.getLogger("worker")
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


class WorkerServicer(minimodal_pb2_grpc.WorkerServicer):
    def __init__(
        self,
        worker_id: str,
        orch_stub: minimodal_pb2_grpc.OrchestratorStub,
        task_pool: futures.ThreadPoolExecutor,
        executor: Executor,
    ) -> None:
        self.worker_id = worker_id
        self._orch = orch_stub
        self._task_pool = task_pool
        self._executor = executor
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active_tasks(self) -> int:
        with self._lock:
            return self._active

    def _inc(self) -> None:
        with self._lock:
            self._active += 1

    def _dec(self) -> None:
        with self._lock:
            self._active -= 1

    def ExecuteTask(self, request, context):  # type: ignore[override]
        log.info(
            "ExecuteTask job_id=%s fn=%s strategy=%s bytes=%d",
            request.job_id, request.function_name, self._executor.name, len(request.function_bytes),
        )
        self._inc()
        self._task_pool.submit(self._run_task, request)
        return minimodal_pb2.ExecuteTaskResponse(accepted=True)

    def _run_task(self, request) -> None:
        try:
            result = self._executor.execute(request.function_bytes, request.args_bytes)
            self._report(
                job_id=request.job_id,
                success=result.success,
                result_bytes=result.result_bytes,
                error=result.error,
                cold_start_ms=result.cold_start_ms,
                execution_ms=result.execution_ms,
            )
        except Exception as e:
            log.exception("unexpected error in _run_task job=%s", request.job_id)
            self._report(
                job_id=request.job_id, success=False, result_bytes=b"",
                error=f"worker internal error: {e}", cold_start_ms=0, execution_ms=0,
            )
        finally:
            self._dec()

    def _report(self, **kwargs) -> None:
        try:
            self._orch.ReportTaskResult(
                minimodal_pb2.ReportTaskResultRequest(worker_id=self.worker_id, **kwargs),
                timeout=5.0,
            )
        except grpc.RpcError as e:
            log.error("ReportTaskResult failed job=%s: %s", kwargs.get("job_id"), e.code())


def resolve_advertise_address(port: int) -> str:
    host = os.environ.get("WORKER_ADVERTISE_HOST") or socket.gethostname()
    return f"{host}:{port}"


def register(stub, worker_id: str, address: str, capacity: int) -> int:
    backoff = 0.5
    while True:
        try:
            resp = stub.RegisterWorker(
                minimodal_pb2.RegisterWorkerRequest(
                    worker_id=worker_id, address=address, capacity=capacity,
                ),
                timeout=5.0,
            )
            log.info(
                "registered with orchestrator id=%s addr=%s capacity=%d hb_interval_ms=%d",
                worker_id, address, capacity, resp.heartbeat_interval_ms,
            )
            return resp.heartbeat_interval_ms or 2000
        except grpc.RpcError as e:
            log.warning("register failed: %s — retrying in %.1fs", e.code(), backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 5.0)


def heartbeat_loop(stub, servicer: WorkerServicer, address: str, capacity: int, interval_s: float, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            resp = stub.Heartbeat(
                minimodal_pb2.HeartbeatRequest(
                    worker_id=servicer.worker_id,
                    active_tasks=servicer.active_tasks,
                ),
                timeout=2.0,
            )
            if resp.should_drain:
                log.warning("orchestrator asked us to drain — re-registering")
                register(stub, servicer.worker_id, address, capacity)
        except grpc.RpcError as e:
            log.warning("heartbeat failed: %s", e.code())
        stop.wait(interval_s)


def main() -> int:
    orch_addr = os.environ.get("ORCH_ADDR", "localhost:50051")
    worker_port = int(os.environ.get("WORKER_PORT", "50100"))
    capacity = int(os.environ.get("WORKER_CAPACITY", "4"))
    worker_id = os.environ.get("WORKER_ID") or f"worker-{uuid.uuid4().hex[:8]}"
    strategy = os.environ.get("MINIMODAL_COLD_START", "inproc")
    advertise = resolve_advertise_address(worker_port)

    # ------------------------------------------------------------------ #
    # STEP 1: build the executor. For `fork` this fork()s the warm pool
    # children BEFORE we touch any threads or sockets. fork() in a
    # multithreaded program is undefined behavior — this ordering is the
    # entire reason we make the executor here, before anything else.
    # ------------------------------------------------------------------ #
    log.info("strategy=%s capacity=%d", strategy, capacity)
    executor = make_executor(strategy, capacity)
    if hasattr(executor, "start"):
        executor.start()  # warm pool: forks now

    # ------------------------------------------------------------------ #
    # STEP 2-onwards: from here on we open sockets and start threads.
    # ------------------------------------------------------------------ #
    channel = grpc.insecure_channel(orch_addr)
    orch_stub = minimodal_pb2_grpc.OrchestratorStub(channel)
    task_pool = futures.ThreadPoolExecutor(max_workers=capacity, thread_name_prefix="task")

    servicer = WorkerServicer(worker_id, orch_stub, task_pool, executor)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(8, capacity * 2)))
    minimodal_pb2_grpc.add_WorkerServicer_to_server(servicer, server)
    server.add_insecure_port(f"[::]:{worker_port}")
    server.start()
    log.info("worker grpc server listening on :%d (advertising %s)", worker_port, advertise)

    hb_interval_ms = register(orch_stub, worker_id, advertise, capacity)

    stop = threading.Event()
    hb_thread = threading.Thread(
        target=heartbeat_loop,
        args=(orch_stub, servicer, advertise, capacity, hb_interval_ms / 1000.0, stop),
        daemon=True,
        name="heartbeat",
    )
    hb_thread.start()

    shutdown = threading.Event()

    def handle_signal(sig, _frame):
        log.info("got signal %s, shutting down", sig)
        shutdown.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    shutdown.wait()

    stop.set()
    server.stop(grace=5.0).wait()
    task_pool.shutdown(wait=True, cancel_futures=True)
    executor.shutdown()
    channel.close()
    log.info("worker exited cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(main())
