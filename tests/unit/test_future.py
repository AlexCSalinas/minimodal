"""Future semantics over the WatchJob event stream."""

from __future__ import annotations

import grpc
import pytest
from minimodal.future import Future, FutureError
from minimodal.pb import minimodal_pb2
from minimodal.serialization import serialize_result

DONE = minimodal_pb2.JOB_STATUS_DONE
FAILED = minimodal_pb2.JOB_STATUS_FAILED
RUNNING = minimodal_pb2.JOB_STATUS_RUNNING


def status_event(state):
    return minimodal_pb2.JobEvent(job_id="job-1", status=state)


def log_event(line, is_stderr=False):
    return minimodal_pb2.JobEvent(
        job_id="job-1", log=minimodal_pb2.LogLine(line=line, is_stderr=is_stderr)
    )


def result_event(state=DONE, **kwargs):
    return minimodal_pb2.JobEvent(
        job_id="job-1", result=minimodal_pb2.JobResult(status=state, **kwargs)
    )


class FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class FakeClient:
    """Serves a canned WatchJob event stream, optionally ending in an error."""

    def __init__(self, *events, error=None):
        self.events = events
        self.error = error
        self.watch_calls = 0

    def watch_job(self, job_id, timeout_s=None):
        self.watch_calls += 1
        yield from self.events
        if self.error is not None:
            raise self.error


def test_streams_logs_and_returns_the_pushed_result(capsys):
    client = FakeClient(
        status_event(RUNNING),
        log_event("step 1"),
        log_event("step 2"),
        result_event(DONE, result_bytes=serialize_result("hi")),
    )
    future = Future("job-1", client)
    assert future.get() == "hi"
    assert future.logs == [("step 1", False), ("step 2", False)]
    assert capsys.readouterr().out == "step 1\nstep 2\n"


def test_stderr_lines_are_relayed_to_stderr(capsys):
    client = FakeClient(
        log_event("oops", is_stderr=True),
        result_event(DONE, result_bytes=serialize_result(None)),
    )
    Future("job-1", client).get()
    captured = capsys.readouterr()
    assert captured.err == "oops\n"
    assert captured.out == ""


def test_print_logs_false_collects_silently(capsys):
    client = FakeClient(
        log_event("quiet"),
        result_event(DONE, result_bytes=serialize_result(None)),
    )
    future = Future("job-1", client)
    future.get(print_logs=False)
    assert future.logs == [("quiet", False)]
    assert capsys.readouterr().out == ""


def test_caches_the_result_instead_of_rewatching():
    client = FakeClient(result_event(DONE, result_bytes=serialize_result(7)))
    future = Future("job-1", client)
    assert future.get() == 7
    assert future.get() == 7
    assert client.watch_calls == 1


def test_a_failed_job_raises_with_the_remote_error():
    client = FakeClient(result_event(FAILED, error="ValueError: nope"))
    with pytest.raises(FutureError, match="ValueError: nope"):
        Future("job-1", client).get()


def test_deadline_exceeded_maps_to_timeout():
    client = FakeClient(
        status_event(RUNNING),
        error=FakeRpcError(grpc.StatusCode.DEADLINE_EXCEEDED),
    )
    with pytest.raises(TimeoutError):
        Future("job-1", client).get(timeout_s=0.05)


def test_other_rpc_errors_propagate():
    client = FakeClient(error=FakeRpcError(grpc.StatusCode.UNAVAILABLE))
    with pytest.raises(grpc.RpcError):
        Future("job-1", client).get()


def test_stream_ending_without_a_result_raises():
    client = FakeClient(status_event(RUNNING), log_event("then nothing"))
    with pytest.raises(FutureError, match="ended without a result"):
        Future("job-1", client).get()


def test_absorbs_timing_metadata_from_the_result_event():
    client = FakeClient(
        result_event(
            DONE,
            result_bytes=serialize_result(None),
            cold_start_ms=12,
            total_duration_ms=50,
            worker_id="worker-a",
        )
    )
    future = Future("job-1", client)
    future.get()
    assert (future.cold_start_ms, future.execution_ms) == (12, 38)
    assert future.worker_id == "worker-a"


def test_execution_time_never_goes_negative():
    client = FakeClient(
        result_event(
            DONE, result_bytes=serialize_result(None), cold_start_ms=80, total_duration_ms=50
        )
    )
    future = Future("job-1", client)
    future.get()
    assert future.execution_ms == 0
