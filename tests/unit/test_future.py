"""Future polling: the SDK's only view of a job's lifecycle."""

from __future__ import annotations

import pytest
from minimodal.future import Future, FutureError
from minimodal.pb import minimodal_pb2
from minimodal.serialization import serialize_result


class FakeClient:
    """Returns each queued status in turn, repeating the last one forever."""

    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.calls = 0

    def get_job_status(self, job_id):
        self.calls += 1
        if len(self.statuses) > 1:
            return self.statuses.pop(0)
        return self.statuses[0]


def status(state, **kwargs):
    return minimodal_pb2.JobStatusResponse(status=state, **kwargs)


PENDING = minimodal_pb2.JOB_STATUS_PENDING
RUNNING = minimodal_pb2.JOB_STATUS_RUNNING
DONE = minimodal_pb2.JOB_STATUS_DONE
FAILED = minimodal_pb2.JOB_STATUS_FAILED


def test_polls_until_the_job_is_done():
    client = FakeClient(
        status(PENDING),
        status(RUNNING),
        status(DONE, result_bytes=serialize_result("hi")),
    )
    assert Future("job-1", client).get(poll_interval_s=0.001) == "hi"
    assert client.calls == 3


def test_caches_the_result_instead_of_repolling():
    client = FakeClient(status(DONE, result_bytes=serialize_result(7)))
    future = Future("job-1", client)
    assert future.get(poll_interval_s=0.001) == 7
    assert future.get(poll_interval_s=0.001) == 7
    assert client.calls == 1


def test_a_failed_job_raises_with_the_remote_error():
    client = FakeClient(status(FAILED, error="ValueError: nope"))
    with pytest.raises(FutureError, match="ValueError: nope"):
        Future("job-1", client).get(poll_interval_s=0.001)


def test_a_job_that_never_finishes_times_out():
    client = FakeClient(status(RUNNING))
    with pytest.raises(TimeoutError):
        Future("job-1", client).get(timeout_s=0.05, poll_interval_s=0.001)


def test_absorbs_timing_metadata_from_the_terminal_status():
    client = FakeClient(
        status(
            DONE,
            result_bytes=serialize_result(None),
            cold_start_ms=12,
            total_duration_ms=50,
            worker_id="worker-a",
        )
    )
    future = Future("job-1", client)
    future.get(poll_interval_s=0.001)
    assert (future.cold_start_ms, future.execution_ms) == (12, 38)
    assert future.worker_id == "worker-a"


def test_execution_time_never_goes_negative():
    client = FakeClient(
        status(DONE, result_bytes=serialize_result(None), cold_start_ms=80, total_duration_ms=50)
    )
    future = Future("job-1", client)
    future.get(poll_interval_s=0.001)
    assert future.execution_ms == 0
