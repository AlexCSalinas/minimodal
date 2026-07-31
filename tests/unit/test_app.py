"""App / @app.function surface: what a user actually touches."""

from __future__ import annotations

import pytest
from minimodal import App
from minimodal.future import Future
from minimodal.serialization import deserialize_args, deserialize_callable, serialize_result


class FakeClient:
    def __init__(self, result=None):
        self.invocations: list[dict] = []
        self._result = result

    def invoke(self, function_bytes, args_bytes, function_name="", idempotency_key=""):
        self.invocations.append(
            {
                "function_bytes": function_bytes,
                "args_bytes": args_bytes,
                "function_name": function_name,
                "idempotency_key": idempotency_key,
            }
        )
        return f"job-{len(self.invocations)}"

    def get_job_status(self, job_id):
        from minimodal.pb import minimodal_pb2

        return minimodal_pb2.JobStatusResponse(
            status=minimodal_pb2.JOB_STATUS_DONE,
            result_bytes=serialize_result(self._result),
        )


@pytest.fixture
def app():
    application = App("test-app")
    application._client = FakeClient()
    return application


def test_default_address_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("MINIMODAL_ORCH_ADDR", "orchestrator:9999")
    assert App("a")._addr == "orchestrator:9999"


def test_explicit_address_wins_over_the_environment(monkeypatch):
    monkeypatch.setenv("MINIMODAL_ORCH_ADDR", "orchestrator:9999")
    assert App("a", orchestrator_addr="explicit:1")._addr == "explicit:1"


def test_local_and_bare_calls_run_in_process(app):
    @app.function
    def double(x):
        return x * 2

    assert double.local(3) == 6
    assert double(3) == 6
    assert app._client.invocations == []


def test_spawn_serializes_the_function_and_its_arguments(app):
    @app.function
    def add(a, b=0):
        return a + b

    assert isinstance(add.spawn(1, b=2), Future)
    sent = app._client.invocations[0]
    assert deserialize_callable(sent["function_bytes"])(1, 2) == 3
    assert deserialize_args(sent["args_bytes"]) == ((1,), {"b": 2})
    assert sent["function_name"].endswith("add")


def test_the_idempotency_key_is_forwarded_and_not_passed_to_the_function(app):
    @app.function
    def echo(**kwargs):
        return kwargs

    echo.spawn(_idempotency_key="key-1")
    sent = app._client.invocations[0]
    assert sent["idempotency_key"] == "key-1"
    assert deserialize_args(sent["args_bytes"]) == ((), {})


def test_remote_forwards_the_idempotency_key_too():
    application = App("test-app")
    application._client = FakeClient(result="done")

    @application.function
    def noop():
        return None

    assert noop.remote(_idempotency_key="key-2") == "done"
    assert application._client.invocations[0]["idempotency_key"] == "key-2"


def test_functions_are_registered_on_the_app(app):
    @app.function
    def one():
        return 1

    assert app._functions == [one]
    assert one.__name__ == "one"
