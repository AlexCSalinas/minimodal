"""Serialization is the contract between SDK, orchestrator bytes, and worker."""

from __future__ import annotations

import pytest
from minimodal.serialization import (
    SerializationError,
    deserialize_args,
    deserialize_callable,
    deserialize_result,
    serialize_args,
    serialize_callable,
    serialize_result,
)


def test_roundtrips_a_plain_function():
    def double(x):
        return x * 2

    assert deserialize_callable(serialize_callable(double))(21) == 42


def test_roundtrips_a_closure():
    factor = 3

    def scale(x):
        return x * factor

    assert deserialize_callable(serialize_callable(scale))(5) == 15


def test_roundtrips_args_and_kwargs():
    args, kwargs = deserialize_args(serialize_args((1, "two"), {"three": 3}))
    assert args == (1, "two")
    assert kwargs == {"three": 3}


def test_roundtrips_results():
    assert deserialize_result(serialize_result({"a": [1, 2]})) == {"a": [1, 2]}


def test_empty_result_payload_is_none():
    # The orchestrator sends empty result_bytes for jobs that never produced
    # one (failed / still pending), which must not blow up the client.
    assert deserialize_result(b"") is None


def test_unpicklable_function_fails_at_submit_time():
    import threading

    lock = threading.Lock()

    def uses_lock():
        with lock:
            return 1

    with pytest.raises(SerializationError, match="function not picklable"):
        serialize_callable(uses_lock)


def test_unpicklable_args_fail_at_submit_time():
    import threading

    with pytest.raises(SerializationError, match="args not picklable"):
        serialize_args((threading.Lock(),), {})


def test_unpicklable_result_is_reported_as_such():
    import threading

    with pytest.raises(SerializationError, match="result not picklable"):
        serialize_result(threading.Lock())
