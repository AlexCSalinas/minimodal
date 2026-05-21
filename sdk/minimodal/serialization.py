"""cloudpickle wrappers used by SDK + worker.

cloudpickle (vs stdlib pickle) handles closures, lambdas, and locally-defined
classes — common in @app.function use. We wrap each call in try/except and
surface a structured error so unpicklable functions fail fast at submit time
rather than crashing the worker.
"""

from __future__ import annotations

from typing import Any, Callable

import cloudpickle


class SerializationError(RuntimeError):
    pass


def serialize_callable(fn: Callable[..., Any]) -> bytes:
    try:
        return cloudpickle.dumps(fn)
    except Exception as e:
        raise SerializationError(f"function not picklable: {e}") from e


def serialize_args(args: tuple, kwargs: dict) -> bytes:
    try:
        return cloudpickle.dumps((args, kwargs))
    except Exception as e:
        raise SerializationError(f"args not picklable: {e}") from e


def deserialize_callable(payload: bytes) -> Callable[..., Any]:
    return cloudpickle.loads(payload)


def deserialize_args(payload: bytes) -> tuple[tuple, dict]:
    return cloudpickle.loads(payload)


def serialize_result(result: Any) -> bytes:
    try:
        return cloudpickle.dumps(result)
    except Exception as e:
        raise SerializationError(f"result not picklable: {e}") from e


def deserialize_result(payload: bytes) -> Any:
    if not payload:
        return None
    return cloudpickle.loads(payload)
