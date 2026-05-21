"""App + @app.function decorator.

PHASE 2 will implement the full remote() path. Phase 1 stubs out the surface
area so examples can import without crashing.
"""

from __future__ import annotations

import os
from typing import Any, Callable

from minimodal.client import OrchestratorClient
from minimodal.future import Future
from minimodal.serialization import serialize_callable, serialize_args


class Func:
    """Wraps a user function with .remote() / .local() entry points."""

    def __init__(self, fn: Callable[..., Any], app: "App") -> None:
        self._fn = fn
        self._app = app
        self.__name__ = getattr(fn, "__name__", "anonymous")
        self.__qualname__ = getattr(fn, "__qualname__", self.__name__)

    def local(self, *args: Any, **kwargs: Any) -> Any:
        """Run synchronously in this process. For tests + debugging."""
        return self._fn(*args, **kwargs)

    def remote(self, *args: Any, **kwargs: Any) -> Any:
        """Submit to orchestrator and block until the result is available."""
        idem = kwargs.pop("_idempotency_key", "")
        return self.spawn(*args, _idempotency_key=idem, **kwargs).get()

    def spawn(self, *args: Any, **kwargs: Any) -> Future:
        """Submit to orchestrator and return a Future (non-blocking).

        Pass `_idempotency_key="some-key"` to deduplicate against any prior
        submission with the same key. The orchestrator returns the existing
        job_id (with whatever state it's in) instead of executing again.
        This upgrades the at-least-once delivery guarantee toward
        exactly-once for callers that supply keys.
        """
        idempotency_key = kwargs.pop("_idempotency_key", "")
        function_bytes = serialize_callable(self._fn)
        args_bytes = serialize_args(args, kwargs)
        job_id = self._app.client.invoke(
            function_bytes=function_bytes,
            args_bytes=args_bytes,
            function_name=self.__qualname__,
            idempotency_key=idempotency_key,
        )
        return Future(job_id=job_id, client=self._app.client)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # Bare `greet(x)` (no .remote / .local) defaults to local execution.
        return self.local(*args, **kwargs)


class App:
    """A namespace + connection holder for a set of functions."""

    def __init__(self, name: str, orchestrator_addr: str | None = None) -> None:
        self.name = name
        self._addr = orchestrator_addr or os.environ.get(
            "MINIMODAL_ORCH_ADDR", "localhost:50051"
        )
        self._client: OrchestratorClient | None = None
        self._functions: list[Func] = []

    @property
    def client(self) -> OrchestratorClient:
        if self._client is None:
            self._client = OrchestratorClient(self._addr)
        return self._client

    def function(self, fn: Callable[..., Any]) -> Func:
        """Decorator: register `fn` as a remote-callable function on this app."""
        wrapped = Func(fn, self)
        self._functions.append(wrapped)
        return wrapped
