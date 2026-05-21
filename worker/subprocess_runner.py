"""Naive cold-start strategy: a brand-new Python process per task.

This module is the entrypoint that the parent worker invokes as
`python -m worker.subprocess_runner`. It reads a framed request from stdin,
runs the user function, and writes a framed response to stdout. Then exits.

Every invocation pays the full Python startup + cloudpickle import cost.
That's the point — this measures the worst case so we can show what the
warm-pool optimization buys us.
"""

from __future__ import annotations

import struct
import sys
import time
import traceback


def _read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError(f"got {len(buf)}/{n} bytes")
        buf.extend(chunk)
    return bytes(buf)


def _read_request(stream) -> tuple[bytes, bytes]:
    fn_len = struct.unpack("!I", _read_exact(stream, 4))[0]
    fn_bytes = _read_exact(stream, fn_len)
    args_len = struct.unpack("!I", _read_exact(stream, 4))[0]
    args_bytes = _read_exact(stream, args_len)
    return fn_bytes, args_bytes


def _write_envelope(stream, envelope: dict) -> None:
    import cloudpickle  # imported here to keep the prelude minimal
    buf = cloudpickle.dumps(envelope)
    stream.write(struct.pack("!I", len(buf)))
    stream.write(buf)
    stream.flush()


def main() -> int:
    try:
        fn_bytes, args_bytes = _read_request(sys.stdin.buffer)
    except Exception as e:
        sys.stderr.write(f"subprocess_runner: read request failed: {e}\n")
        return 2

    # Imports happen here, AFTER reading the request, to make the cold-start
    # cost visible: every task pays cloudpickle import.
    import cloudpickle  # noqa: F401

    try:
        fn = cloudpickle.loads(fn_bytes)
        args, kwargs = cloudpickle.loads(args_bytes)
    except Exception:
        _write_envelope(sys.stdout.buffer, {
            "ok": False,
            "error": f"deserialize failed:\n{traceback.format_exc()}",
            "execution_ms": 0,
        })
        return 0

    t0 = time.monotonic()
    try:
        result = fn(*args, **kwargs)
        execution_ms = int((time.monotonic() - t0) * 1000)
        _write_envelope(sys.stdout.buffer, {
            "ok": True,
            "result_bytes": cloudpickle.dumps(result),
            "execution_ms": execution_ms,
        })
    except Exception:
        execution_ms = int((time.monotonic() - t0) * 1000)
        _write_envelope(sys.stdout.buffer, {
            "ok": False,
            "error": traceback.format_exc(),
            "execution_ms": execution_ms,
        })
    return 0


if __name__ == "__main__":
    sys.exit(main())
