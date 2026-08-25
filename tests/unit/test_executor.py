"""Executor strategies: every one of them must honour the same contract.

Functions under test are defined inside the test bodies on purpose:
cloudpickle serializes locally-defined functions by value, which is what the
SDK relies on and the only thing a fresh subprocess can deserialize.
"""

from __future__ import annotations

import cloudpickle
import pytest
from minimodal.serialization import deserialize_result

from worker.executor import InprocExecutor, NaiveSubprocessExecutor, make_executor


def call(executor, fn, *args, output_sink=None, **kwargs):
    return executor.execute(
        cloudpickle.dumps(fn), cloudpickle.dumps((args, kwargs)), output_sink=output_sink
    )


def test_make_executor_picks_the_strategy_by_name():
    assert make_executor("inproc", 1).name == "inproc"
    assert make_executor("", 1).name == "inproc"
    assert make_executor("NAIVE", 1).name == "naive"


def test_make_executor_rejects_unknown_strategies():
    with pytest.raises(ValueError, match="unknown MINIMODAL_COLD_START"):
        make_executor("teleport", 1)


def test_make_executor_criu_is_explicitly_unimplemented():
    with pytest.raises(NotImplementedError):
        make_executor("criu", 1)


class TestInprocExecutor:
    def test_returns_the_functions_result(self):
        result = call(InprocExecutor(), lambda a, b: a + b, 2, b=3)
        assert result.success
        assert deserialize_result(result.result_bytes) == 5

    def test_reports_user_exceptions_as_a_traceback(self):
        def boom():
            raise ValueError("nope")

        result = call(InprocExecutor(), boom)
        assert not result.success
        assert "ValueError: nope" in result.error
        assert result.result_bytes == b""

    def test_captures_stdout_and_stderr(self):
        def chatty():
            import sys

            print("out")
            print("err", file=sys.stderr)
            return None

        result = call(InprocExecutor(), chatty)
        assert result.success
        assert result.stdout == "out\n"
        assert result.stderr == "err\n"

    def test_unserializable_results_fail_the_job_not_the_worker(self):
        def make_lock():
            import threading

            return threading.Lock()

        result = call(InprocExecutor(), make_lock)
        assert not result.success
        assert "not picklable" in result.error


class TestInprocStreaming:
    """With an output_sink, prints stream out live instead of being buffered."""

    def collect(self):
        chunks = []
        return chunks, lambda text, is_stderr: chunks.append((text, is_stderr))

    def test_output_streams_through_the_sink_not_the_result(self):
        chunks, sink = self.collect()

        def chatty():
            import sys

            print("one")
            print("two")
            print("err", file=sys.stderr)
            return 5

        result = call(InprocExecutor(), chatty, output_sink=sink)
        assert result.success
        assert deserialize_result(result.result_bytes) == 5
        stdout_text = "".join(t for t, is_err in chunks if not is_err)
        stderr_text = "".join(t for t, is_err in chunks if is_err)
        assert stdout_text == "one\ntwo\n"
        assert stderr_text == "err\n"
        # Streamed output must not also come back on the result — the worker
        # would ship it a second time as tail lines.
        assert result.stdout == "" and result.stderr == ""

    def test_sink_sees_output_before_the_function_returns(self, tmp_path):
        # Liveness proof via a filesystem side channel: cloudpickle copies
        # closures by value, so the deserialized function can't share Python
        # state with the test — but it can see a file the sink writes.
        marker = tmp_path / "sink-saw-output"

        def sink(text, is_stderr):
            marker.write_text("seen")

        def observer(marker_path=str(marker)):
            import os

            print("early")
            return os.path.exists(marker_path)

        result = call(InprocExecutor(), observer, output_sink=sink)
        assert result.success
        assert deserialize_result(result.result_bytes) is True

    def test_concurrent_tasks_do_not_cross_streams(self):
        import threading

        def make_fn(tag):
            def fn(tag=tag):
                for _ in range(20):
                    print(tag)

            return fn

        results = {}

        def run(tag):
            chunks, sink = self.collect()
            call(InprocExecutor(), make_fn(tag), output_sink=sink)
            results[tag] = "".join(t for t, _ in chunks)

        threads = [threading.Thread(target=run, args=(tag,)) for tag in ("aaa", "bbb")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert results["aaa"] == "aaa\n" * 20
        assert results["bbb"] == "bbb\n" * 20

    def test_exceptions_still_produce_a_failed_result(self):
        _, sink = self.collect()

        def boom():
            print("about to fail")
            raise ValueError("nope")

        result = call(InprocExecutor(), boom, output_sink=sink)
        assert not result.success
        assert "ValueError: nope" in result.error


class TestNaiveSubprocessExecutor:
    def test_runs_the_function_in_a_fresh_interpreter(self):
        def child_pid():
            import os

            return os.getpid()

        import os

        result = call(NaiveSubprocessExecutor(), child_pid)
        assert result.success
        assert deserialize_result(result.result_bytes) != os.getpid()

    def test_propagates_user_exceptions(self):
        def boom():
            raise RuntimeError("subprocess boom")

        result = call(NaiveSubprocessExecutor(), boom)
        assert not result.success
        assert "RuntimeError: subprocess boom" in result.error

    def test_a_crashing_interpreter_is_reported_not_raised(self):
        def hard_exit():
            import os

            os._exit(3)

        result = call(NaiveSubprocessExecutor(), hard_exit)
        assert not result.success
        assert "exit=3" in result.error

    def test_user_prints_are_captured_and_do_not_corrupt_the_protocol(self):
        # The child's real stdout carries the framed response envelope; a
        # print() from the user function used to land in front of it and
        # corrupt the length prefix. Now it's captured into the envelope.
        def chatty():
            print("from the child")
            return 42

        result = call(NaiveSubprocessExecutor(), chatty)
        assert result.success
        assert deserialize_result(result.result_bytes) == 42
        assert "from the child" in result.stdout
