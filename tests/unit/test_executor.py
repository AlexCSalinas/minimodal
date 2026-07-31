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


def call(executor, fn, *args, **kwargs):
    return executor.execute(cloudpickle.dumps(fn), cloudpickle.dumps((args, kwargs)))


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
