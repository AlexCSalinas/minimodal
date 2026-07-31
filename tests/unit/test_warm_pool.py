"""Warm pool: the fork strategy has to survive children dying and hanging."""

from __future__ import annotations

import os
import time

import cloudpickle
import pytest
from minimodal.serialization import deserialize_result

from worker.warm_pool import ForkWarmPoolExecutor, WarmPoolConfig


@pytest.fixture
def pool(tmp_path):
    def build(size=1, **kwargs):
        executor = ForkWarmPoolExecutor(
            WarmPoolConfig(size=size, socket_dir=str(tmp_path), **kwargs)
        )
        executor.start()
        built.append(executor)
        return executor

    built: list[ForkWarmPoolExecutor] = []
    yield build
    for executor in built:
        executor.shutdown()


def call(executor, fn, *args, **kwargs):
    return executor.execute(cloudpickle.dumps(fn), cloudpickle.dumps((args, kwargs)))


def test_executes_in_a_warm_child(pool):
    executor = pool()

    def child_pid():
        import os

        return os.getpid()

    result = call(executor, child_pid)
    assert result.success
    assert deserialize_result(result.result_bytes) != os.getpid()


def test_reuses_the_same_child_across_tasks(pool):
    executor = pool(size=1)

    def child_pid():
        import os

        return os.getpid()

    first = deserialize_result(call(executor, child_pid).result_bytes)
    second = deserialize_result(call(executor, child_pid).result_bytes)
    assert first == second


def test_propagates_user_exceptions(pool):
    def boom():
        raise ValueError("warm boom")

    result = call(pool(), boom)
    assert not result.success
    assert "ValueError: warm boom" in result.error


def test_a_dead_child_is_replaced_so_later_tasks_still_run(pool):
    # Regression: the pool used to drop dead children without replacing them,
    # so a single crashing task poisoned the slot and every task after it
    # failed with ECONNREFUSED (and with size=1 the pool never recovered).
    executor = pool(size=1)

    def hard_exit():
        import os

        os._exit(9)

    assert not call(executor, hard_exit).success

    def add(a, b):
        return a + b

    result = call(executor, add, 2, 3)
    assert result.success, result.error
    assert deserialize_result(result.result_bytes) == 5


def test_a_hung_child_hits_the_task_timeout_and_is_replaced(pool):
    executor = pool(size=1, task_timeout_s=0.5)

    def sleep_forever():
        import time

        time.sleep(30)

    started = time.monotonic()
    result = call(executor, sleep_forever)
    assert not result.success
    assert time.monotonic() - started < 10
    assert "connection error" in result.error

    assert deserialize_result(call(executor, lambda: "alive").result_bytes) == "alive"


def test_acquire_timeout_bounds_a_saturated_pool(pool):
    executor = pool(size=1, acquire_timeout_s=0.2)
    # Take the only child out of the idle queue to simulate saturation.
    slot = executor._idle.get()
    try:
        result = call(executor, lambda: 1)
        assert not result.success
        assert "no warm child available" in result.error
    finally:
        executor._idle.put(slot)


def test_execute_before_start_is_an_error(tmp_path):
    executor = ForkWarmPoolExecutor(WarmPoolConfig(size=1, socket_dir=str(tmp_path)))
    with pytest.raises(RuntimeError, match="not started"):
        call(executor, lambda: 1)


def test_shutdown_stops_every_child_process(tmp_path):
    executor = ForkWarmPoolExecutor(WarmPoolConfig(size=2, socket_dir=str(tmp_path)))
    executor.start()
    pids = [slot.pid for slot in executor._slots]
    spawner = executor._spawner_pid
    executor.shutdown()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and any(_alive(pid) for pid in pids + [spawner]):
        time.sleep(0.05)
    assert not [pid for pid in pids + [spawner] if _alive(pid)]


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True
