import asyncio
import gc
import time
import warnings
import weakref

import pytest

from tests.helpers import run_in_new_loop


def test_run_in_new_loop_returns_result_and_propagates_exception():
    async def succeed():
        return 79

    error = RuntimeError("test failure")

    async def fail():
        raise error

    assert run_in_new_loop(succeed()) == 79
    with pytest.raises(RuntimeError) as exc_info:
        run_in_new_loop(fail())
    assert exc_info.value is error


def test_run_in_new_loop_cancels_and_gathers_pending_tasks():
    cancelled = []
    loop_errors = []
    task_ref = None

    async def pending():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    async def run():
        nonlocal task_ref
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
        task = asyncio.create_task(pending())
        task_ref = weakref.ref(task)
        await asyncio.sleep(0)

    run_in_new_loop(run())
    gc.collect()

    assert cancelled == [True]
    assert task_ref is not None
    assert task_ref() is None
    assert loop_errors == []


def test_run_in_new_loop_waits_for_default_executor():
    async def run():
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(None, lambda: (time.sleep(0.05), 113)[1])

    future = run_in_new_loop(run())

    assert future.done()
    assert future.result() == 113


@pytest.mark.parametrize("raises", [False, True], ids=["success", "error"])
def test_run_in_new_loop_finalizes_async_generators(raises):
    finalized = []
    error = RuntimeError("test failure")

    async def values():
        try:
            yield 13
            yield 79
        finally:
            finalized.append(True)

    async def run():
        generator = values()
        assert await generator.__anext__() == 13
        if raises:
            raise error
        return 113

    if raises:
        with pytest.raises(RuntimeError) as exc_info:
            run_in_new_loop(run())
        assert exc_info.value is error
    else:
        assert run_in_new_loop(run()) == 113

    assert finalized == [True]


def test_run_in_new_loop_preserves_policy_loop():
    policy = asyncio.get_event_loop_policy()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            original_loop = policy.get_event_loop()
        created_original_loop = bool(caught)
    except RuntimeError:
        original_loop = None
        created_original_loop = False
    policy_loop = asyncio.new_event_loop()
    policy.set_event_loop(policy_loop)
    try:
        assert run_in_new_loop(asyncio.sleep(0, result=13)) == 13
        assert policy.get_event_loop() is policy_loop
    finally:
        policy_loop.close()
        if created_original_loop and original_loop is not None:
            original_loop.close()
            policy.set_event_loop(None)
        else:
            policy.set_event_loop(original_loop)
