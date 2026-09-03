import asyncio
import builtins
import threading
from unittest.mock import AsyncMock

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import asyncclient
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.streaming import StreamingInsertSource
from clickhouse_connect.driver.transform import NativeTransform


class InsertControlFlow(BaseException):
    pass


class FalseySerializerError(Exception):
    def __bool__(self):
        return False


def make_client_context(monkeypatch):
    sources = []

    class NoopInsertSource:
        def __init__(self, **kwargs):
            self.insert_exception = None
            sources.append(self)

        def start_producer(self):
            pass

        async def async_generator(self):
            if False:
                yield b""

        async def close(self, timeout=1.0):
            pass

    monkeypatch.setattr(asyncclient, "StreamingInsertSource", NoopInsertSource)
    client = AsyncClient(interface="http", host="localhost", port=8123)
    context = InsertContext("test_table", ["value"], [get_from_name("Int64")], data=[[13]])
    return client, context, sources


@pytest.mark.asyncio
async def test_data_insert_clears_context_when_source_cleanup_is_cancelled(monkeypatch):
    instances = []

    class CancelledCloseSource:
        def __init__(self, **kwargs):
            self.insert_exception = None
            instances.append(self)

        def start_producer(self):
            pass

        async def async_generator(self):
            if False:
                yield b""

        async def close(self, timeout=1.0):
            raise asyncio.CancelledError

    monkeypatch.setattr(asyncclient, "StreamingInsertSource", CancelledCloseSource)
    client = AsyncClient(interface="http", host="localhost", port=8123)
    client._backend.execute_data_insert = AsyncMock(return_value={})
    context = InsertContext("test_table", ["value"], [get_from_name("Int64")], data=[[13]])

    with pytest.raises(asyncio.CancelledError):
        await client.data_insert(context)

    assert len(instances) == 1
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.parametrize("cleanup_fails", [False, True])
@pytest.mark.asyncio
async def test_data_insert_backend_error_consumes_serializer_error_before_context_reuse(monkeypatch, cleanup_fails):
    instances = []
    cleanup_error = RuntimeError("source cleanup failed")

    class ReusableSource:
        def __init__(self, **kwargs):
            self.close_calls = 0
            self.insert_exception = None
            instances.append(self)

        def start_producer(self):
            pass

        async def async_generator(self):
            if False:
                yield b""

        async def close(self, timeout=1.0):
            self.close_calls += 1
            if cleanup_fails and len(instances) == 1 and self.close_calls == 1:
                raise cleanup_error

    monkeypatch.setattr(asyncclient, "StreamingInsertSource", ReusableSource)
    client = AsyncClient(interface="http", host="localhost", port=8123)
    context = InsertContext("test_table", ["value"], [get_from_name("Int64")], data=[[13]])
    serializer_error = ValueError("serializer failed")
    backend_error = RuntimeError("backend insert failed")
    second_error = RuntimeError("second insert failed")
    execute_calls = 0

    async def execute_data_insert(*args, **kwargs):
        nonlocal execute_calls
        execute_calls += 1
        if execute_calls == 1:
            instances[-1].insert_exception = serializer_error
            raise backend_error
        raise second_error

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)

    expected_first_error = cleanup_error if cleanup_fails else serializer_error
    with pytest.raises(type(expected_first_error)) as first_caught:
        await client.data_insert(context)

    assert first_caught.value is expected_first_error
    assert instances[0].close_calls == 2
    assert context.insert_exception is None
    assert context.empty

    context.data = [[79]]
    with pytest.raises(RuntimeError) as second_caught:
        await client.data_insert(context)

    assert second_caught.value is second_error
    assert len(instances) == 2
    assert instances[1].close_calls == 2
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.asyncio
async def test_data_insert_success_surfaces_serializer_error_before_context_reuse(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")
    second_error = RuntimeError("second insert failed")
    execute_calls = 0

    async def execute_data_insert(*args, **kwargs):
        nonlocal execute_calls
        execute_calls += 1
        if execute_calls == 1:
            sources[-1].insert_exception = serializer_error
            return {}
        raise second_error

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)

    with pytest.raises(ValueError) as first_caught:
        await client.data_insert(context)

    assert first_caught.value is serializer_error
    assert context.insert_exception is None
    assert context.empty

    context.data = [[79]]
    with pytest.raises(RuntimeError) as caught:
        await client.data_insert(context)

    assert caught.value is second_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.asyncio
async def test_data_insert_success_surfaces_falsey_serializer_error(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = FalseySerializerError("serializer failed")

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        return {}

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)

    with pytest.raises(FalseySerializerError) as caught:
        await client.data_insert(context)

    assert caught.value is serializer_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.parametrize(
    ("first_fails", "second_fails", "expected_source"),
    [(True, False, 0), (False, True, 1), (True, True, 0)],
)
@pytest.mark.asyncio
async def test_data_insert_retry_aggregates_first_serializer_error(monkeypatch, first_fails, second_fails, expected_source):
    client, context, sources = make_client_context(monkeypatch)
    errors = [ValueError("first serializer failed"), ValueError("second serializer failed")]

    async def execute_data_insert(_context, _runtime, _body, rebuild_body):
        if first_fails:
            sources[0].insert_exception = errors[0]
        await rebuild_body()
        if second_fails:
            sources[1].insert_exception = errors[1]
        return {}

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)

    with pytest.raises(ValueError) as caught:
        await client.data_insert(context)

    assert caught.value is errors[expected_source]
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.parametrize("use_sink", [False, True])
def test_native_transform_insert_error_sink_is_optional(use_sink):
    context = InsertContext("test_table", ["value"], [get_from_name("Int64")], data=[["not an integer"]])
    sink_errors = []

    if use_sink:
        list(NativeTransform.build_insert(context, sink_errors.append))
        assert context.insert_exception is None
        assert len(sink_errors) == 1
    else:
        list(NativeTransform.build_insert(context))
        assert context.insert_exception is not None
        assert sink_errors == []


@pytest.mark.asyncio
async def test_late_serializer_error_cannot_poison_reused_context(monkeypatch):
    serializer_started = threading.Event()
    release_serializer = threading.Event()
    serializer_finished = threading.Event()
    late_error = ValueError("late serializer failure")

    class LateSerializerTransform:
        @staticmethod
        def build_insert(context, error_handler=None):
            serializer_started.set()
            release_serializer.wait()
            try:
                raise late_error
            except Exception as ex:
                if error_handler is None:
                    context.insert_exception = ex
                else:
                    error_handler(ex)
                yield b"INTERNAL EXCEPTION WHILE SERIALIZING"
            finally:
                serializer_finished.set()

    class FastTimeoutSource(StreamingInsertSource):
        async def close(self, timeout=0.01):
            await super().close(timeout=timeout)

    class NoopInsertSource:
        def __init__(self, **kwargs):
            self.insert_exception = None

        def start_producer(self):
            pass

        async def async_generator(self):
            if False:
                yield b""

        async def close(self, timeout=1.0):
            pass

    monkeypatch.setattr(asyncclient, "StreamingInsertSource", FastTimeoutSource)
    client = AsyncClient(interface="http", host="localhost", port=8123)
    client._transform = LateSerializerTransform()
    context = InsertContext("test_table", ["value"], [get_from_name("Int64")], data=[[13]])

    async def successful_insert(*args, **kwargs):
        while not serializer_started.is_set():
            await asyncio.sleep(0)
        return {}

    client._backend.execute_data_insert = AsyncMock(side_effect=successful_insert)

    try:
        await client.data_insert(context)
    finally:
        release_serializer.set()
        await asyncio.get_running_loop().run_in_executor(None, serializer_finished.wait, 1)
    assert serializer_finished.is_set()

    monkeypatch.setattr(asyncclient, "StreamingInsertSource", NoopInsertSource)
    second_error = RuntimeError("second insert failed")
    client._backend.execute_data_insert = AsyncMock(side_effect=second_error)
    context.data = [[79]]

    with pytest.raises(RuntimeError) as caught:
        await client.data_insert(context)

    assert caught.value is second_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.asyncio
async def test_data_insert_preserves_requested_task_cancellation(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")
    started = asyncio.Event()

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        started.set()
        await asyncio.Future()

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)
    task = asyncio.create_task(client.data_insert(context))
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError) as caught:
        await task

    assert task.cancelled()
    if hasattr(asyncio.Task, "cancelling"):
        assert caught.value.__cause__ is serializer_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.skipif(not hasattr(asyncio, "timeout"), reason="requires Python 3.11")
@pytest.mark.asyncio
async def test_data_insert_preserves_timeout_cancellation(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        await asyncio.Future()

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)
    with pytest.raises(TimeoutError) as caught:
        async with asyncio.timeout(0.01):
            await client.data_insert(context)

    assert isinstance(caught.value.__cause__, asyncio.CancelledError)
    assert caught.value.__cause__.__cause__ is serializer_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.skipif(not hasattr(asyncio, "TaskGroup"), reason="requires Python 3.11")
@pytest.mark.asyncio
async def test_data_insert_preserves_task_group_cancellation(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")
    sibling_error = RuntimeError("sibling failed")
    started = asyncio.Event()

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        started.set()
        await asyncio.Future()

    async def fail_sibling():
        await started.wait()
        raise sibling_error

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)
    insert_task = None

    with pytest.raises(builtins.ExceptionGroup) as caught:
        async with asyncio.TaskGroup() as group:
            insert_task = group.create_task(client.data_insert(context))
            group.create_task(fail_sibling())

    assert insert_task is not None
    assert insert_task.cancelled()
    assert caught.value.exceptions == (sibling_error,)
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.asyncio
async def test_data_insert_internal_cancellation_uses_version_safe_precedence(monkeypatch):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        raise asyncio.CancelledError

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)
    expected_error = ValueError if hasattr(asyncio.Task, "cancelling") else asyncio.CancelledError

    with pytest.raises(expected_error) as caught:
        await client.data_insert(context)

    if expected_error is asyncio.CancelledError:
        assert caught.value.__cause__ is serializer_error
    else:
        assert caught.value is serializer_error
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.parametrize("control_flow_type", [GeneratorExit, InsertControlFlow])
@pytest.mark.asyncio
async def test_data_insert_preserves_non_exception_control_flow(monkeypatch, control_flow_type):
    client, context, sources = make_client_context(monkeypatch)
    serializer_error = ValueError("serializer failed")
    control_flow = control_flow_type()

    async def execute_data_insert(*args, **kwargs):
        sources[-1].insert_exception = serializer_error
        raise control_flow

    client._backend.execute_data_insert = AsyncMock(side_effect=execute_data_insert)

    with pytest.raises(control_flow_type) as caught:
        await client.data_insert(context)

    assert caught.value is control_flow
    assert context.insert_exception is None
    assert context.empty


@pytest.mark.parametrize("control_flow_type", [GeneratorExit, InsertControlFlow, KeyboardInterrupt, SystemExit])
def test_insert_serializer_error_never_replaces_non_exception_control_flow(control_flow_type):
    assert asyncclient._serializer_error_takes_precedence(control_flow_type()) is False
