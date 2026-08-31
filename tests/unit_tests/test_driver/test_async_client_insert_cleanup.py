import asyncio
from unittest.mock import AsyncMock

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import asyncclient
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.insert import InsertContext


@pytest.mark.asyncio
async def test_data_insert_clears_context_when_source_cleanup_is_cancelled(monkeypatch):
    instances = []

    class CancelledCloseSource:
        def __init__(self, **kwargs):
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
    assert context.empty


@pytest.mark.parametrize("cleanup_fails", [False, True])
@pytest.mark.asyncio
async def test_data_insert_cancellation_consumes_serializer_error_before_context_reuse(monkeypatch, cleanup_fails):
    instances = []
    cleanup_error = RuntimeError("source cleanup failed")

    class ReusableSource:
        def __init__(self, **kwargs):
            self.close_calls = 0
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
    second_error = RuntimeError("second insert failed")
    execute_calls = 0

    async def execute_data_insert(*args, **kwargs):
        nonlocal execute_calls
        execute_calls += 1
        if execute_calls == 1:
            context.insert_exception = serializer_error
            raise asyncio.CancelledError
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
