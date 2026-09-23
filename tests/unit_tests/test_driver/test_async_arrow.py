import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DataError, ProgrammingError


@pytest_asyncio.fixture(loop_scope="function")
async def arrow_client(mocker):
    client = AsyncClient("http", "localhost", 8123)
    client.write_compression = None

    class FakeSession:
        closed = False
        headers: dict[str, str] = {}

        async def close(self):
            self.closed = True

    client._backend.session = FakeSession()
    mocker.patch("clickhouse_connect.driver.asyncclient.check_arrow")
    mocker.patch("clickhouse_connect.driver.asyncclient.arrow_buffer", return_value=(["col_1"], b"block"))
    mocker.patch.object(client, "raw_insert", new_callable=AsyncMock)
    yield client
    executor = getattr(client, "_arrow_insert_executor", None)
    if executor is not None:
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def polars_frame(mocker):
    class DataFrame:
        def to_arrow(self):
            return object()

    mocker.patch("clickhouse_connect.driver.asyncclient.options.pd", None)
    mocker.patch("clickhouse_connect.driver.asyncclient.options.pl", SimpleNamespace(DataFrame=DataFrame))
    return DataFrame()


@pytest.mark.asyncio
async def test_insert_arrow_encodes_off_loop(arrow_client, mocker):
    loop_thread = threading.get_ident()
    threads = []
    buffer = Mock()

    def encode(*_args):
        threads.append(threading.get_ident())
        return ["col_1"], buffer

    def to_pybytes():
        threads.append(threading.get_ident())
        return b"block"

    buffer.to_pybytes.side_effect = to_pybytes
    mocker.patch("clickhouse_connect.driver.asyncclient.arrow_buffer", side_effect=encode)
    await arrow_client.insert_arrow("test_table", object(), settings={"async_insert": 1})
    assert len(threads) == 2
    assert all(thread != loop_thread for thread in threads)
    arrow_client.raw_insert.assert_awaited_once_with(
        "`test_table`", ["col_1"], b"block", {"async_insert": 1}, "Arrow", transport_settings=None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("library", ["pandas", "polars"])
@pytest.mark.parametrize("fails", [False, True])
async def test_dataframe_conversion_off_loop(arrow_client, mocker, library, fails):
    loop_thread = threading.get_ident()
    context = ContextVar("arrow_insert_context", default="unset")
    token = context.set("request_13")
    arrow_table = object()
    failure = ValueError("invalid column")
    calls = []

    class ArrowDtype:
        pass

    class DataFrame:
        dtypes = {"col_1": ArrowDtype()}

        def to_arrow(self):
            calls.append((threading.get_ident(), context.get()))
            if fails:
                raise failure
            return arrow_table

    frame = DataFrame()
    from_pandas = Mock(side_effect=lambda *_args, **_kwargs: frame.to_arrow())
    mocker.patch(
        "clickhouse_connect.driver.asyncclient.options.pd",
        SimpleNamespace(DataFrame=DataFrame, ArrowDtype=ArrowDtype) if library == "pandas" else None,
    )
    mocker.patch("clickhouse_connect.driver.asyncclient.options.pl", SimpleNamespace(DataFrame=DataFrame) if library == "polars" else None)
    mocker.patch("clickhouse_connect.driver.asyncclient.options.arrow", SimpleNamespace(Table=SimpleNamespace(from_pandas=from_pandas)))
    insert_arrow = mocker.patch.object(arrow_client, "insert_arrow", new_callable=AsyncMock)
    tags = []
    mocker.patch.object(arrow_client, "_add_integration_tag", side_effect=lambda tag: tags.append((tag, threading.get_ident())))
    kwargs = {"database": "test_db", "settings": {"async_insert": 1}, "transport_settings": {"X-Test": "13"}}
    try:
        if fails:
            with pytest.raises(DataError, match=f"Failed to convert {library} DataFrame to Arrow table: invalid column") as exc:
                await arrow_client.insert_df_arrow("test_table", frame, **kwargs)
            assert exc.value.__cause__ is failure
            insert_arrow.assert_not_awaited()
            assert tags == []
        else:
            summary = await arrow_client.insert_df_arrow("test_table", frame, **kwargs)
            assert summary is insert_arrow.return_value
            insert_arrow.assert_awaited_once_with(table="test_table", arrow_table=arrow_table, **kwargs)
            assert tags == [(library, loop_thread)]
        assert len(calls) == 1
        assert calls[0][0] != loop_thread
        assert calls[0][1] == "request_13"
        if library == "pandas":
            from_pandas.assert_called_once_with(frame, preserve_index=False)
    finally:
        context.reset(token)


@pytest.mark.asyncio
async def test_cancelled_preparation_keeps_worker_until_finished(arrow_client, mocker):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    encoded = []

    def encode(table, _compression):
        encoded.append(table)
        if table == 13:
            loop.call_soon_threadsafe(started.set)
            try:
                assert release.wait(5)
            finally:
                loop.call_soon_threadsafe(finished.set)
        return ["col_1"], b"block"

    mocker.patch("clickhouse_connect.driver.asyncclient.arrow_buffer", side_effect=encode)
    first = asyncio.create_task(arrow_client.insert_arrow("test_table", 13))
    tasks = [first]
    try:
        await asyncio.wait_for(started.wait(), 5)
        queued = asyncio.create_task(arrow_client.insert_arrow("test_table", 27))
        tasks.append(queued)
        await asyncio.sleep(0)
        queued.cancel()
        first.cancel()
        for task in tasks:
            with pytest.raises(asyncio.CancelledError):
                await task
        next_insert = asyncio.create_task(arrow_client.insert_arrow("test_table", 79))
        tasks.append(next_insert)
        await asyncio.sleep(0.05)
        assert encoded == [13]
        arrow_client.raw_insert.assert_not_awaited()
        release.set()
        await asyncio.wait_for(next_insert, 5)
        assert encoded == [13, 79]
        arrow_client.raw_insert.assert_awaited_once()
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.wait_for(finished.wait(), 5)


@pytest.mark.asyncio
@pytest.mark.parametrize("force_close", [False, True])
async def test_close_rejects_queued_preparation_without_waiting(arrow_client, mocker, force_close):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    encoded = []

    def encode(table, _compression):
        encoded.append(table)
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(5)
            return ["col_1"], b"block"
        finally:
            loop.call_soon_threadsafe(finished.set)

    mocker.patch("clickhouse_connect.driver.asyncclient.arrow_buffer", side_effect=encode)
    first = asyncio.create_task(arrow_client.insert_arrow("test_table", 13))
    tasks = [first]
    try:
        await asyncio.wait_for(started.wait(), 5)
        queued = asyncio.create_task(arrow_client.insert_arrow("test_table", 79))
        tasks.append(queued)
        await asyncio.sleep(0)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        if force_close:
            arrow_client._force_close()
        else:
            await asyncio.wait_for(arrow_client.close(), 1)
        assert not finished.is_set()
        release.set()
        with pytest.raises(ProgrammingError, match="Client session is unavailable"):
            await asyncio.wait_for(queued, 5)
        assert not queued.cancelled()
        assert encoded == [13]
        await asyncio.wait_for(finished.wait(), 5)
        arrow_client.raw_insert.assert_not_awaited()
    finally:
        release.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.wait_for(finished.wait(), 5)


@pytest.mark.asyncio
async def test_force_close_shuts_down_idle_executor(arrow_client):
    await arrow_client.insert_arrow("test_table", object())
    executor = arrow_client._arrow_insert_executor
    arrow_client._force_close()
    with pytest.raises(RuntimeError, match="shutdown"):
        executor.submit(lambda: None)
    assert arrow_client._arrow_insert_executor is None
    arrow_client._force_close()


@pytest.mark.asyncio
async def test_force_close_during_executor_creation(arrow_client, mocker):
    constructing = threading.Event()
    backend_closed = threading.Event()
    force_finished = threading.Event()
    executors = []
    thread_errors = []
    original_force_close = arrow_client._backend.force_close

    def close_backend():
        original_force_close()
        backend_closed.set()

    def force_close():
        try:
            assert constructing.wait(5)
            arrow_client._force_close()
        except BaseException as ex:
            thread_errors.append(ex)
        finally:
            force_finished.set()

    def create_executor(**kwargs):
        constructing.set()
        assert backend_closed.wait(5)
        executor = ThreadPoolExecutor(**kwargs)
        executors.append(executor)
        return executor

    def encode(*_args):
        assert force_finished.wait(5)
        return ["col_1"], b"block"

    mocker.patch.object(arrow_client._backend, "force_close", side_effect=close_backend)
    mocker.patch("clickhouse_connect.driver.asyncclient.ThreadPoolExecutor", side_effect=create_executor)
    mocker.patch("clickhouse_connect.driver.asyncclient.arrow_buffer", side_effect=encode)
    closer = threading.Thread(target=force_close)
    closer.start()
    try:
        with pytest.raises(ProgrammingError, match="Client session is unavailable"):
            await asyncio.wait_for(arrow_client.insert_arrow("test_table", object()), 5)
        assert force_finished.wait(5)
        assert arrow_client._arrow_insert_executor is None
        with pytest.raises(RuntimeError, match="shutdown"):
            executors[0].submit(lambda: None)
        arrow_client.raw_insert.assert_not_awaited()
    finally:
        closer.join(timeout=5)
        assert not closer.is_alive()
        assert not thread_errors


@pytest.mark.asyncio
@pytest.mark.parametrize("close_error", [None, RuntimeError("close failed"), asyncio.CancelledError()])
@pytest.mark.parametrize("reopen", ["_initialize", "close_connections"])
async def test_executor_lifecycle(arrow_client, mocker, close_error, reopen):
    factory = mocker.patch("clickhouse_connect.driver.asyncclient.ThreadPoolExecutor", wraps=ThreadPoolExecutor)
    await arrow_client.close()
    factory.assert_not_called()
    arrow_client._initialized = True
    await getattr(arrow_client, reopen)()
    await arrow_client.insert_arrow("test_table", object())
    await arrow_client.insert_arrow("test_table", object())
    assert factory.call_count == 1
    executor = arrow_client._arrow_insert_executor

    async def close_backend():
        await original_close()
        if close_error is not None:
            raise close_error

    original_close = arrow_client._backend.close
    with patch.object(arrow_client._backend, "close", new=close_backend):
        if close_error is None:
            await arrow_client.close()
        else:
            with pytest.raises(type(close_error)) as exc:
                await arrow_client.close()
            assert exc.value is close_error
    with pytest.raises(RuntimeError, match="shutdown"):
        executor.submit(lambda: None)
    await getattr(arrow_client, reopen)()
    try:
        await arrow_client.insert_arrow("test_table", object())
        assert factory.call_count == 2
        assert arrow_client._arrow_insert_executor is not executor
    finally:
        await arrow_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["insert_arrow", "insert_df_arrow"])
@pytest.mark.parametrize("state", ["uninitialized", "initialization_failed", "closed"])
async def test_unavailable_session_does_not_create_executor(arrow_client, polars_frame, mocker, method, state):
    factory = mocker.patch("clickhouse_connect.driver.asyncclient.ThreadPoolExecutor", wraps=ThreadPoolExecutor)
    if state == "uninitialized":
        arrow_client._session = None
    elif state == "initialization_failed":
        mocker.patch.object(arrow_client, "_execute_operation", side_effect=RuntimeError("initialization failed"))
        with pytest.raises(RuntimeError, match="initialization failed"):
            await arrow_client._initialize()
    else:
        await arrow_client.close()
    with pytest.raises(ProgrammingError) as exc:
        await getattr(arrow_client, method)("test_table", polars_frame)
    assert str(exc.value) == "Session not initialized. Use 'async with get_async_client(...)' or call 'await client._initialize()' first."
    factory.assert_not_called()
    arrow_client.raw_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_close_during_connection_rotation_keeps_preparation_closed(arrow_client, mocker):
    factory = mocker.patch("clickhouse_connect.driver.asyncclient.ThreadPoolExecutor", wraps=ThreadPoolExecutor)
    arrow_client._initialized = True
    await arrow_client._initialize()
    lease = arrow_client._backend.session_lease
    lease.acquire()
    rotated = asyncio.Event()
    new_session = arrow_client._backend._new_session

    def create_session():
        session = new_session()
        rotated.set()
        return session

    mocker.patch.object(arrow_client._backend, "_new_session", side_effect=create_session)
    rotation = asyncio.create_task(arrow_client.close_connections())
    try:
        await asyncio.wait_for(rotated.wait(), 5)
        await asyncio.wait_for(arrow_client.close(), 1)
        lease.release()
        await asyncio.wait_for(rotation, 5)
        with pytest.raises(ProgrammingError, match="Session not initialized"):
            await arrow_client.insert_arrow("test_table", object())
        factory.assert_not_called()
        arrow_client.raw_insert.assert_not_awaited()
    finally:
        if lease._inflight:
            lease.release()
        await asyncio.gather(rotation, return_exceptions=True)
        await arrow_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reinitialize", [False, True])
@pytest.mark.parametrize("force_close", [False, True])
async def test_close_during_conversion_rejects_stale_insert(arrow_client, polars_frame, mocker, reinitialize, force_close):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    factory = mocker.patch("clickhouse_connect.driver.asyncclient.ThreadPoolExecutor", wraps=ThreadPoolExecutor)

    def convert():
        loop.call_soon_threadsafe(started.set)
        try:
            assert release.wait(5)
            return object()
        finally:
            loop.call_soon_threadsafe(finished.set)

    mocker.patch.object(polars_frame, "to_arrow", side_effect=convert)
    task = asyncio.create_task(arrow_client.insert_df_arrow("test_table", polars_frame))
    try:
        await asyncio.wait_for(started.wait(), 5)
        if force_close:
            arrow_client._force_close()
        else:
            await asyncio.wait_for(arrow_client.close(), 1)
        if reinitialize:
            arrow_client._initialized = True
            await arrow_client._initialize()
            await asyncio.wait_for(arrow_client.insert_arrow("test_table", object()), 1)
        executor = arrow_client._arrow_insert_executor
        release.set()
        with pytest.raises(ProgrammingError, match="Client session is unavailable"):
            await asyncio.wait_for(task, 5)
        assert factory.call_count == (2 if reinitialize else 1)
        assert arrow_client._arrow_insert_executor is executor
        assert arrow_client.raw_insert.await_count == (1 if reinitialize else 0)
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.wait_for(finished.wait(), 5)
        await arrow_client.close()
