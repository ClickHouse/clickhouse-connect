import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from clickhouse_connect.driver._backend import http_async
from clickhouse_connect.driver._backend.http_async import HttpAsyncBackend, SessionLease, _one_shot
from clickhouse_connect.driver._backend.httpcommon import QueryRequestPlan
from clickhouse_connect.driver._backend.models import QueryExecution, QueryRuntime
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.query import QueryContext, QueryResult
from clickhouse_connect.driver.streaming import StreamingResponseSource


def _build_backend() -> HttpAsyncBackend:
    return HttpAsyncBackend(
        url="http://localhost:8123",
        headers={},
        client_settings={},
        timeout=aiohttp.ClientTimeout(),
        connector_kwargs={},
        ssl_context=None,
        proxy_url=None,
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
    )


def _query_plan(columns_only: bool = False) -> QueryRequestPlan:
    return QueryRequestPlan(columns_only=columns_only, params={}, headers={}, body="SELECT 13")


def _patch_query_plan(monkeypatch, columns_only: bool = False) -> None:
    plan = _query_plan(columns_only)
    monkeypatch.setattr(http_async, "plan_query_request", lambda *args, **kwargs: plan)


class _Response:
    def __init__(self, close_error: BaseException | None = None):
        self.headers: dict[str, str] = {}
        self.closed = False
        self.close_calls = 0
        self.close_error = close_error
        self.read_started = asyncio.Event()
        self._read_blocked = False
        self._lease_release = Mock()

    async def read(self) -> bytes:
        self.read_started.set()
        if self._read_blocked:
            await asyncio.Event().wait()
        return b'{"meta": []}'

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error
        self.closed = True


@pytest.mark.asyncio
async def test_execute_query_releases_response_when_stream_startup_fails(monkeypatch):
    startup_error = RuntimeError("stream startup failed")
    close_error = RuntimeError("response close failed")
    response = _Response(close_error=close_error)
    backend = _build_backend()
    backend.request = AsyncMock(return_value=response)
    _patch_query_plan(monkeypatch)

    async def fail_startup(*args, **kwargs):
        raise startup_error

    monkeypatch.setattr(http_async, "start_streaming_response", fail_startup)

    with pytest.raises(RuntimeError) as caught:
        await backend.execute_query(QueryContext("SELECT 13"), QueryRuntime(), "SELECT 13")

    assert caught.value is startup_error
    assert response.close_calls == 1
    response._lease_release.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_query_releases_response_when_stream_startup_is_cancelled(monkeypatch):
    startup_started = asyncio.Event()
    response = _Response()
    backend = _build_backend()
    backend.request = AsyncMock(return_value=response)
    _patch_query_plan(monkeypatch)

    async def block_startup(*args, **kwargs):
        startup_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(http_async, "start_streaming_response", block_startup)
    query_task = asyncio.create_task(backend.execute_query(QueryContext("SELECT 13"), QueryRuntime(), "SELECT 13"))
    await asyncio.wait_for(startup_started.wait(), timeout=1)

    query_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await query_task

    assert response.closed
    response._lease_release.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_query_closes_columns_only_response_when_read_is_cancelled(monkeypatch):
    response = _Response()
    response._read_blocked = True
    backend = _build_backend()
    backend.request = AsyncMock(return_value=response)
    _patch_query_plan(monkeypatch, columns_only=True)
    query_task = asyncio.create_task(backend.execute_query(QueryContext("SELECT 13 LIMIT 0"), QueryRuntime(), "SELECT 13 LIMIT 0"))
    await asyncio.wait_for(response.read_started.wait(), timeout=1)

    query_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await query_task

    assert response.closed
    response._lease_release.assert_called_once_with()


@pytest.mark.asyncio
async def test_execute_query_transfers_successful_stream_ownership(monkeypatch):
    response = _Response()
    source = object()
    backend = _build_backend()
    backend.request = AsyncMock(return_value=response)
    _patch_query_plan(monkeypatch)
    monkeypatch.setattr(http_async, "start_streaming_response", AsyncMock(return_value=source))

    execution = await backend.execute_query(QueryContext("SELECT 13"), QueryRuntime(), "SELECT 13")

    assert execution.source is source
    assert not response.closed
    response._lease_release.assert_not_called()


class _StaticSource:
    def __init__(self, cleanup_error: BaseException | None = None):
        self.exception_tag = None
        self.gen = iter(())
        self.cleanup_error = cleanup_error
        self.aclose_calls = 0

    async def aclose(self) -> None:
        self.aclose_calls += 1
        if self.cleanup_error is not None:
            raise self.cleanup_error

    def close(self) -> None:
        pass


class _ReturningTransform:
    @staticmethod
    def parse_response(source, context) -> QueryResult:
        return QueryResult()


class _FailingTransform:
    def __init__(self, error: BaseException):
        self.error = error

    def parse_response(self, source, context) -> QueryResult:
        raise self.error


@pytest.mark.asyncio
async def test_query_attaches_stream_source_only_after_parser_succeeds():
    client = AsyncClient(interface="http", host="localhost", port=8123)
    source = _StaticSource()
    client._transform = _ReturningTransform()
    client._backend.execute_query = AsyncMock(return_value=QueryExecution(source=source))
    context = client.create_query_context(query="SELECT 13", streaming=True)

    result = await client._query_with_context(context)

    assert result.source is source
    assert source.aclose_calls == 0
    result.close()


@pytest.mark.asyncio
async def test_query_parser_error_is_preserved_when_source_cleanup_fails(caplog):
    query_error = RuntimeError("query parser failed")
    cleanup_error = RuntimeError("source cleanup failed")
    client = AsyncClient(interface="http", host="localhost", port=8123)
    source = _StaticSource(cleanup_error)
    client._transform = _FailingTransform(query_error)
    client._backend.execute_query = AsyncMock(return_value=QueryExecution(source=source))
    context = client.create_query_context(query="SELECT 13", streaming=True)

    with caplog.at_level("WARNING", logger="clickhouse_connect.driver.asyncclient"):
        with pytest.raises(RuntimeError) as caught:
            await client._query_with_context(context)

    assert caught.value is query_error
    assert source.aclose_calls == 1
    assert "Failed to close streaming response after AsyncClient query error" in caplog.messages


@pytest.mark.asyncio
async def test_query_parser_error_is_preserved_when_source_cleanup_is_cancelled(caplog):
    query_error = RuntimeError("query parser failed")
    client = AsyncClient(interface="http", host="localhost", port=8123)
    source = _StaticSource(asyncio.CancelledError())
    client._transform = _FailingTransform(query_error)
    client._backend.execute_query = AsyncMock(return_value=QueryExecution(source=source))
    context = client.create_query_context(query="SELECT 13", streaming=True)

    with caplog.at_level("DEBUG", logger="clickhouse_connect.driver.asyncclient"):
        with pytest.raises(RuntimeError) as caught:
            await client._query_with_context(context)

    assert caught.value is query_error
    assert source.aclose_calls == 1
    assert "Streaming response cleanup was cancelled after AsyncClient query error" in caplog.messages
    assert not any(record.levelname == "WARNING" for record in caplog.records)


class _BlockingContent:
    def __init__(self):
        self.read_started = asyncio.Event()

    async def read(self, size: int = -1) -> bytes:
        self.read_started.set()
        await asyncio.Event().wait()
        return b""


class _StreamingResponse:
    def __init__(self):
        self.content = _BlockingContent()
        self.headers: dict[str, str] = {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeSession:
    def __init__(self):
        self.closed = False
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _BlockingTransform:
    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def parse_response(self, source, context) -> QueryResult:
        self.started.set()
        try:
            if not self.release.wait(timeout=5):
                raise TimeoutError("parser was not released")
            return QueryResult()
        finally:
            self.finished.set()


async def _wait_for_thread_event(event: threading.Event) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 5
    while not event.is_set():
        if loop.time() >= deadline:
            raise TimeoutError("thread event was not set")
        await asyncio.sleep(0.001)


@pytest.mark.parametrize("parser_state", ["queued", "started"])
@pytest.mark.asyncio
async def test_query_cancellation_releases_lease_before_client_close(parser_state, monkeypatch):
    client = AsyncClient(interface="http", host="localhost", port=8123)
    session = _FakeSession()
    lease = SessionLease(session)
    lease.acquire()
    client._backend.session_lease = lease

    response = _StreamingResponse()
    response._lease_release = _one_shot(lease.release)
    source = StreamingResponseSource(response)
    await source.start_producer(asyncio.get_running_loop())

    transform = _BlockingTransform()
    client._transform = transform
    client._backend.execute_query = AsyncMock(return_value=QueryExecution(source=source))
    context = client.create_query_context(query="SELECT 13", streaming=True)
    parser_submitted = asyncio.Event()

    async def exercise_cancellation():
        query_task = asyncio.create_task(client._query_with_context(context))
        try:
            if parser_state == "started":
                await _wait_for_thread_event(transform.started)
            else:
                await asyncio.wait_for(parser_submitted.wait(), timeout=1)
                assert not transform.started.is_set()
                assert not query_task.done()

            query_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await query_task
            inflight = lease._inflight
            try:
                await asyncio.wait_for(client.close(), timeout=1)
                closed = True
            except asyncio.TimeoutError:
                closed = False
            return inflight, closed
        finally:
            if not query_task.done():
                query_task.cancel()
                try:
                    await query_task
                except BaseException:
                    pass
            transform.release.set()
            await source.aclose()
            if not session.closed:
                await session.close()
            if parser_state == "started":
                await _wait_for_thread_event(transform.finished)

    if parser_state == "queued":
        executor = ThreadPoolExecutor(max_workers=1)
        executor_started = threading.Event()
        executor_release = threading.Event()

        def block_executor() -> None:
            executor_started.set()
            executor_release.wait(timeout=5)

        blocker = executor.submit(block_executor)
        parser_futures = []
        try:
            await _wait_for_thread_event(executor_started)
            loop = asyncio.get_running_loop()

            def route_parser(executor_arg, func, *args):
                assert executor_arg is None
                parser_submitted.set()
                parser_future = executor.submit(func, *args)
                parser_futures.append(parser_future)
                return asyncio.wrap_future(parser_future, loop=loop)

            with monkeypatch.context() as patcher:
                patcher.setattr(loop, "run_in_executor", route_parser)
                inflight_after_cancel, close_completed = await exercise_cancellation()
            assert len(parser_futures) == 1
            assert parser_futures[0].cancelled()
            assert not transform.started.is_set()
        finally:
            executor_release.set()
            await asyncio.wrap_future(blocker)
            executor.shutdown(wait=True)
            await source.aclose()
            if not session.closed:
                await session.close()
    else:
        inflight_after_cancel, close_completed = await exercise_cancellation()

    assert inflight_after_cancel == 0
    assert close_completed
    assert session.close_calls == 1
