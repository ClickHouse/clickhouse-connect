import asyncio
import time

import aiohttp
import pytest

from clickhouse_connect import common
from clickhouse_connect.driver._backend.http_async import HttpAsyncBackend, SessionLease
from clickhouse_connect.driver.asyncclient import AsyncClient


def _build_backend() -> HttpAsyncBackend:
    return HttpAsyncBackend(
        url="http://localhost:8123",
        headers={},
        client_settings={},
        timeout=aiohttp.ClientTimeout(total=1),
        connector_kwargs={},
        ssl_context=None,
        proxy_url=None,
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
    )


async def _wait_until(predicate) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError("condition was not reached")
        await asyncio.sleep(0)


def _current_session(backend: HttpAsyncBackend) -> tuple[SessionLease, aiohttp.ClientSession, aiohttp.BaseConnector]:
    lease = backend.session_lease
    assert lease is not None
    session = lease.session
    connector = session.connector
    assert connector is not None
    return lease, session, connector


@pytest.mark.asyncio
async def test_cancelled_close_force_closes_detached_session():
    backend = _build_backend()
    backend.ensure_session()
    lease, session, connector = _current_session(backend)
    lease.acquire()

    close_task = asyncio.create_task(backend.close())
    await _wait_until(lambda: backend.session_lease is None)
    assert not close_task.done()

    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert session.closed
    assert connector.closed
    lease.release()


@pytest.mark.asyncio
async def test_cancellation_during_session_close_force_closes_owned_connector(monkeypatch):
    close_started = asyncio.Event()
    close_blocked = asyncio.Event()

    async def blocked_close(_session) -> None:
        close_started.set()
        await close_blocked.wait()

    backend = _build_backend()
    backend.ensure_session()
    _, session, connector = _current_session(backend)
    monkeypatch.setattr(aiohttp.ClientSession, "close", blocked_close)

    close_task = asyncio.create_task(backend.close())
    try:
        await asyncio.wait_for(close_started.wait(), timeout=1)
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        assert session.closed
        assert connector.closed
    finally:
        if not close_task.done():
            close_task.cancel()
            await asyncio.gather(close_task, return_exceptions=True)
        if not connector.closed:
            close = getattr(connector, "_close", None)
            assert close is not None
            close()


@pytest.mark.asyncio
async def test_cancelled_async_context_exit_force_closes_detached_session():
    client = AsyncClient(interface="http", host="localhost", port=8123)
    client._backend.ensure_session()
    lease, session, connector = _current_session(client._backend)
    lease.acquire()

    exit_task = asyncio.create_task(client.__aexit__(None, None, None))
    await _wait_until(lambda: client._backend.session_lease is None)

    exit_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await exit_task

    assert session.closed
    assert connector.closed
    lease.release()


@pytest.mark.asyncio
async def test_cancelled_rotation_force_closes_retired_session_and_keeps_replacement():
    backend = _build_backend()
    backend.ensure_session()
    old_lease, old_session, old_connector = _current_session(backend)
    old_lease.acquire()

    rotate_task = asyncio.create_task(backend.close_connections())
    await _wait_until(lambda: backend.session_lease is not old_lease)
    replacement_lease, replacement_session, replacement_connector = _current_session(backend)
    assert not rotate_task.done()

    rotate_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await rotate_task

    assert old_session.closed
    assert old_connector.closed
    assert backend.session_lease is replacement_lease
    assert not replacement_session.closed
    assert not replacement_connector.closed

    old_lease.release()
    await backend.close()


@pytest.mark.asyncio
async def test_cancelled_automatic_age_rotation_force_closes_retired_session(monkeypatch):
    backend = _build_backend()
    backend.ensure_session()
    old_lease, old_session, old_connector = _current_session(backend)
    old_lease.acquire()
    backend._last_pool_reset = time.time() - 2
    monkeypatch.setattr(common, "get_setting", lambda name: 1 if name == "max_connection_age" else None)

    request_task = asyncio.create_task(backend.request(None, {}))
    await _wait_until(lambda: backend.session_lease is not old_lease)
    replacement_lease, replacement_session, replacement_connector = _current_session(backend)
    assert not request_task.done()

    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    assert old_session.closed
    assert old_connector.closed
    assert backend.session_lease is replacement_lease
    assert not replacement_session.closed
    assert not replacement_connector.closed

    old_lease.release()
    await backend.close()


@pytest.mark.asyncio
async def test_close_waits_for_drain_without_an_internal_timeout():
    backend = _build_backend()
    backend.ensure_session()
    lease, session, connector = _current_session(backend)
    lease.acquire()

    close_task = asyncio.create_task(backend.close())
    await _wait_until(lambda: backend.session_lease is None)
    await asyncio.sleep(0.01)
    assert not close_task.done()
    assert not session.closed
    assert not connector.closed

    lease.release()
    await close_task
    assert session.closed
    assert connector.closed


class _RecordingConnector:
    def __init__(self, error: BaseException | None = None):
        self.closed = False
        self.close_calls = 0
        self.error = error

    def _close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self.error is not None:
            raise self.error


class _CloseFailureSession:
    connector_owner = True

    def __init__(self, close_error: BaseException, connector_error: BaseException | None = None):
        self.connector = _RecordingConnector(connector_error)
        self.close_error = close_error
        self.close_calls = 0

    @property
    def closed(self) -> bool:
        return self.connector.closed

    async def close(self) -> None:
        self.close_calls += 1
        raise self.close_error


@pytest.mark.asyncio
async def test_close_failure_force_closes_connector_and_preserves_error():
    close_error = RuntimeError("session close failed")
    session = _CloseFailureSession(close_error)
    backend = _build_backend()
    backend.session_lease = SessionLease(session)

    with pytest.raises(RuntimeError) as caught:
        await backend.close()

    assert caught.value is close_error
    assert session.close_calls == 1
    assert session.connector.close_calls == 1
    assert session.closed


@pytest.mark.asyncio
async def test_force_close_failure_does_not_replace_graceful_close_error(caplog):
    close_error = RuntimeError("session close failed")
    force_error = RuntimeError("connector close failed")
    session = _CloseFailureSession(close_error, force_error)
    backend = _build_backend()
    backend.session_lease = SessionLease(session)

    with caplog.at_level("WARNING", logger="clickhouse_connect.driver._backend.http_async"):
        with pytest.raises(RuntimeError) as caught:
            await backend.close()

    assert caught.value is close_error
    assert session.connector.close_calls == 1
    assert "Failed to force-close aiohttp connector" in caplog.messages


@pytest.mark.asyncio
async def test_close_failure_does_not_force_close_unowned_connector():
    close_error = RuntimeError("session close failed")
    session = _CloseFailureSession(close_error)
    session.connector_owner = False
    backend = _build_backend()
    backend.session_lease = SessionLease(session)

    with pytest.raises(RuntimeError) as caught:
        await backend.close()

    assert caught.value is close_error
    assert session.connector.close_calls == 0
    assert not session.connector.closed
