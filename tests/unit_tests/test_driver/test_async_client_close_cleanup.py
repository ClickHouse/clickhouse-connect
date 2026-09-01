import asyncio
import time

import aiohttp
import pytest

from clickhouse_connect import common
from clickhouse_connect.driver._backend.http_async import HttpAsyncBackend, SessionLease, release_lease
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import OperationalError


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


class _RequestResponse:
    def __init__(self, origin: str):
        self.origin = origin
        self.status = 200
        self.headers = {}


class _RequestConnector:
    def __init__(self, session):
        self.session = session
        self.closed = False
        self.force_close_calls = 0

    def _close(self) -> None:
        self.force_close_calls += 1
        self.closed = True
        self.session.closed = True
        self.session.request_finished.set()


class _RequestSession:
    connector_owner = True

    def __init__(self, name: str):
        self.name = name
        self.closed = False
        self.close_calls = 0
        self.headers = {}
        self.request_data = []
        self.request_started = asyncio.Event()
        self.request_finished = asyncio.Event()
        self.connector = _RequestConnector(self)

    async def request(self, **kwargs):
        self.request_data.append(kwargs["data"])
        self.request_started.set()
        await self.request_finished.wait()
        if self.connector.force_close_calls:
            raise aiohttp.ServerDisconnectedError("retired session was force-closed")
        return _RequestResponse(self.name)

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        self.connector.closed = True


@pytest.mark.asyncio
async def test_session_close_cancelled_before_start_force_closes_connector():
    backend = _build_backend()
    session = _RequestSession("active")
    lease = SessionLease(session)
    lease.acquire()

    close_task = backend._start_session_close(lease, retired=False)
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert session.connector.force_close_calls == 1
    assert session.close_calls == 0
    lease.release()


@pytest.mark.asyncio
async def test_cancelled_automatic_rotation_preserves_request_on_retired_session(monkeypatch):
    backend = _build_backend()
    old_session = _RequestSession("retired")
    replacement_session = _RequestSession("replacement")
    backend.session_lease = SessionLease(old_session)
    monkeypatch.setattr(backend, "_new_session", lambda: replacement_session)
    monkeypatch.setattr(common, "get_setting", lambda name: 1 if name == "max_connection_age" else None)

    sibling_data = {"request": "sibling"}
    sibling_task = asyncio.create_task(backend.request(sibling_data, {}))
    await asyncio.wait_for(old_session.request_started.wait(), timeout=1)

    backend._last_pool_reset = time.time() - 2
    trigger_data = {"request": "trigger"}
    trigger_task = asyncio.create_task(backend.request(trigger_data, {}))
    await _wait_until(lambda: backend.session is replacement_session)
    assert not trigger_task.done()

    trigger_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(trigger_task, timeout=1)

    old_session.request_finished.set()
    replacement_session.request_finished.set()
    sibling_response = await asyncio.wait_for(sibling_task, timeout=1)
    release_lease(sibling_response)
    await _wait_until(lambda: old_session.closed)
    await _wait_until(lambda: not backend._retired_session_tasks)

    assert sibling_response.origin == "retired"
    assert old_session.close_calls == 1
    assert old_session.connector.force_close_calls == 0
    assert replacement_session.request_data == [trigger_data]

    replacement_response = await backend.request({"request": "replacement"}, {})
    assert replacement_response.origin == "replacement"
    release_lease(replacement_response)
    await backend.close()


@pytest.mark.asyncio
async def test_close_waits_for_background_automatic_rotation_cleanup(monkeypatch):
    backend = _build_backend()
    old_session = _RequestSession("retired")
    replacement_session = _RequestSession("replacement")
    old_session.request_finished.set()
    replacement_session.request_finished.set()
    backend.session_lease = SessionLease(old_session)
    monkeypatch.setattr(backend, "_new_session", lambda: replacement_session)
    monkeypatch.setattr(common, "get_setting", lambda name: 1 if name == "max_connection_age" else None)

    sibling_response = await backend.request({"request": "sibling"}, {})
    backend._last_pool_reset = time.time() - 2
    trigger_task = asyncio.create_task(backend.request({"request": "trigger"}, {}))
    trigger_response = await asyncio.wait_for(trigger_task, timeout=1)
    release_lease(trigger_response)

    close_task = asyncio.create_task(backend.close())
    await _wait_until(lambda: replacement_session.closed)
    assert not close_task.done()
    assert not old_session.closed

    release_lease(sibling_response)
    await asyncio.wait_for(close_task, timeout=1)
    assert old_session.closed
    assert old_session.close_calls == 1
    assert old_session.connector.force_close_calls == 0


@pytest.mark.asyncio
async def test_cancelled_explicit_rotation_does_not_retry_request_from_closed_session(monkeypatch):
    backend = _build_backend()
    old_session = _RequestSession("retired")
    replacement_session = _RequestSession("replacement")
    replacement_session.request_finished.set()
    backend.session_lease = SessionLease(old_session)
    monkeypatch.setattr(backend, "_new_session", lambda: replacement_session)
    monkeypatch.setattr(common, "get_setting", lambda name: None)

    retry_calls = 0

    async def retry_body():
        nonlocal retry_calls
        retry_calls += 1
        return {"request": "replayed sibling"}

    sibling_task = asyncio.create_task(backend.request({"request": "sibling"}, {}, retry_body=retry_body))
    await asyncio.wait_for(old_session.request_started.wait(), timeout=1)

    rotation_task = asyncio.create_task(backend.close_connections())
    await _wait_until(lambda: backend.session is replacement_session)
    rotation_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(rotation_task, timeout=1)

    with pytest.raises(OperationalError):
        await asyncio.wait_for(sibling_task, timeout=1)
    assert old_session.connector.force_close_calls == 1
    assert retry_calls == 0
    assert replacement_session.request_data == []
    await backend.close()


class _StaleOnceSession(_RequestSession):
    async def request(self, **kwargs):
        self.request_data.append(kwargs["data"])
        if len(self.request_data) == 1:
            raise aiohttp.ServerDisconnectedError("remote closed a stale connection")
        return _RequestResponse(self.name)


@pytest.mark.asyncio
async def test_remote_close_on_open_session_still_retries(monkeypatch):
    backend = _build_backend()
    session = _StaleOnceSession("active")
    backend.session_lease = SessionLease(session)
    monkeypatch.setattr(common, "get_setting", lambda name: None)

    response = await backend.request({"request": "query"}, {})

    assert response.origin == "active"
    assert len(session.request_data) == 2
    release_lease(response)
    await backend.close()


@pytest.mark.asyncio
async def test_cancelled_close_owns_current_and_background_retirement(monkeypatch):
    backend = _build_backend()
    old_session = _RequestSession("retired")
    replacement_session = _RequestSession("replacement")
    backend.session_lease = SessionLease(old_session)
    monkeypatch.setattr(backend, "_new_session", lambda: replacement_session)
    monkeypatch.setattr(common, "get_setting", lambda name: 1 if name == "max_connection_age" else None)

    sibling_task = asyncio.create_task(backend.request({"request": "sibling"}, {}))
    await asyncio.wait_for(old_session.request_started.wait(), timeout=1)
    backend._last_pool_reset = time.time() - 2
    trigger_task = asyncio.create_task(backend.request({"request": "trigger"}, {}))
    await asyncio.wait_for(replacement_session.request_started.wait(), timeout=1)

    close_task = asyncio.create_task(backend.close())
    await _wait_until(lambda: backend.session_lease is None)
    assert not close_task.done()
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(close_task, timeout=1)

    with pytest.raises(OperationalError):
        await asyncio.wait_for(sibling_task, timeout=1)
    with pytest.raises(OperationalError):
        await asyncio.wait_for(trigger_task, timeout=1)
    assert old_session.connector.force_close_calls == 1
    assert replacement_session.connector.force_close_calls == 1
    await _wait_until(lambda: not backend._retired_session_tasks)


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
