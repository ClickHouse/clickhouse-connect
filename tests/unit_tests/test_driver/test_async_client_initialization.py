import asyncio
import functools
import inspect
import threading
from collections.abc import Awaitable, Callable

import aiohttp
import pytest

from clickhouse_connect.driver._backend.operations import CommandOp, QueryOp, RawQueryOp
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ProgrammingError


async def _async_token_provider() -> str:
    return "token"


async def _async_token_provider_with_prefix(prefix: str) -> str:
    return f"{prefix}token"


class _AsyncTokenCallable:
    async def __call__(self) -> str:
        return "token"


class _CloseFailureSession:
    def __init__(self, error: BaseException):
        self.closed = False
        self.error = error
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        raise self.error


def _build_client(
    token_provider: Callable[[], str | Awaitable[str]] | None = None,
    username: str | None = None,
) -> AsyncClient:
    return AsyncClient(
        interface="http",
        host="localhost",
        port=8123,
        username=username,
        token_provider=token_provider,
    )


def _track_sessions(client: AsyncClient) -> list[tuple[aiohttp.ClientSession, aiohttp.BaseConnector]]:
    sessions: list[tuple[aiohttp.ClientSession, aiohttp.BaseConnector]] = []
    new_session = client._backend._new_session

    def tracked_new_session() -> aiohttp.ClientSession:
        session = new_session()
        connector = session.connector
        assert connector is not None
        sessions.append((session, connector))
        return session

    client._backend._new_session = tracked_new_session
    return sessions


def _assert_transport_closed(
    client: AsyncClient,
    sessions: list[tuple[aiohttp.ClientSession, aiohttp.BaseConnector]],
) -> None:
    assert len(sessions) == 1
    session, connector = sessions[0]
    assert session.closed
    assert connector.closed
    assert client._session is None


def _initialization_response(operation) -> object:
    if isinstance(operation, CommandOp):
        return ("26.6.1.1193", "UTC")
    if isinstance(operation, QueryOp):
        return []
    if isinstance(operation, RawQueryOp):
        return b""
    raise AssertionError(f"Unexpected initialization operation {operation!r}")


@pytest.mark.asyncio
async def test_initialized_client_recreates_missing_session_without_server_initialization():
    operation_calls = 0

    async def execute_operation(_operation) -> object:
        nonlocal operation_calls
        operation_calls += 1
        raise AssertionError("Server initialization must not run again")

    client = _build_client()
    sessions = _track_sessions(client)
    client._execute_operation = execute_operation
    client._initialized = True

    await client._initialize()

    assert operation_calls == 0
    assert len(sessions) == 1
    session, connector = sessions[0]
    assert client._session is session
    assert not session.closed
    assert not connector.closed

    await client._initialize()

    assert operation_calls == 0
    assert len(sessions) == 1
    assert client._session is session
    assert session.connector is connector
    assert not session.closed
    assert not connector.closed
    await client.close()


def test_initialized_client_reopens_on_new_event_loop_after_close():
    client = _build_client()
    client._initialized = True

    async def open_and_close() -> aiohttp.ClientSession:
        await client._initialize()
        session = client._session
        assert session is not None
        assert not session.closed
        await client.close()
        assert session.closed
        return session

    first_session = asyncio.run(open_and_close())
    second_session = asyncio.run(open_and_close())

    assert first_session is not second_session


@pytest.mark.asyncio
async def test_sync_token_provider_failure_closes_session():
    provider_error = RuntimeError("token provider failed")

    def fail_provider() -> str:
        raise provider_error

    client = _build_client(fail_provider)
    sessions = _track_sessions(client)

    with pytest.raises(RuntimeError) as caught:
        await client._initialize()

    assert caught.value is provider_error
    _assert_transport_closed(client, sessions)


@pytest.mark.parametrize(
    "cleanup_error",
    [RuntimeError("session close failed"), asyncio.CancelledError()],
    ids=["error", "cancellation"],
)
@pytest.mark.asyncio
async def test_session_close_failure_preserves_initialization_error(cleanup_error, caplog):
    provider_error = RuntimeError("token provider failed")

    def fail_provider() -> str:
        raise provider_error

    client = _build_client(fail_provider)
    session = _CloseFailureSession(cleanup_error)
    client._session = session

    with caplog.at_level("WARNING", logger="clickhouse_connect.driver.asyncclient"):
        with pytest.raises(RuntimeError) as caught:
            await client._initialize()

    assert caught.value is provider_error
    assert session.close_calls == 1
    assert client._session is None
    assert "Failed to close session after AsyncClient initialization error" in caplog.messages


@pytest.mark.parametrize(
    "cleanup_error",
    [RuntimeError("session close failed"), asyncio.CancelledError()],
    ids=["error", "cancellation"],
)
@pytest.mark.asyncio
async def test_real_session_close_failure_force_closes_connector(cleanup_error, monkeypatch, caplog):
    provider_error = RuntimeError("token provider failed")

    def fail_provider() -> str:
        raise provider_error

    async def fail_close(_session) -> None:
        raise cleanup_error

    client = _build_client(fail_provider)
    sessions = _track_sessions(client)
    monkeypatch.setattr(aiohttp.ClientSession, "close", fail_close)

    with caplog.at_level("WARNING", logger="clickhouse_connect.driver.asyncclient"):
        with pytest.raises(RuntimeError) as caught:
            await client._initialize()

    assert caught.value is provider_error
    _assert_transport_closed(client, sessions)
    assert "Failed to close session after AsyncClient initialization error" in caplog.messages


@pytest.mark.asyncio
async def test_token_installation_failure_closes_session():
    client = _build_client(lambda: "token", username="user_1")
    sessions = _track_sessions(client)

    with pytest.raises(ProgrammingError, match="different auth type"):
        await client._initialize()

    _assert_transport_closed(client, sessions)


@pytest.mark.asyncio
async def test_token_provider_cancellation_closes_session():
    provider_started = asyncio.Event()
    provider_blocked = asyncio.Event()

    async def blocked_provider() -> str:
        provider_started.set()
        await provider_blocked.wait()
        return "token"

    client = _build_client(blocked_provider)
    sessions = _track_sessions(client)
    loop = asyncio.get_running_loop()
    previous_debug = loop.get_debug()
    loop.set_debug(True)
    try:
        initialize = asyncio.create_task(client._initialize())
        await asyncio.wait_for(provider_started.wait(), timeout=1)

        initialize.cancel()
        with pytest.raises(asyncio.CancelledError):
            await initialize
    finally:
        loop.set_debug(previous_debug)

    _assert_transport_closed(client, sessions)


@pytest.mark.asyncio
async def test_cancelled_sync_provider_closes_late_coroutine():
    loop = asyncio.get_running_loop()
    provider_started = asyncio.Event()
    provider_returned = asyncio.Event()
    release_provider = threading.Event()
    retained_coroutines = []

    async def late_token() -> str:
        return "token"

    def blocked_provider():
        loop.call_soon_threadsafe(provider_started.set)
        release_provider.wait()
        result = late_token()
        retained_coroutines.append(result)
        loop.call_soon_threadsafe(provider_returned.set)
        return result

    client = _build_client(blocked_provider)
    sessions = _track_sessions(client)
    initialize = asyncio.create_task(client._initialize())
    coroutine_state = None
    try:
        await asyncio.wait_for(provider_started.wait(), timeout=1)
        initialize.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(initialize, timeout=1)
        _assert_transport_closed(client, sessions)

        release_provider.set()
        await asyncio.wait_for(provider_returned.wait(), timeout=1)
        deadline = loop.time() + 1
        while True:
            coroutine_state = inspect.getcoroutinestate(retained_coroutines[0])
            if coroutine_state == inspect.CORO_CLOSED or loop.time() >= deadline:
                break
            await asyncio.sleep(0)
    finally:
        release_provider.set()
        if not initialize.done():
            initialize.cancel()
            await asyncio.gather(initialize, return_exceptions=True)
        if retained_coroutines:
            coroutine_state = inspect.getcoroutinestate(retained_coroutines[0])
            if coroutine_state != inspect.CORO_CLOSED:
                retained_coroutines[0].close()

    assert coroutine_state == inspect.CORO_CLOSED


@pytest.mark.asyncio
async def test_session_close_failure_preserves_initialization_cancellation(caplog):
    provider_started = asyncio.Event()
    provider_blocked = asyncio.Event()

    async def blocked_provider() -> str:
        provider_started.set()
        await provider_blocked.wait()
        return "token"

    client = _build_client(blocked_provider)
    session = _CloseFailureSession(RuntimeError("session close failed"))
    client._session = session
    initialize = asyncio.create_task(client._initialize())
    await asyncio.wait_for(provider_started.wait(), timeout=1)

    initialize.cancel()
    with caplog.at_level("WARNING", logger="clickhouse_connect.driver.asyncclient"):
        with pytest.raises(asyncio.CancelledError):
            await initialize

    assert session.close_calls == 1
    assert client._session is None
    assert "Failed to close session after AsyncClient initialization error" in caplog.messages


@pytest.mark.asyncio
async def test_initialization_cancellation_closes_session():
    operation_started = asyncio.Event()
    operation_blocked = asyncio.Event()

    async def blocked_operation(_operation) -> object:
        operation_started.set()
        await operation_blocked.wait()
        return None

    client = _build_client()
    sessions = _track_sessions(client)
    client._execute_operation = blocked_operation
    initialize = asyncio.create_task(client._initialize())
    await asyncio.wait_for(operation_started.wait(), timeout=1)

    initialize.cancel()
    with pytest.raises(asyncio.CancelledError):
        await initialize

    _assert_transport_closed(client, sessions)


@pytest.mark.asyncio
async def test_concurrent_initialization_cancellation_preserves_successful_session():
    first_operation_started = asyncio.Event()
    release_first_operation = asyncio.Event()
    peer_operation_blocked = asyncio.Event()
    peer_operation_calls = 0
    first_initialize = None

    async def execute_operation(operation) -> object:
        nonlocal peer_operation_calls
        if asyncio.current_task() is first_initialize:
            if not first_operation_started.is_set():
                first_operation_started.set()
                await release_first_operation.wait()
        else:
            peer_operation_calls += 1
            await peer_operation_blocked.wait()
        return _initialization_response(operation)

    client = _build_client()
    sessions = _track_sessions(client)
    client._execute_operation = execute_operation
    first_initialize = asyncio.create_task(client._initialize())
    await asyncio.wait_for(first_operation_started.wait(), timeout=1)
    peer_initialize = asyncio.create_task(client._initialize())
    await asyncio.sleep(0)

    peer_initialize.cancel()
    with pytest.raises(asyncio.CancelledError):
        await peer_initialize
    release_first_operation.set()
    await first_initialize

    assert peer_operation_calls == 0
    assert client._initialized
    assert len(sessions) == 1
    session, connector = sessions[0]
    assert client._session is session
    assert not session.closed
    assert not connector.closed
    await client.close()


@pytest.mark.asyncio
async def test_initialization_can_retry_after_failure():
    provider_calls = 0

    def provider() -> str:
        nonlocal provider_calls
        provider_calls += 1
        if provider_calls == 1:
            raise RuntimeError("token provider failed")
        return "token"

    async def execute_operation(operation) -> object:
        return _initialization_response(operation)

    client = _build_client(provider)
    sessions = _track_sessions(client)
    client._execute_operation = execute_operation

    with pytest.raises(RuntimeError, match="token provider failed"):
        await client._initialize()
    await client._initialize()

    assert provider_calls == 2
    assert client._initialized
    assert len(sessions) == 2
    first_session, first_connector = sessions[0]
    second_session, second_connector = sessions[1]
    assert first_session.closed
    assert first_connector.closed
    assert client._session is second_session
    assert not second_session.closed
    assert not second_connector.closed
    await client.close()


@pytest.mark.parametrize(
    ("provider", "expected"),
    [
        (_async_token_provider, "token"),
        (functools.partial(_async_token_provider_with_prefix, "partial_"), "partial_token"),
        (_AsyncTokenCallable(), "token"),
    ],
    ids=["function", "partial", "callable_object"],
)
@pytest.mark.asyncio
async def test_async_token_provider_supported_in_debug_mode(provider, expected):
    client = _build_client(provider)
    loop = asyncio.get_running_loop()
    previous_debug = loop.get_debug()
    loop.set_debug(True)
    try:
        assert await client._resolve_token() == expected
    finally:
        loop.set_debug(previous_debug)


@pytest.mark.parametrize("returns_awaitable", [False, True])
@pytest.mark.asyncio
async def test_sync_token_provider_runs_in_executor(returns_awaitable):
    provider_thread = None

    async def async_result() -> str:
        return "token"

    def provider():
        nonlocal provider_thread
        provider_thread = threading.get_ident()
        return async_result() if returns_awaitable else "token"

    client = _build_client(provider)
    loop_thread = threading.get_ident()

    assert await client._resolve_token() == "token"
    assert provider_thread is not None
    assert provider_thread != loop_thread
