import asyncio
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("pytest_asyncio")
pytest.importorskip("sqlalchemy", minversion="2.0.44")

from sqlalchemy.dialects import registry
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, PoolProxiedConnection
from sqlalchemy.util.concurrency import await_only, greenlet_spawn

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.asyncio import ClickHouseAsyncDialect, _AsyncAdaptedConnection
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import create_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import StreamFailureError
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.summary import QuerySummary


def test_async_dialect_registry_and_flags():
    assert registry.load("clickhousedb.async") is ClickHouseAsyncDialect
    assert ClickHouseAsyncDialect.__dict__["supports_statement_cache"] is False
    assert "import_dbapi" in ClickHouseAsyncDialect.__dict__
    assert ClickHouseAsyncDialect.is_async is True
    assert ClickHouseAsyncDialect.has_terminate is True
    assert issubclass(ClickHouseAsyncDialect.poolclass, AsyncAdaptedQueuePool)
    assert ClickHouseAsyncDialect.supports_server_side_cursors is False


def test_async_dbapi_mirrors_pep249_surface():
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()

    assert async_dbapi.apilevel == dbapi.apilevel
    assert async_dbapi.threadsafety == dbapi.threadsafety
    assert async_dbapi.paramstyle == dbapi.paramstyle
    for name in (
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ):
        assert getattr(async_dbapi, name) is getattr(dbapi, name)
    assert async_dbapi.Binary([0, 255]) == b"\x00\xff"
    assert async_dbapi.DateFromTicks(0) == dbapi.DateFromTicks(0)


@pytest.mark.asyncio
async def test_async_pool_generation_closes_stale_returns_during_direct_reuse():
    class Connection:
        def __init__(self):
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection()
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=0)
    old_checkout = await greenlet_spawn(pool.connect)
    old_connection = old_checkout.dbapi_connection
    assert isinstance(old_connection, Connection)

    await greenlet_spawn(pool.dispose)
    new_checkout = await greenlet_spawn(pool.connect)
    new_connection = new_checkout.dbapi_connection
    assert isinstance(new_connection, Connection)

    await greenlet_spawn(old_checkout.close)
    assert old_connection.close_calls == 1
    assert new_connection.close_calls == 0

    await greenlet_spawn(new_checkout.close)
    assert pool.checkedout() == 0

    reused_checkout = await greenlet_spawn(pool.connect)

    assert reused_checkout.dbapi_connection is new_connection
    assert len(connections) == 2
    assert pool.checkedout() == 1
    await greenlet_spawn(reused_checkout.close)
    assert pool.checkedout() == 0

    replacement_pool = pool.recreate()
    post_recreate_checkout = await greenlet_spawn(pool.connect)
    assert post_recreate_checkout.dbapi_connection is new_connection
    await greenlet_spawn(post_recreate_checkout.close)
    assert new_connection.close_calls == 0

    await greenlet_spawn(pool.dispose)
    assert pool.checkedin() == 1
    assert pool.overflow() == 0
    assert not pool._clickhouse_pending_records
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_recreate_without_dispose_preserves_checked_out_connection():
    class Connection:
        def __init__(self):
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

        def rollback(self) -> None:
            pass

    connection = Connection()
    pool = ClickHouseAsyncDialect.poolclass(lambda: connection, pool_size=1, max_overflow=0)
    checkout = await greenlet_spawn(pool.connect)

    replacement_pool = pool.recreate()
    await greenlet_spawn(checkout.close)

    assert connection.close_calls == 0
    reused_checkout = await greenlet_spawn(pool.connect)
    assert reused_checkout.dbapi_connection is connection
    await greenlet_spawn(reused_checkout.close)

    await greenlet_spawn(pool.dispose)
    assert connection.close_calls == 1
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_suspended_direct_reuse_clears_pending_recreate():
    class Connection:
        def __init__(self):
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

        def rollback(self) -> None:
            pass

    create_started = asyncio.Event()
    create_release = asyncio.Event()
    connection = Connection()

    async def async_creator() -> Connection:
        create_started.set()
        await create_release.wait()
        return connection

    def creator() -> Connection:
        return await_only(async_creator())

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=0)
    await greenlet_spawn(pool.dispose)
    checkout_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.wait_for(create_started.wait(), timeout=1.0)

    replacement_pool = pool.recreate()
    create_release.set()
    checkout = await asyncio.wait_for(checkout_task, timeout=1.0)
    await greenlet_spawn(checkout.close)

    assert connection.close_calls == 0
    reused_checkout = await greenlet_spawn(pool.connect)
    assert reused_checkout.dbapi_connection is connection
    await greenlet_spawn(reused_checkout.close)

    await greenlet_spawn(pool.dispose)
    assert connection.close_calls == 1
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_dispose_wakes_waiter_on_replaced_pool():
    class Connection:
        def __init__(self):
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection()
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=0, timeout=5.0)
    holder = await greenlet_spawn(pool.connect)
    waiter_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.sleep(0)
    assert not waiter_task.done()

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()
    await greenlet_spawn(holder.close)
    waiter = await asyncio.wait_for(waiter_task, timeout=1.0)

    assert len(connections) == 2
    assert connections[0].close_calls == 1
    assert connections[1].close_calls == 0

    await greenlet_spawn(waiter.close)
    assert connections[1].close_calls == 1
    assert pool.checkedin() == 1
    assert pool.overflow() == 0
    assert not pool._clickhouse_pending_records
    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_dispose_during_slow_creation_closes_stale_return():
    live_connections = 0
    max_live_connections = 0

    class Connection:
        def __init__(self):
            nonlocal live_connections, max_live_connections
            self.close_calls = 0
            live_connections += 1
            max_live_connections = max(max_live_connections, live_connections)

        def close(self) -> None:
            nonlocal live_connections
            self.close_calls += 1
            live_connections -= 1

        def rollback(self) -> None:
            pass

    create_started = asyncio.Event()
    create_release = asyncio.Event()
    connections: list[Connection] = []

    async def async_creator() -> Connection:
        create_started.set()
        await create_release.wait()
        connection = Connection()
        connections.append(connection)
        return connection

    def creator() -> Connection:
        return await_only(async_creator())

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=0)
    checkout_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.wait_for(create_started.wait(), timeout=1.0)

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()
    create_release.set()
    checkout = await asyncio.wait_for(checkout_task, timeout=1.0)

    await greenlet_spawn(checkout.close)
    assert connections[0].close_calls == 1
    assert max_live_connections <= 1
    assert live_connections == 0
    assert pool.checkedin() == 1
    assert pool.overflow() == 0
    assert not pool._clickhouse_pending_records
    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.parametrize(("max_overflow", "holder_count"), ((0, 1), (1, 2)))
@pytest.mark.asyncio
async def test_async_pool_waiter_creation_uses_post_dispose_accounting_epoch(max_overflow, holder_count):
    class Connection:
        def __init__(self):
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection()
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=max_overflow, timeout=0.05)
    holders = [await greenlet_spawn(pool.connect) for _ in range(holder_count)]
    waiter_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.sleep(0)
    assert not waiter_task.done()

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()
    waiter = await asyncio.wait_for(waiter_task, timeout=1.0)

    assert len(connections) == holder_count + 1
    assert pool.checkedout() == 1
    assert pool.overflow() == 0
    await greenlet_spawn(waiter.close)
    assert connections[holder_count].close_calls == 1
    assert pool.checkedout() == 0

    for holder in holders:
        await greenlet_spawn(holder.close)
    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.parametrize("max_overflow", (-1, 0, 1))
@pytest.mark.parametrize("error_type", (RuntimeError, asyncio.CancelledError))
@pytest.mark.asyncio
async def test_async_pool_creation_failure_does_not_decrement_new_accounting_epoch(max_overflow, error_type):
    create_started = asyncio.Event()
    create_release = asyncio.Event()
    create_error = error_type("creation failed")

    async def async_creator():
        create_started.set()
        await create_release.wait()
        raise create_error

    def creator():
        return await_only(async_creator())

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=max_overflow)
    checkout_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.wait_for(create_started.wait(), timeout=1.0)

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()
    create_release.set()
    with pytest.raises(error_type) as caught:
        await checkout_task

    assert caught.value is create_error
    assert pool._clickhouse_accounting_epoch == 1
    assert pool.overflow() == -1
    assert pool.checkedout() == 0

    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_overflow_return_does_not_decrement_new_accounting_epoch():
    class Connection:
        def __init__(self, block_close: bool):
            self.close_calls = 0
            self.close_started = asyncio.Event()
            self.close_release = asyncio.Event()
            if not block_close:
                self.close_release.set()

        async def _close(self) -> None:
            self.close_started.set()
            await self.close_release.wait()
            self.close_calls += 1

        def close(self) -> None:
            await_only(self._close())

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection(block_close=len(connections) == 1)
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=1, timeout=1.0)
    checkouts = [await greenlet_spawn(pool.connect) for _ in range(2)]
    await greenlet_spawn(checkouts[0].close)
    overflow_return = asyncio.create_task(greenlet_spawn(checkouts[1].close))
    await asyncio.wait_for(connections[1].close_started.wait(), timeout=1.0)

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()
    connections[1].close_release.set()
    await asyncio.wait_for(overflow_return, timeout=1.0)

    assert pool.checkedout() == 0
    assert pool.overflow() == 0
    assert len(pool._clickhouse_pending_records) == 1

    reused = [await greenlet_spawn(pool.connect) for _ in range(2)]
    assert pool.checkedout() == 2
    assert pool.overflow() == 1
    assert not pool._clickhouse_pending_records
    for checkout in reused:
        await greenlet_spawn(checkout.close)

    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_multiple_stale_returns_preserve_overflow_accounting():
    live_connections = 0
    max_live_connections = 0

    class Connection:
        def __init__(self):
            nonlocal live_connections, max_live_connections
            self.close_calls = 0
            live_connections += 1
            max_live_connections = max(max_live_connections, live_connections)

        def close(self) -> None:
            nonlocal live_connections
            self.close_calls += 1
            live_connections -= 1

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection()
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=1, max_overflow=1, timeout=5.0)
    holders = [await greenlet_spawn(pool.connect) for _ in range(2)]
    waiters = [asyncio.create_task(greenlet_spawn(pool.connect)) for _ in range(3)]
    await asyncio.sleep(0)
    assert not any(waiter.done() for waiter in waiters)

    await greenlet_spawn(pool.dispose)
    replacement_pool = pool.recreate()

    def close_holders() -> None:
        for holder in holders:
            holder.close()

    await greenlet_spawn(close_holders)
    first_waiter, second_waiter = await asyncio.wait_for(asyncio.gather(*waiters[:2]), timeout=1.0)
    assert not waiters[2].done()
    assert pool.overflow() == 1
    assert max_live_connections == 2

    await greenlet_spawn(first_waiter.close)
    third_waiter = await asyncio.wait_for(waiters[2], timeout=1.0)
    await greenlet_spawn(second_waiter.close)
    await greenlet_spawn(third_waiter.close)

    assert len(connections) == 5
    assert all(connection.close_calls == 1 for connection in connections)
    assert max_live_connections == 2
    assert live_connections == 0
    assert pool.checkedout() == 0
    assert pool.overflow() == 0
    assert not pool._clickhouse_pending_records
    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_return_during_suspended_dispose_wakes_waiter_and_closes_on_recreate():
    live_connections = 0
    max_live_connections = 0

    class Connection:
        def __init__(self, block_close: bool):
            nonlocal live_connections, max_live_connections
            self.block_close = block_close
            self.close_calls = 0
            self.close_started = asyncio.Event()
            self.close_release = asyncio.Event()
            live_connections += 1
            max_live_connections = max(max_live_connections, live_connections)
            if not block_close:
                self.close_release.set()

        async def _close(self) -> None:
            nonlocal live_connections
            self.close_started.set()
            await self.close_release.wait()
            self.close_calls += 1
            live_connections -= 1

        def close(self) -> None:
            await_only(self._close())

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection(block_close=len(connections) == 1)
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=2, max_overflow=0, timeout=5.0)
    holder = await greenlet_spawn(pool.connect)
    idle_checkout = await greenlet_spawn(pool.connect)
    await greenlet_spawn(idle_checkout.close)
    idle_connection = connections[1]

    def dispose_and_recreate():
        pool.dispose()
        return pool.recreate()

    dispose_task = asyncio.create_task(greenlet_spawn(dispose_and_recreate))
    await asyncio.wait_for(idle_connection.close_started.wait(), timeout=1.0)
    waiter_task = asyncio.create_task(greenlet_spawn(pool.connect))
    await asyncio.sleep(0)
    assert not waiter_task.done()

    await greenlet_spawn(holder.close)
    assert connections[0].close_calls == 1
    assert not waiter_task.done()

    idle_connection.close_release.set()
    replacement_pool = await asyncio.wait_for(dispose_task, timeout=1.0)
    waiter = await asyncio.wait_for(waiter_task, timeout=1.0)
    assert connections[2].close_calls == 0
    await greenlet_spawn(waiter.close)

    assert all(connection.close_calls == 1 for connection in connections)
    assert max_live_connections == 2
    assert live_connections == 0
    assert pool.checkedout() == 0
    assert pool.checkedin() == 2
    assert pool.overflow() == 0
    assert not pool._clickhouse_pending_records
    await greenlet_spawn(pool.dispose)
    await greenlet_spawn(replacement_pool.dispose)


@pytest.mark.asyncio
async def test_async_pool_cancelled_dispose_restores_accounting_without_requeueing_dead_connection():
    close_interruption = asyncio.CancelledError()

    class Connection:
        def __init__(self, interrupt_close: bool):
            self.close_calls = 0
            self.interrupt_close = interrupt_close

        async def _close(self) -> None:
            self.close_calls += 1
            if self.interrupt_close and self.close_calls == 1:
                raise close_interruption

        def close(self) -> None:
            await_only(self._close())

        def rollback(self) -> None:
            pass

    connections: list[Connection] = []

    def creator() -> Connection:
        connection = Connection(interrupt_close=len(connections) == 1)
        connections.append(connection)
        return connection

    pool = ClickHouseAsyncDialect.poolclass(creator, pool_size=2, max_overflow=0)
    checkouts = [await greenlet_spawn(pool.connect) for _ in range(2)]
    for checkout in checkouts:
        await greenlet_spawn(checkout.close)

    with pytest.raises(asyncio.CancelledError) as caught:
        await greenlet_spawn(pool.dispose)

    assert caught.value is close_interruption
    assert connections[0].close_calls == 1
    assert connections[1].close_calls == 2
    assert pool._clickhouse_accounting_epoch == 0
    assert not pool._clickhouse_disposing
    assert not pool._clickhouse_pending_records
    assert pool.checkedin() == 2
    assert pool.overflow() == 0
    assert pool.checkedout() == 0

    replacement_checkouts = [await greenlet_spawn(pool.connect) for _ in range(2)]
    assert all(checkout.dbapi_connection not in connections[:2] for checkout in replacement_checkouts)
    for checkout in replacement_checkouts:
        await greenlet_spawn(checkout.close)

    assert pool.checkedout() == 0
    await greenlet_spawn(pool.dispose)
    assert not pool._clickhouse_pending_records


@pytest.mark.parametrize(
    ("connect_options", "expected"),
    (
        ({}, (1, 1, 30.0)),
        (
            {"connector_limit": "13", "connector_limit_per_host": "7", "keepalive_timeout": "4.5"},
            (13, 7, 4.5),
        ),
    ),
)
@pytest.mark.asyncio
async def test_async_dbapi_connector_options(connect_options, expected):
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        adapted = await greenlet_spawn(
            lambda: async_dbapi.connect(host="localhost", **connect_options),
        )

    raw = adapted.driver_connection
    assert raw._backend.connector_kwargs["limit"] == expected[0]
    assert raw._backend.connector_kwargs["limit_per_host"] == expected[1]
    assert raw._backend.connector_kwargs["keepalive_timeout"] == expected[2]
    assert raw._autogenerate_session_id_param is True
    assert ClickHouseAsyncDialect().get_driver_connection(adapted) is raw
    await greenlet_spawn(adapted.close)


@pytest.mark.parametrize(
    "connect_options",
    (
        {"autogenerate_session_id": False},
        {"autogenerate_session_id": "false"},
        {"dsn": "http://localhost:8123/default?autogenerate_session_id=false"},
        {
            "dsn": "http://localhost:8123/default?autogenerate_session_id=true",
            "autogenerate_session_id": False,
        },
    ),
)
@pytest.mark.asyncio
async def test_async_dbapi_session_id_default_can_be_disabled(connect_options):
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        adapted = await greenlet_spawn(lambda: async_dbapi.connect(**connect_options))

    assert adapted.driver_connection._autogenerate_session_id_param is False
    await greenlet_spawn(adapted.close)


@pytest.mark.parametrize(
    ("connect_options", "expected"),
    (
        (
            {"dsn": ("http://localhost:8123/default?connector_limit=13&connector_limit_per_host=7&keepalive_timeout=4.5")},
            (13, 7, 4.5),
        ),
        (
            {
                "dsn": ("http://localhost:8123/default?connector_limit=13&connector_limit_per_host=7&keepalive_timeout=4.5"),
                "connector_limit": 79,
                "connector_limit_per_host": 71,
                "keepalive_timeout": 17.5,
            },
            (79, 71, 17.5),
        ),
    ),
)
@pytest.mark.asyncio
async def test_async_dbapi_nested_dsn_connector_options(connect_options, expected):
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        adapted = await greenlet_spawn(lambda: async_dbapi.connect(**connect_options))

    raw = adapted.driver_connection
    assert raw._backend.connector_kwargs["limit"] == expected[0]
    assert raw._backend.connector_kwargs["limit_per_host"] == expected[1]
    assert raw._backend.connector_kwargs["keepalive_timeout"] == expected[2]
    await greenlet_spawn(adapted.close)


@pytest.mark.asyncio
async def test_async_dbapi_accepts_async_creator():
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        raw = await create_async_client(host="localhost")

    async def creator() -> AsyncClient:
        return raw

    with patch("clickhouse_connect.cc_sqlalchemy.asyncio.create_async_client", new=AsyncMock()) as factory:
        adapted = await greenlet_spawn(lambda: async_dbapi.connect(async_creator_fn=creator))

    factory.assert_not_awaited()
    assert adapted.driver_connection is raw
    await greenlet_spawn(adapted.close)


class _SerializedClient:
    server_tz = timezone.utc

    def __init__(self):
        self.active = 0
        self.max_active = 0
        self.calls = 0

    async def query(self, *args: Any, **kwargs: Any) -> QueryResult:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.calls += 1
        await asyncio.sleep(0.01)
        self.active -= 1
        return QueryResult(
            [[13]],
            column_names=("value",),
            column_types=(get_from_name("UInt64"),),
            summary={},
        )

    async def insert(self, *args: Any, **kwargs: Any) -> QuerySummary:
        await asyncio.sleep(0)
        return QuerySummary({})

    async def command(self, *args: Any, **kwargs: Any) -> int:
        await asyncio.sleep(0)
        return 13

    async def close(self) -> None:
        await asyncio.sleep(0)

    def _add_integration_tag(self, name: str) -> None:
        pass


class _LifecycleClient(_SerializedClient):
    def __init__(self, close_error: BaseException | None = None, block_close: bool = False):
        super().__init__()
        self.close_error = close_error
        self.close_calls = 0
        self.force_close_calls = 0
        self.close_started = asyncio.Event()
        self.close_finished = asyncio.Event()
        self.close_release = asyncio.Event()
        if not block_close:
            self.close_release.set()

    async def close(self) -> None:
        self.close_calls += 1
        self.close_started.set()
        await self.close_release.wait()
        if self.close_error is not None:
            raise self.close_error
        self.close_finished.set()

    def _force_close(self) -> None:
        self.force_close_calls += 1
        self.close_release.set()


class _Session:
    def __init__(self, closed: bool):
        self.closed = closed


class _SessionStateClient:
    server_tz = timezone.utc

    def __init__(self, session: _Session | None):
        self._session = session

    async def close(self) -> None:
        await asyncio.sleep(0)


def _operational_error(cause: BaseException) -> dbapi.OperationalError:
    error = dbapi.OperationalError("Network Error")
    error.__cause__ = cause
    return error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(dbapi.DatabaseError("Unknown table", code=60, name="UNKNOWN_TABLE"), id="server"),
        pytest.param(_operational_error(TimeoutError()), id="timeout"),
        pytest.param(_operational_error(ConnectionRefusedError()), id="connection-refused"),
        pytest.param(StreamFailureError("Stream failed"), id="stream"),
    ],
)
@pytest.mark.parametrize(
    ("session", "expected"),
    [
        pytest.param(_Session(False), False, id="open"),
        pytest.param(_Session(True), True, id="closed"),
        pytest.param(None, True, id="absent"),
    ],
)
def test_async_dialect_is_disconnect_uses_adapted_client_session_state(
    error: dbapi.Error,
    session: _Session | None,
    expected: bool,
):
    dialect = ClickHouseAsyncDialect()
    client = _SessionStateClient(session)
    connection = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    assert dialect.is_disconnect(error, connection, None) is expected


@pytest.mark.asyncio
async def test_async_dialect_is_disconnect_uses_pool_proxied_adapted_state():
    dialect = ClickHouseAsyncDialect()
    client = _SessionStateClient(None)
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]
    pool = ClickHouseAsyncDialect.poolclass(lambda: adapted, pool_size=1, max_overflow=0)
    connection = await greenlet_spawn(pool.connect)

    try:
        assert isinstance(connection, PoolProxiedConnection)
        assert dialect.is_disconnect(dbapi.OperationalError("Network Error"), connection, None) is True
    finally:
        await greenlet_spawn(connection.close)
        await greenlet_spawn(pool.dispose)


def test_async_dialect_is_disconnect_requires_client_state():
    dialect = ClickHouseAsyncDialect()
    error = dbapi.OperationalError("Network Error")
    connection = _AsyncAdaptedConnection(
        ClickHouseAsyncDialect.import_dbapi(),
        _SerializedClient(),  # type: ignore[arg-type]
    )

    assert dialect.is_disconnect(error, None, None) is False
    assert dialect.is_disconnect(error, object(), None) is False  # type: ignore[arg-type]
    assert dialect.is_disconnect(error, connection, None) is False


class _PrePingClient(_SerializedClient):
    def __init__(self):
        super().__init__()
        self._session: _Session | None = _Session(False)
        self.query_calls: list[str | None] = []
        self.close_calls = 0

    async def query(self, query: str | None = None, *args: Any, **kwargs: Any) -> QueryResult:
        self.query_calls.append(query)
        if self._session is None or self._session.closed:
            raise dbapi.ProgrammingError("Session not initialized")
        return await super().query(query, *args, **kwargs)

    async def close(self) -> None:
        self.close_calls += 1
        if self._session is not None:
            self._session.closed = True
        self._session = None

    def force_close(self) -> None:
        if self._session is not None:
            self._session.closed = True
        self._session = None


@pytest.mark.asyncio
async def test_async_engine_pool_pre_ping_uses_select_one_and_replaces_closed_client():
    clients: list[_PrePingClient] = []

    async def creator() -> _PrePingClient:
        client = _PrePingClient()
        clients.append(client)
        return client

    with patch.object(ClickHouseAsyncDialect, "initialize"):
        engine = create_async_engine(
            "clickhousedb+async://",
            async_creator=creator,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
        )

        try:
            async with engine.connect():
                pass
            assert clients[0].query_calls == []

            async with engine.connect():
                pass
            assert clients[0].query_calls == ["SELECT 1"]

            clients[0].force_close()
            async with engine.connect():
                pass

            assert len(clients) == 2
            assert clients[0].close_calls == 1
            assert clients[1].query_calls == []
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_async_dialect_do_close_uses_graceful_close():
    dialect = ClickHouseAsyncDialect()
    client = _LifecycleClient()
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    await greenlet_spawn(lambda: dialect.do_close(adapted))

    assert client.close_calls == 1
    assert client.force_close_calls == 0


@pytest.mark.asyncio
async def test_async_dialect_do_terminate_uses_graceful_close_in_greenlet():
    dialect = ClickHouseAsyncDialect()
    client = _LifecycleClient()
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    await greenlet_spawn(lambda: dialect.do_terminate(adapted))

    assert client.close_calls == 1
    assert client.force_close_calls == 0


@pytest.mark.asyncio
async def test_async_dialect_do_terminate_force_closes_outside_greenlet():
    dialect = ClickHouseAsyncDialect()
    client = _LifecycleClient()
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    dialect.do_terminate(adapted)

    assert client.close_calls == 0
    assert client.force_close_calls == 1


@pytest.mark.asyncio
async def test_async_dialect_cancelled_terminate_force_closes_and_preserves_cancellation():
    dialect = ClickHouseAsyncDialect()
    client = _LifecycleClient(block_close=True)
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    terminate_task = asyncio.create_task(greenlet_spawn(lambda: dialect.do_terminate(adapted)))
    await asyncio.wait_for(client.close_started.wait(), timeout=1)
    terminate_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await terminate_task
    await asyncio.wait_for(client.close_finished.wait(), timeout=1)

    assert client.close_calls == 1
    assert client.force_close_calls == 1


@pytest.mark.asyncio
async def test_async_dialect_unexpected_terminate_error_is_not_force_closed():
    dialect = ClickHouseAsyncDialect()
    close_error = RuntimeError("graceful close failed")
    client = _LifecycleClient(close_error=close_error)
    adapted = _AsyncAdaptedConnection(ClickHouseAsyncDialect.import_dbapi(), client)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError) as caught:
        await greenlet_spawn(lambda: dialect.do_terminate(adapted))

    assert caught.value is close_error
    assert client.close_calls == 1
    assert client.force_close_calls == 0


class _InsertRoutingClient(_SerializedClient):
    def __init__(self, summaries: list[dict[str, Any]] | None = None):
        super().__init__()
        self.query_calls: list[tuple[str | None, Any]] = []
        self.insert_calls = 0
        self.summaries = list(summaries or [])

    async def query(self, query: str | None = None, parameters: Any = None, **kwargs: Any) -> QueryResult:
        self.query_calls.append((query, parameters))
        await asyncio.sleep(0)
        summary = self.summaries.pop(0) if self.summaries else {}
        return QueryResult([], column_names=(), column_types=(), summary=summary)

    async def insert(self, *args: Any, **kwargs: Any) -> QuerySummary:
        self.insert_calls += 1
        await asyncio.sleep(0)
        return QuerySummary({})


@pytest.mark.asyncio
async def test_async_cursor_serializes_execution_and_preserves_buffered_rows():
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    client = _SerializedClient()
    adapted = _AsyncAdaptedConnection(async_dbapi, client)  # type: ignore[arg-type]

    async def execute() -> Any:
        def run():
            cursor = adapted.cursor()
            cursor.execute("SELECT 13")
            return cursor

        return await greenlet_spawn(run)

    cursor_1, cursor_2 = await asyncio.gather(execute(), execute())

    assert client.max_active == 1
    assert client.calls == 2
    await cursor_1._async_soft_close()
    assert cursor_1.fetchall() == [[13]]
    cursor_1.close()
    cursor_2.close()
    with pytest.raises(dbapi.NotSupportedError, match="Server-side cursors"):
        adapted.cursor(server_side=True)


@pytest.mark.parametrize(
    ("operation", "parameters"),
    (
        (
            "INSERT INTO events (left_value, right_value) VALUES (%(right_value)s, %(left_value)s)",
            [
                {"left_value": 13, "right_value": 79},
                {"left_value": 17, "right_value": 101},
            ],
        ),
        (
            "INSERT INTO events (value) VALUES (%(value)s + 1)",
            [{"value": 13}, {"value": 79}],
        ),
        (
            "INSERT INTO events VALUES (%(value)s, %(value)s)",
            [{"value": 13}, {"value": 79}],
        ),
    ),
)
@pytest.mark.asyncio
async def test_async_cursor_preserves_executemany_sql(operation, parameters):
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    client = _InsertRoutingClient()
    adapted = _AsyncAdaptedConnection(async_dbapi, client)  # type: ignore[arg-type]
    cursor = adapted.cursor()

    await greenlet_spawn(lambda: cursor.executemany(operation, parameters))

    assert client.query_calls == [(operation, row) for row in parameters]
    assert client.insert_calls == 0


@pytest.mark.parametrize(
    ("summaries", "expected_rowcount"),
    (
        ([{"written_rows": "1"}, {"written_rows": "2"}], 3),
        ([{"written_rows": "1"}, {}], -1),
        ([{"written_rows": "1"}, {"written_rows": "many"}], -1),
        ([{"written_rows": "1"}, {"written_rows": "-1"}], -1),
    ),
)
@pytest.mark.asyncio
async def test_async_cursor_executemany_rowcount(summaries, expected_rowcount):
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    client = _InsertRoutingClient(summaries)
    adapted = _AsyncAdaptedConnection(async_dbapi, client)  # type: ignore[arg-type]
    cursor = adapted.cursor()

    await greenlet_spawn(
        lambda: cursor.executemany(
            "INSERT INTO events (value) VALUES (%(value)s)",
            [{"value": 13}, {"value": 79}],
        )
    )

    assert cursor.rowcount == expected_rowcount


@pytest.mark.asyncio
async def test_async_cursor_executemany_rowcount_ignores_prior_summaries():
    async_dbapi = ClickHouseAsyncDialect.import_dbapi()
    client = _InsertRoutingClient(
        [
            {"written_rows": "3"},
            {"written_rows": "5"},
            {"written_rows": "1"},
            {"written_rows": "1"},
        ]
    )
    adapted = _AsyncAdaptedConnection(async_dbapi, client)  # type: ignore[arg-type]
    cursor = adapted.cursor()

    await greenlet_spawn(
        lambda: cursor.executemany(
            "INSERT INTO events (value) VALUES (%(value)s)",
            [{"value": 13}, {"value": 79}],
        )
    )
    assert cursor.rowcount == 8

    await greenlet_spawn(
        lambda: cursor.executemany(
            "INSERT INTO events (value) VALUES (%(value)s)",
            [{"value": 101}, {"value": 107}],
        )
    )
    assert cursor.rowcount == 2
