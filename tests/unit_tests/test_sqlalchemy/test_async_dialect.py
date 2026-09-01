import asyncio
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("pytest_asyncio")
pytest.importorskip("sqlalchemy", minversion="2.0.44")

from sqlalchemy.dialects import registry
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy.util.concurrency import greenlet_spawn

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.asyncio import ClickHouseAsyncDialect, _AsyncAdaptedConnection
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import create_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.summary import QuerySummary


def test_async_dialect_registry_and_flags():
    assert registry.load("clickhousedb.async") is ClickHouseAsyncDialect
    assert ClickHouseAsyncDialect.__dict__["supports_statement_cache"] is False
    assert "import_dbapi" in ClickHouseAsyncDialect.__dict__
    assert ClickHouseAsyncDialect.is_async is True
    assert ClickHouseAsyncDialect.poolclass is AsyncAdaptedQueuePool
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
    assert raw._autogenerate_session_id_param is False
    assert ClickHouseAsyncDialect().get_driver_connection(adapted) is raw
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
