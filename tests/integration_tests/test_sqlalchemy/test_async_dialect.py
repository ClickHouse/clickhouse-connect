import asyncio
import gc
import time
import uuid
import weakref

import pytest

pytest.importorskip("pytest_asyncio")
pytest.importorskip("sqlalchemy", minversion="2.0.44")

import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.exc import DatabaseError as SQLAlchemyDatabaseError
from sqlalchemy.exc import DBAPIError as SQLAlchemyDBAPIError
from sqlalchemy.exc import InvalidRequestError, SAWarning
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base

from clickhouse_connect.cc_sqlalchemy.asyncio import ClickHouseAsyncDialect
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.datatypes.format import clear_all_formats, set_default_formats
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError as DriverDatabaseError
from clickhouse_connect.driver.exceptions import ProgrammingError as DriverProgrammingError
from tests.integration_tests.conftest import TestConfig


def _async_url(test_config: TestConfig, query: dict[str, str] | None = None) -> URL:
    default_query = {}
    if test_config.cloud:
        default_query["select_sequential_consistency"] = "1"
    if test_config.insert_quorum:
        default_query["insert_quorum"] = str(test_config.insert_quorum)
    return URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
        query={**default_query, **(query or {})},
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_query_behavior_parity(test_config: TestConfig) -> None:
    engine = create_async_engine(_async_url(test_config))

    try:
        async with engine.connect() as connection:
            connection = await connection.execution_options(
                settings={"max_threads": 2},
                query_formats={"UUID": "string"},
            )
            result = await connection.exec_driver_sql(
                "SELECT number AS value, concat('user_', toString(number)) AS label FROM numbers(3) ORDER BY number"
            )
            cursor = result.cursor

            assert list(result.keys()) == ["value", "label"]
            assert result.rowcount == 3
            assert result.returns_rows is True
            assert cursor.summary[-1]["read_rows"] == "3"
            assert result.fetchone() == (0, "user_0")
            assert result.fetchmany(1) == [(1, "user_1")]
            assert result.all() == [(2, "user_2")]

            mapping_result = await connection.exec_driver_sql("SELECT 13 AS value, 'user_1' AS label")
            assert mapping_result.mappings().one() == {"value": 13, "label": "user_1"}
            scalar_result = await connection.exec_driver_sql("SELECT number FROM numbers(4) ORDER BY number")
            assert [list(partition) for partition in scalar_result.scalars().partitions(3)] == [[0, 1, 2], [3]]

            empty_result = await connection.exec_driver_sql(
                "/* outer /* nested */ done */\n#! clickhouse\n# hash\n// slash\n-- dash\nWITH 13 AS value_1 SELECT value_1 WHERE 0"
            )
            assert empty_result.all() == []
            assert list(empty_result.keys()) == ["value_1"]
            assert empty_result.returns_rows is True

            setting_result = await connection.execute(
                sa.text("SELECT getSetting('max_threads')").execution_options(settings={"max_threads": 3})
            )
            assert setting_result.scalar_one() == 3
            assert (await connection.exec_driver_sql("SELECT getSetting('max_threads')")).scalar_one() == 2

            format_result = await connection.execute(
                sa.text("SELECT toUUID('00000000-0000-0000-0000-00000000000d') AS value").execution_options(
                    query_formats={"UUID": "native"}
                )
            )
            assert format_result.scalar_one() == uuid.UUID("00000000-0000-0000-0000-00000000000d")
            connection_format_result = await connection.exec_driver_sql("SELECT toUUID('00000000-0000-0000-0000-00000000000d') AS value")
            assert connection_format_result.scalar_one() == "00000000-0000-0000-0000-00000000000d"

            percent_result = await connection.execute(sa.text("SELECT :value, 'single% adjacent%% %(token)s tail%'").bindparams(value=79))
            assert percent_result.one() == (79, "single% adjacent%% %(token)s tail%")
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_ddl_reflection_and_percent_identifiers(test_config: TestConfig) -> None:
    engine = create_async_engine(_async_url(test_config))
    table_name = f"test_async_parity%_{uuid.uuid4().hex}"
    value_name = "value%pct"
    metadata = sa.MetaData()
    value_column = sa.Column(value_name, UInt32)
    table = sa.Table(
        table_name,
        metadata,
        value_column,
        sa.Column("label", String),
        MergeTree(order_by=value_column),
    )

    try:
        async with engine.connect() as connection:
            await connection.run_sync(metadata.create_all)
            insert_result = await connection.execute(
                table.insert(),
                [
                    {value_name: 13, "label": "user_1"},
                    {value_name: 79, "label": "user_2"},
                ],
            )
            rows = (await connection.execute(sa.select(table).order_by(table.c[value_name]))).all()

            def reflect(sync_connection):
                inspector = sa.inspect(sync_connection)
                reflected = sa.Table(table_name, sa.MetaData(), autoload_with=sync_connection)
                return (
                    inspector.has_table(table_name),
                    [column["name"] for column in inspector.get_columns(table_name)],
                    [column.name for column in reflected.columns],
                    reflected.engine.name,
                )

            set_default_formats("String", "bytes")
            try:
                reflected = await connection.run_sync(reflect)
            finally:
                clear_all_formats()

        assert insert_result.rowcount == 2
        assert rows == [(13, "user_1"), (79, "user_2")]
        assert reflected == (True, [value_name, "label"], [value_name, "label"], "MergeTree")
    finally:
        try:
            async with engine.connect() as connection:
                await connection.run_sync(metadata.drop_all)
        finally:
            await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_server_binds_and_native_client_surface(test_config: TestConfig) -> None:
    engine = create_async_engine(
        _async_url(test_config, {"query_limit": "2333", "compression": "false"}),
        server_side_params=True,
    )

    try:
        async with engine.connect() as connection:
            value = sa.bindparam("value", type_=UInt32())
            label = sa.bindparam("label", type_=String())
            result = await connection.execute(sa.select(value, label), {"value": 113, "label": "value%pct"})
            assert result.one() == (113, "value%pct")

            in_result = await connection.execute(
                sa.select(value).where(value.in_([13, 113])),
                {"value": 113},
            )
            assert in_result.scalar_one() == 113
            tuple_result = await connection.execute(
                sa.select(value, label).where(sa.tuple_(value, label).in_([(13, "other"), (113, "value%pct")])),
                {"value": 113, "label": "value%pct"},
            )
            assert tuple_result.one() == (113, "value%pct")

            raw_connection = await connection.get_raw_connection()
            client = raw_connection.driver_connection
            del raw_connection

            assert isinstance(client, AsyncClient)
            assert client.min_version("20.1")
            assert client.query_limit == 2333
            assert client.compression is None
            assert "sqlalchemy" in client.headers["User-Agent"]
            assert client.get_client_setting("session_id")
            assert (await client.query("SELECT 127 AS value")).result_rows == [(127,)]
            assert await client.command("SELECT 131") == 131
            assert await client.raw_query("SELECT 137 FORMAT TabSeparated") == b"137\n"

            async with await client.query_rows_stream("SELECT number FROM numbers(3) ORDER BY number") as stream:
                assert [row async for row in stream] == [(0,), (1,), (2,)]

            assert (await connection.exec_driver_sql("SELECT 139")).scalar_one() == 139
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_session_stream_is_buffered_but_iterable(test_config: TestConfig) -> None:
    engine = create_async_engine(_async_url(test_config))

    try:
        async with AsyncSession(engine) as session:
            await_started = time.monotonic()
            result = await session.stream(sa.text("SELECT number, sleepEachRow(0.2) FROM numbers(5) SETTINGS max_block_size=1"))
            await_elapsed = time.monotonic() - await_started

            consume_started = time.monotonic()
            rows = [row async for row in result]
            consume_elapsed = time.monotonic() - consume_started

            assert rows == [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0)]
            assert await_elapsed >= 0.7
            assert consume_elapsed < 0.25
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_executemany_preserves_sql_bind_order(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url)
    table_name = f"test_async_executemany_{uuid.uuid4().hex}"

    try:
        async with engine.connect() as connection:
            await connection.exec_driver_sql(f"CREATE TABLE {table_name} (a UInt32, b UInt32) ENGINE MergeTree ORDER BY a")
            insert_result = await connection.exec_driver_sql(
                f"INSERT INTO {table_name} (a, b) VALUES (%(b)s, %(a)s)",
                [
                    {"a": 13, "b": 79},
                    {"a": 17, "b": 101},
                ],
            )
            rows = (await connection.exec_driver_sql(f"SELECT a, b FROM {table_name} ORDER BY a")).all()

        assert insert_result.rowcount == 2
        assert rows == [(79, 13), (101, 17)]
    finally:
        try:
            async with engine.connect() as connection:
                await connection.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")
        finally:
            await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_dialect_acceptance(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
        query={
            "connector_limit": "3",
            "connector_limit_per_host": "2",
            "keepalive_timeout": "7.5",
        },
    )
    engine = create_async_engine(url, pool_size=2, max_overflow=0)
    table_name = f"test_async_dialect_{uuid.uuid4().hex}"
    raw_clients: dict[int, AsyncClient] = {}

    Base = declarative_base()  # noqa: N806

    class Event(Base):
        __tablename__ = table_name

        id = sa.Column(sa.Integer, primary_key=True)
        label = sa.Column(sa.String)

    try:
        assert isinstance(engine.sync_engine.dialect, ClickHouseAsyncDialect)

        async with engine.connect() as connection:
            result = await connection.execute(sa.text("SELECT :value"), {"value": 13})
            assert result.scalar_one() == 13

            raw_connection = await connection.get_raw_connection()
            raw_client = raw_connection.driver_connection
            raw_clients[id(raw_client)] = raw_client
            assert isinstance(raw_client, AsyncClient)
            assert engine.sync_engine.dialect.get_driver_connection(raw_connection.dbapi_connection) is raw_client
            assert raw_client._backend.connector_kwargs["limit"] == 3
            assert raw_client._backend.connector_kwargs["limit_per_host"] == 2
            assert raw_client._backend.connector_kwargs["keepalive_timeout"] == 7.5
            assert raw_client._autogenerate_session_id_param is True

            await connection.execute(sa.text(f"CREATE TABLE {table_name} (id UInt32, label String) ENGINE MergeTree ORDER BY id"))
            await connection.execute(
                sa.text(f"INSERT INTO {table_name} (id, label) VALUES (:id, :label)"),
                {"id": 13, "label": "user_1"},
            )
            await connection.execute(
                sa.text(f"INSERT INTO {table_name} (id, label) VALUES (:id, :label)"),
                [
                    {"id": 79, "label": "user_2"},
                    {"id": 101, "label": "user_3"},
                ],
            )

            with pytest.raises(SQLAlchemyDatabaseError) as exc_info:
                await connection.execute(sa.text("SELECT * FROM async_dialect_missing_table"))
            assert isinstance(exc_info.value.orig, DriverDatabaseError)
            assert exc_info.value.connection_invalidated is False
            assert (await connection.execute(sa.text("SELECT 17"))).scalar_one() == 17

            with pytest.raises(InvalidRequestError, match="server side cursors"):
                await connection.stream(sa.text("SELECT 19"))

        async with AsyncSession(engine) as session:
            session.add(Event(id=107, label="user_4"))
            await session.commit()
            orm_result = await session.execute(sa.select(Event).order_by(Event.id))
            assert [(event.id, event.label) for event in orm_result.scalars()] == [
                (13, "user_1"),
                (79, "user_2"),
                (101, "user_3"),
                (107, "user_4"),
            ]

        ready = asyncio.Event()
        checked_out_clients: set[int] = set()

        async def checked_out(value: int) -> int:
            async with engine.connect() as connection:
                raw_connection = await connection.get_raw_connection()
                raw_client = raw_connection.driver_connection
                checked_out_clients.add(id(raw_client))
                raw_clients[id(raw_client)] = raw_client
                if len(checked_out_clients) == 2:
                    ready.set()
                await asyncio.wait_for(ready.wait(), 5.0)
                return int((await connection.execute(sa.text("SELECT :value"), {"value": value})).scalar_one())

        assert await asyncio.gather(checked_out(109), checked_out(113)) == [109, 113]
        assert len(checked_out_clients) == 2
    finally:
        try:
            async with engine.connect() as connection:
                await connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))
        finally:
            await asyncio.wait_for(engine.dispose(), 10.0)

    assert raw_clients
    session_ids = [client.get_client_setting("session_id") for client in raw_clients.values()]
    assert all(session_ids)
    assert len(set(session_ids)) == len(session_ids)
    assert all(client._session is None or client._session.closed for client in raw_clients.values())


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_connection_preserves_session_state(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url)
    table_name = f"test_async_session_{uuid.uuid4().hex}"

    try:
        async with engine.connect() as connection:
            await connection.exec_driver_sql("SET max_threads = 1")
            setting = await connection.exec_driver_sql("SELECT getSetting('max_threads')")
            assert setting.scalar_one() == 1

            await connection.exec_driver_sql(f"CREATE TEMPORARY TABLE {table_name} (value UInt32)")
            await connection.exec_driver_sql(f"INSERT INTO {table_name} VALUES (13)")
            rows = await connection.exec_driver_sql(f"SELECT value FROM {table_name}")
            assert rows.all() == [(13,)]
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_fixed_session_id_rejects_concurrent_requests(test_config: TestConfig) -> None:
    session_id = f"test_async_fixed_session_{uuid.uuid4().hex}"
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
        query={"session_id": session_id},
    )
    engine = create_async_engine(url, pool_size=2, max_overflow=0)

    async def slow_query() -> int:
        async with engine.connect() as connection:
            result = await connection.exec_driver_sql("SELECT sleep(2), 13")
            return int(result.one()[1])

    first_query = asyncio.create_task(slow_query())
    try:
        await asyncio.sleep(0.1)
        async with engine.connect() as connection:
            with pytest.raises(SQLAlchemyDatabaseError) as exc_info:
                await connection.exec_driver_sql("SELECT 79")
        assert isinstance(exc_info.value.orig, DriverDatabaseError)
        assert exc_info.value.orig.code == 373
        assert await first_query == 13
    finally:
        if not first_query.done():
            first_query.cancel()
        await asyncio.gather(first_query, return_exceptions=True)
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_pool_pre_ping_replaces_closed_native_client(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_pre_ping=True)

    try:
        async with engine.connect() as connection:
            raw_connection = await connection.get_raw_connection()
            stale_client = raw_connection.driver_connection
            stale_session = stale_client._session
            assert stale_session is not None
            stale_connector = stale_session.connector
            assert stale_connector is not None
            del raw_connection
            assert (await connection.exec_driver_sql("SELECT 13")).scalar_one() == 13

        stale_client._force_close()
        assert stale_session.closed
        assert stale_connector.closed

        async with engine.connect() as connection:
            raw_connection = await connection.get_raw_connection()
            replacement_client = raw_connection.driver_connection
            del raw_connection
            assert replacement_client is not stale_client
            assert (await connection.exec_driver_sql("SELECT 79")).scalar_one() == 79
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_force_closed_connection_is_invalidated(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0)
    connection = await engine.connect()

    try:
        raw_connection = await connection.get_raw_connection()
        stale_client = raw_connection.driver_connection
        del raw_connection
        stale_client._force_close()

        with pytest.raises(SQLAlchemyDBAPIError) as exc_info:
            await connection.exec_driver_sql("SELECT 13")
        assert isinstance(exc_info.value.orig, DriverProgrammingError)
        assert exc_info.value.connection_invalidated is True
        await connection.close()

        async with engine.connect() as replacement_connection:
            raw_connection = await replacement_connection.get_raw_connection()
            replacement_client = raw_connection.driver_connection
            del raw_connection
            assert replacement_client is not stale_client
            assert (await replacement_connection.exec_driver_sql("SELECT 79")).scalar_one() == 79
    finally:
        if not connection.closed:
            await connection.close()
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_server_error_keeps_connection_usable(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0)

    try:
        async with engine.connect() as connection:
            raw_connection = await connection.get_raw_connection()
            raw_client = raw_connection.driver_connection
            del raw_connection

            with pytest.raises(SQLAlchemyDatabaseError) as exc_info:
                await connection.exec_driver_sql("SELECT * FROM async_dialect_missing_health_table")
            assert isinstance(exc_info.value.orig, DriverDatabaseError)
            assert exc_info.value.connection_invalidated is False
            assert (await connection.exec_driver_sql("SELECT 101")).scalar_one() == 101

            raw_connection = await connection.get_raw_connection()
            assert raw_connection.driver_connection is raw_client
            del raw_connection
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_invalidation_and_recycle_close_native_clients(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_recycle=3600)

    try:
        connection = await engine.connect()
        raw_connection = await connection.get_raw_connection()
        invalidated_client = raw_connection.driver_connection
        invalidated_session = invalidated_client._session
        assert invalidated_session is not None
        invalidated_connector = invalidated_session.connector
        assert invalidated_connector is not None
        assert not invalidated_session.closed
        assert not invalidated_connector.closed
        del raw_connection

        await connection.invalidate()

        assert invalidated_session.closed
        assert invalidated_connector.closed
        await connection.close()

        async with engine.connect() as recycled_connection:
            raw_connection = await recycled_connection.get_raw_connection()
            recycled_client = raw_connection.driver_connection
            recycled_session = recycled_client._session
            assert recycled_session is not None
            recycled_connector = recycled_session.connector
            assert recycled_connector is not None
            assert not recycled_session.closed
            assert not recycled_connector.closed
            del raw_connection
            assert (await recycled_connection.exec_driver_sql("SELECT 13")).scalar_one() == 13

        engine.sync_engine.pool._recycle = 0
        async with engine.connect() as replacement_connection:
            raw_connection = await replacement_connection.get_raw_connection()
            replacement_client = raw_connection.driver_connection
            del raw_connection
            assert (await replacement_connection.exec_driver_sql("SELECT 79")).scalar_one() == 79

        assert recycled_client is not replacement_client
        assert recycled_session.closed
        assert recycled_connector.closed
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_checked_out_connection_closes_after_pool_dispose(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0)
    connection = await engine.connect()
    raw_connection = await connection.get_raw_connection()
    raw_client = raw_connection.driver_connection
    session = raw_client._session
    assert session is not None
    connector = session.connector
    assert connector is not None
    del raw_connection

    try:
        await engine.dispose()
        assert not session.closed
        assert not connector.closed

        await connection.close()

        assert session.closed
        assert connector.closed
        async with engine.connect() as replacement_connection:
            assert (await replacement_connection.exec_driver_sql("SELECT 127")).scalar_one() == 127
    finally:
        await connection.close()
        await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_gc_force_closes_checked_out_connection(test_config: TestConfig) -> None:
    url = URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
    )
    engine = create_async_engine(url, pool_size=1, max_overflow=0)

    try:
        connection = await engine.connect()
        raw_connection = await connection.get_raw_connection()
        raw_client = raw_connection.driver_connection
        session = raw_client._session
        assert session is not None
        connector = session.connector
        assert connector is not None
        connection_ref = weakref.ref(connection)
        del raw_connection
        await engine.dispose()
        assert not session.closed
        assert not connector.closed

        with pytest.warns(SAWarning, match="garbage collector.*will be terminated"):
            del connection
            gc.collect()

        assert connection_ref() is None
        assert session.closed
        assert connector.closed
    finally:
        await asyncio.wait_for(engine.dispose(), 10.0)
