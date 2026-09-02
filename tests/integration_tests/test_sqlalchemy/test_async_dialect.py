import asyncio
import gc
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
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError as DriverDatabaseError
from clickhouse_connect.driver.exceptions import ProgrammingError as DriverProgrammingError
from tests.integration_tests.conftest import TestConfig


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
