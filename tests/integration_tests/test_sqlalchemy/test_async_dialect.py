import asyncio
import uuid

import pytest

pytest.importorskip("pytest_asyncio")
pytest.importorskip("sqlalchemy", minversion="2.0.44")

import sqlalchemy as sa
from sqlalchemy.engine import URL
from sqlalchemy.exc import DatabaseError as SQLAlchemyDatabaseError
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base

from clickhouse_connect.cc_sqlalchemy.asyncio import ClickHouseAsyncDialect
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError as DriverDatabaseError
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
