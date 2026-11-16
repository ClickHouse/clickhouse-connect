import asyncio
import uuid

import pytest

pytest.importorskip("pytest_asyncio")
pytest.importorskip("sqlalchemy", minversion="2.0.44")
pytest.importorskip("alembic", minversion="1.18")

import sqlalchemy as sa
from alembic.autogenerate import produce_migrations
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from clickhouse_connect.cc_sqlalchemy import engines, types
from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseImpl, include_object
from clickhouse_connect.driver.asyncclient import AsyncClient
from tests.integration_tests.conftest import TestConfig


def _async_url(test_config: TestConfig) -> URL:
    query = {}
    if test_config.cloud:
        query["select_sequential_consistency"] = "1"
    if test_config.insert_quorum:
        query["insert_quorum"] = str(test_config.insert_quorum)
    return URL.create(
        "clickhousedb+async",
        username=test_config.username,
        password=test_config.password,
        host=test_config.host,
        port=test_config.port,
        database=test_config.test_database,
        query=query,
    )


@pytest.mark.asyncio(loop_scope="function")
async def test_async_sqlalchemy_alembic_run_sync(test_config: TestConfig) -> None:
    engine = create_async_engine(_async_url(test_config))
    table_name = f"test_async_alembic_{uuid.uuid4().hex}"
    version_table = f"test_async_alembic_version_{uuid.uuid4().hex}"

    try:
        async with engine.begin() as connection:

            def run_migrations(sync_connection):
                context = MigrationContext.configure(
                    connection=sync_connection,
                    opts={"version_table": version_table},
                )
                assert isinstance(context.impl, ClickHouseImpl)
                Operations(context).execute(sa.text(f"CREATE TABLE `{table_name}` (`id` UInt32) ENGINE MergeTree ORDER BY id"))
                context._ensure_version_table()
                context.impl._exec(context._version.insert().values(version_num=sa.literal_column("'slice_7'")))
                return context.get_current_revision()

            assert await connection.run_sync(run_migrations) == "slice_7"
            raw_connection = await connection.get_raw_connection()
            raw_client = raw_connection.driver_connection
            assert isinstance(raw_client, AsyncClient)
            assert "alembic/" in raw_client.headers["User-Agent"]
    finally:
        try:
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{table_name}`")
                await connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{version_table}`")
        finally:
            await asyncio.wait_for(engine.dispose(), 10.0)


@pytest.mark.asyncio(loop_scope="function")
@pytest.mark.parametrize("table_engine", ["merge", "null"])
async def test_async_autogenerate_equivalent_table_options(test_config: TestConfig, table_engine: str) -> None:
    engine = create_async_engine(_async_url(test_config))
    table_name = f"test_async_table_options_{uuid.uuid4().hex}"
    try:
        async with engine.begin() as connection:
            engine_sql = (
                "MergeTree ORDER BY a TTL ts + INTERVAL 30 DAY DELETE WHERE a = 13 SETTINGS ttl_only_drop_parts = true"
                if table_engine == "merge"
                else "Null"
            )
            await connection.exec_driver_sql(f"CREATE TABLE `{table_name}` (a UInt32, ts DateTime, val UInt32) ENGINE {engine_sql}")

            def compare(sync_connection):
                metadata = sa.MetaData()
                declared_engine = (
                    engines.MergeTree(order_by="a", ttl="ts + INTERVAL 30 DAY DELETE WHERE a = 13", settings={"ttl_only_drop_parts": True})
                    if table_engine == "merge"
                    else engines.Null()
                )
                sa.Table(
                    table_name,
                    metadata,
                    sa.Column("a", types.UInt32),
                    sa.Column("ts", types.DateTime),
                    sa.Column("val", types.UInt32),
                    declared_engine,
                )
                context = MigrationContext.configure(
                    connection=sync_connection,
                    opts={
                        "include_name": lambda name, kind, parents: kind != "table" or name == table_name,
                        "include_object": include_object,
                    },
                )
                assert produce_migrations(context, metadata).upgrade_ops.is_empty()
                context.impl.alter_column(table_name, "val", clickhousedb_codec="LZ4")
                context.impl.alter_column(table_name, "val", clickhousedb_codec=None, clickhouse_codec="ZSTD(3)")
                reflected = sa.inspect(sync_connection).get_columns(table_name)
                assert next(column for column in reflected if column["name"] == "val").get("clickhouse_codec") is None

            await connection.run_sync(compare)
    finally:
        try:
            async with engine.begin() as connection:
                await connection.exec_driver_sql(f"DROP TABLE IF EXISTS `{table_name}`")
        finally:
            await asyncio.wait_for(engine.dispose(), 10.0)
