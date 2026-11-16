# ruff: noqa: F811
import logging
from pathlib import Path

import pytest
from alembic import command
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, inspect, text
from sqlalchemy.engine import Engine

import clickhouse_connect.cc_sqlalchemy.alembic  # noqa: F401  (registers ClickHouseImpl)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Int32, UInt32, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from tests.integration_tests.test_sqlalchemy.test_alembic import _alembic_config, ch_name  # noqa: F401

logging.getLogger("alembic").setLevel(logging.WARNING)


def _column(conn, table_name: str, schema: str, column_name: str) -> dict:
    columns = inspect(conn).get_columns(table_name, schema=schema)
    return next(column for column in columns if column["name"] == column_name)


def test_alter_column_removes_default_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alter_rm_default")
    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32, `val` UInt32 DEFAULT 13) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        assert str(_column(conn, table_name, test_db, "val")["server_default"]) == "13"

        context.impl.alter_column(
            table_name, "val", schema=test_db, server_default=None, existing_type=UInt32(), clickhouse_settings={"alter_sync": 2}
        )

        assert _column(conn, table_name, test_db, "val").get("server_default") is None


def test_alter_column_type_change_preserves_default_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alter_type_keep_default")
    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32, `val` UInt32 DEFAULT 13) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        context.impl.alter_column(
            table_name, "val", schema=test_db, type_=UInt64(), existing_type=UInt32(), clickhouse_settings={"alter_sync": 2}
        )

        reflected = _column(conn, table_name, test_db, "val")
        assert reflected["type"].name == "UInt64"
        assert str(reflected["server_default"]) == "13"


def test_alter_column_type_change_with_removal_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alter_type_drop_default")
    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32, `val` UInt32 DEFAULT 13) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        context.impl.alter_column(
            table_name,
            "val",
            schema=test_db,
            type_=UInt64(),
            server_default=None,
            existing_type=UInt32(),
            clickhouse_settings={"alter_sync": 2},
        )

        reflected = _column(conn, table_name, test_db, "val")
        assert reflected["type"].name == "UInt64"
        assert reflected.get("server_default") is None


@pytest.mark.parametrize(
    "column_sql, column_name, remove_kw, reflect_key",
    [
        ("`m` UInt32 MATERIALIZED id * 3", "m", {"clickhouse_materialized": None}, "clickhouse_materialized"),
        ("`a` UInt32 ALIAS id + 1", "a", {"clickhousedb_alias": None}, "clickhouse_alias"),
        ("`c` UInt32 CODEC(LZ4)", "c", {"clickhouse_codec": None}, "clickhouse_codec"),
        ("`tt` UInt32 DEFAULT 0 TTL ts + INTERVAL 1 DAY", "tt", {"clickhousedb_ttl": None}, "clickhouse_ttl"),
    ],
    ids=["materialized", "alias", "codec", "ttl"],
)
def test_alter_column_removes_property_live(test_engine: Engine, test_db: str, ch_name, column_sql, column_name, remove_kw, reflect_key):
    table_name = ch_name("alter_rm_property")
    with test_engine.begin() as conn:
        conn.execute(
            text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` UInt32, `ts` DateTime, {column_sql}) ENGINE MergeTree ORDER BY id")
        )

        assert _column(conn, table_name, test_db, column_name).get(reflect_key) is not None

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        context.impl.alter_column(
            table_name, column_name, schema=test_db, existing_type=UInt32(), clickhouse_settings={"alter_sync": 2}, **remove_kw
        )

        assert _column(conn, table_name, test_db, column_name).get(reflect_key) is None


@pytest.mark.parametrize(
    ("prop", "value", "expected"),
    [
        ("codec", "LZ4", "LZ4"),
        ("alias", text("id + 1"), "id + 1"),
        ("materialized", text("id * 3"), "id * 3"),
        ("ttl", text("ts + INTERVAL 1 DAY"), "ts + toIntervalDay(1)"),
    ],
)
def test_alter_column_sets_property_without_type_live(test_engine: Engine, test_db: str, ch_name, prop, value, expected):
    table_name = ch_name("alter_set_property")
    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (id UInt32, ts DateTime, val UInt32) ENGINE MergeTree ORDER BY id"))
        context = MigrationContext.configure(connection=conn)
        context.impl.alter_column(table_name, "val", schema=test_db, **{f"clickhousedb_{prop}": value})
        assert str(_column(conn, table_name, test_db, "val")[f"clickhouse_{prop}"]) == expected


def test_alter_column_autogenerate_removes_default_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alter_autogen_default")
    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32, `val` UInt32 DEFAULT 13) ENGINE MergeTree ORDER BY id"))

    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("id", Int32, nullable=False),
        Column("val", UInt32, nullable=False),
        MergeTree(order_by="id"),
    )

    with test_engine.connect().execution_options(settings={"alter_sync": 2}) as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="drop default", autogenerate=True)
        assert revision is not None
        assert not isinstance(revision, list)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "alter_column" in contents
        assert "server_default=None" in contents
        command.upgrade(config, "head")

        assert _column(conn, table_name, test_db, "val").get("server_default") is None

        noop_revision = command.revision(config, message="default noop", autogenerate=True)
        assert noop_revision is not None
        assert not isinstance(noop_revision, list)
        noop_contents = Path(noop_revision.path).read_text(encoding="utf-8")
        assert "alter_column" not in noop_contents
