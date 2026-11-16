from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import Column, MetaData, Table, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NoSuchTableError

from clickhouse_connect.cc_sqlalchemy.alembic import UnsupportedTableChangeError
from clickhouse_connect.cc_sqlalchemy.alembic.compare import _expressions_match
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree, Null, ReplacingMergeTree

# ruff: noqa: F811
from tests.integration_tests.test_sqlalchemy.test_alembic import _alembic_config, ch_name  # noqa: F401


def _engine_full(conn, test_db: str, table_name: str) -> str:
    return conn.execute(
        text("SELECT engine_full FROM system.tables WHERE database = :database AND name = :name"),
        {"database": test_db, "name": table_name},
    ).scalar()


def _create_events(conn, test_db: str, table_name: str, settings: str = "") -> None:
    conn.execute(
        text(
            f"CREATE TABLE `{test_db}`.`{table_name}` (`a` UInt32, `ts` DateTime) ENGINE MergeTree "
            f"PARTITION BY toYYYYMM(ts) PRIMARY KEY a ORDER BY (a, ts) TTL ts + INTERVAL 30 DAY{settings}"
        )
    )


def test_views_are_not_proposed_as_dropped_tables_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    source = ch_name("alembic_view_source")
    sink = ch_name("alembic_view_sink")
    view = ch_name("alembic_plain_view")
    materialized = ch_name("alembic_materialized")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{source}` (`a` UInt32) ENGINE MergeTree ORDER BY a"))
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{sink}` (`a` UInt32) ENGINE MergeTree ORDER BY a"))
        conn.execute(text(f"CREATE VIEW `{test_db}`.`{view}` AS SELECT a FROM `{test_db}`.`{source}` WHERE a > 1"))
        conn.execute(
            text(f"CREATE MATERIALIZED VIEW `{test_db}`.`{materialized}` TO `{test_db}`.`{sink}` AS SELECT a FROM `{test_db}`.`{source}`")
        )

    metadata = MetaData(schema=test_db)
    Table(source, metadata, Column("a", UInt32), MergeTree(order_by="a"))

    with test_engine.connect() as conn:
        inspector = inspect(conn)
        assert inspector.get_view_definition(view, schema=test_db) == f"SELECT a FROM {test_db}.{source} WHERE a > 1"
        assert inspector.get_view_definition(materialized, schema=test_db) == f"SELECT a FROM {test_db}.{source}"
        with pytest.raises(NoSuchTableError):
            inspector.get_view_definition(source, schema=test_db)
        reflected = Table(view, MetaData(schema=test_db), autoload_with=conn)
        assert reflected.kwargs["clickhousedb_table_type"] == "view"
        assert reflected.engine is None

        config = _alembic_config(tmp_path, conn, metadata, frozenset({source, sink, view, materialized}))
        revision = command.revision(config, message="drop sink only", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert f"op.drop_table('{sink}'" in contents
        assert view not in contents
        assert materialized not in contents


@pytest.mark.parametrize(
    ("engine", "option"),
    [
        (MergeTree(order_by="a", partition_by="toYYYYMM(ts)", ttl="ts + INTERVAL 30 DAY"), "order_by"),
        (MergeTree(order_by=("a", "ts"), primary_key="a", ttl="ts + INTERVAL 30 DAY"), "partition_by"),
        (MergeTree(order_by=("a", "ts"), primary_key="a", partition_by="toYYYYMM(ts)", ttl="ts + INTERVAL 7 DAY"), "ttl"),
        (ReplacingMergeTree(order_by=("a", "ts"), primary_key="a", partition_by="toYYYYMM(ts)", ttl="ts + INTERVAL 30 DAY"), "engine"),
    ],
    ids=["order-by", "partition-by", "ttl", "engine"],
)
def test_autogenerate_refuses_table_option_drift_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name, engine, option):
    table_name = ch_name("alembic_drift")
    with test_engine.begin() as conn:
        _create_events(conn, test_db, table_name)

    metadata = MetaData(schema=test_db)
    Table(table_name, metadata, Column("a", UInt32), Column("ts", DateTime), engine)

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        with pytest.raises(UnsupportedTableChangeError) as excinfo:
            command.revision(config, message="drift", autogenerate=True)
    (drift,) = excinfo.value.drifts
    assert (drift.table_name, drift.option) == (table_name, option)
    # Alembic reports the connection's current database as schema None.
    assert drift.schema in (None, test_db)
    assert not list((tmp_path / "alembic" / "versions").glob("*.py"))


def test_autogenerate_accepts_equivalent_table_options_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alembic_same")
    with test_engine.begin() as conn:
        _create_events(conn, test_db, table_name, " SETTINGS ttl_only_drop_parts = 1")

    metadata = MetaData(schema=test_db)
    a = Column("a", UInt32)
    ts = Column("ts", DateTime)
    Table(
        table_name,
        metadata,
        a,
        ts,
        MergeTree(
            order_by=(a, ts),
            primary_key=a,
            partition_by="toYYYYMM( `ts` )",
            ttl="ts + INTERVAL 30 DAY",
            settings={"ttl_only_drop_parts": True},
        ),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="no change", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
    assert "op." not in contents


def test_autogenerate_settings_change_round_trip_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alembic_settings")
    with test_engine.begin() as conn:
        _create_events(conn, test_db, table_name, " SETTINGS merge_with_ttl_timeout = 3600")

    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("a", UInt32),
        Column("ts", DateTime),
        MergeTree(
            order_by=("a", "ts"),
            primary_key="a",
            partition_by="toYYYYMM(ts)",
            ttl="ts + INTERVAL 30 DAY",
            settings={"merge_with_ttl_timeout": 7200, "ttl_only_drop_parts": 1},
        ),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="settings", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        # The connection's current database renders as the default schema, so no schema argument appears.
        assert (
            f"op.modify_clickhouse_table_settings('{table_name}', {{'merge_with_ttl_timeout': 7200, 'ttl_only_drop_parts': 1}})" in contents
        )
        assert f"op.modify_clickhouse_table_settings('{table_name}', {{'merge_with_ttl_timeout': 3600}})" in contents
        assert f"op.reset_clickhouse_table_settings('{table_name}', ['ttl_only_drop_parts'])" in contents
        assert "existing_settings" not in contents

        command.upgrade(config, "head")
        engine_full = _engine_full(conn, test_db, table_name)
        assert "merge_with_ttl_timeout = 7200" in engine_full
        assert "ttl_only_drop_parts = 1" in engine_full

        second = command.revision(config, message="unchanged", autogenerate=True)
        assert "op." not in Path(second.path).read_text(encoding="utf-8")

        command.downgrade(config, "base")
        engine_full = _engine_full(conn, test_db, table_name)
        assert "merge_with_ttl_timeout = 3600" in engine_full
        assert "ttl_only_drop_parts" not in engine_full


@pytest.mark.parametrize(
    "action",
    [
        "",
        "DELETE",
        "DELETE WHERE a = 13",
        "TO VOLUME 'cold'",
        "TO DISK 'cold'",
        "GROUP BY a SET ts = max(ts)",
        "RECOMPRESS CODEC(ZSTD(3))",
        "DELETE WHERE a = 13, ts + INTERVAL 60 DAY RECOMPRESS CODEC(ZSTD(3))",
    ],
)
def test_ttl_action_normalization_live(test_engine: Engine, action):
    declared = f"ts + INTERVAL 30 DAY {action}".strip()
    reflected = f"ts + toIntervalDay(30) {action}".replace(" DELETE", "").strip()
    with test_engine.connect() as conn:
        assert _expressions_match(conn, declared, reflected, ttl=True)
        assert not _expressions_match(conn, declared.replace("30", "31"), reflected, ttl=True)
        assert not _expressions_match(conn, declared, "ts + toIntervalDay(30) TO VOLUME 'other'", ttl=True)


@pytest.mark.parametrize(
    ("server_settings", "model_settings"),
    [
        ("ttl_only_drop_parts = true", {"ttl_only_drop_parts": True}),
        ("ttl_only_drop_parts = false", {"ttl_only_drop_parts": "false"}),
        ("merge_with_ttl_timeout = '3600'", {"merge_with_ttl_timeout": 3600.0}),
        ("index_granularity = '4096'", {"index_granularity": 4096}),
    ],
)
def test_autogenerate_typed_settings_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name, server_settings, model_settings):
    table_name = ch_name("typed_settings")
    with test_engine.begin() as conn:
        _create_events(conn, test_db, table_name, f" SETTINGS {server_settings}")
        metadata = MetaData(schema=test_db)
        Table(
            table_name,
            metadata,
            Column("a", UInt32),
            Column("ts", DateTime),
            MergeTree(
                order_by=("a", "ts"),
                primary_key="a",
                partition_by="toYYYYMM(ts)",
                ttl="ts + INTERVAL 30 DAY DELETE",
                settings=model_settings,
            ),
        )
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="equivalent settings", autogenerate=True)
        assert "op." not in Path(revision.path).read_text(encoding="utf-8")


def test_null_engine_reflection_and_downgrade_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("null_engine")
    metadata = MetaData(schema=test_db)
    Table(table_name, metadata, Column("a", UInt32), Null())
    with test_engine.begin() as conn:
        metadata.create_all(conn)
        reflected = Table(table_name, MetaData(schema=test_db), autoload_with=conn)
        assert isinstance(reflected.engine, Null)
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="same null engine", autogenerate=True)
        assert "op." not in Path(revision.path).read_text(encoding="utf-8")
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = MetaData(schema=test_db)
        revision = command.revision(config, message="drop null engine", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "Null()" in contents
        command.upgrade(config, "head")
        assert not inspect(conn).has_table(table_name, schema=test_db)
        command.downgrade(config, "-1")
        assert isinstance(Table(table_name, MetaData(schema=test_db), autoload_with=conn).engine, Null)
