import logging
import random
import textwrap
import time
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column, MetaData, Table, inspect, literal_column, text
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.alembic import ClickHouseImpl
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    Boolean,
    DateTime64,
    Int32,
    Int64,
    String,
    UInt32,
)
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    MergeTree,
    ReplacingMergeTree,
)

logging.getLogger("alembic").setLevel(logging.WARNING)

# pylint: disable=protected-access,redefined-outer-name


def _name(prefix: str) -> str:
    return f"{prefix}_{random.randint(100000, 999999)}_{int(time.time() * 1000)}"


@pytest.fixture
def ch_name(test_engine, test_db):
    """Generates unique names and guarantees cleanup of all created objects after the test."""
    created = []

    def _make(prefix):
        name = _name(prefix)
        created.append(name)
        return name

    yield _make

    with test_engine.begin() as conn:
        for name in created:
            try:
                conn.execute(text(f"DROP DICTIONARY IF EXISTS `{test_db}`.`{name}`"))
            except Exception:  # pylint: disable=broad-except
                pass
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS `{test_db}`.`{name}`"))
            except Exception:  # pylint: disable=broad-except
                pass
        _drop_version_table(conn, test_db)


_STANDARD_SCRIPT_MAKO = '''\
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
'''


def _write_alembic_environment(script_dir: Path):
    versions_dir = script_dir / "versions"
    versions_dir.mkdir(parents=True, exist_ok=True)
    (script_dir / "script.py.mako").write_text(_STANDARD_SCRIPT_MAKO, encoding="utf-8")
    (script_dir / "env.py").write_text(
        textwrap.dedent(
            """
            from alembic import context
            from clickhouse_connect.cc_sqlalchemy import alembic as ch_alembic

            config = context.config
            target_metadata = config.attributes["target_metadata"]
            connection = config.attributes["connection"]
            default_schema = connection.exec_driver_sql("SELECT currentDatabase()").scalar()
            base_include_name = ch_alembic.make_include_name(
                include_schemas=frozenset({default_schema}),
                default_schema=default_schema,
            )
            include_table_names = frozenset(config.attributes.get("include_table_names", ()))


            def include_name(name, type_, parent_names):
                if not base_include_name(name, type_, parent_names):
                    return False
                if type_ == "table" and include_table_names:
                    return name in include_table_names or name == "alembic_version"
                return True


            def run_migrations_online():
                context.configure(
                    connection=connection,
                    target_metadata=target_metadata,
                    include_schemas=True,
                    include_name=include_name,
                    compare_server_default=True,
                    include_object=ch_alembic.include_object,
                    process_revision_directives=ch_alembic.clickhouse_writer,
                    version_table="alembic_version",
                )
                with context.begin_transaction():
                    context.run_migrations()


            run_migrations_online()
            """
        ),
        encoding="utf-8",
    )


def _alembic_config(tmp_path: Path, connection, metadata: MetaData, include_table_names: frozenset[str] = frozenset()) -> Config:
    script_dir = tmp_path / "alembic"
    _write_alembic_environment(script_dir)
    config = Config()
    config.set_main_option("script_location", str(script_dir))
    config.set_main_option("sqlalchemy.url", str(connection.engine.url))
    config.attributes["connection"] = connection
    config.attributes["target_metadata"] = metadata
    config.attributes["include_table_names"] = include_table_names
    return config


def _drop_version_table(conn, database: str):
    conn.execute(text(f"DROP TABLE IF EXISTS `{database}`.`alembic_version`"))


def test_alembic_version_table_live(test_engine: Engine, test_db: str, ch_name):
    version_table = ch_name("alembic_version")

    with test_engine.begin() as conn:
        context = MigrationContext.configure(
            connection=conn,
            opts={"version_table": version_table},
        )
        assert isinstance(context.impl, ClickHouseImpl)

        context._ensure_version_table()

        engine_full = conn.execute(
            text("SELECT engine_full FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": version_table},
        ).scalar()
        assert engine_full == "MergeTree ORDER BY version_num SETTINGS index_granularity = 8192"

        version = context._version
        context.impl._exec(version.insert().values(version_num=literal_column("'base'")))
        context.impl._exec(
            version.update().values(version_num=literal_column("'head'")).where(version.c.version_num == literal_column("'base'"))
        )

        rows = conn.execute(text(f"SELECT version_num FROM `{test_db}`.`{version_table}` ORDER BY version_num")).fetchall()
        assert rows == [("head",)]


def test_alembic_column_operations_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alembic_probe")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` " "(`id` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(
            connection=conn,
            opts={"version_table": ch_name("alembic_version")},
        )

        context.impl.add_column(
            table_name,
            Column(
                "payload",
                String(),
                server_default=text("'{}'"),
                clickhouse_after="id",
            ),
            schema=test_db,
            if_not_exists=True,
            clickhouse_settings={"alter_sync": 2},
        )

        columns = inspect(conn).get_columns(table_name, schema=test_db)
        assert [column["name"] for column in columns] == ["id", "payload"]
        payload = next(column for column in columns if column["name"] == "payload")
        assert str(payload["server_default"]) == "'{}'"

        context.impl.alter_column(
            table_name,
            "payload",
            schema=test_db,
            existing_type=String(),
            server_default=text("'[]'"),
            if_exists=True,
            clickhouse_settings={"alter_sync": 2},
        )

        columns = inspect(conn).get_columns(table_name, schema=test_db)
        payload = next(column for column in columns if column["name"] == "payload")
        assert str(payload["server_default"]) == "'[]'"

        context.impl.alter_column(
            table_name,
            "payload",
            schema=test_db,
            existing_type=String(),
            name="payload_json",
            server_default=text("'[1]'"),
            if_exists=True,
            clickhouse_settings={"alter_sync": 2},
        )

        columns = inspect(conn).get_columns(table_name, schema=test_db)
        assert [column["name"] for column in columns] == ["id", "payload_json"]
        payload_json = next(column for column in columns if column["name"] == "payload_json")
        assert str(payload_json["server_default"]) == "'[1]'"

        context.impl.drop_column(
            table_name,
            Column("payload_json", String()),
            schema=test_db,
            if_exists=True,
        )

        columns = inspect(conn).get_columns(table_name, schema=test_db)
        assert [column["name"] for column in columns] == ["id"]


def test_alembic_autogenerate_positional_engine_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alembic_events")
    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("id", Int32, nullable=False),
        MergeTree(order_by="id", settings={"index_granularity": 1024}),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="create events", autogenerate=True)
        assert revision is not None
        assert not isinstance(revision, list)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "clickhouse_engine=MergeTree(order_by='id', settings={'index_granularity': 1024})" in contents
        command.upgrade(config, "head")

    with test_engine.begin() as conn:
        engine_full = conn.execute(
            text("SELECT engine_full FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert engine_full == "MergeTree ORDER BY id SETTINGS index_granularity = 1024"
        rows = conn.execute(text(f"SELECT version_num FROM `{test_db}`.`alembic_version`")).fetchall()
        assert rows


def test_alembic_autogenerate_text_expression_ttl_round_trip_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alembic_resource_access")
    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("id", String, nullable=False),
        Column("timestamp", DateTime64(3, "UTC"), nullable=False),
        Column("resource_type", String, nullable=False),
        Column("workload_identity", String, nullable=False),
        Column("resource", String, nullable=True),
        Column("access_type", String, nullable=True),
        Column("access_allowed", Boolean, nullable=True),
        Column("metadata", String, nullable=True),
        Column("clickhouse_created_at", DateTime64(3, "UTC"), nullable=False, server_default=text("now()")),
        Column("kafka_offset", Int64, nullable=False),
        Column("kafka_partition", Int32, nullable=False),
        Column("kafka_timestamp", DateTime64(3, "UTC"), nullable=False),
        ReplacingMergeTree(  # pyright: ignore[reportArgumentType]
            version="clickhouse_created_at",
            partition_by=(text("toStartOfDay(timestamp)"), "resource_type"),
            order_by=(text("toStartOfDay(timestamp)"), "resource_type", "workload_identity", "id"),
            ttl=text("toDateTime(timestamp) + INTERVAL 30 DAY"),
        ),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="create resource access", autogenerate=True)
        assert revision is not None
        assert not isinstance(revision, list)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "ttl=sa.text('toDateTime(timestamp) + INTERVAL 30 DAY')" in contents
        assert "sa.text('toStartOfDay(timestamp)')" in contents
        command.upgrade(config, "head")

        noop_revision = command.revision(config, message="resource access noop", autogenerate=True)
        assert noop_revision is not None
        assert not isinstance(noop_revision, list)
        noop_contents = Path(noop_revision.path).read_text(encoding="utf-8")
        assert "pass" in noop_contents
        assert "alter_column" not in noop_contents

    with test_engine.begin() as conn:
        create_sql = conn.execute(text(f"SHOW CREATE TABLE `{test_db}`.`{table_name}`")).scalar()
        assert create_sql is not None
        assert "TTL toDateTime(timestamp)" in create_sql
        assert "toIntervalDay(30)" in create_sql
        assert "PARTITION BY (toStartOfDay(timestamp), resource_type)" in create_sql
        assert "ORDER BY (toStartOfDay(timestamp), resource_type, workload_identity, id)" in create_sql


def test_alembic_autogenerate_dictionary_round_trip_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    dictionary_name = ch_name("alembic_dictionary")
    metadata = MetaData(schema=test_db)
    Dictionary(
        dictionary_name,
        metadata,
        Column("id", UInt32),
        Column("value", String),
        source="CLICKHOUSE(TABLE 'system.one')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        primary_key="id",
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({dictionary_name}))
        first_revision = command.revision(config, message="create dictionary", autogenerate=True)
        contents = Path(first_revision.path).read_text(encoding="utf-8")
        assert "clickhouse_table_type='dictionary'" in contents
        assert "clickhouse_dictionary_source=" in contents
        assert "clickhouse_dictionary_layout='FLAT'" in contents
        assert "clickhouse_dictionary_lifetime='MIN 0 MAX 10'" in contents
        assert "clickhouse_dictionary_primary_key='id'" in contents

        command.upgrade(config, "head")

        second_revision = command.revision(config, message="noop dictionary", autogenerate=True)
        noop_contents = Path(second_revision.path).read_text(encoding="utf-8")
        assert "pass" in noop_contents
        assert "alter_column" not in noop_contents

    with test_engine.begin() as conn:
        create_sql = conn.execute(text(f"SHOW CREATE DICTIONARY `{test_db}`.`{dictionary_name}`")).scalar()
        assert "PRIMARY KEY id" in create_sql
        assert "`id` UInt32" in create_sql
        assert "SOURCE(CLICKHOUSE(TABLE 'system.one'))" in create_sql
        assert "LIFETIME(MIN 0 MAX 10)" in create_sql
        assert "LAYOUT(FLAT())" in create_sql


def test_alembic_dictionary_downgrade_uses_drop_dictionary_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    dictionary_name = ch_name("alembic_dictionary")
    metadata = MetaData(schema=test_db)
    Dictionary(
        dictionary_name,
        metadata,
        Column("id", UInt32),
        Column("value", String),
        source="CLICKHOUSE(TABLE 'system.one')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        primary_key="id",
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({dictionary_name}))
        revision = command.revision(config, message="create dictionary", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "op.drop_table" in contents
        assert "clickhouse_table_type='dictionary'" in contents
        command.upgrade(config, "head")
        command.downgrade(config, "base")

    with test_engine.begin() as conn:
        still_exists = conn.execute(
            text("SELECT count() FROM system.dictionaries WHERE database = :database AND name = :name"),
            {"database": test_db, "name": dictionary_name},
        ).scalar()
        assert still_exists == 0


def test_alembic_autogenerate_comment_change_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    table_name = ch_name("alembic_comment")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32 COMMENT 'old') ENGINE MergeTree ORDER BY id"))

    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("id", Int32, comment="new"),
        MergeTree(order_by="id"),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="update comment", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert "comment='new'" in contents
        assert "drop_table" not in contents
        command.upgrade(config, "head")

    with test_engine.begin() as conn:
        rows = conn.execute(text(f"DESCRIBE TABLE `{test_db}`.`{table_name}`")).fetchall()
        assert rows[0].comment == "new"


def test_alembic_multi_step_upgrade_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    """Regression: multi-step upgrades must update the version table correctly.

    The version table UPDATE was falling through as a raw SQL UPDATE (which
    ClickHouse rejects) because of a schema mismatch between the _version Table
    object and the auto-detected version_table_schema in context_opts.
    """
    table_name = ch_name("alembic_multistep")
    metadata1 = MetaData(schema=test_db)
    Table(
        table_name,
        metadata1,
        Column("id", Int32, nullable=False),
        MergeTree(order_by="id"),
    )

    # Step 1: create table
    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata1, frozenset({table_name}))
        command.revision(config, message="create", autogenerate=True)
        command.upgrade(config, "head")

    # Step 2: add a column (triggers version UPDATE, not just INSERT)
    metadata2 = MetaData(schema=test_db)
    Table(
        table_name,
        metadata2,
        Column("id", Int32, nullable=False),
        Column("extra", String),
        MergeTree(order_by="id"),
    )
    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata2, frozenset({table_name}))
        command.revision(config, message="add column", autogenerate=True)
        command.upgrade(config, "head")

    with test_engine.begin() as conn:
        cols = inspect(conn).get_columns(table_name, schema=test_db)
        assert [c["name"] for c in cols] == ["id", "extra"]


def test_alembic_reflected_replacing_merge_tree_downgrade_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    """Regression: reflected engine repr must include positional args so that
    autogenerated downgrades for dropped tables can recreate the table.
    """
    table_name = ch_name("alembic_rmt")
    metadata1 = MetaData(schema=test_db)
    Table(
        table_name,
        metadata1,
        Column("id", Int32, nullable=False),
        Column("ts", Int32, nullable=False),
        ReplacingMergeTree(version="ts", order_by="id"),
    )

    # Step 1: create with ReplacingMergeTree
    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata1, frozenset({table_name}))
        command.revision(config, message="create rmt", autogenerate=True)
        command.upgrade(config, "head")

    # Step 2: drop table (remove from metadata)
    metadata2 = MetaData(schema=test_db)
    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata2, frozenset({table_name}))
        rev = command.revision(config, message="drop rmt", autogenerate=True)
        contents = Path(rev.path).read_text(encoding="utf-8")
        assert "ReplacingMergeTree(" in contents
        assert "order_by=" in contents
        command.upgrade(config, "head")

    # Step 3: downgrade to recreate via the reflected engine
    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata2, frozenset({table_name}))
        command.downgrade(config, "-1")

    with test_engine.begin() as conn:
        engine_full = conn.execute(
            text("SELECT engine_full FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert "ReplacingMergeTree" in engine_full
