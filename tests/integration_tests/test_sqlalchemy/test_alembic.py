import logging
import random
import textwrap
import time
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import Column, Index, MetaData, Table, inspect, literal_column, text
from sqlalchemy.engine import Engine
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy.alembic import (
    ClickHouseImpl,
    ClickHouseIndex,
    ClickHouseProjection,
)
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
            except Exception:
                pass
            try:
                conn.execute(text(f"DROP TABLE IF EXISTS `{test_db}`.`{name}`"))
            except Exception:
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
        assert "ORDER BY version_num" in engine_full
        assert "index_granularity = 8192" in engine_full

        version = context._version
        context.impl._exec(version.insert().values(version_num=literal_column("'base'")))
        context.impl._exec(
            version.update().values(version_num=literal_column("'head'")).where(version.c.version_num == literal_column("'base'"))
        )

        rows = conn.execute(text(f"SELECT version_num FROM `{test_db}`.`{version_table}` ORDER BY version_num")).fetchall()
        assert rows == [("head",)]


def test_alembic_user_agent_integration_tag(test_engine: Engine):
    with test_engine.begin() as conn:
        context = MigrationContext.configure(connection=conn)
        assert isinstance(context.impl, ClickHouseImpl)
        ua = conn.connection.driver_connection.client.headers["User-Agent"]
        assert "alembic/" in ua


def test_alembic_column_operations_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alembic_probe")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` String) ENGINE MergeTree ORDER BY id"))

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


def test_alembic_create_table_with_table_comment_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    """Autogen of a Table(..., comment="...") creates it with COMMENT '...', a second autogen against unchanged metadata is a noop, and subsequent updates and drops of the comment apply via ALTER TABLE ... MODIFY COMMENT."""
    table_name = ch_name("alembic_table_comment")
    metadata = MetaData(schema=test_db)
    table = Table(
        table_name,
        metadata,
        Column("id", Int32, nullable=False),
        MergeTree(order_by="id"),
        comment="Application events table",
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="create with table comment", autogenerate=True)
        assert revision is not None
        assert not isinstance(revision, list)
        command.upgrade(config, "head")

        create_sql = conn.execute(
            text("SELECT create_table_query FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert "COMMENT 'Application events table'" in create_sql

        noop_revision = command.revision(config, message="table comment noop", autogenerate=True)
        assert noop_revision is not None
        assert not isinstance(noop_revision, list)
        noop_contents = Path(noop_revision.path).read_text(encoding="utf-8")
        assert "pass" in noop_contents
        assert "create_table_comment" not in noop_contents
        command.upgrade(config, "head")

        table.comment = "Updated application events table"
        update_revision = command.revision(config, message="update table comment", autogenerate=True)
        assert update_revision is not None
        assert not isinstance(update_revision, list)
        update_contents = Path(update_revision.path).read_text(encoding="utf-8")
        assert "create_table_comment" in update_contents
        assert "Updated application events table" in update_contents
        command.upgrade(config, "head")

        updated_comment = conn.execute(
            text("SELECT comment FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert updated_comment == "Updated application events table"

        table.comment = None
        drop_revision = command.revision(config, message="drop table comment", autogenerate=True)
        assert drop_revision is not None
        assert not isinstance(drop_revision, list)
        drop_contents = Path(drop_revision.path).read_text(encoding="utf-8")
        assert "drop_table_comment" in drop_contents
        command.upgrade(config, "head")

        dropped_comment = conn.execute(
            text("SELECT comment FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert dropped_comment == ""


def test_alembic_create_table_with_column_comment_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name):
    """Autogen of a Table whose columns carry comment=... emits an inline COMMENT '...' in CREATE TABLE."""
    table_name = ch_name("alembic_comment_initial")
    metadata = MetaData(schema=test_db)
    Table(
        table_name,
        metadata,
        Column("id", Int32, nullable=False),
        Column("label", String, nullable=False, comment="Display label"),
        MergeTree(order_by="id"),
    )

    with test_engine.connect() as conn:
        config = _alembic_config(tmp_path, conn, metadata, frozenset({table_name}))
        revision = command.revision(config, message="create with comment", autogenerate=True)
        assert revision is not None
        assert not isinstance(revision, list)
        command.upgrade(config, "head")

    with test_engine.begin() as conn:
        create_sql = conn.execute(
            text("SELECT create_table_query FROM system.tables WHERE database = :database AND name = :table_name"),
            {"database": test_db, "table_name": table_name},
        ).scalar()
        assert "COMMENT 'Display label'" in create_sql


def test_alembic_operations_add_column_public_api_live(test_engine: Engine, test_db: str, ch_name):
    """op.add_column via the public Operations facade with clickhouse_settings adds the column on a live connection."""
    table_name = ch_name("alembic_op_probe")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(
            connection=conn,
            opts={"version_table": ch_name("alembic_version")},
        )
        op = Operations(context)

        op.add_column(
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


def test_standard_index_operations_fail_before_partial_ddl_live(test_engine: Engine, test_db: str, ch_name):
    table_name = ch_name("alembic_standard_index")
    create_table_name = ch_name("alembic_standard_index_create")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        with pytest.raises(CommandError, match="op.add_clickhouse_index"):
            op.add_column(table_name, Column("payload", String(), index=True), schema=test_db)

        columns = inspect(conn).get_columns(table_name, schema=test_db)
        assert [column["name"] for column in columns] == ["id"]

        with pytest.raises(CommandError, match="op.add_clickhouse_index"):
            op.create_table(
                create_table_name,
                Column("id", String(), nullable=False),
                Index("idx_payload", "id"),
                MergeTree(order_by="id"),
                schema=test_db,
            )

        exists = conn.execute(
            text("SELECT count() FROM system.tables WHERE database = :db AND name = :name"),
            {"db": test_db, "name": create_table_name},
        ).scalar()
        assert exists == 0

        indexed_table = Table(
            create_table_name,
            MetaData(schema=test_db),
            Column("id", String(), nullable=False),
            Index("idx_payload", "id"),
            MergeTree(order_by="id"),
        )
        with pytest.raises(CommandError, match="op.add_clickhouse_index"):
            context.impl.create_table(indexed_table)

        exists = conn.execute(
            text("SELECT count() FROM system.tables WHERE database = :db AND name = :name"),
            {"db": test_db, "name": create_table_name},
        ).scalar()
        assert exists == 0

        copy_from = Table(table_name, MetaData(), Column("id", String()), schema=test_db)
        with pytest.raises(CommandError, match="op.add_clickhouse_index"):
            with op.batch_alter_table(table_name, schema=test_db, recreate="always", copy_from=copy_from) as batch_op:
                batch_op.create_index("idx_payload", ["id"])

        still_exists = conn.execute(
            text("SELECT count() FROM system.tables WHERE database = :db AND name = :name"),
            {"db": test_db, "name": table_name},
        ).scalar()
        assert still_exists == 1


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
        assert "ORDER BY id" in engine_full
        assert "index_granularity = 1024" in engine_full
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


def test_alembic_autogenerate_dictionary_round_trip_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name, test_config):
    if test_config.cloud:
        pytest.skip("Dictionary SOURCE references cross-database tables not accessible on Cloud")
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


def test_alembic_dictionary_downgrade_uses_drop_dictionary_live(test_engine: Engine, test_db: str, tmp_path: Path, ch_name, test_config):
    if test_config.cloud:
        pytest.skip("Dictionary SOURCE references cross-database tables not accessible on Cloud")
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
    """A second alembic upgrade after the initial migration updates the version table and applies the new revision."""
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
    """Downgrade of a dropped ReplacingMergeTree table recreates it from the reflected engine repr in the generated migration."""
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


def test_alembic_index_lifecycle_live(test_engine: Engine, test_db: str, ch_name):
    """add_clickhouse_indexes emits one ALTER for two indexes, then materialize and drop apply on a live table."""
    table_name = ch_name("alembic_index")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` String, `name` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.add_clickhouse_indexes(
            table_name,
            [
                ClickHouseIndex("idx_name", "name", "bloom_filter(0.01)", granularity=4),
                ClickHouseIndex("idx_id", "id", "minmax", granularity=1),
            ],
            schema=test_db,
        )
        op.add_clickhouse_index(table_name, "idx_after_name", "id", "minmax", after_index="idx_name", schema=test_db)

        names = [
            row[0]
            for row in conn.execute(
                text("SELECT name FROM system.data_skipping_indices WHERE database = :db AND table = :table ORDER BY name"),
                {"db": test_db, "table": table_name},
            ).fetchall()
        ]
        assert names == ["idx_after_name", "idx_id", "idx_name"]

        op.materialize_clickhouse_index(table_name, "idx_name", if_exists=True, schema=test_db, clickhouse_settings={"mutations_sync": 1})

        op.drop_clickhouse_indexes(
            table_name,
            ["idx_name", "idx_after_name"],
            if_exists=True,
            schema=test_db,
            clickhouse_settings={"alter_sync": 2},
        )

        remaining = [
            row[0]
            for row in conn.execute(
                text("SELECT name FROM system.data_skipping_indices WHERE database = :db AND table = :table ORDER BY name"),
                {"db": test_db, "table": table_name},
            ).fetchall()
        ]
        assert remaining == ["idx_id"]


def test_alembic_projection_lifecycle_live(test_engine: Engine, test_db: str, ch_name):
    """add, materialize, and drop of a projection apply on a live table."""
    table_name = ch_name("alembic_projection")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32, `category` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.add_clickhouse_projection(
            table_name,
            "proj_category",
            "SELECT category, count() GROUP BY category",
            schema=test_db,
        )
        op.add_clickhouse_projections(
            table_name,
            [
                ClickHouseProjection("proj_id", "SELECT id ORDER BY id", after_projection="proj_category"),
                ClickHouseProjection("proj_category_id", "SELECT category, id ORDER BY category, id"),
            ],
            schema=test_db,
        )

        create_sql = conn.execute(text(f"SHOW CREATE TABLE `{test_db}`.`{table_name}`")).scalar()
        assert "PROJECTION proj_category" in create_sql
        assert "PROJECTION proj_id" in create_sql
        assert "PROJECTION proj_category_id" in create_sql

        op.materialize_clickhouse_projection(
            table_name,
            "proj_category",
            if_exists=True,
            schema=test_db,
            clickhouse_settings={"mutations_sync": 1},
        )

        op.drop_clickhouse_projections(
            table_name,
            ["proj_category", "proj_id", "proj_category_id"],
            if_exists=True,
            schema=test_db,
        )

        create_sql = conn.execute(text(f"SHOW CREATE TABLE `{test_db}`.`{table_name}`")).scalar()
        assert "proj_category" not in create_sql
        assert "proj_id" not in create_sql
        assert "proj_category_id" not in create_sql


def test_alembic_modify_and_reset_table_settings_live(test_engine: Engine, test_db: str, ch_name):
    """modify_clickhouse_table_settings then reset_clickhouse_table_settings apply and revert a MergeTree setting."""
    table_name = ch_name("alembic_settings")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{table_name}` (`id` Int32) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        # clickhouse_settings appends a trailing query SETTINGS clause after the MODIFY/RESET SETTING list.
        op.modify_clickhouse_table_settings(
            table_name, {"merge_with_ttl_timeout": 3600}, schema=test_db, clickhouse_settings={"alter_sync": 2}
        )
        create_sql = conn.execute(text(f"SHOW CREATE TABLE `{test_db}`.`{table_name}`")).scalar()
        assert "merge_with_ttl_timeout = 3600" in create_sql

        op.reset_clickhouse_table_settings(table_name, ["merge_with_ttl_timeout"], schema=test_db, clickhouse_settings={"alter_sync": 2})
        create_sql = conn.execute(text(f"SHOW CREATE TABLE `{test_db}`.`{table_name}`")).scalar()
        assert "merge_with_ttl_timeout = 3600" not in create_sql


def test_alembic_rename_table_round_trip_live(test_engine: Engine, test_db: str, ch_name):
    """rename_table emits RENAME TABLE so the object moves to the new name and the old name is gone."""
    old_name = ch_name("alembic_rename_old")
    new_name = ch_name("alembic_rename_new")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{old_name}` (`id` Int32) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.rename_table(old_name, new_name, schema=test_db)

        present = {
            row[0]
            for row in conn.execute(
                text("SELECT name FROM system.tables WHERE database = :db AND name IN (:old, :new)"),
                {"db": test_db, "old": old_name, "new": new_name},
            ).fetchall()
        }
        assert present == {new_name}


def test_alembic_materialized_view_helpers_live(test_engine: Engine, test_db: str, ch_name):
    """create/drop materialized view helpers apply a TO-table view on a live server."""
    source_name = ch_name("alembic_mv_source")
    sink_name = ch_name("alembic_mv_sink")
    view_name = ch_name("alembic_mv")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{source_name}` (`id` UInt32, `value` String) ENGINE MergeTree ORDER BY id"))
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{sink_name}` (`id` UInt32, `value` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.create_clickhouse_materialized_view(
            view_name,
            sink_name,
            f"SELECT id, value FROM `{test_db}`.`{source_name}`",
            if_not_exists=True,
            schema=test_db,
            to_schema=test_db,
        )

        conn.execute(text(f"INSERT INTO `{test_db}`.`{source_name}` VALUES (13, 'user_1')"))
        rows = conn.execute(text(f"SELECT id, value FROM `{test_db}`.`{sink_name}` ORDER BY id")).fetchall()
        assert rows == [(13, "user_1")]

        op.drop_clickhouse_materialized_view(view_name, if_exists=True, schema=test_db)
        still_exists = conn.execute(
            text("SELECT count() FROM system.tables WHERE database = :db AND name = :name"),
            {"db": test_db, "name": view_name},
        ).scalar()
        assert still_exists == 0


def test_alembic_materialized_view_raw_sql_colon_literal_live(test_engine: Engine, test_db: str, ch_name):
    """Raw SELECT fragments with colon literals must reach ClickHouse without SQLAlchemy bind parsing."""
    source_name = ch_name("alembic_mv_colon_source")
    sink_name = ch_name("alembic_mv_colon_sink")
    view_name = ch_name("alembic_mv_colon")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{source_name}` (`id` UInt32) ENGINE MergeTree ORDER BY id"))
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{sink_name}` (`id` UInt32, `value` String) ENGINE MergeTree ORDER BY id"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.create_clickhouse_materialized_view(
            view_name,
            sink_name,
            f"SELECT id, 'path\\\\:tenant' AS value FROM `{test_db}`.`{source_name}`",
            schema=test_db,
            to_schema=test_db,
        )

        conn.execute(text(f"INSERT INTO `{test_db}`.`{source_name}` VALUES (13)"))
        rows = conn.execute(text(f"SELECT id, value FROM `{test_db}`.`{sink_name}` ORDER BY id")).fetchall()
        assert rows == [(13, "path\\:tenant")]


def test_alembic_dictionary_helpers_live(test_engine: Engine, test_db: str, ch_name, test_config):
    """create/drop dictionary helpers apply on a live server."""
    if test_config.cloud:
        pytest.skip("Dictionary source references a local table not accessible on Cloud")
    source_name = ch_name("alembic_dict_helper_source")
    dictionary_name = ch_name("alembic_dict_helper")

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{source_name}` (`id` UInt32, `value` String) ENGINE MergeTree ORDER BY id"))
        conn.execute(text(f"INSERT INTO `{test_db}`.`{source_name}` VALUES (13, 'user_1')"))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.create_clickhouse_dictionary(
            dictionary_name,
            [Column("id", UInt32), Column("value", String)],
            primary_key="id",
            source=f"CLICKHOUSE(TABLE '{source_name}')",
            layout="FLAT",
            lifetime="MIN 0 MAX 10",
            if_not_exists=True,
            schema=test_db,
            clickhouse_settings={"log_queries": 0},
        )

        op.reload_clickhouse_dictionary(dictionary_name, schema=test_db)
        status, element_count = conn.execute(
            text("SELECT status, element_count FROM system.dictionaries WHERE database = :db AND name = :name"),
            {"db": test_db, "name": dictionary_name},
        ).fetchone()
        assert status == "LOADED"
        assert element_count == 1

        op.drop_clickhouse_dictionary(dictionary_name, if_exists=True, schema=test_db)
        still_exists = conn.execute(
            text("SELECT count() FROM system.dictionaries WHERE database = :db AND name = :name"),
            {"db": test_db, "name": dictionary_name},
        ).scalar()
        assert still_exists == 0


def test_alembic_reload_dictionary_live(test_engine: Engine, test_db: str, ch_name, test_config):
    """reload_clickhouse_dictionary issues SYSTEM RELOAD DICTIONARY so a Dictionary construct loads its source data."""
    if test_config.cloud:
        pytest.skip("Dictionary reload sources a local table not accessible on Cloud")
    source_name = ch_name("alembic_dict_source")
    dictionary_name = ch_name("alembic_reload_dict")
    metadata = MetaData(schema=test_db)
    dictionary = Dictionary(
        dictionary_name,
        metadata,
        Column("id", UInt32),
        Column("value", String),
        source=f"CLICKHOUSE(TABLE '{source_name}')",
        layout="FLAT",
        lifetime="MIN 0 MAX 10",
        primary_key="id",
    )

    with test_engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE `{test_db}`.`{source_name}` (`id` UInt32, `value` String) ENGINE MergeTree ORDER BY id"))
        conn.execute(text(f"INSERT INTO `{test_db}`.`{source_name}` VALUES (13, 'user_1')"))
        conn.execute(CreateTable(dictionary))

        context = MigrationContext.configure(connection=conn, opts={"version_table": ch_name("alembic_version")})
        op = Operations(context)

        op.reload_clickhouse_dictionary(dictionary_name, schema=test_db)

        status, element_count = conn.execute(
            text("SELECT status, element_count FROM system.dictionaries WHERE database = :db AND name = :name"),
            {"db": test_db, "name": dictionary_name},
        ).fetchone()
        assert status == "LOADED"
        assert element_count == 1
