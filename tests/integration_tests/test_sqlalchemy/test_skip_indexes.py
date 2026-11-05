from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import produce_migrations
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy.engine import URL

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex
from clickhouse_connect.cc_sqlalchemy.alembic import include_object
from clickhouse_connect.cc_sqlalchemy.alembic.indexes import IndexComparisonError
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime, String, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.sql import full_table

# ruff: noqa: F811
from tests.integration_tests.test_sqlalchemy.test_alembic import _alembic_config, ch_name  # noqa: F401


@pytest.fixture
def run_sqlalchemy(client_mode, call, test_engine, test_config):
    """Run the same schema workflow through sync and async SQLAlchemy connections."""
    if client_mode == "sync":

        def run(work):
            with test_engine.begin() as connection:
                return work(connection)

        return run
    pytest.importorskip("sqlalchemy", minversion="2.0.44")
    from sqlalchemy.ext.asyncio import create_async_engine

    async def run_async(work):
        engine = create_async_engine(
            URL.create(
                "clickhousedb+async",
                username=test_config.username,
                password=test_config.password,
                host=test_config.host,
                port=test_config.port,
                database=test_config.test_database,
            )
        )
        try:
            async with engine.begin() as connection:
                return await connection.run_sync(work)
        finally:
            await engine.dispose()

    return lambda work: call(run_async, work)


def _metadata(table_name, test_db, *indexes):
    metadata = sa.MetaData(schema=test_db)
    sa.Table(
        table_name,
        metadata,
        sa.Column("value", UInt32),
        sa.Column("label", String),
        sa.Column("ts", DateTime),
        *indexes,
        MergeTree(order_by="value"),
    )
    return metadata


def _diff(connection, metadata, table_name, **options):
    context = MigrationContext.configure(
        connection,
        opts={
            "include_object": include_object,
            "include_name": lambda name, kind, parents: kind != "table" or name == table_name,
            **options,
        },
    )
    return produce_migrations(context, metadata)


def test_declarative_index_revision_lifecycle(run_sqlalchemy, test_db, tmp_path: Path, ch_name):
    table_name = ch_name("declarative quoted table")
    index_name = "quoted index"
    initial = _metadata(table_name, test_db, ClickHouseIndex(index_name, "value", "minmax"))

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        created = command.revision(config, message="create indexes inline", autogenerate=True)
        contents = Path(created.path).read_text(encoding="utf-8")
        assert "ClickHouseIndex(" in contents
        assert "add_clickhouse_index" not in contents
        command.upgrade(config, "head")
        inspector = sa.inspect(connection)
        assert inspector.get_indexes(table_name, test_db) == []
        reflected = inspector.get_clickhouse_indexes(table_name, test_db)
        assert reflected == [ClickHouseIndex(index_name, "tuple(value)", "minmax", 1)]
        for _ in range(2):
            assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

        edited = _metadata(
            table_name,
            test_db,
            ClickHouseIndex(index_name, "value + 13", "set(79)", granularity=4),
            ClickHouseIndex("label_idx", "label", "bloom_filter(0.01)", granularity=2),
        )
        config.attributes["target_metadata"] = edited
        changed = command.revision(config, message="add and edit indexes", autogenerate=True)
        contents = Path(changed.path).read_text(encoding="utf-8")
        assert f"op.drop_clickhouse_index('{table_name}', '{index_name}'" in contents
        assert f"op.add_clickhouse_index('{table_name}', '{index_name}'" in contents
        assert "materialize_clickhouse_index" not in contents
        command.upgrade(config, "head")
        assert _diff(connection, edited, table_name).upgrade_ops.is_empty()
        assert {index.name for index in inspector.get_clickhouse_indexes(table_name, test_db)} == {index_name, "label_idx"}
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = _metadata(table_name, test_db)
        command.revision(config, message="remove explicit indexes", autogenerate=True)
        command.upgrade(config, "head")
        assert inspector.get_clickhouse_indexes(table_name, test_db) == []
        command.downgrade(config, "-1")
        assert _diff(connection, edited, table_name).upgrade_ops.is_empty()
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = initial
        command.revision(config, message="restore declaration before table removal", autogenerate=True)
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = sa.MetaData(schema=test_db)
        dropped = command.revision(config, message="drop table with index snapshot", autogenerate=True)
        assert "ClickHouseIndex(" in Path(dropped.path).read_text(encoding="utf-8")
        command.upgrade(config, "head")
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(
    ("expression", "type_", "granularity"),
    [
        ("value", "minmax", None),
        ("( `value` )", "MinMax()", 1),
        ("(value, label)", "minmax", 2),
        ("tuple(tuple(value, label))", "minmax", 2),
        ("tuple(tuple(value, (13, 79)))", "minmax", 2),
        ("date(ts)", "minmax", None),
        ("value + 13", "set(79)", 4),
        ("substring(label, 1, 2)", "bloom_filter(0.01)", 4),
        ("(value, concat(label, ':tag'))", "minmax", 2),
        ("concat(LCASE(label), ':tag')", "minmax", 1),
    ],
)
def test_index_server_normalization_and_replay(run_sqlalchemy, test_db, ch_name, expression, type_, granularity):
    table_name = ch_name("index_normalization")
    metadata = _metadata(table_name, test_db, ClickHouseIndex("value_idx", expression, type_, granularity))

    def workflow(connection):
        metadata.create_all(connection)
        assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
        reflected = sa.inspect(connection).get_clickhouse_indexes(table_name, test_db)
        connection.exec_driver_sql(f"ALTER TABLE {full_table(table_name, test_db)} DROP INDEX value_idx")
        context = MigrationContext.configure(connection)
        Operations(context).add_clickhouse_indexes(table_name, reflected, schema=test_db)
        assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_autogenerated_placement_flags_do_not_affect_declarative_adds(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("declarative_placement")
    initial = _metadata(table_name, test_db)
    desired = _metadata(
        table_name,
        test_db,
        ClickHouseIndex("a", "value", "minmax", if_not_exists=True, after_index="z"),
        ClickHouseIndex("z", "label", "minmax", first=True),
    )

    def workflow(connection):
        initial.create_all(connection)
        script = _diff(connection, desired, table_name)
        operation = Operations(MigrationContext.configure(connection))
        for group in script.upgrade_ops.ops:
            for item in group.ops:
                operation.invoke(item)
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("copy_from", [False, True])
def test_batch_recreation_with_explicit_indexes(run_sqlalchemy, test_db, ch_name, copy_from):
    table_name = ch_name("batch_indexes")
    metadata = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", "minmax"))

    def workflow(connection):
        metadata.create_all(connection)
        connection.exec_driver_sql(f"INSERT INTO {full_table(table_name, test_db)} VALUES (13, 'user_1', '2026-01-01 00:00:00')")
        operation = Operations(MigrationContext.configure(connection))

        def recreate():
            with operation.batch_alter_table(
                table_name,
                schema=test_db,
                recreate="always",
                copy_from=metadata.tables[f"{test_db}.{table_name}"] if copy_from else None,
                table_kwargs={"clickhousedb_engine": MergeTree(order_by="value")},
            ) as batch:
                batch.add_column(sa.Column("extra", UInt32))

        if copy_from:
            recreate()
        else:
            with pytest.raises(CommandError, match="copy_from"):
                recreate()
        assert [index.name for index in sa.inspect(connection).get_clickhouse_indexes(table_name, test_db)] == ["value_idx"]
        columns = {column["name"] for column in sa.inspect(connection).get_columns(table_name, test_db)}
        assert ("extra" in columns) is copy_from
        assert connection.exec_driver_sql(f"SELECT value, label FROM {full_table(table_name, test_db)}").fetchall() == [(13, "user_1")]

    run_sqlalchemy(workflow)


def test_alias_index_uses_raw_expression_normalization(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("alias_indexes")
    metadata = sa.MetaData(schema=test_db)
    table = sa.Table(
        table_name,
        metadata,
        sa.Column("value", UInt32),
        sa.Column("alias_value", UInt32, clickhousedb_alias=sa.text("value + 13")),
        ClickHouseIndex("alias_idx", "alias_value", "minmax"),
        ClickHouseIndex("simple_idx", "value", "minmax"),
        MergeTree(order_by="value"),
    )

    def workflow(connection):
        metadata.create_all(connection)
        for _ in range(2):
            assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
        reflected = sa.inspect(connection).get_clickhouse_indexes(table_name, test_db)
        assert reflected[0].expression == "tuple(value + 13)"
        table.c.alias_value.kwargs["clickhousedb_alias"] = sa.text("value + 79")
        with pytest.raises(IndexComparisonError, match="changed ALIAS"):
            _diff(connection, metadata, table_name)
        assert _diff(
            connection,
            metadata,
            table_name,
            include_object=lambda obj, name, kind, reflected, compare_to: (
                name != "alias_idx" and include_object(obj, name, kind, reflected, compare_to)
            ),
        ).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_catalog_failures_and_opt_out(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("catalog_failures")
    metadata = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", "minmax"))

    def workflow(connection):
        metadata.create_all(connection)
        statements = []

        def deny_catalog(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            if "system.data_skipping_indices" in statement:
                raise sa.exc.SQLAlchemyError("skip index catalog denied")

        sa.event.listen(connection, "before_cursor_execute", deny_catalog)
        try:
            assert _diff(connection, metadata, table_name, clickhousedb_compare_indexes=False).upgrade_ops.is_empty()
            assert not any("system.data_skipping_indices" in statement for statement in statements)
            assert _diff(
                connection,
                metadata,
                table_name,
                include_object=lambda obj, name, kind, reflected, compare_to: kind != "table",
            ).upgrade_ops.is_empty()
            with pytest.raises(IndexComparisonError, match="skip index catalog denied"):
                _diff(connection, metadata, table_name)
            with pytest.raises(sa.exc.SQLAlchemyError, match="skip index catalog denied"):
                sa.inspect(connection).get_clickhouse_indexes(table_name, test_db)
        finally:
            sa.event.remove(connection, "before_cursor_execute", deny_catalog)

    run_sqlalchemy(workflow)


def test_filters_legacy_indexes_and_implicit_indexes(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("filtered_indexes")
    unrelated = ch_name("unrelated_indexes")
    metadata = _metadata(table_name, test_db, ClickHouseIndex("declared_idx", "value", "minmax"))

    def workflow(connection):
        metadata.create_all(connection)
        connection.exec_driver_sql(
            f"CREATE TABLE {full_table(unrelated, test_db)} (value UInt32, INDEX unrelated_idx value TYPE minmax) "
            "ENGINE MergeTree ORDER BY value"
        )
        op = Operations(MigrationContext.configure(connection))
        op.add_clickhouse_index(table_name, "legacy_idx", "value", "minmax", schema=test_db)
        script = _diff(connection, metadata, table_name)
        removed = [operation for group in script.upgrade_ops.ops for operation in getattr(group, "ops", [])]
        assert len(removed) == 1 and removed[0].name == "legacy_idx"
        assert removed[0].reverse().expression == "tuple(value)"

        def object_filter(obj, name, kind, reflected, compare_to):
            if kind == "index" and reflected and compare_to is None:
                assert obj.table.name == table_name
                return False
            return include_object(obj, name, kind, reflected, compare_to)

        assert _diff(connection, metadata, table_name, include_object=object_filter).upgrade_ops.is_empty()
        assert _diff(
            connection,
            metadata,
            table_name,
            include_name=lambda name, kind, parents: name == table_name if kind == "table" else name != "declared_idx",
            include_object=object_filter,
        ).upgrade_ops.is_empty()
        assert _diff(connection, metadata, table_name, clickhousedb_compare_indexes=False).upgrade_ops.is_empty()
        assert _diff(
            connection,
            metadata,
            table_name,
            include_object=lambda obj, name, kind, reflected, compare_to: kind != "table",
        ).upgrade_ops.is_empty()

        implicit_table = ch_name("implicit_indexes")
        connection.exec_driver_sql(
            f"CREATE TABLE {full_table(implicit_table, test_db)} (value UInt32) ENGINE MergeTree ORDER BY value "
            "SETTINGS add_minmax_index_for_numeric_columns = 1"
        )
        implicit = sa.MetaData(schema=test_db)
        sa.Table(implicit_table, implicit, sa.Column("value", UInt32), MergeTree(order_by="value"))
        assert sa.inspect(connection).get_clickhouse_indexes(implicit_table, test_db) == []
        assert _diff(connection, implicit, implicit_table).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("expression", ["value", "date(ts)", "concat(LCASE(label), ':tag')", "(value, label)"])
def test_analyzer_coverage_is_required_only_when_used(run_sqlalchemy, test_client, test_db, ch_name, enabled, expression):
    if not enabled and test_client.min_version("26.9"):
        pytest.skip("enable_analyzer cannot be disabled on ClickHouse 26.9+")
    table_name = ch_name("analyzer_coverage")
    metadata = _metadata(table_name, test_db, ClickHouseIndex("value_idx", expression, "minmax"))
    requires_analyzer = expression != "value"

    def workflow(connection):
        metadata.create_all(connection)
        configured = connection.execution_options(settings={"enable_analyzer": int(enabled)})
        statements = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        sa.event.listen(configured, "before_cursor_execute", capture)
        try:
            assert _diff(configured, metadata, table_name, clickhousedb_compare_indexes=False).upgrade_ops.is_empty()
            assert not any(
                "system.data_skipping_indices" in statement or "getSetting('enable_analyzer')" in statement for statement in statements
            )
            statements.clear()
            if requires_analyzer and not enabled:
                with pytest.raises(IndexComparisonError, match="enable_analyzer") as excinfo:
                    _diff(configured, metadata, table_name)
                assert excinfo.value.index_name == "value_idx"
            else:
                assert _diff(configured, metadata, table_name).upgrade_ops.is_empty()
            if not requires_analyzer:
                assert not any("getSetting('enable_analyzer')" in statement for statement in statements)
        finally:
            sa.event.remove(configured, "before_cursor_execute", capture)
            configured.execution_options(settings={})

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(("stored_type", "declared_type"), [("MinMax()", "minmax"), ("minmax", "MinMax()"), ("minmax()", "MINMAX()")])
def test_empty_index_type_arguments_are_spelling_only(run_sqlalchemy, test_db, ch_name, stored_type, declared_type):
    table_name = ch_name("empty_type_arguments")
    initial = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", stored_type))
    desired = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", declared_type))

    def workflow(connection):
        initial.create_all(connection)
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_autogenerate_shares_catalog_and_analyzer_reads(run_sqlalchemy, test_db, ch_name):
    table_names = [ch_name("shared_index_reads") for _ in range(2)]
    metadata = sa.MetaData(schema=test_db)
    for name in table_names:
        _metadata(
            name,
            test_db,
            ClickHouseIndex("date_idx", "date(ts)", "minmax"),
            ClickHouseIndex("label_idx", "LCASE(label)", "minmax"),
        ).tables[f"{test_db}.{name}"].to_metadata(metadata)

    def workflow(connection):
        metadata.create_all(connection)
        statements = []

        def capture(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        sa.event.listen(connection, "before_cursor_execute", capture)
        try:
            _diff(
                connection,
                metadata,
                table_names[0],
                clickhousedb_compare_indexes=False,
                include_name=lambda name, kind, parents: kind != "table" or name in table_names,
            )
            metadata_reads = sum("SELECT engine, engine_full, comment FROM system.tables" in statement for statement in statements)
            database_reads = sum("SELECT currentDatabase()" in statement for statement in statements)
            for _ in range(2):
                statements.clear()
                script = _diff(
                    connection,
                    metadata,
                    table_names[0],
                    include_name=lambda name, kind, parents: kind != "table" or name in table_names,
                )
                assert script.upgrade_ops.is_empty()
                assert sum("system.data_skipping_indices" in statement for statement in statements) == 1
                assert sum("getSetting('enable_analyzer')" in statement for statement in statements) == 1
                assert sum("SELECT currentDatabase()" in statement for statement in statements) == database_reads + 1
                assert (
                    sum("SELECT engine, engine_full, comment FROM system.tables" in statement for statement in statements) == metadata_reads
                )
        finally:
            sa.event.remove(connection, "before_cursor_execute", capture)

    run_sqlalchemy(workflow)


def test_index_edit_can_reference_column_added_in_same_revision(run_sqlalchemy, test_db, tmp_path, ch_name):
    table_name = ch_name("added_index_column")
    initial = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", "minmax"))
    desired = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "new_value + 13", "minmax"))
    desired.tables[f"{test_db}.{table_name}"].append_column(sa.Column("new_value", UInt32, key="new_value_key"))

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        command.revision(config, message="initial indexed table", autogenerate=True)
        command.upgrade(config, "head")
        config.attributes["target_metadata"] = desired
        revision = command.revision(config, message="add column and edit index", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert contents.index("op.add_column(") < contents.index("op.drop_clickhouse_index(") < contents.index("op.add_clickhouse_index(")
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()
        assert "new_value" not in {column["name"] for column in sa.inspect(connection).get_columns(table_name, test_db)}
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("expression", ["new_vlaue + 13", "new_value + missing", "missing + new_value"])
def test_index_edit_unknown_column_still_raises(run_sqlalchemy, test_db, ch_name, expression):
    table_name = ch_name("unknown_index_column")
    initial = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", "minmax"))
    desired = _metadata(table_name, test_db, ClickHouseIndex("value_idx", expression, "minmax"))
    desired.tables[f"{test_db}.{table_name}"].append_column(sa.Column("new_value", UInt32))

    def workflow(connection):
        initial.create_all(connection)
        with pytest.raises(IndexComparisonError, match="Unknown expression"):
            _diff(connection, desired, table_name)
        assert sa.inspect(connection).get_clickhouse_indexes(table_name, test_db)[0].expression == "tuple(value)"

    run_sqlalchemy(workflow)


def test_index_edit_cannot_reference_filtered_new_column(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("filtered_index_column")
    initial = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "value", "minmax"))
    desired = _metadata(table_name, test_db, ClickHouseIndex("value_idx", "new_value + 13", "minmax"))
    desired.tables[f"{test_db}.{table_name}"].append_column(sa.Column("new_value", UInt32))

    def filter_column(obj, name, kind, reflected, compare_to):
        return False if kind == "column" and name == "new_value" else include_object(obj, name, kind, reflected, compare_to)

    def workflow(connection):
        initial.create_all(connection)
        with pytest.raises(IndexComparisonError, match="new_value"):
            _diff(connection, desired, table_name, include_object=filter_column)

    run_sqlalchemy(workflow)
