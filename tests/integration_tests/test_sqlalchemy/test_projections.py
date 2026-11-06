import time
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex, ClickHouseProjection
from clickhouse_connect.cc_sqlalchemy.alembic import ProjectionComparisonError, include_object, make_include_name
from clickhouse_connect.cc_sqlalchemy.alembic.operations import (
    _AddClickHouseIndexOp,
    _AddClickHouseProjectionOp,
    _DropClickHouseIndexOp,
    _DropClickHouseProjectionOp,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import AggregatingMergeTree, MergeTree
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.datatypes.format import clear_read_format, set_read_format
from clickhouse_connect.driver.binding import quote_identifier

# ruff: noqa: F811
from tests.integration_tests.test_sqlalchemy.test_alembic import _alembic_config, ch_name  # noqa: F401
from tests.integration_tests.test_sqlalchemy.test_skip_indexes import _diff, _metadata, run_sqlalchemy  # noqa: F401


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        (ClickHouseIndex("managed", "value", "minmax", 1), ClickHouseIndex("managed", "value + 13", "minmax", 1)),
        (
            ClickHouseProjection("managed", "SELECT value ORDER BY value"),
            ClickHouseProjection("managed", "SELECT value + 13 AS extra ORDER BY extra"),
        ),
    ],
    ids=["index", "projection"],
)
def test_schema_qualified_index_and_projection_edits(run_sqlalchemy, ch_name, original, replacement):
    database = ch_name("qualified_schema")
    table_name = ch_name("qualified_objects")
    initial = _metadata(table_name, database, original)
    desired = _metadata(table_name, database, replacement)
    is_index = isinstance(original, ClickHouseIndex)
    operation_types = (
        [_DropClickHouseIndexOp, _AddClickHouseIndexOp] if is_index else [_DropClickHouseProjectionOp, _AddClickHouseProjectionOp]
    )

    def workflow(connection):
        default_database = connection.exec_driver_sql("SELECT currentDatabase()").scalar_one()
        assert database != default_database
        schema_filter = make_include_name(frozenset({database}), default_schema=default_database)

        def include_name(name, kind, parents):
            return schema_filter(name, kind, parents) and (kind != "table" or name == table_name)

        options = {"include_schemas": True, "include_name": include_name}
        collision = _metadata(table_name, default_database, replacement)
        inspector = sa.inspect(connection)
        inspect_objects = inspector.get_clickhouse_indexes if is_index else inspector.get_clickhouse_projections
        connection.exec_driver_sql(f"CREATE DATABASE {quote_identifier(database)}")
        try:
            for _ in range(30):
                if database in inspector.get_schema_names():
                    break
                time.sleep(0.1)
            else:
                pytest.fail(f"Database {database} not visible after waiting")
            initial.create_all(connection)
            collision.create_all(connection)
            default_objects = inspect_objects(table_name, default_database)
            reflected = inspect_objects(table_name, database)
            assert reflected != default_objects
            for _ in range(2):
                assert _diff(connection, initial, table_name, **options).upgrade_ops.is_empty()
            script = _diff(connection, desired, table_name, **options)
            assert len(script.upgrade_ops.ops) == 1
            group = script.upgrade_ops.ops[0]
            assert (group.schema, group.table_name) == (database, table_name)
            assert [type(operation) for operation in group.ops] == operation_types
            drop = group.ops[0]
            assert (drop.existing_index if is_index else drop.existing_projection) == reflected[0]
            reverse = group.reverse()
            assert [type(operation) for operation in reverse.ops] == operation_types
            migration = Operations(MigrationContext.configure(connection))
            for operations, metadata in ((group.ops, desired), (reverse.ops, initial)):
                for operation in operations:
                    assert (operation.schema, operation.table_name, operation.name) == (database, table_name, original.name)
                    migration.invoke(operation)
                for _ in range(2):
                    assert _diff(connection, metadata, table_name, **options).upgrade_ops.is_empty()
                assert inspect_objects(table_name, default_database) == default_objects
            assert inspect_objects(table_name, database) == reflected
        finally:
            connection.exec_driver_sql(f"DROP DATABASE {quote_identifier(database)}")

    run_sqlalchemy(workflow)


def test_declarative_projection_revision_lifecycle(run_sqlalchemy, test_db, tmp_path: Path, ch_name):
    table_name = ch_name("declarative quoted table")
    projection_name = "quoted projection"
    initial = _metadata(table_name, test_db, ClickHouseProjection(projection_name, "SELECT value ORDER BY value"))

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        created = command.revision(config, message="create projections inline", autogenerate=True)
        contents = Path(created.path).read_text(encoding="utf-8")
        assert "ClickHouseProjection(" in contents
        assert "add_clickhouse_projection" not in contents
        command.upgrade(config, "head")
        inspector = sa.inspect(connection)
        reflected = inspector.get_clickhouse_projections(table_name, test_db)
        assert reflected == [ClickHouseProjection(projection_name, "SELECT value ORDER BY value")]
        for _ in range(2):
            assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

        edited = _metadata(
            table_name,
            test_db,
            ClickHouseProjection(projection_name, "SELECT value + 13 AS extra ORDER BY extra"),
            ClickHouseProjection("label_projection", "SELECT label, value ORDER BY label"),
        )
        config.attributes["target_metadata"] = edited
        changed = command.revision(config, message="add and edit projections", autogenerate=True)
        contents = Path(changed.path).read_text(encoding="utf-8")
        assert f"op.drop_clickhouse_projection('{table_name}', '{projection_name}'" in contents
        assert f"op.add_clickhouse_projection('{table_name}', '{projection_name}'" in contents
        assert "materialize_clickhouse_projection" not in contents
        command.upgrade(config, "head")
        assert _diff(connection, edited, table_name).upgrade_ops.is_empty()
        assert {projection.name for projection in inspector.get_clickhouse_projections(table_name, test_db)} == {
            projection_name,
            "label_projection",
        }
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = _metadata(table_name, test_db)
        command.revision(config, message="remove explicit projections", autogenerate=True)
        command.upgrade(config, "head")
        assert inspector.get_clickhouse_projections(table_name, test_db) == []
        command.downgrade(config, "-1")
        assert _diff(connection, edited, table_name).upgrade_ops.is_empty()
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = initial
        command.revision(config, message="restore declaration before table removal", autogenerate=True)
        command.upgrade(config, "head")

        config.attributes["target_metadata"] = sa.MetaData(schema=test_db)
        dropped = command.revision(config, message="drop table with projection snapshot", autogenerate=True)
        assert "ClickHouseProjection(" in Path(dropped.path).read_text(encoding="utf-8")
        command.upgrade(config, "head")
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(
    "select",
    [
        "SELECT value ORDER BY value",
        "select `value` order by (`value`)",
        "SELECT * ORDER BY (label, value)",
        "SELECT value AS v, label AS category ORDER BY (category, v)",
        "SELECT value + 13 AS extra ORDER BY extra",
        "SELECT date(ts) AS day ORDER BY day",
        'SELECT "DaTe"(ts) AS day ORDER BY day',
        "SELECT SUBSTR(label, 1, 2) AS prefix ORDER BY prefix",
        "SELECT concat(label, ':tag date(count())') AS category ORDER BY category",
        "SELECT sum(value) AS total, count() AS count GROUP BY label",
        "SELECT SuM(value) AS total, CoUnT() AS count GROUP BY label",
        "SELECT quantile(0.5)(value) AS median GROUP BY label",
        "SELECT sumIf(value, value > 13) AS total GROUP BY label",
        "WITH value + 13 AS extra SELECT extra ORDER BY extra",
        "WITH (item -> item + 13) AS date SELECT date(value) AS extra ORDER BY extra",
        "SELECT arrayMap(item -> item + 13, [value]) AS items ORDER BY items",
        "SELECT value, _part_offset ORDER BY value",
        "SELECT /* preserve aliases */ value AS v ORDER BY v",
    ],
)
def test_server_normalization_and_replay(run_sqlalchemy, test_db, ch_name, select):
    table_name = ch_name("projection_normalization")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("p", select))

    def workflow(connection):
        metadata.create_all(connection)
        for _ in range(2):
            assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
        reflected = sa.inspect(connection).get_clickhouse_projections(table_name, test_db)
        Operations(MigrationContext.configure(connection)).drop_clickhouse_projection(table_name, "p", schema=test_db)
        Operations(MigrationContext.configure(connection)).add_clickhouse_projections(table_name, reflected, schema=test_db)
        upgrade_ops = _diff(connection, metadata, table_name).upgrade_ops
        if not upgrade_ops.is_empty():
            replayed = sa.inspect(connection).get_clickhouse_projections(table_name, test_db)
            formatted = {
                text: connection.execute(
                    sa.text("SELECT formatQuerySingleLine(:query)"),
                    {"query": f"CREATE TABLE _t (_v UInt8, PROJECTION _p ({text})) ENGINE MergeTree ORDER BY tuple()"},
                ).scalar_one()
                for text in (select, reflected[0].select, replayed[0].select)
            }
            pytest.fail(
                f"Replayed projection drifted: created={reflected[0].select!r} replayed={replayed[0].select!r} formatted={formatted!r}"
            )

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("SELECT value + 13 AS extra ORDER BY extra", "SELECT value + 79 AS extra ORDER BY extra"),
        ("SELECT concat(label, ':tag') AS category ORDER BY category", "SELECT concat(label, ':Tag') AS category ORDER BY category"),
        ("SELECT value AS v ORDER BY v", "SELECT value AS other ORDER BY other"),
        ("SELECT label, value ORDER BY (label, value)", "SELECT label, value ORDER BY (value, label)"),
        ("SELECT quantile(0.5)(value) AS total GROUP BY label", "SELECT quantile(0.9)(value) AS total GROUP BY label"),
        ("SELECT sumIf(value, value > 13) AS total GROUP BY label", "SELECT SumIf(value, value > 13) AS total GROUP BY label"),
        ("SELECT sumIf(value, value > 13) AS total GROUP BY label", "SELECT sumIf(value, value > 79) AS total GROUP BY label"),
        ("SELECT sum(value) AS total GROUP BY label", "SELECT sumDistinct(value) AS total GROUP BY label"),
        ("SELECT plus(value, 13) AS extra ORDER BY extra", "SELECT value + 13 AS extra ORDER BY extra"),
    ],
)
def test_different_projection_definitions_generate_replacements(run_sqlalchemy, test_db, ch_name, before, after):
    table_name = ch_name("projection_differences")
    initial = _metadata(table_name, test_db, ClickHouseProjection("p", before))
    desired = _metadata(table_name, test_db, ClickHouseProjection("p", after))

    def workflow(connection):
        initial.create_all(connection)
        for declared, select in ((desired, after), (initial, before)):
            original = sa.inspect(connection).get_clickhouse_projections(table_name, test_db)[0]
            script = _diff(connection, declared, table_name)
            operations = [operation for group in script.upgrade_ops.ops for operation in getattr(group, "ops", [])]
            assert [type(operation) for operation in operations] == [_DropClickHouseProjectionOp, _AddClickHouseProjectionOp]
            drop, add = operations
            assert drop.existing_projection == original
            assert drop.reverse().select == original.select
            assert add.select == select
            assert (drop.schema, drop.table_name, drop.name) == (add.schema, add.table_name, add.name) == (None, table_name, "p")
            migration = Operations(MigrationContext.configure(connection))
            for operation in operations:
                migration.invoke(operation)
            assert _diff(connection, declared, table_name).upgrade_ops.is_empty()
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ("SELECT (value + 13) AS extra ORDER BY extra", "SELECT value + 13 AS extra ORDER BY extra"),
        ("SELECT value ORDER BY (value)", "SELECT value ORDER BY value"),
    ],
)
def test_redundant_parentheses_follow_server_formatting(run_sqlalchemy, test_db, ch_name, before, after):
    table_name = ch_name("projection_parentheses")
    initial = _metadata(table_name, test_db, ClickHouseProjection("p", before))
    desired = _metadata(table_name, test_db, ClickHouseProjection("p", after))

    def workflow(connection):
        initial.create_all(connection)
        stored = sa.inspect(connection).get_clickhouse_projections(table_name, test_db)[0].select
        operations = [operation for group in _diff(connection, desired, table_name).upgrade_ops.ops for operation in group.ops]
        if stored == after:
            # Some servers drop redundant parentheses when formatting, so both spellings are one structure.
            assert operations == []
            return
        assert stored == before
        assert [type(operation) for operation in operations] == [_DropClickHouseProjectionOp, _AddClickHouseProjectionOp]
        migration = Operations(MigrationContext.configure(connection))
        for operation in operations:
            migration.invoke(operation)
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_aggregating_merge_tree_projection_lifecycle(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("aggregating_projections")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT sum(value) AS total GROUP BY label"))
    table = metadata.tables[f"{test_db}.{table_name}"]
    table.engine = AggregatingMergeTree(order_by="value", settings={"deduplicate_merge_projection_mode": "rebuild"})
    table.kwargs["clickhousedb_engine"] = table.engine

    def workflow(connection):
        metadata.create_all(connection)
        connection.exec_driver_sql(f"INSERT INTO {full_table(table_name, test_db)} VALUES (13, 'user_1', '2026-01-01 00:00:00')")
        assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
        projection = sa.inspect(connection).get_clickhouse_projections(table_name, test_db)[0]
        assert projection == ClickHouseProjection("p", "SELECT sum(value) AS total GROUP BY label")
        migration = Operations(MigrationContext.configure(connection))
        migration.drop_clickhouse_projection(table_name, projection.name, schema=test_db)
        migration.add_clickhouse_projections(table_name, [projection], schema=test_db)
        assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
        assert connection.exec_driver_sql(f"SELECT sum(value) FROM {full_table(table_name, test_db)}").scalar_one() == 13

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("copy_from", [False, True])
def test_batch_recreation_with_projections(run_sqlalchemy, test_db, ch_name, copy_from):
    table_name = ch_name("batch_projections")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT value ORDER BY value"))

    def workflow(connection):
        metadata.create_all(connection)
        connection.exec_driver_sql(f"INSERT INTO {full_table(table_name, test_db)} VALUES (13, 'user_1', '2026-01-01 00:00:00')")
        operations = Operations(MigrationContext.configure(connection))

        def recreate():
            with operations.batch_alter_table(
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
        assert [projection.name for projection in sa.inspect(connection).get_clickhouse_projections(table_name, test_db)] == ["p"]
        columns = {column["name"] for column in sa.inspect(connection).get_columns(table_name, test_db)}
        assert ("extra" in columns) is copy_from
        assert connection.exec_driver_sql(f"SELECT value, label FROM {full_table(table_name, test_db)}").fetchall() == [(13, "user_1")]

    run_sqlalchemy(workflow)


def test_catalog_errors_and_opt_out(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("projection_catalog_errors")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT value ORDER BY value"))

    def workflow(connection):
        metadata.create_all(connection)
        statements = []

        def deny_catalog(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)
            if "system.projections" in statement:
                raise sa.exc.SQLAlchemyError("projection catalog denied")

        sa.event.listen(connection, "before_cursor_execute", deny_catalog)
        try:
            assert _diff(connection, metadata, table_name, clickhousedb_compare_projections=False).upgrade_ops.is_empty()
            assert not any("system.projections" in statement for statement in statements)
            assert _diff(
                connection, metadata, table_name, include_object=lambda obj, name, kind, reflected, compare_to: kind != "table"
            ).upgrade_ops.is_empty()
            with pytest.raises(ProjectionComparisonError, match="projection catalog denied"):
                _diff(connection, metadata, table_name)
            with pytest.raises(sa.exc.SQLAlchemyError, match="projection catalog denied"):
                sa.inspect(connection).get_clickhouse_projections(table_name, test_db)
        finally:
            sa.event.remove(connection, "before_cursor_execute", deny_catalog)

    run_sqlalchemy(workflow)


def test_filters_and_imperative_only_projections(run_sqlalchemy, test_db, ch_name):
    table_name = ch_name("filtered_projections")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("declared", "SELECT value ORDER BY value"))

    def workflow(connection):
        metadata.create_all(connection)
        Operations(MigrationContext.configure(connection)).add_clickhouse_projection(
            table_name, "imperative", "SELECT label ORDER BY label", schema=test_db
        )
        script = _diff(connection, metadata, table_name)
        removed = [operation for group in script.upgrade_ops.ops for operation in getattr(group, "ops", [])]
        assert len(removed) == 1 and removed[0].name == "imperative"
        assert removed[0].reverse().select == "SELECT label ORDER BY label"

        def object_filter(obj, name, kind, reflected, compare_to):
            if kind == "projection" and reflected and compare_to is None:
                assert obj.table.name == table_name
                return False
            return include_object(obj, name, kind, reflected, compare_to)

        assert _diff(connection, metadata, table_name, include_object=object_filter).upgrade_ops.is_empty()
        assert _diff(
            connection,
            metadata,
            table_name,
            include_name=lambda name, kind, parents: name == table_name if kind == "table" else name != "imperative",
        ).upgrade_ops.is_empty()
        assert _diff(connection, metadata, table_name, clickhousedb_compare_projections=False).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("filter_kind", ["name", "object"])
def test_projection_settings_refused_after_filters(run_sqlalchemy, test_db, ch_name, filter_kind):
    table_name = ch_name("projection_settings")
    unrelated = ch_name("unrelated_projection_settings")
    metadata = _metadata(table_name, test_db)

    def workflow(connection):
        metadata.create_all(connection)
        connection.exec_driver_sql(
            f"ALTER TABLE {full_table(table_name, test_db)} ADD PROJECTION unsupported (SELECT value ORDER BY value) "
            "WITH SETTINGS (index_granularity=13)"
        )
        connection.exec_driver_sql(
            f"CREATE TABLE {full_table(unrelated, test_db)} (value UInt32, "
            "PROJECTION unsupported (SELECT value ORDER BY value) WITH SETTINGS (index_granularity=13)) ENGINE MergeTree ORDER BY value"
        )
        with pytest.raises(ProjectionComparisonError, match="WITH SETTINGS"):
            _diff(connection, metadata, table_name)
        with pytest.raises(sa.exc.InvalidRequestError, match="WITH SETTINGS"):
            sa.inspect(connection).get_clickhouse_projections(table_name, test_db)
        with pytest.raises(CommandError, match="ClickHouseProjection definitions as copy_from"):
            with Operations(MigrationContext.configure(connection)).batch_alter_table(
                table_name, schema=test_db, recreate="always", table_kwargs={"clickhousedb_engine": MergeTree(order_by="value")}
            ) as batch:
                batch.add_column(sa.Column("extra", UInt32))
        options = (
            {"include_name": lambda name, kind, parents: name == table_name if kind == "table" else name != "unsupported"}
            if filter_kind == "name"
            else {"include_object": lambda obj, name, kind, reflected, compare_to: name != "unsupported"}
        )
        assert _diff(connection, metadata, table_name, **options).upgrade_ops.is_empty()
        assert _diff(connection, metadata, table_name, clickhousedb_compare_projections=False).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_edit_can_reference_new_physical_column(run_sqlalchemy, test_db, tmp_path, ch_name):
    table_name = ch_name("projection_new_column")
    initial = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT value ORDER BY value"))
    desired = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT new_value + 13 AS extra ORDER BY extra"))
    desired.tables[f"{test_db}.{table_name}"].append_column(sa.Column("new_value", UInt32, key="new_value_key"))

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        command.revision(config, message="initial projection", autogenerate=True)
        command.upgrade(config, "head")
        config.attributes["target_metadata"] = desired
        revision = command.revision(config, message="add column and edit projection", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert (
            contents.index("op.drop_clickhouse_projection(")
            < contents.index("op.add_column(")
            < contents.index("op.add_clickhouse_projection(")
        )
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()
        assert "new_value" not in {column["name"] for column in sa.inspect(connection).get_columns(table_name, test_db)}

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("replacement", [False, True])
def test_projection_removal_precedes_column_type_change(run_sqlalchemy, test_db, tmp_path, ch_name, replacement):
    table_name = ch_name("projection_column_type")

    def model(type_, projection):
        metadata = sa.MetaData(schema=test_db)
        sa.Table(
            table_name,
            metadata,
            sa.Column("id", UInt32),
            sa.Column("value", type_),
            *([projection] if projection is not None else []),
            MergeTree(order_by="id"),
        )
        return metadata

    initial = model(UInt32, ClickHouseProjection("p", "SELECT sum(value)"))
    desired = model(String, ClickHouseProjection("p", "SELECT value ORDER BY value") if replacement else None)

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        command.revision(config, message="initial aggregate projection", autogenerate=True)
        command.upgrade(config, "head")
        connection.exec_driver_sql(f"INSERT INTO {full_table(table_name, test_db)} VALUES (13,13),(79,79)")
        config.attributes["target_metadata"] = desired
        revision = command.revision(config, message="change type and projection", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert contents.index("op.drop_clickhouse_projection(") < contents.index("op.alter_column(")
        if replacement:
            assert contents.index("op.alter_column(") < contents.index("op.add_clickhouse_projection(")
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        assert connection.exec_driver_sql(f"SELECT value FROM {full_table(table_name, test_db)} ORDER BY id").fetchall() == [
            ("13",),
            ("79",),
        ]
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()
        assert connection.exec_driver_sql(f"SELECT value FROM {full_table(table_name, test_db)} ORDER BY id").fetchall() == [(13,), (79,)]
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


def test_projection_rename_respects_capacity(run_sqlalchemy, test_db, tmp_path, ch_name):
    table_name = ch_name("projection_capacity")
    initial = _metadata(table_name, test_db, ClickHouseProjection("z", "SELECT value ORDER BY value"))
    desired = _metadata(table_name, test_db, ClickHouseProjection("a", "SELECT value ORDER BY value"))
    for metadata in (initial, desired):
        table = metadata.tables[f"{test_db}.{table_name}"]
        table.engine = MergeTree(order_by="value", settings={"max_projections": 1})
        table.kwargs["clickhousedb_engine"] = table.engine

    def workflow(connection):
        config = _alembic_config(tmp_path, connection, initial, frozenset({table_name}))
        command.revision(config, message="initial limited projection", autogenerate=True)
        command.upgrade(config, "head")
        config.attributes["target_metadata"] = desired
        revision = command.revision(config, message="rename limited projection", autogenerate=True)
        contents = Path(revision.path).read_text(encoding="utf-8")
        assert contents.index("op.drop_clickhouse_projection(") < contents.index("op.add_clickhouse_projection(")
        command.upgrade(config, "head")
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        command.downgrade(config, "-1")
        assert _diff(connection, initial, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize(
    ("pattern", "format_", "expected"),
    [("Enum*", "int", (0, 13, "user_1")), ("UInt8", "string", ("Normal", "13", "user_1")), ("String", "bytes", ("Normal", 13, b"user_1"))],
)
def test_projection_metadata_ignores_user_read_formats(run_sqlalchemy, test_db, ch_name, pattern, format_, expected):
    table_name = ch_name("projection_read_formats")
    metadata = _metadata(table_name, test_db, ClickHouseProjection("p", "SELECT date(ts) AS day ORDER BY day"))

    def workflow(connection):
        metadata.create_all(connection)
        set_read_format(pattern, format_)
        try:
            assert sa.inspect(connection).get_clickhouse_projections(table_name, test_db) == [
                ClickHouseProjection("p", "SELECT DATE(ts) AS day ORDER BY day")
            ]
            assert _diff(connection, metadata, table_name).upgrade_ops.is_empty()
            row = connection.exec_driver_sql("SELECT CAST('Normal' AS Enum8('Normal' = 0)), toUInt8(13), 'user_1'").fetchone()
            assert tuple(row) == expected
        finally:
            clear_read_format(pattern)

    run_sqlalchemy(workflow)
