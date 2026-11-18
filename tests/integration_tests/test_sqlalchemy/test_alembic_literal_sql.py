from io import StringIO

import pytest
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy.exc import DatabaseError

from clickhouse_connect.cc_sqlalchemy import alembic as ch
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.driver.binding import format_str, quote_identifier

# ruff: noqa: F811
from tests.integration_tests.test_sqlalchemy.test_alembic import ch_name  # noqa: F401
from tests.integration_tests.test_sqlalchemy.test_skip_indexes import _diff, run_sqlalchemy  # noqa: F401

LITERAL = "100% | 100%% | %s | %(name)s | owner's \\ path"


def _offline_sql(connection, operation):
    buffer = StringIO()
    context = MigrationContext.configure(dialect=connection.dialect, opts={"as_sql": True, "output_buffer": buffer})
    Operations(context).invoke(operation)
    return buffer.getvalue().strip()


@pytest.mark.parametrize("engine_kind", ["target", "string", "object"])
@pytest.mark.parametrize("refresh", [False, True])
@pytest.mark.parametrize("percent_identifiers", [False, True])
def test_materialized_view_literal_sql(run_sqlalchemy, test_db, ch_name, engine_kind, refresh, percent_identifiers):
    suffix = " `\\ % %% %s %(name)s" if percent_identifiers else ""
    source, sink, view, dependency = (
        ch_name(prefix + suffix) for prefix in ("literal_source", "literal_sink", "literal_mv", "literal_dep")
    )

    def qualified(name):
        return f"{quote_identifier(test_db)}.{quote_identifier(name)}"

    def workflow(connection):
        migration = Operations(MigrationContext.configure(connection))
        for name in (source, sink):
            connection.execute(sa.text(f"CREATE TABLE {qualified(name)} (id UInt64, value String) ENGINE MergeTree ORDER BY id"))
        if refresh:
            migration.create_clickhouse_materialized_view(
                dependency,
                None,
                f"SELECT id FROM {qualified(source)}",
                schema=test_db,
                engine="MergeTree ORDER BY id",
                refresh=ch.ClickHouseRefresh(after="1 DAY"),
                empty=True,
            )
        select = f"SELECT id % 5 AS id, {format_str(LITERAL)} AS value FROM {qualified(source)} WHERE value LIKE 'user_%'"
        engine = (
            None
            if engine_kind == "target"
            else (
                MergeTree(order_by=f"(id, concat(value, {format_str(LITERAL)}))")
                if engine_kind == "object"
                else f"MergeTree ORDER BY (id, concat(value, {format_str(LITERAL)}))"
            )
        )
        operation = ch.CreateClickHouseMaterializedViewOp(
            view,
            sink if engine_kind == "target" else None,
            select,
            schema=test_db,
            to_schema=test_db if engine_kind == "target" else None,
            engine=engine,
            refresh=ch.ClickHouseRefresh(every="1 DAY", depends_on=((test_db, dependency),)) if refresh else None,
            empty=refresh,
            comment=LITERAL,
        )
        offline = _offline_sql(connection, operation)
        assert format_str(LITERAL) in offline
        online_stored = None
        for replay in (False, True):
            if replay:
                connection.execute(sa.text(offline))
            else:
                migration.invoke(operation)
            inspector = sa.inspect(connection)
            assert inspector.get_table_comment(view, schema=test_db) == {"text": LITERAL}
            definition = inspector.get_view_definition(view, schema=test_db)
            assert format_str(LITERAL) in definition
            assert "id % 5 AS id" in definition and "LIKE 'user_%'" in definition
            stored = connection.execute(sa.text(f"SHOW CREATE TABLE {qualified(view)}")).scalar_one()
            if replay:
                assert stored == online_stored
            else:
                online_stored = stored
            migration.drop_clickhouse_materialized_view(view, schema=test_db)
            assert not sa.inspect(connection).has_table(view, schema=test_db)
        with pytest.raises(DatabaseError):
            migration.drop_clickhouse_materialized_view(view, schema=test_db)
        migration.drop_clickhouse_materialized_view(view, schema=test_db, if_exists=True)

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("percent_identifiers", [False, True])
def test_dictionary_literal_sql(run_sqlalchemy, test_db, ch_name, test_config, percent_identifiers):
    if test_config.cloud:
        pytest.skip("Dictionary query uses the local ClickHouse server")
    suffix = " `\\ % %% %s %(name)s" if percent_identifiers else ""
    name = ch_name("literal_dictionary" + suffix)
    key, label = "id" + suffix, "value" + suffix
    qualified = f"{quote_identifier(test_db)}.{quote_identifier(name)}"
    query = (
        f"SELECT toUInt64(13 % 5) AS {quote_identifier(key)}, {format_str(LITERAL)} AS {quote_identifier(label)} "
        "WHERE 'user_1' LIKE 'user_%'"
    )
    operation = ch.CreateClickHouseDictionaryOp(
        name,
        [sa.Column(key, UInt64), sa.Column(label, String, server_default=LITERAL)],
        primary_key=quote_identifier(key),
        source=f"CLICKHOUSE(QUERY {format_str(query)})",
        layout="FLAT(/* % %% %s %(name)s */)",
        lifetime="MIN 0 MAX 13 /* % %% %s %(name)s */",
        schema=test_db,
        comment=LITERAL,
    )

    def workflow(connection):
        migration = Operations(MigrationContext.configure(connection))
        offline = _offline_sql(connection, operation)
        assert format_str(LITERAL) in offline and format_str(query) in offline
        online_stored = None
        for replay in (False, True):
            if replay:
                connection.execute(sa.text(offline))
            else:
                migration.invoke(operation)
            migration.reload_clickhouse_dictionary(name, schema=test_db)
            assert connection.execute(sa.text(f"SELECT * FROM {qualified}")).fetchall() == [(3, LITERAL)]
            stored = connection.execute(sa.text(f"SHOW CREATE DICTIONARY {qualified}")).scalar_one()
            if replay:
                assert stored == online_stored
            else:
                online_stored = stored
            assert sa.inspect(connection).get_table_comment(name, schema=test_db) == {"text": LITERAL}
            migration.drop_clickhouse_dictionary(name, schema=test_db)
        with pytest.raises(DatabaseError):
            migration.reload_clickhouse_dictionary(name, schema=test_db)
        migration.drop_clickhouse_dictionary(name, schema=test_db, if_exists=True)

    run_sqlalchemy(workflow)


def test_table_helpers_literal_sql(run_sqlalchemy, test_db, ch_name):
    table = ch_name("literal_table `\\ % %% %s %(name)s")
    name = "item `\\ % %% %s %(name)s"
    qualified = f"{quote_identifier(test_db)}.{quote_identifier(table)}"

    def workflow(connection):
        migration = Operations(MigrationContext.configure(connection))
        connection.execute(sa.text(f"CREATE TABLE {qualified} (id UInt64, value String) ENGINE MergeTree ORDER BY id"))
        index = ch.ClickHouseIndex(name, f"value LIKE {format_str(LITERAL)}", "set(13)")
        projection = ch.ClickHouseProjection(name, f"SELECT id % 5 AS id, concat(value, {format_str(LITERAL)}) AS value ORDER BY id")
        for multiple in (False, True):
            if multiple:
                migration.add_clickhouse_indexes(table, [index], schema=test_db)
                migration.add_clickhouse_projections(table, [projection], schema=test_db)
            else:
                migration.add_clickhouse_index(table, name, index.expression, index.type_, schema=test_db)
                migration.add_clickhouse_projection(table, name, projection.select, schema=test_db)
            stored = connection.execute(sa.text(f"SHOW CREATE TABLE {qualified}")).scalar_one()
            assert format_str(LITERAL) in stored and quote_identifier(name) in stored
            migration.materialize_clickhouse_index(table, name, schema=test_db, clickhouse_settings={"mutations_sync": 1})
            migration.materialize_clickhouse_projection(table, name, schema=test_db, clickhouse_settings={"mutations_sync": 1})
            if multiple:
                migration.drop_clickhouse_indexes(table, [name], schema=test_db)
                migration.drop_clickhouse_projections(table, [name], schema=test_db)
            else:
                migration.drop_clickhouse_index(table, name, schema=test_db)
                migration.drop_clickhouse_projection(table, name, schema=test_db)
        migration.modify_clickhouse_table_settings(table, {"replicated_deduplication_window": 79}, schema=test_db)
        stored = connection.execute(sa.text(f"SHOW CREATE TABLE {qualified}")).scalar_one()
        assert "replicated_deduplication_window = 79" in stored
        migration.reset_clickhouse_table_settings(table, ["replicated_deduplication_window"], schema=test_db)
        assert "replicated_deduplication_window = 79" not in connection.execute(sa.text(f"SHOW CREATE TABLE {qualified}")).scalar_one()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("tuple_expression", [False, True])
@pytest.mark.parametrize("alias", [False, True])
def test_index_comparison_preserves_literal_percents(run_sqlalchemy, test_db, ch_name, tuple_expression, alias):
    table_name = ch_name("literal_index_comparison")
    column = "alias_label" if alias else "label"

    def metadata(pattern, *, index=True):
        result = sa.MetaData(schema=test_db)
        expression = f"{column} LIKE '{pattern}'"
        if tuple_expression:
            expression = f"({expression}, value)"
        sa.Table(
            table_name,
            result,
            sa.Column("value", UInt64),
            sa.Column("label", String),
            *(
                [sa.Column("alias_label", String, clickhousedb_alias=sa.func.concat(sa.column("label"), sa.literal(" % %% %s %(name)s")))]
                if alias
                else []
            ),
            *([ch.ClickHouseIndex("literal_index", expression, "set(13)")] if index else []),
            MergeTree(order_by="value"),
        )
        return result

    def workflow(connection):
        metadata("a%%b", index=False).create_all(connection)
        migration = Operations(MigrationContext.configure(connection))
        original, desired = metadata("a%%b"), metadata("a%b")
        index = original.tables[f"{test_db}.{table_name}"].kwargs["clickhousedb_indexes"][0]
        migration.add_clickhouse_index(table_name, index.name, index.expression, index.type_, schema=test_db)
        assert _diff(connection, original, table_name).upgrade_ops.is_empty()
        changes = _diff(connection, desired, table_name)
        operations = [operation for group in changes.upgrade_ops.ops for operation in group.ops]
        assert [type(operation) for operation in operations] == [ch.DropClickHouseIndexOp, ch.AddClickHouseIndexOp]
        for operation in operations:
            migration.invoke(operation)
        assert _diff(connection, desired, table_name).upgrade_ops.is_empty()
        assert not _diff(connection, original, table_name).upgrade_ops.is_empty()

    run_sqlalchemy(workflow)


@pytest.mark.parametrize("creation", ["create_all", "create_table"])
@pytest.mark.parametrize("server_side_params", [False, True])
@pytest.mark.parametrize("percent_identifiers", [False, True])
def test_create_table_preserves_inline_percents(run_sqlalchemy, test_db, ch_name, creation, server_side_params, percent_identifiers):
    suffix = "%%" if percent_identifiers else ""
    name = ch_name("inline_table" + suffix)
    key, value = "id" + suffix, "value" + suffix
    metadata = sa.MetaData(schema=test_db)
    table = sa.Table(
        name,
        metadata,
        sa.Column(key, UInt64),
        sa.Column(value, String, server_default="a% %% %s %(name)s", comment="c% %% %s %(name)s"),
        ch.ClickHouseIndex("index%%", f"`{value}` LIKE 'a% %% %s %(name)s'", "set(13)"),
        ch.ClickHouseProjection("projection%%", f"SELECT `{key}`, concat(`{value}`, 'p% %% %s %(name)s') AS label ORDER BY `{key}`"),
        MergeTree(
            order_by=f"(`{key}`, concat(`{value}`, 'o% %% %s %(name)s'))",
            partition_by=f"concat(`{value}`, 'k% %% %s %(name)s')",
            primary_key=f"`{key}`",
            sample_by=f"`{key}`",
        ),
        comment="t% %% %s %(name)s",
    )

    def workflow(connection):
        buffer = StringIO()
        context = MigrationContext.configure(
            url=str(connection.engine.url),
            dialect_opts={"paramstyle": "named"},
            opts={"as_sql": True, "output_buffer": buffer},
        )
        offline_operations = Operations(context)
        offline_operations.create_table(
            name,
            *(column._copy() for column in table.columns),
            clickhousedb_engine=table.engine,
            schema=test_db,
            comment=table.comment,
            clickhousedb_indexes=table.kwargs["clickhousedb_indexes"],
            clickhousedb_projections=table.kwargs["clickhousedb_projections"],
        )
        offline = buffer.getvalue().strip()
        expected = str(sa.schema.CreateTable(table).compile(dialect=context.dialect)) + ";"
        assert offline == expected
        stored_online = None
        for replay in (False, True):
            if replay:
                connection.execute(sa.text(offline))
            elif creation == "create_all":
                metadata.create_all(connection)
            else:
                Operations(MigrationContext.configure(connection)).create_table(
                    name,
                    *(column._copy() for column in table.columns),
                    schema=test_db,
                    comment=table.comment,
                    clickhousedb_engine=table.engine,
                    clickhousedb_indexes=table.kwargs["clickhousedb_indexes"],
                    clickhousedb_projections=table.kwargs["clickhousedb_projections"],
                )
            stored = connection.execute(sa.text(f"SHOW CREATE TABLE {quote_identifier(test_db)}.{quote_identifier(name)}")).scalar_one()
            for prefix in ("a", "c", "p", "o", "k", "t"):
                assert f"'{prefix}% %% %s %(name)s'" in stored
            changes = _diff(connection, metadata, name)
            assert changes.upgrade_ops.is_empty(), [
                (type(op).__name__, vars(op)) for group in changes.upgrade_ops.ops for op in getattr(group, "ops", [group])
            ]
            changed_metadata = sa.MetaData(schema=test_db)
            changed_table = table.to_metadata(changed_metadata)
            projection = table.kwargs["clickhousedb_projections"][0]
            changed_table.kwargs["clickhousedb_projections"] = (
                ch.ClickHouseProjection(projection.name, projection.select.replace("p% %%", "p% %")),
            )
            assert not _diff(connection, changed_metadata, name).upgrade_ops.is_empty()
            migration = Operations(MigrationContext.configure(connection))
            for prefix, alter in (("x", False), ("y", True)):
                default = f"{prefix}% %% %s %(name)s"
                if alter:
                    migration.alter_column(name, "extra%%", existing_type=String(), server_default=default, comment=default, schema=test_db)
                else:
                    migration.add_column(name, sa.Column("extra%%", String, server_default=default, comment=default), schema=test_db)
                extra = next(column for column in sa.inspect(connection).get_columns(name, schema=test_db) if column["name"] == "extra%%")
                assert extra["server_default"] == format_str(default)
                assert extra["comment"] == default
            migration.drop_column(name, "extra%%", schema=test_db)
            if replay:
                assert stored == stored_online
            else:
                stored_online = stored
            table.drop(connection)

    run_sqlalchemy(workflow, server_side_params=server_side_params)


@pytest.mark.parametrize("server_side_params", [False, True])
def test_create_dictionary_compiler_literal_sql(run_sqlalchemy, test_db, ch_name, server_side_params):
    name = ch_name("compiler_dictionary%%")
    value = "a% %% %s %(name)s"
    query = f"SELECT toUInt64(13) AS `id%%`, {format_str(value)} AS `value%%`"
    metadata = sa.MetaData(schema=test_db)
    table = sa.Table(
        name,
        metadata,
        sa.Column("id%%", UInt64),
        sa.Column("value%%", String, server_default=value),
        clickhousedb_table_type="dictionary",
        clickhousedb_dictionary_primary_key="`id%%`",
        clickhousedb_dictionary_source=f"CLICKHOUSE(QUERY {format_str(query)})",
        clickhousedb_dictionary_layout="FLAT(/* % %% %s %(name)s */)",
        clickhousedb_dictionary_lifetime="0 /* % %% %s %(name)s */",
        comment=value,
    )

    def workflow(connection):
        buffer = StringIO()
        offline = MigrationContext.configure(
            url=str(connection.engine.url),
            dialect_opts={"paramstyle": "named"},
            opts={"as_sql": True, "output_buffer": buffer},
        )
        Operations(offline).impl.create_table(table)
        stored_online = None
        for replay in (False, True):
            if replay:
                connection.execute(sa.text(buffer.getvalue()))
            else:
                metadata.create_all(connection)
            qualified = f"{quote_identifier(test_db)}.{quote_identifier(name)}"
            assert connection.execute(sa.text(f"SELECT * FROM {qualified}")).fetchall() == [(13, value)]
            assert sa.inspect(connection).get_table_comment(name, schema=test_db) == {"text": value}
            stored = connection.execute(sa.text(f"SHOW CREATE DICTIONARY {qualified}")).scalar_one()
            if replay:
                assert stored == stored_online
            else:
                stored_online = stored
            table.drop(connection)

    run_sqlalchemy(workflow, server_side_params=server_side_params)
