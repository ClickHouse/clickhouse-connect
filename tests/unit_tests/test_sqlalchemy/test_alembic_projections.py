from io import StringIO
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from alembic.autogenerate.api import AutogenContext, render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.exc import SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy import ClickHouseProjection
from clickhouse_connect.cc_sqlalchemy.alembic import include_object
from clickhouse_connect.cc_sqlalchemy.alembic.impl import ClickHouseImpl
from clickhouse_connect.cc_sqlalchemy.alembic.operations import _AddClickHouseProjectionOp, _DropClickHouseProjectionOp
from clickhouse_connect.cc_sqlalchemy.alembic.projections import (
    COMPARE_PROJECTIONS_OPT,
    ProjectionComparisonError,
    _compare_clickhouse_projections,
    _definitions_match,
    _filter_created_table_projections,
    _function_names,
    _normalize_function_names,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.projection import table_projections
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import _ProjectionCatalogEntry

PROJECTIONS_MODULE = "clickhouse_connect.cc_sqlalchemy.alembic.projections"


def _table(*projections):
    return Table("events", MetaData(), Column("value", UInt32), *projections, MergeTree(order_by="value"), schema="warehouse")


def _context(**options):
    context = AutogenContext(MigrationContext.configure(dialect=ClickHouseDialect(), opts=options))
    context.connection = Mock()
    context.connection.execute.return_value = MagicMock()
    context.connection.execute.return_value.__iter__.return_value = iter(
        [SimpleNamespace(name="DATE", alias_to="", is_aggregate="0"), SimpleNamespace(name="sum", alias_to="", is_aggregate="1")]
    )
    return context


def _compare(monkeypatch, old, new, context=None):
    context = context or _context(include_object=include_object)
    read = Mock(return_value={"events": [_ProjectionCatalogEntry(projection.name, projection.select) for projection in old]})
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", read)
    operation = ops.ModifyTableOps("events", [], schema="warehouse")
    _compare_clickhouse_projections(context, operation, "warehouse", "events", _table(), _table(*new))
    return context, operation, read


def test_add_remove_and_reversal(monkeypatch):
    old = ClickHouseProjection("old_projection", "SELECT value")
    new = ClickHouseProjection("new_projection", "SELECT value + 13")
    _, operation, _ = _compare(monkeypatch, [old], [new])
    assert [type(op) for op in operation.ops] == [_DropClickHouseProjectionOp, _AddClickHouseProjectionOp]
    drop, add = operation.ops
    assert drop.existing_projection == old
    assert drop.reverse().select == old.select
    assert add.reverse().reverse().select == new.select
    rendered = render_python_code(ops.UpgradeOps([operation]), migration_context=_context().migration_context)
    assert "op.add_clickhouse_projection('events', 'new_projection', 'SELECT value + 13', schema='warehouse')" in rendered
    assert "op.drop_clickhouse_projection('events', 'old_projection', schema='warehouse')" in rendered
    assert "materialize" not in rendered


@pytest.mark.parametrize("column_operation", [ops.AddColumnOp("events", Column("extra", UInt32)), ops.AlterColumnOp("events", "value")])
def test_scheduled_drops_precede_column_changes_and_additions(monkeypatch, column_operation):
    old = ClickHouseProjection("z", "SELECT sum(value)")
    new = ClickHouseProjection("a", "SELECT value ORDER BY value")
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", lambda *args: {"events": [old]})
    operation = ops.ModifyTableOps("events", [column_operation], schema="warehouse")
    _compare_clickhouse_projections(_context(), operation, "warehouse", "events", _table(), _table(new))
    assert [type(item) for item in operation.ops] == [_DropClickHouseProjectionOp, type(column_operation), _AddClickHouseProjectionOp]
    assert operation.ops[0].existing_projection == old
    assert operation.ops[2].select == new.select


def test_edits_pair_by_schema_table_and_name_for_policy_hooks(monkeypatch, caplog):
    caplog.set_level("INFO", logger="alembic.autogenerate.compare.clickhouse_projections")
    old = ClickHouseProjection("p", "SELECT value")
    new = ClickHouseProjection("p", "SELECT value + 13")
    context = _context()
    context.connection.execute.return_value.fetchone.return_value = ("new definition", "old definition")
    other_old = ClickHouseProjection("other", "SELECT value")
    other_new = ClickHouseProjection("other", "SELECT value + 79")
    _, operation, _ = _compare(monkeypatch, [old, other_old], [new, other_new], context)
    assert [type(op) for op in operation.ops] == [
        _DropClickHouseProjectionOp,
        _DropClickHouseProjectionOp,
        _AddClickHouseProjectionOp,
        _AddClickHouseProjectionOp,
    ]
    drops = {(op.schema, op.table_name, op.name): op for op in operation.ops if isinstance(op, _DropClickHouseProjectionOp)}
    additions = {(op.schema, op.table_name, op.name): op for op in operation.ops if isinstance(op, _AddClickHouseProjectionOp)}
    assert drops.keys() == additions.keys() == {("warehouse", "events", "other"), ("warehouse", "events", "p")}
    drop, add = drops["warehouse", "events", "p"], additions["warehouse", "events", "p"]
    assert drop.existing_projection == old
    assert add.select == new.select
    assert "Detected changed projection" in caplog.text
    reverse = operation.reverse()
    assert [type(op) for op in reverse.ops] == [
        _DropClickHouseProjectionOp,
        _DropClickHouseProjectionOp,
        _AddClickHouseProjectionOp,
        _AddClickHouseProjectionOp,
    ]
    assert {op.name: op.select for op in reverse.ops if isinstance(op, _AddClickHouseProjectionOp)} == {
        old.name: old.select,
        other_old.name: other_old.select,
    }


def test_unchanged_ignores_imperative_flags(monkeypatch):
    old = ClickHouseProjection("p", "SELECT value")
    new = ClickHouseProjection("p", "SELECT value", if_not_exists=True, after_projection="missing")
    context, operation, _ = _compare(monkeypatch, [old], [new])
    assert operation.is_empty()
    context.connection.execute.assert_not_called()


@pytest.mark.parametrize("enabled", [False, True])
def test_opt_out_precedes_catalog_read(monkeypatch, enabled):
    context = _context(**{COMPARE_PROJECTIONS_OPT: enabled})
    read = Mock(side_effect=SQLAlchemyError("projection catalog denied"))
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", read)
    operation = ops.ModifyTableOps("events", [])
    if enabled:
        with pytest.raises(ProjectionComparisonError, match="projection catalog denied") as excinfo:
            _compare_clickhouse_projections(context, operation, None, "events", _table(), _table())
        assert excinfo.value.projection_name is None
        assert excinfo.value.reason.startswith("cannot read explicit projection catalog")
    else:
        _compare_clickhouse_projections(context, operation, None, "events", _table(), _table())
        read.assert_not_called()


def test_normalization_failure_is_structured(monkeypatch):
    old = ClickHouseProjection("p", "SELECT value")
    new = ClickHouseProjection("p", "select value + 13")
    context = _context()
    context.connection.execute.side_effect = SQLAlchemyError("formatter unavailable")
    with pytest.raises(ProjectionComparisonError, match="formatter unavailable") as excinfo:
        _compare(monkeypatch, [old], [new], context)
    error = excinfo.value
    assert (error.schema, error.table_name, error.projection_name) == ("warehouse", "events", "p")
    assert error.reflected == old
    assert error.declared == new


@pytest.mark.parametrize("kind", ["add", "remove", "edit"])
def test_object_filter_arguments(monkeypatch, kind):
    old = ClickHouseProjection("p", "SELECT value")
    new = ClickHouseProjection("p", "SELECT value + 13")
    calls = []

    def filter_object(obj, name, type_, reflected, compare_to):
        calls.append((obj, name, type_, reflected, compare_to))
        assert (obj.table.name, obj.table.schema) == ("events", "warehouse")
        assert isinstance(obj, ClickHouseProjection)
        return False

    context = _context(include_object=filter_object)
    _, operation, _ = _compare(monkeypatch, [] if kind == "add" else [old], [] if kind == "remove" else [new], context)
    assert operation.is_empty()
    obj, name, type_, reflected, compare_to = calls[0]
    assert (name, type_, reflected) == ("p", "projection", kind == "remove")
    assert (compare_to.select if compare_to else None) == (old.select if kind == "edit" else None)
    assert obj.select == (old.select if kind == "remove" else new.select)
    context.connection.execute.assert_not_called()


def test_name_filter_receives_parent_names(monkeypatch):
    names = []

    def filter_name(name, type_, parents):
        names.append((name, type_, parents))
        return False

    projection = ClickHouseProjection("p", "SELECT value")
    context = _context(include_name=filter_name)
    _, operation, _ = _compare(monkeypatch, [projection], [projection], context)
    assert operation.is_empty()
    assert all(kind == "projection" for _, kind, _ in names)
    assert all(
        parents == {"schema_name": "warehouse", "table_name": "events", "schema_qualified_table_name": "warehouse.events"}
        for _, _, parents in names
    )


@pytest.mark.parametrize("legacy", [False, True])
def test_created_table_filters_inline_definitions_without_changing_metadata(legacy):
    excluded = ClickHouseProjection("excluded", "SELECT value")
    included = ClickHouseProjection("included", "SELECT value")
    table = _table() if legacy else _table(excluded, included)
    if legacy:
        table.kwargs["clickhouse_projections"] = (excluded, included)
    context = _context(include_object=lambda obj, name, type_, reflected, compare_to: name != "excluded")
    operation = ops.CreateTableOp.from_table(table)
    upgrade = ops.UpgradeOps([operation])
    _filter_created_table_projections(context, upgrade, {"warehouse"})
    assert operation.kw["clickhousedb_projections"] == (included,)
    assert table_projections(table) == (excluded, included)
    rendered = render_python_code(upgrade, migration_context=context.migration_context)
    assert "ClickHouseProjection('included'" in rendered
    assert "ClickHouseProjection('excluded'" not in rendered
    assert "add_clickhouse_projection" not in rendered
    assert "clickhouse_projections" not in operation.kw


def test_removed_table_snapshot_keeps_definition(monkeypatch):
    projection = ClickHouseProjection("p", "SELECT * ORDER BY value")
    catalog_entry = _ProjectionCatalogEntry(projection.name, projection.select)
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", lambda *args: {"events": [catalog_entry]})
    table = _table()
    context = _context()
    operation = ops.ModifyTableOps("events", [])
    _compare_clickhouse_projections(context, operation, "warehouse", "events", table, None)
    assert operation.is_empty()
    recreate = ops.DropTableOp.from_table(table).reverse()
    assert recreate.kw["clickhousedb_projections"] == (projection,)
    assert "ClickHouseProjection(" in render_python_code(ops.UpgradeOps([recreate]), migration_context=context.migration_context)


@pytest.mark.parametrize("filter_kind", [None, "name", "object"])
@pytest.mark.parametrize("removed_table", [False, True])
def test_unsupported_catalog_entry_is_filter_aware(monkeypatch, filter_kind, removed_table):
    unsupported = _ProjectionCatalogEntry("p", "SELECT value", unsupported_reason="WITH SETTINGS cannot be preserved")
    read = Mock(return_value={"events": [unsupported], "unrelated": [unsupported]})
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", read)
    options = {}
    if filter_kind == "name":
        options["include_name"] = lambda name, kind, parents: name != "p"
    elif filter_kind == "object":
        options["include_object"] = lambda obj, name, kind, reflected, compare_to: name != "p"
    context = _context(**options)
    table = _table()
    operation = ops.ModifyTableOps("events", [])
    if filter_kind:
        _compare_clickhouse_projections(context, operation, "warehouse", "events", table, None if removed_table else _table())
        assert operation.is_empty()
        assert not dict(table.kwargs).get("clickhousedb_projections")
    else:
        with pytest.raises(ProjectionComparisonError, match="WITH SETTINGS") as excinfo:
            _compare_clickhouse_projections(context, operation, "warehouse", "events", table, None if removed_table else _table())
        assert excinfo.value.projection_name == "p"
        assert excinfo.value.reflected == ClickHouseProjection(unsupported.name, unsupported.select)
        assert not operation.ops


def test_offline_replacement_is_two_quoted_statements():
    output = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    operations = Operations(context)
    old = ClickHouseProjection("quoted projection", "SELECT value")
    operations.invoke(_DropClickHouseProjectionOp("quoted table", old.name, schema="quoted database", existing_projection=old))
    operations.invoke(_AddClickHouseProjectionOp("quoted table", old.name, "SELECT value + 13", schema="quoted database"))
    assert output.getvalue().splitlines() == [
        "ALTER TABLE `quoted database`.`quoted table` DROP PROJECTION `quoted projection`;",
        "",
        "ALTER TABLE `quoted database`.`quoted table` ADD PROJECTION `quoted projection` (SELECT value + 13);",
        "",
    ]


def test_batch_recreation_preserves_inline_definitions():
    output = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    table = _table(ClickHouseProjection("p", "SELECT value"))
    with Operations(context).batch_alter_table("events", schema="warehouse", copy_from=table, recreate="always") as batch:
        batch.add_column(Column("extra", UInt32))
    assert output.getvalue().count("PROJECTION `p` (SELECT value)") == 1
    assert "ADD PROJECTION" not in output.getvalue()


@pytest.mark.parametrize("unsupported_reason", [None, "projection WITH SETTINGS cannot be preserved"])
def test_reflected_batch_recreation_refuses_catalog_entries(monkeypatch, unsupported_reason):
    connection = Mock()
    row = SimpleNamespace(
        table="events", name="p", type="Normal", query="SELECT value", settings={"index_granularity": "13"} if unsupported_reason else {}
    )
    connection.execute.side_effect = lambda statement, params: iter([object()] if "system.tables" in str(statement) else [row])
    impl = ClickHouseImpl(ClickHouseDialect(), connection, False, False, None, {})
    monkeypatch.setattr("clickhouse_connect.cc_sqlalchemy.alembic.impl.get_clickhouse_indexes", lambda *args: [])
    with pytest.raises(CommandError, match="ClickHouseProjection definitions as copy_from"):
        impl.prep_table_for_batch(SimpleNamespace(new_indexes={}, indexes={}, reflected=True), _table())


@pytest.mark.parametrize("kind", ["add", "drop", "edit"])
def test_generated_operations_refuse_batch_rendering(kind):
    old = ClickHouseProjection("p", "SELECT value")
    drop = _DropClickHouseProjectionOp("events", old.name, existing_projection=old)
    add = _AddClickHouseProjectionOp("events", old.name, "SELECT value + 13")
    operations = [drop, add] if kind == "edit" else [add if kind == "add" else drop]
    upgrade = ops.UpgradeOps([ops.ModifyTableOps("events", operations)])
    with pytest.raises(CommandError, match="ClickHouse projection operations.*render_as_batch=False"):
        render_python_code(upgrade, migration_context=_context().migration_context, render_as_batch=True)


def test_autogenerated_additions_ignore_placement_flags(monkeypatch):
    projections = [
        ClickHouseProjection("a", "SELECT value", if_not_exists=True, after_projection="z"),
        ClickHouseProjection("z", "SELECT value", first=True),
    ]
    _, operation, _ = _compare(monkeypatch, [], projections)
    assert [op.name for op in operation.ops] == ["a", "z"]
    assert all((op.if_not_exists, op.first, op.after_projection) == (False, False, None) for op in operation.ops)


def test_catalog_is_cached_per_schema_and_context(monkeypatch):
    context = _context()
    context.connection.execute.return_value.scalar.return_value = "warehouse"
    read = Mock(return_value={})
    monkeypatch.setattr(f"{PROJECTIONS_MODULE}._get_clickhouse_projection_catalog", read)
    for schema in (None, None, "warehouse", "other", "other"):
        operation = ops.ModifyTableOps("events", [], schema=schema)
        _compare_clickhouse_projections(context, operation, schema, "events", _table(), _table())
        assert operation.is_empty()
    assert [call.args[1] for call in read.call_args_list] == ["warehouse", "other"]
    context.connection.execute.assert_called_once()
    _compare_clickhouse_projections(_context(), ops.ModifyTableOps("events", []), "warehouse", "events", _table(), _table())
    assert read.call_count == 3


@pytest.mark.parametrize("row", [None, ("only one",), ("", ""), (b"bytes", b"bytes")])
def test_unrecognized_formatter_output_refused(row):
    context = _context()
    context.connection.execute.return_value.fetchone.return_value = row
    with pytest.raises(ValueError, match="no formatted projection"):
        _definitions_match(context, ClickHouseProjection("p", "select value"), ClickHouseProjection("p", "SELECT value"))


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT date(ts)", "SELECT DATE(ts)"),
        ("SELECT DaTe(ts)", "SELECT DATE(ts)"),
        ("SELECT SUBSTR(label, 1, 2)", "SELECT substring(label, 1, 2)"),
        ("SELECT SuM(value)", "SELECT sum(value)"),
        ("SELECT aggregateAlias(value)", "SELECT aggregateCanonical(value)"),
        ("SELECT quantile(0.5)(value)", "SELECT quantile(0.5)(value)"),
        ("SELECT SUMIf(value, value > 13)", "SELECT SUMIf(value, value > 13)"),
        ("SELECT toString(value)", "SELECT toString(value)"),
        ("SELECT TOSTRING(value)", "SELECT TOSTRING(value)"),
        ("SELECT 'date(SuM(value))'", "SELECT 'date(SuM(value))'"),
        ("SELECT `date`, date(ts) AS `sum`", "SELECT `date`, DATE(ts) AS `sum`"),
        ("WITH value AS date SELECT date", "WITH value AS date SELECT date"),
        ("WITH (item -> item + 13) AS date SELECT date(value)", "WITH (item -> item + 13) AS date SELECT DATE(value)"),
        ("SELECT arrayMap(item -> date(item), values)", "SELECT arrayMap(item -> DATE(item), values)"),
        ("SELECT date /* comment */ (ts)", "SELECT DATE /* comment */ (ts)"),
        ("SELECT $body$date(ts)$body$", "SELECT $body$date(ts)$body$"),
        ("SELECT namespace.date(ts)", "SELECT namespace.date(ts)"),
        ("SELECT date(ts), date", "SELECT DATE(ts), date"),
    ],
)
def test_function_normalization_preserves_arguments_and_identifiers(query, expected):
    scalar = {"date": "DATE", "substr": "substring", "aggregatealias": "aggregateBase"}
    aggregate = {"sum": "sum", "aggregatebase": "aggregateCanonical"}
    assert _normalize_function_names(query, (scalar, aggregate)) == expected


def test_function_catalog_is_cached_per_autogenerate_context():
    context = _context()
    context.connection.execute.return_value.__iter__.return_value = iter(
        [
            SimpleNamespace(name="DATE", alias_to="", is_aggregate="0"),
            SimpleNamespace(name="substr", alias_to="substring", is_aggregate="0"),
            SimpleNamespace(name="sum", alias_to="", is_aggregate="1"),
        ]
    )
    expected = {"date": "DATE", "substr": "substring"}, {"sum": "sum"}
    assert _function_names(context) == expected
    assert _function_names(context) == expected
    context.connection.execute.assert_called_once()
    statement = context.connection.execute.call_args.args[0]
    assert "WHERE case_insensitive = 1" in str(statement)
    assert "alias_to" in str(statement)


@pytest.mark.parametrize("rows", [[], [SimpleNamespace(name="DATE", alias_to="", is_aggregate="0")]])
def test_incomplete_function_metadata_refused(rows):
    context = _context()
    context.connection.execute.return_value.__iter__.return_value = iter(rows)
    with pytest.raises(ValueError, match="incomplete case-insensitive function metadata"):
        _function_names(context)


def test_lexically_invalid_formatted_projection_is_refused():
    with pytest.raises(ValueError, match="Cannot tokenize"):
        _normalize_function_names("SELECT 'unfinished", ({}, {}))


def test_string_aggregate_flags_keep_scalar_and_aggregate_functions_separate():
    context = _context()
    context.connection.execute.return_value.__iter__.return_value = iter(
        [
            SimpleNamespace(name="DATE", alias_to="", is_aggregate="0"),
            SimpleNamespace(name="sum", alias_to="", is_aggregate="1"),
        ]
    )
    assert _function_names(context) == ({"date": "DATE"}, {"sum": "sum"})
