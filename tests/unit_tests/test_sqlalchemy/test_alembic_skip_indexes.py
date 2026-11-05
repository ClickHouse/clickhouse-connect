from io import StringIO
from unittest.mock import Mock

import pytest
from alembic.autogenerate.api import AutogenContext, render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from alembic.util import CommandError
from sqlalchemy import Column, MetaData, Table
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex
from clickhouse_connect.cc_sqlalchemy.alembic import include_object
from clickhouse_connect.cc_sqlalchemy.alembic.indexes import (
    COMPARE_INDEXES_OPT,
    IndexComparisonError,
    _analyzed_expression,
    _compare_clickhouse_indexes,
    _definitions_match,
    _filter_created_table_indexes,
    _key_expression,
)
from clickhouse_connect.cc_sqlalchemy.alembic.operations import _AddClickHouseIndexOp, _DropClickHouseIndexOp
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.index import table_indexes
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import _INTERNAL_QUERY_OPTION, _INTERNAL_QUERY_SENTINEL, ChInspector
from clickhouse_connect.driver.exceptions import DatabaseError

INDEXES_MODULE = "clickhouse_connect.cc_sqlalchemy.alembic.indexes"


def _table(*indexes):
    return Table(
        "events",
        MetaData(),
        Column("value", UInt32),
        *indexes,
        MergeTree(order_by="value"),
        schema="warehouse",
    )


def _context(**options):
    context = AutogenContext(MigrationContext.configure(dialect=ClickHouseDialect(), opts=options))
    context.connection = Mock()
    context.connection.execute.return_value.scalar.return_value = True
    return context


def _compare(monkeypatch, old, new, context=None):
    context = context or _context(include_object=include_object)
    read = Mock(return_value={"events": list(old)})
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", read)
    operation = ops.ModifyTableOps("events", [], schema="warehouse")
    _compare_clickhouse_indexes(context, operation, "warehouse", "events", _table(), _table(*new))
    return context, operation, read


def test_add_remove_and_reversal(monkeypatch):
    old = ClickHouseIndex("old_idx", "(value)", "minmax", 1)
    new = ClickHouseIndex("new_idx", "value + 13", "set(79)", 4)
    _, operation, _ = _compare(monkeypatch, [old], [new])
    assert [type(op) for op in operation.ops] == [_AddClickHouseIndexOp, _DropClickHouseIndexOp]
    add, drop = operation.ops
    assert drop.existing_index == old
    assert drop.reverse().expression == old.expression
    assert drop.reverse().granularity == 1
    assert add.reverse().reverse().expression == new.expression
    rendered = render_python_code(ops.UpgradeOps([operation]), migration_context=_context().migration_context)
    assert "op.add_clickhouse_index('events', 'new_idx', 'value + 13', 'set(79)', granularity=4, schema='warehouse')" in rendered
    assert "op.drop_clickhouse_index('events', 'old_idx', schema='warehouse')" in rendered
    assert "materialize" not in rendered


def test_edit_is_adjacent_drop_add_and_policy_visible(monkeypatch, caplog):
    caplog.set_level("INFO", logger="alembic.autogenerate.compare.clickhouse_indexes")
    old = ClickHouseIndex("value_idx", "value", "minmax", 1)
    new = ClickHouseIndex("value_idx", "value + 13", "set(79)", 4)
    context = _context()
    context.connection.execute.return_value.fetchone.return_value = (
        "old declaration",
        "old shape",
        "SELECT value",
        "new declaration",
        "new shape",
        "SELECT value + 13",
    )
    _, operation, _ = _compare(monkeypatch, [old], [new], context)
    assert [type(op) for op in operation.ops] == [_DropClickHouseIndexOp, _AddClickHouseIndexOp]
    drop, add = operation.ops
    assert drop.existing_index == old
    assert (add.expression, add.type_, add.granularity) == ("value + 13", "set(79)", 4)
    assert "Detected changed index" in caplog.text
    reverse = operation.reverse()
    assert [type(op) for op in reverse.ops] == [_DropClickHouseIndexOp, _AddClickHouseIndexOp]
    assert reverse.ops[1].expression == "value"


def test_unchanged_ignores_position_and_if_not_exists(monkeypatch):
    old = ClickHouseIndex("value_idx", "value", "minmax", 1)
    new = ClickHouseIndex("value_idx", "value", "minmax", 1, if_not_exists=True, first=True)
    context, operation, _ = _compare(monkeypatch, [old], [new])
    assert operation.is_empty()
    context.connection.execute.assert_not_called()


@pytest.mark.parametrize("enabled", [False, True])
def test_opt_out_precedes_catalog_read(monkeypatch, enabled):
    context = _context(**{COMPARE_INDEXES_OPT: enabled})
    read = Mock(side_effect=SQLAlchemyError("catalog denied"))
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", read)
    operation = ops.ModifyTableOps("events", [])
    if enabled:
        with pytest.raises(IndexComparisonError, match="catalog denied") as excinfo:
            _compare_clickhouse_indexes(context, operation, None, "events", _table(), _table())
        assert excinfo.value.index_name is None
        assert excinfo.value.reason.startswith("cannot read explicit index catalog")
    else:
        _compare_clickhouse_indexes(context, operation, None, "events", _table(), _table())
        read.assert_not_called()


def test_normalization_failure_is_structured(monkeypatch):
    before = ClickHouseIndex("value_idx", "(value)", "minmax", 1)
    after = ClickHouseIndex("value_idx", "value + 13", "minmax")
    context = _context()
    context.connection.execute.side_effect = SQLAlchemyError("formatter unavailable")
    with pytest.raises(IndexComparisonError, match="formatter unavailable") as excinfo:
        _compare(monkeypatch, [before], [after], context)
    error = excinfo.value
    assert (error.schema, error.table_name, error.index_name) == ("warehouse", "events", "value_idx")
    assert error.reflected == before
    assert error.declared == after


@pytest.mark.parametrize("kind", ["add", "remove", "edit"])
def test_object_filter_arguments(monkeypatch, kind):
    old = ClickHouseIndex("value_idx", "value", "minmax", 1)
    new = ClickHouseIndex("value_idx", "value + 13", "minmax", 1)
    calls = []

    def filter_object(obj, name, type_, reflected, compare_to):
        calls.append((obj, name, type_, reflected, compare_to))
        assert obj.table.name == "events"
        assert obj.table.schema == "warehouse"
        assert isinstance(obj, ClickHouseIndex)
        return False

    context = _context(include_object=filter_object)
    _, operation, _ = _compare(monkeypatch, [] if kind == "add" else [old], [] if kind == "remove" else [new], context)
    assert operation.is_empty()
    obj, name, type_, reflected, compare_to = calls[0]
    assert (name, type_, reflected) == ("value_idx", "index", kind == "remove")
    assert (compare_to.expression if compare_to else None) == ("value" if kind == "edit" else None)
    assert obj.expression == ("value" if kind == "remove" else "value + 13")
    context.connection.execute.assert_not_called()


def test_name_filter_leaves_matching_declared_index_unmanaged(monkeypatch):
    names = []

    def filter_name(name, type_, parents):
        names.append((name, type_, parents))
        return False

    index = ClickHouseIndex("value_idx", "value", "minmax", 1)
    context = _context(include_name=filter_name)
    _, operation, _ = _compare(monkeypatch, [index], [index], context)
    assert operation.is_empty()
    assert all(
        parents == {"schema_name": "warehouse", "table_name": "events", "schema_qualified_table_name": "warehouse.events"}
        for _, _, parents in names
    )


def test_created_table_filters_inline_indexes_without_changing_metadata():
    excluded = ClickHouseIndex("excluded", "value", "minmax")
    included = ClickHouseIndex("included", "value", "minmax")
    table = _table(excluded, included)
    context = _context(include_object=lambda obj, name, type_, reflected, compare_to: name != "excluded")
    operation = ops.CreateTableOp.from_table(table)
    upgrade = ops.UpgradeOps([operation])
    _filter_created_table_indexes(context, upgrade, {"warehouse"})
    assert operation.kw["clickhousedb_indexes"] == (included,)
    assert table_indexes(table) == (excluded, included)
    rendered = render_python_code(upgrade, migration_context=context.migration_context)
    assert "ClickHouseIndex('included'" in rendered
    assert "ClickHouseIndex('excluded'" not in rendered
    assert "add_clickhouse_index" not in rendered


def test_removed_table_snapshot_keeps_indexes(monkeypatch):
    index = ClickHouseIndex("value_idx", "(value)", "minmax", 1)
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", lambda *args: {"events": [index]})
    table = _table()
    context = _context()
    operation = ops.ModifyTableOps("events", [])
    _compare_clickhouse_indexes(context, operation, "warehouse", "events", table, None)
    assert operation.is_empty()
    recreate = ops.DropTableOp.from_table(table).reverse()
    assert recreate.kw["clickhousedb_indexes"] == (index,)
    assert "ClickHouseIndex(" in render_python_code(ops.UpgradeOps([recreate]), migration_context=context.migration_context)


def test_offline_replacement_keeps_two_statements():
    output = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    operation = Operations(context)
    before = ClickHouseIndex("quoted index", "(value)", "minmax", 1)
    drop = _DropClickHouseIndexOp("quoted table", before.name, schema="quoted database", existing_index=before)
    add = _AddClickHouseIndexOp("quoted table", before.name, "value + 13", "set(79)", schema="quoted database")
    operation.invoke(drop)
    operation.invoke(add)
    assert output.getvalue().splitlines() == [
        "ALTER TABLE `quoted database`.`quoted table` DROP INDEX `quoted index`;",
        "",
        "ALTER TABLE `quoted database`.`quoted table` ADD INDEX `quoted index` value + 13 TYPE set(79);",
        "",
    ]


@pytest.mark.parametrize(
    ("expression", "root", "expected"),
    [
        ("value", None, "tuple(value)"),
        ("((value))", None, "tuple(value)"),
        ("tuple(value)", None, "tuple(value)"),
        ("tuple(tuple(value, 13))", None, "tuple(tuple(value, 13))"),
        (
            "(value, 13)",
            "      FUNCTION id: 2, alias: _index, function_name: tuple, function_type: ordinary",
            "tuple(value, 13)",
        ),
        (
            "(13, 79)",
            "      CONSTANT id: 2, alias: _index, constant_value: (13, 79), constant_value_type: Tuple(UInt64, UInt64)",
            "tuple((13, 79))",
        ),
    ],
)
def test_index_key_list_normalization(expression, root, expected):
    context = _context()
    context.connection.execute.return_value.fetchall.return_value = [
        (line,) for line in ["QUERY id: 0", "  PROJECTION", "    LIST id: 1, nodes: 1", root]
    ]
    context.connection.exec_driver_sql.return_value.fetchall.return_value = context.connection.execute.return_value.fetchall.return_value
    assert _key_expression(context, expression) == expected
    assert context.connection.execute.call_count + context.connection.exec_driver_sql.call_count == (2 if root else 0)


def test_unknown_server_tree_shape_refused():
    context = _context()
    context.connection.execute.return_value.fetchall.return_value = [("changed output format",)]
    context.connection.exec_driver_sql.return_value.fetchall.return_value = [("changed output format",)]
    with pytest.raises(ValueError, match="Cannot identify"):
        _key_expression(context, "(value, 13)")


def test_batch_recreation_preserves_inline_indexes():
    output = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    table = _table(ClickHouseIndex("value_idx", "value", "minmax"))
    with Operations(context).batch_alter_table("events", schema="warehouse", copy_from=table, recreate="always") as batch:
        batch.add_column(Column("extra", UInt32))
    sql = output.getvalue()
    assert sql.count("INDEX `value_idx` value TYPE minmax") == 1
    assert "ADD INDEX" not in sql
    assert "CREATE TABLE `warehouse`.`_alembic_tmp_events`" in sql


@pytest.mark.parametrize("kind", ["add", "drop", "edit"])
def test_generated_index_operations_refuse_batch_rendering(kind):
    old = ClickHouseIndex("value_idx", "value", "minmax", 1)
    drop = _DropClickHouseIndexOp("events", old.name, existing_index=old)
    add = _AddClickHouseIndexOp("events", old.name, "value + 13", "minmax")
    operations = [drop, add] if kind == "edit" else [add if kind == "add" else drop]
    upgrade = ops.UpgradeOps([ops.ModifyTableOps("events", operations)])
    with pytest.raises(CommandError, match="render_as_batch=False"):
        render_python_code(upgrade, migration_context=_context().migration_context, render_as_batch=True)


def test_autogenerated_additions_ignore_imperative_placement(monkeypatch):
    indexes = [
        ClickHouseIndex("a", "value", "minmax", if_not_exists=True, after_index="z"),
        ClickHouseIndex("z", "value", "minmax", first=True),
    ]
    _, operation, _ = _compare(monkeypatch, [], indexes)
    assert [op.name for op in operation.ops] == ["a", "z"]
    assert all((op.if_not_exists, op.first, op.after_index) == (False, False, None) for op in operation.ops)


def test_autogenerated_replacement_ignores_stale_placement(monkeypatch):
    old = ClickHouseIndex("value_idx", "value", "minmax", 1)
    new = ClickHouseIndex("value_idx", "value", "set(79)", 1, if_not_exists=True, after_index="missing")
    context = _context()
    context.connection.execute.return_value.fetchone.return_value = (
        "old declaration",
        "old shape",
        "SELECT value",
        "new declaration",
        "new shape",
        "SELECT value",
    )
    _, operation, _ = _compare(monkeypatch, [old], [new], context)
    assert (operation.ops[1].if_not_exists, operation.ops[1].first, operation.ops[1].after_index) == (False, False, None)


@pytest.mark.parametrize("filter_kind", ["name", "object"])
def test_legacy_inline_indexes_obey_create_filters(filter_kind):
    excluded = ClickHouseIndex("excluded", "value", "minmax")
    table = _table()
    table.kwargs["clickhouse_indexes"] = (excluded,)
    options = (
        {
            "include_name": lambda name, kind, parents: name != "excluded",
        }
        if filter_kind == "name"
        else {
            "include_object": lambda obj, name, kind, reflected, compare_to: name != "excluded",
        }
    )
    context = _context(**options)
    operation = ops.CreateTableOp.from_table(table)
    upgrade = ops.UpgradeOps([operation])
    _filter_created_table_indexes(context, upgrade, {"warehouse"})
    assert "ClickHouseIndex('excluded'" not in render_python_code(upgrade, migration_context=context.migration_context)
    assert operation.kw["clickhousedb_indexes"] == ()
    assert "clickhouse_indexes" not in operation.kw


def test_existing_table_comparison_reads_legacy_declarations(monkeypatch):
    index = ClickHouseIndex("value_idx", "value", "minmax", 1)
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", lambda *args: {"events": [index]})
    metadata_table = _table()
    metadata_table.kwargs["clickhouse_indexes"] = (index,)
    operation = ops.ModifyTableOps("events", [])
    _compare_clickhouse_indexes(_context(), operation, "warehouse", "events", _table(), metadata_table)
    assert operation.is_empty()


def test_standard_index_error_points_to_declarative_api():
    with pytest.raises(CommandError, match="ClickHouseIndex"):
        include_object(Mock(), "unsupported", "index", False, None)


def test_raw_explain_preserves_colon_literal_and_metadata_options():
    context = _context()
    rows = [
        (line,)
        for line in [
            "QUERY id: 0",
            "  PROJECTION",
            "    LIST id: 1, nodes: 1",
            "      FUNCTION id: 2, alias: _index, function_name: tuple, function_type: ordinary",
        ]
    ]
    context.connection.execute.return_value.fetchall.return_value = rows
    context.connection.exec_driver_sql.return_value.fetchall.return_value = rows
    expression = "(value, concat(label, ':tag'))"
    assert _key_expression(context, expression) == "tuple(value, concat(label, ':tag'))"
    context.connection.exec_driver_sql.assert_called_once()
    args, kwargs = context.connection.exec_driver_sql.call_args
    assert expression in args[0]
    options = kwargs["execution_options"]
    assert options[_INTERNAL_QUERY_OPTION] is _INTERNAL_QUERY_SENTINEL
    assert options["query_formats"]["String"] == "string"


@pytest.mark.parametrize("kind", ["scalar", "tuple"])
def test_analyzer_dependent_normalization_requires_enabled_analyzer(kind):
    context = _context()
    context.connection.execute.return_value.scalar.return_value = False
    context.connection.exec_driver_sql.return_value.fetchall.return_value = [
        (line,)
        for line in [
            "QUERY id: 0",
            "  PROJECTION",
            "    LIST id: 1, nodes: 1",
            "      FUNCTION id: 2, alias: _index, function_name: tuple, function_type: ordinary",
        ]
    ]
    with pytest.raises(ValueError, match="enable_analyzer"):
        if kind == "scalar":
            _analyzed_expression(context, "tuple(date(value))", _table())
        else:
            _key_expression(context, "(value, 13)")
    context.connection.exec_driver_sql.assert_not_called()
    statement = context.connection.execute.call_args.args[0]
    assert "getSetting('enable_analyzer')" in str(statement)
    assert statement.get_execution_options()[_INTERNAL_QUERY_OPTION] is _INTERNAL_QUERY_SENTINEL


def test_analyzer_setting_is_cached_per_autogenerate_context():
    context = _context()
    context.connection.execute.return_value.scalar.side_effect = [True, False]
    context.connection.exec_driver_sql.return_value.fetchall.return_value = [("SELECT tuple(value)",)]
    for _ in range(2):
        assert _analyzed_expression(context, "tuple(value)", _table()) == ["SELECT tuple(value)"]
    assert context.connection.execute.call_count == 1
    other_context = _context()
    other_context.connection = context.connection
    with pytest.raises(ValueError, match="enable_analyzer"):
        _analyzed_expression(other_context, "tuple(value)", _table())
    assert context.connection.execute.call_count == 2


def test_index_catalog_is_cached_per_schema_and_autogenerate_context(monkeypatch):
    context = _context()
    context.connection.execute.return_value.scalar.return_value = "warehouse"
    read = Mock(return_value={})
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", read)
    for schema in (None, None, "warehouse", "other", "other"):
        operation = ops.ModifyTableOps("events", [], schema=schema)
        _compare_clickhouse_indexes(context, operation, schema, "events", _table(), _table())
        assert operation.is_empty()
    assert [call.args[1] for call in read.call_args_list] == ["warehouse", "other"]
    context.connection.execute.assert_called_once()
    other = _context()
    _compare_clickhouse_indexes(other, ops.ModifyTableOps("events", []), "warehouse", "events", _table(), _table())
    assert read.call_count == 3


@pytest.mark.parametrize("filtered", [False, True])
def test_removed_table_without_managed_indexes_omits_snapshot(monkeypatch, filtered):
    index = ClickHouseIndex("value_idx", "value", "minmax", 1)
    monkeypatch.setattr(f"{INDEXES_MODULE}._get_clickhouse_index_catalog", lambda *args: {"events": [index]} if filtered else {})
    context = _context(include_object=lambda *args: False)
    table = _table()
    _compare_clickhouse_indexes(context, ops.ModifyTableOps("events", []), "warehouse", "events", table, None)
    drop = ops.DropTableOp.from_table(table)
    assert "clickhousedb_indexes" not in drop.table_kw
    for operation in (drop, drop.reverse()):
        assert "indexes=" not in render_python_code(ops.UpgradeOps([operation]), migration_context=context.migration_context)


@pytest.mark.parametrize(("declared_type", "reflected_type"), [("minmax", "MinMax()"), ("MinMax()", "minmax"), ("MINMAX()", "minmax()")])
def test_empty_type_arguments_compare_equally(declared_type, reflected_type):
    context = _context()
    declared = ClickHouseIndex("value_idx", "value", declared_type, 1)
    reflected = ClickHouseIndex("value_idx", "tuple(value)", reflected_type, 1)
    context.connection.execute.return_value.fetchone.return_value = (
        "full declared definition",
        f"CREATE TABLE _table (_value UInt8, INDEX _index _value TYPE {declared_type} GRANULARITY 1)",
        "SELECT value",
        "full reflected definition",
        f"CREATE TABLE _table (_value UInt8, INDEX _index _value TYPE {reflected_type} GRANULARITY 1)",
        "SELECT tuple(value)",
    )
    assert _definitions_match(context, declared, reflected, _table(), _table())
    context.connection.exec_driver_sql.assert_not_called()
    assert (declared.type_, reflected.type_) == (declared_type, reflected_type)


@pytest.mark.parametrize(
    ("identifier", "code", "is_alias", "is_reflected", "changed"),
    [
        ("extra", 47, False, False, True),
        ("missing", 47, False, False, False),
        ("extra", 47, True, False, False),
        ("value", 47, False, False, False),
        ("extra", 47, False, True, False),
        ("extra", 46, False, False, False),
    ],
)
@pytest.mark.parametrize("lookup_kind", ["expression", "expression or function"])
@pytest.mark.parametrize("scope_separator", [" in scope", ". In scope", ". Maybe you meant: ['value']. In scope"])
def test_unknown_identifier_requires_new_physical_declared_column(
    identifier, code, is_alias, is_reflected, changed, lookup_kind, scope_separator
):
    context = _context()
    declared = ClickHouseIndex("value_idx", "extra", "minmax", 1)
    reflected = ClickHouseIndex("value_idx", "value", "minmax", 1)
    metadata_table = _table()
    added_column = Column("extra", UInt32, key="extra_key", **({"clickhouse_alias": "value"} if is_alias else {}))
    metadata_table.append_column(added_column)
    live_table = _table()
    if is_alias:
        live_table.append_column(Column("extra", UInt32, clickhouse_alias="value"))
    context.connection.execute.return_value.fetchone.return_value = (
        "full declared definition",
        "same shape",
        "SELECT extra",
        "full reflected definition",
        "same shape",
        "SELECT value",
    )
    error = DBAPIError(
        None,
        None,
        DatabaseError(f"Unknown {lookup_kind} identifier `{identifier}`{scope_separator} SELECT ...", code=code),
    )
    result = Mock()
    result.fetchall.return_value = ["analyzed extra"]
    context.connection.exec_driver_sql.side_effect = [error, result] if changed else [result, error] if is_reflected else error
    if changed:
        assert not _definitions_match(context, declared, reflected, metadata_table, live_table, [added_column])
        assert r"CROSS JOIN null('\`extra\` UInt32')" in context.connection.exec_driver_sql.call_args.args[0]
    else:
        with pytest.raises(DBAPIError):
            _definitions_match(context, declared, reflected, metadata_table, live_table, [added_column])


def test_inspector_routes_explicit_index_reflection_through_dialect():
    connection = Mock()
    connection.engine = connection
    connection.dialect = ClickHouseDialect()
    connection.dialect.get_clickhouse_indexes = Mock(return_value=[])
    inspector = ChInspector._construct(ChInspector._init_connection, connection)
    assert inspector.get_clickhouse_indexes("events", "warehouse") == []
    connection.dialect.get_clickhouse_indexes.assert_called_once_with(connection, "events", "warehouse")
