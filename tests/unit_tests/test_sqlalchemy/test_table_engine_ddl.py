import subprocess
import sys
from dataclasses import replace
from io import StringIO
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy.dialects.sqlite import dialect as sqlite_dialect
from sqlalchemy.exc import ArgumentError, CompileError

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex, ClickHouseProjection, engines, types
from clickhouse_connect.cc_sqlalchemy.alembic import ModifyClickHouseTableSettingsOp, ResetClickHouseTableSettingsOp
from clickhouse_connect.cc_sqlalchemy.alembic import operations as ch_operations
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import build_engine
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import render_settings


def _offline(dialect=None):
    output = StringIO()
    context = MigrationContext.configure(dialect=dialect or ClickHouseDialect(), opts={"as_sql": True, "output_buffer": output})
    return Operations(context), output


@pytest.mark.parametrize("with_definitions", [False, True])
@pytest.mark.parametrize("if_not_exists", [None, False, True])
def test_create_table_positional_items_match_kwargs(with_definitions, if_not_exists):
    engine = engines.MergeTree(order_by="id")
    indexes = (ClickHouseIndex("id_idx", "id", "minmax", 1),) if with_definitions else ()
    projections = (ClickHouseProjection("id_projection", "SELECT id ORDER BY id"),) if with_definitions else ()
    positional, positional_sql = _offline()
    keyword, keyword_sql = _offline()
    operation = ops.CreateTableOp(
        "events",
        [sa.Column("id", types.UInt32), engine, *indexes, *projections],
        if_not_exists=if_not_exists,
    )
    original_columns = list(operation.columns)
    table = positional.invoke(operation)
    keyword.create_table(
        "events",
        sa.Column("id", types.UInt32),
        clickhousedb_engine=engine,
        clickhousedb_indexes=indexes,
        clickhousedb_projections=projections,
        if_not_exists=if_not_exists,
    )
    assert positional_sql.getvalue() == keyword_sql.getvalue()
    assert table.kwargs["clickhousedb_engine"] is engine
    assert tuple(operation.columns) == tuple(original_columns)
    assert operation.kw == {}
    if with_definitions:
        assert "INDEX `id_idx` id TYPE minmax GRANULARITY 1" in positional_sql.getvalue()
        assert "PROJECTION `id_projection` (SELECT id ORDER BY id)" in positional_sql.getvalue()


@pytest.mark.parametrize("engine_keyword", ["clickhousedb_engine", "clickhouse_engine"])
def test_create_table_positional_engine_matches_table_precedence(engine_keyword):
    positional = engines.MergeTree(order_by="id")
    keyword = engines.Null()
    operations, output = _offline()
    table = operations.create_table("events", sa.Column("id", types.UInt32), positional, **{engine_keyword: keyword})
    expected = sa.Table("events", sa.MetaData(), sa.Column("id", types.UInt32), positional, **{engine_keyword: keyword})
    assert table.engine is positional
    assert dict(table.kwargs) == dict(expected.kwargs)
    assert output.getvalue() == "CREATE TABLE `events` (`id` UInt32) Engine MergeTree ORDER BY id;\n\n"
    assert output.getvalue().strip() == str(sa.schema.CreateTable(expected).compile(dialect=ClickHouseDialect())) + ";"


@pytest.mark.parametrize("engine_keyword", ["clickhousedb_engine", "clickhouse_engine"])
def test_create_table_positional_engine_with_none_keyword(engine_keyword):
    engine = engines.MergeTree(order_by="id")
    operations, output = _offline()
    table = operations.create_table("events", sa.Column("id", types.UInt32), engine, **{engine_keyword: None})
    expected = sa.Table("events", sa.MetaData(), sa.Column("id", types.UInt32), engine, **{engine_keyword: None})
    assert table.kwargs["clickhousedb_engine"] is engine
    assert output.getvalue().strip() == str(sa.schema.CreateTable(expected).compile(dialect=ClickHouseDialect())) + ";"


@pytest.mark.parametrize(
    "items, keyword",
    [
        (
            [ClickHouseIndex("id_idx", "id", "minmax"), ClickHouseIndex("id_idx", "id + 1", "minmax")],
            {},
        ),
        (
            [ClickHouseProjection("id_projection", "SELECT id"), ClickHouseProjection("id_projection", "SELECT id + 1")],
            {},
        ),
        (
            [ClickHouseIndex("id_idx", "id", "minmax")],
            {"clickhousedb_indexes": (ClickHouseIndex("id_idx", "id + 1", "minmax"),)},
        ),
    ],
)
def test_create_table_positional_duplicate_checks(items, keyword):
    operations, _ = _offline()
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse"):
        operations.create_table("events", sa.Column("id", types.UInt32), engines.MergeTree(order_by="id"), *items, **keyword)


@pytest.mark.parametrize("same_object", [False, True])
@pytest.mark.parametrize("use_keyword", [False, True])
@pytest.mark.parametrize(
    "definition, option",
    [
        (ClickHouseIndex("id_idx", "id", "minmax"), "clickhousedb_indexes"),
        (ClickHouseProjection("id_projection", "SELECT id ORDER BY id"), "clickhousedb_projections"),
    ],
)
def test_create_table_identical_duplicates_match_table(definition, option, same_object, use_keyword):
    duplicate = definition if same_object else replace(definition)
    items = [definition] if use_keyword else [definition, duplicate]
    kwargs = {option: (duplicate,)} if use_keyword else {}
    operations, _ = _offline()
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse"):
        sa.Table("events", sa.MetaData(), sa.Column("id", types.UInt32), *items, **kwargs)
    with pytest.raises(ArgumentError, match="Duplicate ClickHouse"):
        operations.create_table("events", sa.Column("id", types.UInt32), engines.MergeTree(order_by="id"), *items, **kwargs)


def test_create_table_still_requires_engine():
    operations, _ = _offline()
    with pytest.raises(CompileError, match="engine"):
        operations.create_table("events", sa.Column("id", types.UInt32))


def test_create_table_other_dialect_matches_stock():
    operations, actual = _offline(sqlite_dialect())
    stock_operations, expected = _offline(sqlite_dialect())
    table = operations.create_table("events", sa.Column("id", sa.Integer), if_not_exists=True)
    stock_table = ch_operations._DEFAULT_CREATE_TABLE_IMPL(
        stock_operations, ops.CreateTableOp("events", [sa.Column("id", sa.Integer)], if_not_exists=True)
    )
    assert table.name == stock_table.name
    assert actual.getvalue() == expected.getvalue()


def test_create_table_implementation_reload_safe():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib
from io import StringIO
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from clickhouse_connect.cc_sqlalchemy.alembic import operations
from clickhouse_connect.cc_sqlalchemy import engines, types
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
stock = operations._DEFAULT_CREATE_TABLE_IMPL
for _ in range(2):
    importlib.reload(operations)
assert operations._DEFAULT_CREATE_TABLE_IMPL is stock
for name in ("sqlite", "clickhousedb"):
    output = StringIO()
    context = MigrationContext.configure(
        dialect=ClickHouseDialect() if name == "clickhousedb" else sa.create_engine("sqlite://").dialect,
        opts={"as_sql": True, "output_buffer": output},
    )
    args = [sa.Column("id", types.UInt32), engines.MergeTree(order_by="id")] if name == "clickhousedb" else [sa.Column("id", sa.Integer)]
    Operations(context).create_table("events", *args)
    assert "CREATE TABLE" in output.getvalue()
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "raw, literal",
    [
        ("it's", r"'it\'s'"),
        (r"path\x", r"'path\\x'"),
        (r"it's\x", r"'it\'s\\x'"),
    ],
)
@pytest.mark.parametrize(
    "engine_cls, arguments",
    [
        (engines.ReplicatedMergeTree, ("zk_path", "replica")),
        (engines.GraphiteMergeTree, ("config_section",)),
    ],
)
def test_quoted_engine_arguments_round_trip(engine_cls, arguments, raw, literal):
    kwargs = dict.fromkeys(arguments, raw)
    engine = engine_cls(**kwargs, order_by="id")
    expected = f"Engine {engine_cls.__name__}({', '.join([literal] * len(arguments))}) ORDER BY id"
    assert engine.compile() == expected
    for name in arguments:
        assert f"{name}={raw!r}" in repr(engine)
    reflected = build_engine(engine.compile().removeprefix("Engine "))
    assert reflected is not None
    for name in arguments:
        assert reflected._orig_kwargs[name] == raw
    assert eval(repr(reflected), vars(engines)).compile() == expected
    assert eval(repr(engine), vars(engines)).compile() == expected


def test_reflected_quoted_engine_argument_raw_repr():
    reflected = build_engine(r"ReplicatedMergeTree('/ch/it\'s\\x', 'r1') ORDER BY id")
    assert reflected is not None
    assert reflected._orig_kwargs["zk_path"] == r"/ch/it's\x"
    assert repr(reflected) == """ReplicatedMergeTree(zk_path="/ch/it's\\\\x", replica='r1', order_by='id')"""
    assert eval(repr(reflected), vars(engines)).compile() == r"Engine ReplicatedMergeTree('/ch/it\'s\\x', 'r1') ORDER BY id"


def test_reflected_unquoted_engine_argument_keeps_quote_stripping():
    engine = build_engine(r"ReplacingMergeTree('version\\column') ORDER BY id")
    assert engine is not None
    assert engine._orig_kwargs["version"] == r"version\\column"


@pytest.mark.parametrize(
    "name",
    [
        "a = 1, b",
        "a..b",
        ".a",
        "a.",
        "1abc",
        "",
        "a b",
        "a-b",
        "SQL_\u00e9",
        "`unterminated",
        '"unterminated',
        "`a`b`",
        "SQL_a\n",
        "``",
        '""',
        13,
        None,
    ],
)
@pytest.mark.parametrize("entry", ["engine", "render", "modify", "reset", "adapter_modify", "adapter_reset", "add_column"])
def test_setting_names_require_identifiers(name, entry):
    operations, _ = _offline()
    with pytest.raises(ValueError, match="Invalid ClickHouse setting name") as error:
        if entry == "engine":
            engines.MergeTree(order_by="id", settings={name: 13})
        elif entry == "render":
            render_settings({name: 13})
        elif entry == "modify":
            ModifyClickHouseTableSettingsOp("events", {name: 13})
        elif entry == "reset":
            ResetClickHouseTableSettingsOp("events", [name])
        elif entry == "adapter_modify":
            operations.modify_clickhouse_table_settings("events", {name: 13})
        elif entry == "adapter_reset":
            operations.reset_clickhouse_table_settings("events", [name])
        else:
            operations.add_column("events", sa.Column("value", types.UInt32), clickhouse_settings={name: 13})
    assert repr(name) in str(error.value)


@pytest.mark.parametrize(
    "name",
    [
        "index_granularity",
        "_custom13",
        "Setting13",
        "a.b",
        "custom_tenant.id",
        "$",
        "$SQL_x",
        "SQL_$x",
        "`x`",
        "`SQL_a-b`",
        "`SQL_a``b`",
        '"SQL_q"',
        "SQL_1.`x y`",
        "`a = 1, max_threads`",
        r"`SQL_a\`b`",
        r'"SQL_a\"b"',
        '"SQL_a""b"',
    ],
)
def test_valid_setting_names_preserve_output(name):
    assert render_settings({name: 13}) == f"{name} = 13"
    assert engines.MergeTree(order_by="id", settings={name: 13}).compile() == f"Engine MergeTree ORDER BY id SETTINGS {name} = 13"
    assert ModifyClickHouseTableSettingsOp("events", {name: 13}).settings == {name: 13}
    assert ResetClickHouseTableSettingsOp("events", [name]).names == (name,)
    operations, output = _offline()
    operations.modify_clickhouse_table_settings("events", {name: 13})
    operations.reset_clickhouse_table_settings("events", [name])
    operations.add_column("events", sa.Column("value", types.UInt32), clickhouse_settings={name: 13})
    assert output.getvalue() == (
        f"ALTER TABLE `events` MODIFY SETTING {name} = 13;\n\n"
        f"ALTER TABLE `events` RESET SETTING {name};\n\n"
        f"ALTER TABLE `events` ADD COLUMN `value` UInt32 SETTINGS {name} = 13;\n\n"
    )


@pytest.mark.parametrize("as_sql", [False, True])
def test_alter_column_validates_settings_before_rename(as_sql, monkeypatch):
    output = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(), opts={"as_sql": as_sql, "output_buffer": output})
    execute = Mock(wraps=context.impl._exec if as_sql else None)
    monkeypatch.setattr(context.impl, "_exec", execute)
    with pytest.raises(ValueError, match="Invalid ClickHouse setting name"):
        Operations(context).alter_column(
            "events",
            "value",
            new_column_name="renamed_value",
            type_=types.UInt64,
            clickhouse_settings={"a = 1, b": 13},
        )
    execute.assert_not_called()
    assert output.getvalue() == ""


def test_reflected_setting_names_are_preserved():
    engine = build_engine("MergeTree ORDER BY id SETTINGS custom-name = 13")
    assert engine is not None
    assert engine.settings == {"custom-name": 13}
