from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.exceptions import ProgrammingError


def _context(statement, dialect, column_keys=None, schema_translate_map=None):
    compiled = statement.compile(
        dialect=dialect,
        column_keys=column_keys,
        schema_translate_map=schema_translate_map,
    )
    execution_options = {}
    if schema_translate_map is not None:
        execution_options["schema_translate_map"] = schema_translate_map
    return SimpleNamespace(
        compiled=compiled,
        execution_options=execution_options,
        invoked_statement=statement,
    )


def _query_result(summary=None):
    return SimpleNamespace(result_set=[], column_names=[], column_types=[], summary=summary or {})


def _insert_result(written_rows):
    return SimpleNamespace(written_rows=written_rows, summary={"written_rows": str(written_rows)})


def test_plain_compiled_insert_uses_ordered_native_plan():
    dialect = ClickHouseDialect(dbapi=dbapi)
    metadata = sa.MetaData()
    table = sa.Table(
        "events%2026",
        metadata,
        sa.Column("db_id", sa.Integer, key="id"),
        sa.Column("db_name", sa.String, key="name"),
        schema="analytics",
    )
    statement = sa.insert(table)
    context = _context(statement, dialect, ["id", "name"])
    client = Mock()
    client.insert.return_value = _insert_result(2)
    cursor = Cursor(client)

    dialect.do_executemany(
        cursor,
        str(context.compiled),
        [{"name": "user_1", "id": 13}, {"id": 79, "name": "user_2"}],
        context,
    )

    client.insert.assert_called_once_with(
        "`analytics`.`events%2026`",
        [[13, "user_1"], [79, "user_2"]],
        ("db_id", "db_name"),
        settings=None,
    )
    client.query.assert_not_called()
    assert cursor.rowcount == 2


def test_plain_compiled_insert_supports_schema_translation_and_python_defaults():
    dialect = ClickHouseDialect(dbapi=dbapi)
    metadata = sa.MetaData()
    table = sa.Table(
        "events",
        metadata,
        sa.Column("id", sa.Integer),
        sa.Column("category", sa.String, default=lambda: "default"),
        sa.Column("created", sa.String, server_default=sa.text("'server'")),
        schema="logical",
    )
    statement = sa.insert(table)
    context = _context(statement, dialect, ["id"], {"logical": "physical"})

    plan = dialect._ch_native_insert_plan(context, {"max_threads": 3})

    assert plan is not None
    assert plan.table == "`physical`.`events`"
    assert plan.column_names == ("id", "category")
    assert plan.parameter_keys == ("id", "category")
    assert plan.settings == {"max_threads": 3}


def test_escaped_bind_name_uses_compiled_parameter_key():
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("value%pct", sa.Integer))
    statement = sa.insert(table)
    context = _context(statement, dialect, ["value%pct"])
    client = Mock()
    client.insert.return_value = _insert_result(2)
    cursor = Cursor(client)

    dialect.do_executemany(cursor, str(context.compiled), [{"valuePpct": 13}, {"valuePpct": 79}], context)

    client.insert.assert_called_once_with("`events`", [[13], [79]], ("value%pct",), settings=None)


def test_bind_expression_type_does_not_get_native_plan():
    class HexString(sa.TypeDecorator):
        impl = sa.String
        cache_ok = True

        def bind_expression(self, bindvalue):
            return sa.func.hex(bindvalue)

    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table(
        "events",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("value", HexString()),
    )
    statement = sa.insert(table)
    context = _context(statement, dialect, ["id", "value"])

    assert "hex(" in str(context.compiled).lower()
    assert dialect._ch_native_insert_plan(context, None) is None


def test_native_plan_uses_compile_state_statement_table():
    dialect = ClickHouseDialect(dbapi=dbapi)
    metadata = sa.MetaData()
    base_table = sa.Table("base_events", metadata, sa.Column("id", sa.Integer))
    child_table = sa.Table("child_events", metadata, sa.Column("id", sa.Integer))
    context = _context(sa.insert(base_table), dialect, ["id"])
    context.compiled.statement = sa.insert(child_table)

    plan = dialect._ch_native_insert_plan(context, None)

    assert plan is not None
    assert plan.table == "`base_events`"


@pytest.mark.parametrize("missing_attr", ["bind_names", "escaped_bind_names", "insert_prefetch"])
def test_native_plan_fails_closed_when_required_compiled_attribute_is_missing(missing_attr):
    class MissingAttribute:
        def __init__(self, compiled):
            self._compiled = compiled

        def __getattr__(self, name):
            if name == missing_attr:
                raise AttributeError(name)
            return getattr(self._compiled, name)

    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("id", sa.Integer))
    context = _context(sa.insert(table), dialect, ["id"])
    context.compiled = MissingAttribute(context.compiled)

    assert dialect._ch_native_insert_plan(context, None) is None


@pytest.mark.parametrize("missing_attr", ["_values", "_independent_ctes"])
def test_native_plan_fails_closed_when_effective_statement_attribute_is_missing(missing_attr):
    class MissingAttributeInsert(sa.sql.dml.Insert):
        def __init__(self, table):
            self._missing_attr = None
            super().__init__(table)
            self._missing_attr = missing_attr

        def __getattribute__(self, name):
            if name == object.__getattribute__(self, "_missing_attr"):
                raise AttributeError(name)
            return super().__getattribute__(name)

    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("id", sa.Integer))
    context = _context(sa.insert(table), dialect, ["id"])
    context.compiled.compile_state.statement = MissingAttributeInsert(table)

    assert dialect._ch_native_insert_plan(context, None) is None


@pytest.mark.skipif(sa.__version__.startswith("1."), reason="SQLAlchemy 1.4 does not expose these fields")
@pytest.mark.parametrize("missing_attr", ["effective_returning", "implicit_returning"])
def test_native_plan_fails_closed_when_sqlalchemy_2_returning_attribute_is_missing(missing_attr):
    class MissingAttribute:
        def __init__(self, compiled):
            self._compiled = compiled

        def __getattr__(self, name):
            if name == missing_attr:
                raise AttributeError(name)
            return getattr(self._compiled, name)

    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("id", sa.Integer))
    context = _context(sa.insert(table), dialect, ["id"])
    context.compiled = MissingAttribute(context.compiled)

    assert dialect._ch_native_insert_plan(context, None) is None


def test_native_plan_falls_back_when_dialect_bind_type_resolution_fails():
    class InvalidBindType:
        @staticmethod
        def dialect_impl(_dialect):
            raise RuntimeError("cannot resolve bind type")

    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("id", sa.Integer))
    context = _context(sa.insert(table), dialect, ["id"])
    bind = next(iter(context.compiled.bind_names))
    bind.type = InvalidBindType()

    assert dialect._ch_native_insert_plan(context, None) is None


@pytest.mark.parametrize(
    "statement_factory",
    [
        lambda table: sa.insert(table).values(id=13),
        lambda table: sa.insert(table).values(id=sa.func.abs(sa.bindparam("value"))),
        lambda table: sa.insert(table).values([{"id": 13}, {"id": 79}]),
        lambda table: sa.insert(table).from_select(["id"], sa.select(sa.literal(13))),
        lambda table: sa.insert(table).prefix_with("FOO"),
        lambda table: sa.insert(table).returning(table.c.id),
    ],
)
def test_non_plain_compiled_inserts_do_not_get_native_plan(statement_factory):
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table("events", sa.MetaData(), sa.Column("id", sa.Integer))
    statement = statement_factory(table)
    try:
        context = _context(statement, dialect)
    except sa.exc.CompileError:
        pytest.skip("SQLAlchemy rejects this statement before dialect execution")

    assert dialect._ch_native_insert_plan(context, None) is None


def test_sql_expression_default_does_not_get_native_plan():
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table(
        "events",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("created", sa.String, default=sa.func.toString(sa.func.now())),
    )
    statement = sa.insert(table)
    context = _context(statement, dialect, ["id"])

    assert dialect._ch_native_insert_plan(context, None) is None


def test_raw_and_expression_insert_executemany_preserve_sql_and_parameters():
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table(
        "events",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("name", sa.String),
    )
    statement = sa.insert(table).values(id=sa.bindparam("source_id"), name=sa.func.hex(sa.bindparam("source_name")))
    context = _context(statement, dialect)
    operation = str(context.compiled)
    parameters = [
        {"source_name": b"user_1", "source_id": 13},
        {"source_id": 79, "source_name": b"user_2"},
    ]
    client = Mock()
    client.query.return_value = _query_result({"written_rows": "1"})
    cursor = Cursor(client)

    dialect.do_executemany(cursor, operation, parameters, context)

    assert [item.args[:2] for item in client.query.call_args_list] == [(operation, row) for row in parameters]
    client.insert.assert_not_called()
    assert cursor.rowcount == 2


def test_text_insert_executemany_preserves_sql_and_parameters():
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.text("INSERT INTO events (id, name) VALUES (:id, hex(:name))")
    context = _context(statement, dialect)
    operation = str(context.compiled)
    parameters = [{"id": 13, "name": b"user_1"}, {"name": b"user_2", "id": 79}]
    client = Mock()
    client.query.return_value = _query_result({"written_rows": "1"})
    cursor = Cursor(client)

    dialect.do_executemany(cursor, operation, parameters, context)

    assert [item.args[:2] for item in client.query.call_args_list] == [(operation, row) for row in parameters]
    client.insert.assert_not_called()
    assert cursor.rowcount == 2


def test_native_plan_mapping_mismatch_fails_before_execution():
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.Table(
        "events",
        sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("name", sa.String),
    )
    statement = sa.insert(table)
    context = _context(statement, dialect, ["id", "name"])
    operation = str(context.compiled)
    parameters = [{"id": 13, "name": "user_1"}, {"id": 79}]
    client = Mock()
    client.query.return_value = _query_result({"written_rows": "1"})

    with pytest.raises(ProgrammingError, match="matching mapping parameters"):
        dialect.do_executemany(Cursor(client), operation, parameters, context)

    client.insert.assert_not_called()
    client.query.assert_not_called()
