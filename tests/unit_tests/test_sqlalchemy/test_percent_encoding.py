"""
Tests for correct handling of percent signs (%) in SQL queries through the
SQLAlchemy compilation pipeline.

See https://github.com/ClickHouse/clickhouse-connect/issues/297

The pyformat paramstyle (PEP 249) requires that literal % be doubled to %%
during compilation, and then unescaped by the DBAPI cursor.  This must work
correctly for both parameterized and non-parameterized queries.
"""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import Integer, column, select, table, text

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.preparer import ChIdentifierPreparer
from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.binding import finalize_query


def _make_dialect():
    """Create a dialect that mirrors real create_engine() behavior."""
    return ClickHouseDialect(dbapi=dbapi)


def _make_cursor():
    """Create a Cursor backed by a mock client."""
    client = Mock()
    query_result = Mock()
    query_result.result_set = []
    query_result.column_names = ["formatted"]
    query_result.column_types = [Mock(name="String")]
    query_result.summary = {}
    client.query.return_value = query_result
    return Cursor(client), client


def test_percent_in_non_parameterized_query():
    """Literal % in a non-parameterized text() query must survive compilation
    and reach the driver as a single %."""
    dialect = _make_dialect()

    stmt = text("SELECT formatDateTime(toDate('2010-01-04'), '%g')")
    compiled = stmt.compile(dialect=dialect)
    compiled_sql = compiled.string

    # SQLAlchemy should have doubled the % for pyformat
    assert "%%g" in compiled_sql

    # Now simulate what the dialect's do_execute_no_params does:
    # cursor.execute(compiled_sql)  -- no parameters
    cursor, client = _make_cursor()
    cursor.execute(compiled_sql)

    actual_query = client.query.call_args[0][0]
    assert "%g" in actual_query
    assert "%%g" not in actual_query


def test_percent_in_parameterized_query():
    """Literal % in a parameterized text() query must survive both compilation
    and parameter substitution."""
    dialect = _make_dialect()

    stmt = text("SELECT formatDateTime(toDate(:d), '%g')")
    compiled = stmt.compile(dialect=dialect)
    compiled_sql = compiled.string

    # Should have %(d)s for the bind param and %%g for the literal %
    assert "%(d)s" in compiled_sql
    assert "%%g" in compiled_sql

    # Simulate do_execute with parameters (as SQLAlchemy would call it)
    cursor, client = _make_cursor()
    cursor.execute(compiled_sql, {"d": "2010-01-04"})

    # Parameters are passed through; finalize_query in the driver handles
    # both %(d)s substitution and %% -> % unescaping via Python's % operator
    actual_query = client.query.call_args[0][0]
    actual_params = client.query.call_args[0][1]
    assert actual_query == compiled_sql
    assert actual_params == {"d": "2010-01-04"}


def test_format_datetime_full_pattern():
    """A realistic formatDateTime pattern with many % format specifiers."""
    dialect = _make_dialect()

    stmt = text("SELECT formatDateTime(now(), '%Y-%m-%d %H:%M:%S')")
    compiled = stmt.compile(dialect=dialect)

    cursor, client = _make_cursor()
    cursor.execute(compiled.string)

    actual_query = client.query.call_args[0][0]
    assert actual_query == "SELECT formatDateTime(now(), '%Y-%m-%d %H:%M:%S')"


def test_preparer_double_percents_enabled():
    """Verify that the ClickHouse dialect keeps _double_percents = True
    (the default for pyformat paramstyle), rather than disabling it."""
    dialect = _make_dialect()
    preparer = dialect.preparer(dialect)
    assert preparer._double_percents is True


def test_percent_in_identifier_survives_parameter_binding():
    dialect = _make_dialect()
    events = table("events%2026", column("value%pct", Integer))
    compiled = select(events.c["value%pct"]).where(events.c["value%pct"] == 13).compile(dialect=dialect)

    assert "`events%%2026`.`value%%pct`" in compiled.string
    final_sql = finalize_query(compiled.string, compiled.params)
    assert "`events%2026`.`value%pct`" in final_sql
    assert " = 13" in final_sql


def test_quote_identifier_keeps_existing_direct_call_contract():
    dialect = _make_dialect()
    preparer = dialect.identifier_preparer

    assert preparer.quote_identifier("value%pct") == "`value%pct`"
    assert ChIdentifierPreparer.quote_identifier("value%pct") == "`value%pct`"
    assert preparer.quote("value%pct") == "`value%%pct`"


@pytest.mark.parametrize("execute_no_params", [False, True], ids=["do-execute", "do-execute-no-params"])
@pytest.mark.parametrize("compile_server_side", [False, True], ids=["client-compiled", "server-compiled"])
@pytest.mark.parametrize("receive_server_side", [False, True], ids=["client-received", "server-received"])
def test_compiled_execute_percent_provenance(execute_no_params, compile_server_side, receive_server_side):
    statement = "SELECT 'single% adjacent%% %(token)s tail%'"
    compile_dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=compile_server_side)
    compiled = text(statement).compile(dialect=compile_dialect)
    context = SimpleNamespace(execution_options={}, compiled=compiled)
    cursor, client = _make_cursor()
    dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=receive_server_side)

    if execute_no_params:
        dialect.do_execute_no_params(cursor, compiled.string, context=context)
    else:
        dialect.do_execute(cursor, compiled.string, {}, context=context)

    assert compiled.preparer._double_percents is not compile_server_side
    assert client.query.call_args.args[0] == statement


@pytest.mark.parametrize("execute_no_params", [False, True], ids=["do-execute", "do-execute-no-params"])
@pytest.mark.parametrize("receive_server_side", [False, True], ids=["client-received", "server-received"])
@pytest.mark.parametrize(
    "context",
    [SimpleNamespace(execution_options={}, compiled=None), None],
    ids=["compiled-none", "context-none"],
)
def test_raw_execute_percent_provenance(execute_no_params, receive_server_side, context):
    statement = "SELECT 'single% adjacent%% %(token)s tail%'"
    pyformat_statement = statement.replace("%", "%%")
    cursor, client = _make_cursor()
    dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=receive_server_side)

    if execute_no_params:
        dialect.do_execute_no_params(cursor, pyformat_statement, context=context)
    else:
        dialect.do_execute(cursor, pyformat_statement, {}, context=context)

    assert client.query.call_args.args[0] == statement
