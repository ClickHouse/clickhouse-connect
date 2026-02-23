"""
Tests for correct handling of percent signs (%) in SQL queries through the
SQLAlchemy compilation pipeline.

See https://github.com/ClickHouse/clickhouse-connect/issues/297

The pyformat paramstyle (PEP 249) requires that literal % be doubled to %%
during compilation, and then unescaped by the DBAPI cursor.  This must work
correctly for both parameterized and non-parameterized queries.
"""

from unittest.mock import Mock

from sqlalchemy import text

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.dbapi.cursor import Cursor


def _make_dialect():
    """Create a dialect that mirrors real create_engine() behavior."""
    return ClickHouseDialect(dbapi=dbapi)


def _make_cursor():
    """Create a Cursor backed by a mock client."""
    client = Mock()
    query_result = Mock()
    query_result.result_set = []
    query_result.column_names = []
    query_result.column_types = []
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
    assert '%%g' in compiled_sql

    # Now simulate what the dialect's do_execute_no_params does:
    # cursor.execute(compiled_sql)  -- no parameters
    cursor, client = _make_cursor()
    cursor.execute(compiled_sql)

    actual_query = client.query.call_args[0][0]
    assert '%g' in actual_query
    assert '%%g' not in actual_query


def test_percent_in_parameterized_query():
    """Literal % in a parameterized text() query must survive both compilation
    and parameter substitution."""
    dialect = _make_dialect()

    stmt = text("SELECT formatDateTime(toDate(:d), '%g')")
    compiled = stmt.compile(dialect=dialect)
    compiled_sql = compiled.string

    # Should have %(d)s for the bind param and %%g for the literal %
    assert '%(d)s' in compiled_sql
    assert '%%g' in compiled_sql

    # Simulate do_execute with parameters (as SQLAlchemy would call it)
    cursor, client = _make_cursor()
    cursor.execute(compiled_sql, {'d': '2010-01-04'})

    # Parameters are passed through; finalize_query in the driver handles
    # both %(d)s substitution and %% -> % unescaping via Python's % operator
    actual_query = client.query.call_args[0][0]
    actual_params = client.query.call_args[0][1]
    assert actual_query == compiled_sql
    assert actual_params == {'d': '2010-01-04'}


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
    assert preparer._double_percents is True  # pylint: disable=protected-access
