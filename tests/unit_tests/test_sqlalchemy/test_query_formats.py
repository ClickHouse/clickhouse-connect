"""Per-query query_formats threading and SQLAlchemy metadata internal formats (issue #920)."""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.inspector import with_internal_query_formats
from clickhouse_connect.datatypes.format import clear_all_formats, set_default_formats
from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.client import _INTERNAL_QUERY_FORMATS


class _StubQueryResult:
    result_set: list[Any] = []
    column_names: list[str] = []
    column_types: list[Any] = []
    summary: dict[str, Any] = {}


class _StubInsertResult:
    written_rows = 0
    summary: dict[str, Any] = {}


class _FakeContext:
    def __init__(self, execution_options, invoked_statement=None):
        self.execution_options = execution_options
        self.invoked_statement = invoked_statement


def _mock_client():
    client = Mock()
    client.query.return_value = _StubQueryResult()
    client.insert.return_value = _StubInsertResult()
    return client


def _query_formats(client):
    return [call.kwargs.get("query_formats") for call in client.query.call_args_list]


def test_cursor_execute_forwards_query_formats():
    client = _mock_client()
    Cursor(client).execute("SELECT 13", query_formats={"String": "string"})
    # Main query plus the LIMIT 0 introspection re-query both carry the formats.
    assert _query_formats(client) == [{"String": "string"}, {"String": "string"}]


def test_cursor_executemany_forwards_query_formats():
    client = _mock_client()
    Cursor(client).executemany("SELECT %(v)s", [{"v": 13}, {"v": 79}], query_formats={"String": "string"})
    assert _query_formats(client) == [{"String": "string"}, {"String": "string"}]


def test_dialect_do_execute_forwards_query_formats():
    client = _mock_client()
    context = _FakeContext({"query_formats": {"String": "string"}})
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=context)
    assert _query_formats(client)[0] == {"String": "string"}


def test_dialect_do_executemany_forwards_query_formats():
    client = _mock_client()
    context = _FakeContext({"query_formats": {"String": "string"}})
    ClickHouseDialect().do_executemany(Cursor(client), "SELECT %(v)s", [{"v": 13}], context=context)
    assert _query_formats(client)[0] == {"String": "string"}


def test_dialect_do_execute_no_params_forwards_query_formats():
    client = _mock_client()
    context = _FakeContext({"query_formats": {"String": "string"}})
    ClickHouseDialect().do_execute_no_params(Cursor(client), "SELECT 13", context=context)
    assert _query_formats(client)[0] == {"String": "string"}


def test_dialect_do_execute_context_none_forwards_none_formats():
    client = _mock_client()
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=None)
    assert _query_formats(client)[0] is None


def test_dialect_do_execute_missing_query_formats_key_forwards_none():
    client = _mock_client()
    context = _FakeContext({})
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=context)
    assert _query_formats(client)[0] is None


def test_with_internal_query_formats_sets_execution_option():
    clause = with_internal_query_formats(text("SHOW TABLES"))
    assert clause.get_execution_options()["query_formats"] == dict(_INTERNAL_QUERY_FORMATS)


class _StubColType:
    name = "String"


class _NamedRowsResult:
    def __init__(self, rows: list[list[Any]], names: list[str]):
        self.result_set = rows
        self.column_names = names
        self.column_types = [_StubColType()] * len(names)
        self.summary: dict[str, Any] = {}


@pytest.fixture(name="mock_engine")
def mock_engine_fixture():
    client = Mock()
    client.query.return_value = _NamedRowsResult([["default"]], ["name"])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine: Engine = create_engine("clickhousedb://user_1:pwd@localhost:8123/default")
        try:
            yield engine, client
        finally:
            engine.dispose()


def test_public_statement_query_formats_reach_client(mock_engine):
    engine, client = mock_engine
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        client.query.reset_mock()
        client.query.return_value = _NamedRowsResult([["x"]], ["name"])
        stmt = text("SHOW TABLES").execution_options(query_formats={"String": "string"})
        conn.execute(stmt)
    assert _query_formats(client) == [{"String": "string"}]


def test_get_table_names_uses_internal_query_formats(mock_engine):
    engine, client = mock_engine
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        client.query.reset_mock()
        client.query.return_value = _NamedRowsResult([["my_table"]], ["name"])
        names = conn.dialect.get_table_names(conn)
    assert names == ["my_table"]
    assert all(fmt == dict(_INTERNAL_QUERY_FORMATS) for fmt in _query_formats(client))


def test_get_schema_names_uses_internal_query_formats(mock_engine):
    engine, client = mock_engine
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        client.query.reset_mock()
        client.query.return_value = _NamedRowsResult([["system"], ["default"]], ["name"])
        names = conn.dialect.get_schema_names(conn)
    assert names == ["system", "default"]
    assert all(fmt == dict(_INTERNAL_QUERY_FORMATS) for fmt in _query_formats(client))


def test_get_columns_uses_internal_query_formats(mock_engine):
    engine, client = mock_engine

    def _side_effect(operation, *args, **kwargs):
        op = str(operation)
        if "system.tables" in op:
            return _NamedRowsResult([["MergeTree", "MergeTree ORDER BY k", ""]], ["engine", "engine_full", "comment"])
        if op.upper().startswith("DESCRIBE") or "DESCRIBE TABLE" in op.upper():
            return _NamedRowsResult(
                [["k", "UInt32", "", "", "", "", ""], ["s", "String", "", "", "", "", ""]],
                ["name", "type", "default_type", "default_expression", "comment", "codec_expression", "ttl_expression"],
            )
        if "currentDatabase" in op:
            return _NamedRowsResult([["default"]], ["name"])
        return _NamedRowsResult([[1]], ["result"])

    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        client.query.reset_mock()
        client.query.side_effect = _side_effect
        cols = conn.dialect.get_columns(conn, "scratch_bytes_fmt")
    assert [c["name"] for c in cols] == ["k", "s"]
    formats = _query_formats(client)
    assert formats
    assert all(fmt == dict(_INTERNAL_QUERY_FORMATS) for fmt in formats)


def test_user_query_without_formats_stays_unaffected(mock_engine):
    """Ordinary user SELECTs must not receive the internal metadata format override."""
    engine, client = mock_engine
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
        client.query.reset_mock()
        client.query.return_value = _NamedRowsResult([[b"hello"]], ["s"])
        conn.execute(text("SELECT s FROM t"))
    assert _query_formats(client) == [None]


def test_internal_formats_constant_matches_core_driver():
    assert _INTERNAL_QUERY_FORMATS == {"String": "string"}


def test_set_default_formats_bytes_does_not_alter_internal_constant():
    try:
        set_default_formats("String", "bytes")
        assert dict(_INTERNAL_QUERY_FORMATS) == {"String": "string"}
        clause = with_internal_query_formats(text("SELECT 1"))
        assert clause.get_execution_options()["query_formats"] == {"String": "string"}
    finally:
        clear_all_formats()
