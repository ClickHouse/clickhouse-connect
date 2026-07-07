"""Per-query ClickHouse settings threading through the DB-API cursor and dialect (issue #838)."""

from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.dbapi.cursor import Cursor


class _StubQueryResult:
    result_set: list[Any] = []
    column_names: list[str] = []
    column_types: list[Any] = []
    summary: dict[str, Any] = {}


class _StubInsertResult:
    written_rows = 0
    summary: dict[str, Any] = {}


class _FakeContext:
    def __init__(self, execution_options):
        self.execution_options = execution_options


def _mock_client():
    client = Mock()
    client.query.return_value = _StubQueryResult()
    client.insert.return_value = _StubInsertResult()
    return client


def _query_settings(client):
    return [call.kwargs.get("settings") for call in client.query.call_args_list]


def test_cursor_execute_forwards_settings():
    client = _mock_client()
    Cursor(client).execute("SELECT 13", settings={"max_threads": 3})
    # Main query plus the LIMIT 0 introspection re-query both carry the settings.
    assert _query_settings(client) == [{"max_threads": 3}, {"max_threads": 3}]


def test_cursor_executemany_forwards_settings():
    client = _mock_client()
    Cursor(client).executemany("SELECT %(v)s", [{"v": 13}, {"v": 79}], settings={"max_threads": 3})
    assert _query_settings(client) == [{"max_threads": 3}, {"max_threads": 3}]


def test_cursor_bulk_insert_forwards_settings():
    client = _mock_client()
    Cursor(client).executemany("INSERT INTO tbl (a, b) VALUES", [{"a": 13, "b": 79}], settings={"max_threads": 3})
    client.insert.assert_called_once_with("tbl", [[13, 79]], ["a", "b"], settings={"max_threads": 3})


def test_dialect_do_execute_forwards_settings():
    client = _mock_client()
    context = _FakeContext({"settings": {"max_threads": 3}})
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=context)
    assert _query_settings(client)[0] == {"max_threads": 3}


def test_dialect_do_executemany_forwards_settings():
    client = _mock_client()
    context = _FakeContext({"settings": {"max_threads": 3}})
    ClickHouseDialect().do_executemany(Cursor(client), "SELECT %(v)s", [{"v": 13}], context=context)
    assert _query_settings(client)[0] == {"max_threads": 3}


def test_dialect_do_execute_no_params_forwards_settings():
    client = _mock_client()
    context = _FakeContext({"settings": {"max_threads": 3}})
    ClickHouseDialect().do_execute_no_params(Cursor(client), "SELECT 13", context=context)
    assert _query_settings(client)[0] == {"max_threads": 3}


def test_dialect_do_execute_context_none_forwards_none():
    client = _mock_client()
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=None)
    assert _query_settings(client)[0] is None


def test_dialect_do_execute_missing_settings_key_forwards_none():
    client = _mock_client()
    context = _FakeContext({})
    ClickHouseDialect().do_execute(Cursor(client), "SELECT 13", None, context=context)
    assert _query_settings(client)[0] is None


class _StubColType:
    name = "Int32"


class _OneRowResult:
    result_set = [[13]]
    column_names = ["v"]
    column_types = [_StubColType()]
    summary: dict[str, Any] = {}


@pytest.fixture(name="mock_engine")
def mock_engine_fixture():
    client = Mock()
    client.query.return_value = _OneRowResult()
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine: Engine = create_engine("clickhousedb://user_1:pwd@localhost:8123/default")
        try:
            yield engine, client
        finally:
            engine.dispose()


def _run_with_settings(engine, client, conn_settings, stmt_settings):
    """Execute one statement through the public path and return the settings dicts seen by the client."""
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")  # force dialect init before we assert
        if conn_settings is not None:
            conn = conn.execution_options(settings=conn_settings)
        client.query.reset_mock()
        stmt = text("SELECT 13")
        if stmt_settings is not None:
            stmt = stmt.execution_options(settings=stmt_settings)
        conn.execute(stmt)
    return _query_settings(client)


def test_public_disjoint_settings_compose(mock_engine):
    engine, client = mock_engine
    seen = _run_with_settings(engine, client, {"log_comment": "req_1"}, {"max_execution_time": 30})
    assert seen == [{"log_comment": "req_1", "max_execution_time": 30}]


def test_public_same_key_statement_wins(mock_engine):
    engine, client = mock_engine
    seen = _run_with_settings(engine, client, {"max_execution_time": 30}, {"max_execution_time": 60})
    assert seen == [{"max_execution_time": 60}]


def test_public_engine_level_default_composes(mock_engine):
    # Engine-level defaults are the recommended shape; they must compose like connection-level ones.
    engine, client = mock_engine
    derived = engine.execution_options(settings={"log_comment": "eng_default"})
    with derived.connect() as conn:
        conn.exec_driver_sql("SELECT 1")  # force dialect init before we assert
        client.query.reset_mock()
        stmt = text("SELECT 13").execution_options(settings={"max_execution_time": 30})
        conn.execute(stmt)
    assert _query_settings(client) == [{"log_comment": "eng_default", "max_execution_time": 30}]


def test_public_statement_only_unchanged(mock_engine):
    engine, client = mock_engine
    seen = _run_with_settings(engine, client, None, {"max_threads": 3})
    assert seen == [{"max_threads": 3}]


def test_public_connection_only_unchanged(mock_engine):
    engine, client = mock_engine
    seen = _run_with_settings(engine, client, {"log_comment": "req_1"}, None)
    assert seen == [{"log_comment": "req_1"}]


def test_public_no_settings_forwards_none(mock_engine):
    engine, client = mock_engine
    seen = _run_with_settings(engine, client, None, None)
    assert seen == [None]
