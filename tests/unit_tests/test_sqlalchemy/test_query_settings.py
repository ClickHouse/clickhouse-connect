"""Per-query ClickHouse settings threading through the DB-API cursor and dialect (issue #838)."""

from typing import Any
from unittest.mock import Mock

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
