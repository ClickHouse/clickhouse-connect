from unittest import mock

from sqlalchemy.sql.compiler import IdentifierPreparer

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


def _make_preparer():
    return ClickHouseDialect(dbapi=dbapi).identifier_preparer


def test_quote_passes_only_ident_to_parent():
    preparer = _make_preparer()
    with mock.patch.object(IdentifierPreparer, "quote", return_value="`user_1`") as parent_quote:
        assert preparer.quote("user_1") == "`user_1`"
    parent_quote.assert_called_once_with("user_1")


def test_quote_still_accepts_force_from_direct_callers():
    preparer = _make_preparer()
    assert preparer.quote("user_1", True) == "`user_1`"
    assert preparer.quote("user_2", force=False) == "`user_2`"
    assert preparer.quote("user_3") == "`user_3`"
