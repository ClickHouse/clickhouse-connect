from sqlalchemy import String
from sqlalchemy.types import TypeDecorator

from clickhouse_connect.cc_sqlalchemy.datatypes.base import _datetime64_bind_signature
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


class _StringDecorator(TypeDecorator):
    impl = String
    cache_ok = True


class _DialectWithStringDecorator(ClickHouseDialect):
    # String adapts to a decorator whose impl is String again.
    colspecs = {String: _StringDecorator}


def test_self_adapting_decorator_terminates():
    dialect = _DialectWithStringDecorator()
    assert isinstance(String().dialect_impl(dialect), TypeDecorator)
    assert _datetime64_bind_signature(String(), dialect) is False


def test_plain_decorator_still_unwraps():
    assert _datetime64_bind_signature(_StringDecorator(), ClickHouseDialect()) is False
