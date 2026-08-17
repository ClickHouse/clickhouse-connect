from sqlalchemy import TypeDecorator, literal, select

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


class WrappedString(TypeDecorator):
    impl = String
    cache_ok = True


def test_wrapped_literal_roundtrip(param_client, call):
    payload = "wrapped'value"
    sql = str(
        select(literal(payload, type_=WrappedString())).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    result = call(param_client.query, sql)
    assert result.result_rows == [(payload,)]
