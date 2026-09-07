from decimal import Decimal as PythonDecimal

from sqlalchemy import cast, func, literal, select

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Decimal
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


def decimal_arithmetic_statement():
    sales = select(cast(literal("123456789123456789.123456789"), Decimal(38, 9)).label("amount")).subquery("sales")
    amount = func.sum(sales.c.amount)
    return select(
        (amount * 13).label("multiplied"),
        (13 * amount).label("reversed"),
        (amount + 79).label("added"),
        (79 - amount).label("subtracted"),
        (amount / 2).label("divided"),
        (amount * 13 + 79).label("chained"),
    )


EXPECTED = (
    PythonDecimal("1604938258604938258.604938257"),
    PythonDecimal("1604938258604938258.604938257"),
    PythonDecimal("123456789123456868.123456789"),
    PythonDecimal("-123456789123456710.123456789"),
    PythonDecimal("61728394561728394.561728394"),
    PythonDecimal("1604938258604938337.604938257"),
)


def test_decimal_arithmetic_roundtrip(param_client, call):
    compiled = decimal_arithmetic_statement().compile(dialect=ClickHouseDialect(dbapi=dbapi))
    result = call(param_client.query, str(compiled), parameters=compiled.params)

    assert result.result_rows == [EXPECTED]
    assert all(isinstance(value, PythonDecimal) for value in result.first_row)


def test_decimal_arithmetic_result_processing(test_engine):
    with test_engine.connect() as connection:
        row = connection.execute(decimal_arithmetic_statement()).one()

    assert tuple(row) == EXPECTED
    assert all(isinstance(value, PythonDecimal) for value in row)
