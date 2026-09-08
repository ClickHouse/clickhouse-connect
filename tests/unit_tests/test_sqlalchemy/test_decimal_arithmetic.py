import operator
from decimal import Decimal as PythonDecimal

import pytest
from sqlalchemy import Float, Integer, Interval, Numeric, TypeDecorator, column, func, select, type_coerce
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    Decimal,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    Float64,
    UInt64,
)
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@pytest.mark.parametrize("op", [operator.add, operator.sub, operator.mul, operator.truediv])
@pytest.mark.parametrize("value", [13, 1.25, PythonDecimal("1.25")], ids=["integer", "float", "decimal"])
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_decimal_arithmetic_with_literals(op, value, reverse):
    amount = column("amount", Decimal(38, 9))
    expression = op(value, amount) if reverse else op(amount, value)

    assert expression.type is amount.type
    assert expression.type.result_processor(ClickHouseDialect(), None) is None
    compiled = select(expression).compile(dialect=ClickHouseDialect())
    assert compiled.params == {"amount_1": value}


@pytest.mark.parametrize(
    "type_",
    [
        Decimal(38, 9),
        Decimal32(9, 2),
        Decimal64(18, 4),
        Decimal128(38, 9),
        Decimal256(76, 18),
        sqla_type_from_name("Nullable(Decimal(38, 9))"),
        sqla_type_from_name("LowCardinality(Decimal(38, 9))"),
        sqla_type_from_name("dEcImAl(38, 9)"),
    ],
    ids=["decimal", "decimal32", "decimal64", "decimal128", "decimal256", "nullable", "low-cardinality", "mixed-case"],
)
def test_decimal_arithmetic_preserves_configured_type(type_):
    amount = column("amount", type_)
    original = (type_.precision, type_.scale, type_.name, type_.type_def, type_._static_cache_key)
    expression = (amount * 13 + 79).label("adjusted")

    assert expression.type is type_
    assert expression.type._type_affinity is Numeric
    assert (type_.precision, type_.scale, type_.name, type_.type_def, type_._static_cache_key) == original
    assert type_.compile(dialect=ClickHouseDialect()) == type_.name
    assert select(expression)._generate_cache_key() is not None


@pytest.mark.parametrize(
    "compose",
    [
        func.sum,
        lambda amount: amount.label("total"),
        lambda amount: type_coerce(column("untyped_amount"), amount.type),
        lambda amount: 13 * amount + 79,
    ],
    ids=["sum", "label", "coerce", "chain"],
)
def test_decimal_composed_arithmetic(compose):
    amount = column("amount", Decimal(38, 9))
    expression = compose(amount)

    assert (expression * 13).type is amount.type
    assert str(select(expression * 13).compile(dialect=ClickHouseDialect()))


@pytest.mark.parametrize("op", [operator.add, operator.sub, operator.mul, operator.truediv])
@pytest.mark.parametrize(
    "other_type", [Numeric(18, 3), Decimal(18, 4), UInt64(), Float64()], ids=["numeric", "decimal", "integer", "float"]
)
@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_decimal_mixed_arithmetic_preserves_numeric_promotion(op, other_type, reverse):
    amount = column("amount", Decimal(38, 9))
    other = column("other", other_type)
    expression = op(other, amount) if reverse else op(amount, other)

    if isinstance(other_type, Float):
        assert type(expression.type) is Float64
    elif other_type._type_affinity is Integer or reverse:
        assert expression.type is amount.type
    else:
        assert expression.type is other_type
    assert str(select(expression).compile(dialect=ClickHouseDialect()))


def test_decimal_interval_multiplication_preserves_interval_type():
    duration = column("duration", Interval())
    assert (column("amount", Decimal(38, 9)) * duration).type is duration.type


def test_decimal_arithmetic_preserves_type_decorator_processing():
    class WrappedDecimal(TypeDecorator):
        impl = Decimal(38, 9)
        cache_ok = True

        def process_result_value(self, value, dialect):
            return value, "processed"

    amount = column("amount", WrappedDecimal())
    expression = amount * 13
    assert expression.type is amount.type
    process = expression.type.result_processor(ClickHouseDialect(), None)
    value = PythonDecimal("123456789123456789.123456789")
    assert process(value) == (value, "processed")


@pytest.mark.parametrize("args", [(), (0,), (0, 0), (38, -1), (38, 39)])
def test_decimal_constructor_still_requires_valid_precision_and_scale(args):
    with pytest.raises(ArgumentError, match="Invalid precision or scale for ClickHouse Decimal type"):
        Decimal(*args)
