import pytest
from sqlalchemy import String, TypeDecorator, literal, select

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, Nullable, Tuple, UInt64
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String as ChString
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import format_str

_LITERAL_PAYLOAD = "path'part"


class WrappedChString(TypeDecorator):
    impl = ChString
    cache_ok = True


class NestedWrappedChString(TypeDecorator):
    impl = WrappedChString
    cache_ok = True


class TransformedVariantString(TypeDecorator):
    impl = String().with_variant(ChString(), "clickhousedb")
    cache_ok = True

    def process_literal_param(self, value, dialect):
        return f"transformed-{value}"


@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (ChString(), _LITERAL_PAYLOAD, format_str(_LITERAL_PAYLOAD)),
        (Array(ChString), [_LITERAL_PAYLOAD], f"[{format_str(_LITERAL_PAYLOAD)}]"),
        (Tuple(ChString, UInt64), (_LITERAL_PAYLOAD, 13), f"({format_str(_LITERAL_PAYLOAD)}, 13)"),
        (Array(Tuple(ChString, UInt64)), [(_LITERAL_PAYLOAD, 13)], f"[({format_str(_LITERAL_PAYLOAD)}, 13)]"),
        (Nullable(ChString), _LITERAL_PAYLOAD, format_str(_LITERAL_PAYLOAD)),
        (sqla_type_from_name("string"), _LITERAL_PAYLOAD, format_str(_LITERAL_PAYLOAD)),
        (sqla_type_from_name("sTrInG"), _LITERAL_PAYLOAD, format_str(_LITERAL_PAYLOAD)),
    ],
    ids=["scalar", "array", "tuple", "array-tuple", "nullable", "parsed-lowercase", "parsed-mixed-case"],
)
def test_clickhouse_literal_processor_contract(type_, value, expected):
    processor = type_.literal_processor(ClickHouseDialect(dbapi=dbapi))

    assert processor is not None
    assert processor(value) == expected


@pytest.mark.parametrize(
    "type_",
    [WrappedChString(), NestedWrappedChString(), String().with_variant(ChString(), "clickhousedb")],
    ids=["type-decorator", "nested-type-decorator", "variant"],
)
def test_wrapped_clickhouse_literal_binds(type_):
    sql = str(
        select(literal(_LITERAL_PAYLOAD, type_=type_)).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql == f"SELECT {format_str(_LITERAL_PAYLOAD)} AS `anon_1`"


def test_type_decorator_composes_with_variant_literal_processor():
    transformed = f"transformed-{_LITERAL_PAYLOAD}"
    sql = str(
        select(literal(_LITERAL_PAYLOAD, type_=TransformedVariantString())).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql == f"SELECT {format_str(transformed)} AS `anon_1`"
