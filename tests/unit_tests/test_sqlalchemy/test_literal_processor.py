import pytest
import sqlalchemy
from sqlalchemy import String, TypeDecorator, bindparam, literal, select
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import sqltypes

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    Array,
    Int32,
    LowCardinality,
    Map,
    Nullable,
    Tuple,
    UInt64,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String as ChString
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import finalize_query, format_str, str_query_value

_LITERAL_PAYLOAD = "path\\'part"
_PERCENT_LITERAL_PAYLOAD = "plain% %(token)s %% quote\\'tail%"


class DecoratedGenericString(TypeDecorator):
    impl = String
    cache_ok = True

    def process_literal_param(self, value, dialect):
        return f"decorated-{value}"


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
    ("type_", "prefix"),
    [
        (None, ""),
        (String(), ""),
        (DecoratedGenericString(), "decorated-"),
    ],
    ids=["implicit", "generic-string", "generic-type-decorator"],
)
@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
@pytest.mark.parametrize(
    "value",
    ["middle\\path", "trailing\\", "backslash\\'quote% adjacent%%"],
    ids=["middle-backslash", "trailing-backslash", "backslash-quote-percent"],
)
def test_generic_literal_binds_use_clickhouse_backslash_escaping(type_, prefix, server_side, value):
    expression = literal(value) if type_ is None else literal(value, type_=type_)
    compiled = str(
        select(expression).compile(
            dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=server_side),
            compile_kwargs={"literal_binds": True},
        )
    )
    rendered_value = f"{prefix}{value}".replace("'", "''").replace("\\", "\\\\")
    if not server_side:
        rendered_value = rendered_value.replace("%", "%%")

    assert compiled == f"SELECT '{rendered_value}' AS `anon_1`"


@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
def test_generic_array_of_clickhouse_type_is_not_double_escaped(server_side):
    def compile_literal():
        return str(
            select(literal([_LITERAL_PAYLOAD], type_=sqltypes.ARRAY(ChString()))).compile(
                dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=server_side),
                compile_kwargs={"literal_binds": True},
            )
        )

    if sqlalchemy.__version__.startswith("1.4."):
        with pytest.raises(CompileError, match="No literal value renderer"):
            compile_literal()
        return

    assert compile_literal() == f"SELECT [{format_str(_LITERAL_PAYLOAD)}] AS `anon_1`"


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


@pytest.mark.parametrize(
    ("type_", "value", "rendered_value"),
    [
        (ChString(), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (Array(ChString), [_PERCENT_LITERAL_PAYLOAD], [_PERCENT_LITERAL_PAYLOAD]),
        (Tuple(ChString, UInt64), (_PERCENT_LITERAL_PAYLOAD, 13), (_PERCENT_LITERAL_PAYLOAD, 13)),
        (
            Array(Tuple(ChString, UInt64)),
            [(_PERCENT_LITERAL_PAYLOAD, 13)],
            [(_PERCENT_LITERAL_PAYLOAD, 13)],
        ),
        (
            Map(ChString, ChString),
            {_PERCENT_LITERAL_PAYLOAD: _PERCENT_LITERAL_PAYLOAD},
            {_PERCENT_LITERAL_PAYLOAD: _PERCENT_LITERAL_PAYLOAD},
        ),
        (Nullable(ChString), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (LowCardinality(ChString), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (sqla_type_from_name("string"), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (sqla_type_from_name("sTrInG"), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (WrappedChString(), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (NestedWrappedChString(), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (String().with_variant(ChString(), "clickhousedb"), _PERCENT_LITERAL_PAYLOAD, _PERCENT_LITERAL_PAYLOAD),
        (
            TransformedVariantString(),
            _PERCENT_LITERAL_PAYLOAD,
            f"transformed-{_PERCENT_LITERAL_PAYLOAD}",
        ),
    ],
    ids=[
        "scalar",
        "array",
        "tuple",
        "array-tuple",
        "map",
        "nullable",
        "low-cardinality",
        "parsed-lowercase",
        "parsed-mixed-case",
        "type-decorator",
        "nested-type-decorator",
        "variant",
        "transformed-variant",
    ],
)
def test_clickhouse_literal_percent_escaping(type_, value, rendered_value):
    statement = select(
        bindparam("literal_value", value, type_=type_, literal_execute=True),
        bindparam("remaining", 13, type_=Int32()),
    )
    rendered = str_query_value(rendered_value)

    compiled = statement.compile(
        dialect=ClickHouseDialect(dbapi=dbapi),
        compile_kwargs={"render_postcompile": True},
    )
    assert compiled.string == f"SELECT {rendered.replace('%', '%%')} AS `anon_1`, %(remaining)s AS `anon_2`"
    assert finalize_query(compiled.string, compiled.params) == f"SELECT {rendered} AS `anon_1`, 13 AS `anon_2`"

    server_compiled = statement.compile(
        dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=True),
        compile_kwargs={"render_postcompile": True},
    )
    assert server_compiled.string == f"SELECT {rendered} AS `anon_1`, {{remaining:Int32}} AS `anon_2`"
