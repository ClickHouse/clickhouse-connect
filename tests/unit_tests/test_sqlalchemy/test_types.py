import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, Table, TypeDecorator
from sqlalchemy.exc import ArgumentError
from sqlalchemy.schema import CreateTable

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name, sqla_type_map
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    JSON,
    UUID,
    AggregateFunction,
    Array,
    Bool,
    Boolean,
    Date,
    DateTime64,
    Decimal,
    Enum,
    FixedString,
    Float64,
    Geometry,
    Int64,
    LowCardinality,
    Map,
    Nested,
    Nullable,
    QBit,
    SimpleAggregateFunction,
    String,
    Time,
    Time64,
    Tuple,
    UInt32,
    UInt64,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime as ChDateTime
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.binding import _format_identifier


def test_mapping():
    assert issubclass(sqla_type_map["UInt64"], Integer)
    assert issubclass(sqla_type_map["DateTime"], DateTime)


def test_type_argument_metadata_drives_sqlalchemy_classification():
    from clickhouse_connect.cc_sqlalchemy.datatypes import sqltypes
    from clickhouse_connect.datatypes.registry import _WRAPPER_TYPE_ARGS, type_map

    for name, ch_type in type_map.items():
        assert sqltypes._TYPE_ARGS[name.lower()] is ch_type._type_args
    for name, type_args in _WRAPPER_TYPE_ARGS.items():
        assert sqltypes._TYPE_ARGS[name.lower()] is type_args

    expected_zero_argument_names = {
        name.lower() for name, ch_type in type_map.items() if ch_type._type_args.min_args == 0 and ch_type._type_args.max_args == 0
    }
    assert sqltypes._ZERO_ARGUMENT_TYPE_NAMES == expected_zero_argument_names


def test_all_matches_schema_types():
    from clickhouse_connect.cc_sqlalchemy.datatypes import sqltypes
    from clickhouse_connect.cc_sqlalchemy.datatypes.base import schema_types

    public_schema_types = [name for name in schema_types if sqla_type_map[name].__name__ == name]
    private_schema_types = set(schema_types) - set(public_schema_types)

    assert private_schema_types == {"Variant"}
    assert sqltypes.__all__ == sorted(public_schema_types) + ["LowCardinality", "Nullable"]


def test_variant_is_reflection_only():
    from clickhouse_connect.cc_sqlalchemy import types
    from clickhouse_connect.cc_sqlalchemy.datatypes import sqltypes

    assert not hasattr(sqltypes, "Variant")
    assert not hasattr(types, "Variant")
    with pytest.raises(ImportError):
        exec("from clickhouse_connect.cc_sqlalchemy.types import Variant")


def test_sqla():
    int16 = sqla_type_from_name("Int16")
    assert "Int16" == int16._compiler_dispatch(None)
    enum = sqla_type_from_name("Enum8('value1' = 7, 'value2'=5)")
    assert "Enum8('value2' = 5, 'value1' = 7)" == enum._compiler_dispatch(None)


def test_geometry():
    geometry = sqla_type_from_name("Geometry")
    assert geometry.__class__ == Geometry
    assert geometry.python_type is object
    assert geometry.name == "Geometry"
    assert geometry._compiler_dispatch(None) == "Geometry"
    assert sqla_type_from_name("GEOMETRY").name == "Geometry"


def test_nullable():
    nullable = Nullable(Int64)
    assert nullable.__class__ == Int64
    nullable = Nullable(DateTime64(6))
    assert nullable.__class__ == DateTime64
    assert nullable.name == "Nullable(DateTime64(6))"


def test_low_cardinality():
    lc_str = LowCardinality(Nullable(String))
    assert lc_str.__class__ == String
    assert lc_str.name == "LowCardinality(Nullable(String))"


def test_compound_accepts_wrapped_element():
    assert Array(LowCardinality(String)).name == "Array(LowCardinality(String))"
    assert Array(Nullable(String)).name == "Array(Nullable(String))"


def test_bool_accepts_schema_kwargs():
    # SQLAlchemy's SchemaType copy/adapt path passes internal kwargs like _create_events
    Bool(_create_events=False)


def test_qbit():
    qbit = sqla_type_from_name("QBit(Float32, 768)")
    assert qbit.__class__ == QBit
    assert qbit.name == "QBit(Float32, 768)"
    assert qbit._compiler_dispatch(None) == "QBit(Float32, 768)"

    qbit2 = QBit("Float32", 768)
    assert qbit2.name == "QBit(Float32, 768)"

    qbit_bf16 = sqla_type_from_name("QBit(BFloat16, 128)")
    assert qbit_bf16.name == "QBit(BFloat16, 128)"

    qbit_f64 = sqla_type_from_name("QBit(Float64, 1536)")
    assert qbit_f64.name == "QBit(Float64, 1536)"


def test_datetime_timezone_alias():
    assert ChDateTime(timezone="UTC").name == ChDateTime(tz="UTC").name


def test_datetime64_timezone_alias():
    assert DateTime64(3, timezone="America/New_York").name == DateTime64(3, tz="America/New_York").name


def test_datetime_both_tz_and_timezone_raises():
    with pytest.raises(ArgumentError):
        ChDateTime(tz="UTC", timezone="UTC")
    with pytest.raises(ArgumentError):
        DateTime64(3, tz="UTC", timezone="UTC")


def test_datetime_timezone_true_raises():
    with pytest.raises(ArgumentError) as exc_info:
        ChDateTime(timezone=True)
    assert "zone" in str(exc_info.value).lower()
    with pytest.raises(ArgumentError) as exc_info:
        DateTime64(3, timezone=True)
    assert "zone" in str(exc_info.value).lower()


def test_datetime_timezone_false_is_noop():
    """timezone=False is silently accepted; SA passes it during type cloning."""
    assert ChDateTime(timezone=False).name == ChDateTime().name
    assert DateTime64(3, timezone=False).name == DateTime64(3).name
    assert ChDateTime(tz="UTC", timezone=False).name == ChDateTime(tz="UTC").name


def test_time64_accepts_all_server_precisions():
    for precision in range(10):
        assert Time64(precision).name == f"Time64({precision})"
    with pytest.raises(ArgumentError):
        Time64(10)


@pytest.mark.parametrize("precision", [True, "3", 3.0])
def test_time64_rejects_non_integer_precision(precision):
    with pytest.raises(ArgumentError, match="precision"):
        Time64(precision)
    with pytest.raises(ArgumentError, match="precision"):
        Time64(type_def=TypeDef(values=(precision,)))


def test_tuple_variadic():
    assert Tuple(UInt32, UInt64).name == Tuple(elements=[UInt32, UInt64]).name


def test_tuple_variadic_single():
    tup = Tuple(UInt32)
    assert tup.name == Tuple(elements=[UInt32]).name


def test_tuple_variadic_with_uuid():
    assert Tuple(UInt32, UUID, UInt64).name == Tuple(elements=[UInt32, UUID, UInt64]).name


def test_tuple_both_positional_and_kwarg_raises():
    with pytest.raises(ArgumentError):
        Tuple(UInt32, elements=[UInt64])


def test_tuple_zero_args_does_not_crash():
    """Tuple() with no args returns an empty Tuple instead of crashing."""
    Tuple()


def test_tuple_adapt_preserves_type_def():
    """Tuple.adapt() preserves the source instance's type_def."""
    source = Tuple(UInt32, UInt64)
    adapted = source.adapt(type(source))
    assert adapted.type_def == source.type_def
    assert adapted.name == source.name


@pytest.mark.parametrize(
    "type_name, expected_class, expected_name",
    [
        ("Variant(UInt32, String)", sqla_type_map["Variant"], "Variant(String, UInt32)"),
        ("vArIaNt(UInt64, String)", sqla_type_map["Variant"], "Variant(String, UInt64)"),
        ("aRrAy(Variant(UInt32, String))", Array, "Array(Variant(String, UInt32))"),
        ("Tuple(Variant(UInt32, String), UInt64)", Tuple, "Tuple(Variant(String, UInt32), UInt64)"),
        (
            "Array(Tuple(Variant(UInt32, String), UInt64))",
            Array,
            "Array(Tuple(Variant(String, UInt32), UInt64))",
        ),
        ("Nullable(Variant(UInt32, String))", sqla_type_map["Variant"], "Nullable(Variant(String, UInt32))"),
    ],
)
def test_variant_reflection_factory(type_name, expected_class, expected_name):
    variant = sqla_type_from_name(type_name)

    assert variant.__class__ is expected_class
    assert variant.name == expected_name


def test_variant_subclass_does_not_replace_schema_registration():
    from clickhouse_connect.cc_sqlalchemy.datatypes.base import schema_types

    original_variant = sqla_type_map["Variant"]
    original_schema_types = schema_types.copy()
    original_type_map = dict(sqla_type_map)

    try:
        type("_DerivedVariant", (original_variant,), {})
        assert sqla_type_map["Variant"] is original_variant
        assert schema_types.count("Variant") == original_schema_types.count("Variant")
    finally:
        schema_types[:] = original_schema_types
        sqla_type_map.clear()
        sqla_type_map.update(original_type_map)


def test_variant_copy_adapt_and_dialect_impl_preserve_members():
    source = sqla_type_from_name("Variant(UInt32, String)")
    variant_class = type(source)

    for copied in (source.copy(), source.adapt(variant_class), source.dialect_impl(ClickHouseDialect())):
        assert copied.__class__ is variant_class
        assert copied.name == source.name
        assert copied.type_def == source.type_def


@pytest.mark.parametrize(
    "type_hint, expected",
    [
        (UInt32, "UInt32"),
        (UInt32(), "UInt32"),
        (Array(UInt32), "Array(UInt32)"),
        (Map(String, UInt32), "Map(String, UInt32)"),
        (Tuple(UInt32, String), "Tuple(UInt32, String)"),
        ("sTrInG", "String"),
        ("Boolean", "Bool"),
        ("bOoLeAn", "Bool"),
        (Boolean, "Bool"),
        (Boolean(), "Bool"),
        ("Array(bOoLeAn)", "Array(Bool)"),
        ("Array(UInt32)", "Array(UInt32)"),
        ("Tuple(UInt32, String)", "Tuple(UInt32, String)"),
        ("Tuple(id UInt32, value String)", "Tuple(`id` UInt32, `value` String)"),
        ("Array(Tuple(UInt32, Nullable(String)))", "Array(Tuple(UInt32, Nullable(String)))"),
        ("Nullable(UInt64)", "Nullable(UInt64)"),
        ("Variant(UInt32, String)", "Variant(String, UInt32)"),
        ("Dynamic", "Dynamic"),
        ("JSON()", "JSON"),
        ("JSON(`child` UInt32)", "JSON(`child` UInt32)"),
        ("json(`child` UInt32)", "JSON(`child` UInt32)"),
        ("Array(json(`child` UInt32))", "Array(JSON(`child` UInt32))"),
        ("Map(String, jSoN(SKIP REGEXP 'x)y'))", "Map(String, JSON(SKIP REGEXP 'x)y'))"),
        ("jSoN(sKiP rEgExP 'x)y')", "JSON(SKIP REGEXP 'x)y')"),
        ("json(`doc` json(`child` UInt32))", "JSON(`doc` JSON(`child` UInt32))"),
        ("Array(Variant(UInt32, String))", "Array(Variant(String, UInt32))"),
        ("Array(Variant(UInt32, UInt32))", "Array(Variant(UInt32))"),
        ("Variant(Decimal32(2), Decimal(9, 2))", "Variant(Decimal(9, 2))"),
        ("aRrAy(vArIaNt(uInT32, sTrInG))", "Array(Variant(String, UInt32))"),
        ("Array(Dynamic)", "Array(Dynamic)"),
        ("Map(String, Variant(UInt32,String))", "Map(String, Variant(String, UInt32))"),
        ("Tuple(Variant(UInt32, String), Nullable(UInt8))", "Tuple(Variant(String, UInt32), Nullable(UInt8))"),
        (
            "Tuple(value Variant(UInt32, String), count UInt8)",
            "Tuple(`value` Variant(String, UInt32), `count` UInt8)",
        ),
        (
            "SimpleAggregateFunction(any, Variant(UInt32, String))",
            "SimpleAggregateFunction(any, Variant(String, UInt32))",
        ),
        ("SimpleAggregateFunction(any, Time64(2))", "SimpleAggregateFunction(any, Time64(2))"),
        ("JSON(`a(b` UInt32)", "JSON(`a(b` UInt32)"),
        ("JSON(`a)b` UInt32)", "JSON(`a)b` UInt32)"),
        ("JSON(`a\\`(b` UInt32)", "JSON(`a\\`(b` UInt32)"),
        ("Array(JSON(`a)b` UInt32))", "Array(JSON(`a)b` UInt32))"),
        ("Map(String, JSON(`a(b` UInt32))", "Map(String, JSON(`a(b` UInt32))"),
        ('JSON("child" UInt32, "SKIP" String)', "JSON(`SKIP` String, `child` UInt32)"),
    ],
)
def test_json_typed_path_type_hints(type_hint, expected):
    assert JSON(typed_paths={"value": type_hint}).name == f"JSON(`value` {expected})"


@pytest.mark.parametrize(
    "type_hint, expected",
    [
        ("FixedString(13)", "FixedString(13)"),
        ("String()", "String"),
        ("UInt32()", "UInt32"),
        ("UUID()", "UUID"),
        ("Bool()", "Bool"),
        ("IPv4()", "IPv4"),
        ("Date()", "Date"),
        ("DateTime()", "DateTime"),
        ("DateTime(0)", "DateTime"),
        ("DateTime(0, 'UTC')", "DateTime('UTC')"),
        ("DateTime(3)", "DateTime64(3)"),
        ("DateTime(6, 'UTC')", "DateTime64(6, 'UTC')"),
        ("Time()", "Time"),
        ("Time(0)", "Time"),
        ("Time(3)", "Time64(3)"),
        ("DateTime('UTC')", "DateTime('UTC')"),
        ("DateTime64(3)", "DateTime64(3)"),
        ("DateTime64()", "DateTime64(3)"),
        ("DateTime64(6, 'UTC')", "DateTime64(6, 'UTC')"),
        ("Time64()", "Time64(3)"),
        ("Time64(0)", "Time64(0)"),
        ("Time64(3)", "Time64(3)"),
        ("Time64(9)", "Time64(9)"),
        ("Decimal", "Decimal(10, 0)"),
        ("Decimal()", "Decimal(10, 0)"),
        ("Decimal(10)", "Decimal(10, 0)"),
        ("Decimal(10, 2)", "Decimal(10, 2)"),
        ("Decimal32(2)", "Decimal(9, 2)"),
        ("Decimal256(76)", "Decimal(76, 76)"),
        ("Enum8('user_1' = 1)", "Enum8('user_1' = 1)"),
        ("Enum('low' = -128, 'high' = 127)", "Enum8('low' = -128, 'high' = 127)"),
        ("Enum('low' = -129, 'high' = 128)", "Enum16('low' = -129, 'high' = 128)"),
        ("Enum('low' = -32768, 'high' = 32767)", "Enum16('low' = -32768, 'high' = 32767)"),
        ("QBit(Float32, 13)", "QBit(Float32, 13)"),
        ("qBiT(fLoAt32, 13)", "QBit(Float32, 13)"),
        ("Dynamic()", "Dynamic"),
        ("Dynamic(max_types=0)", "Dynamic(max_types=0)"),
        ("Dynamic(max_types=13)", "Dynamic(max_types=13)"),
        ("Dynamic(max_types=254)", "Dynamic(max_types=254)"),
    ],
)
def test_json_typed_path_accepts_valid_parameterized_scalars(type_hint, expected):
    assert JSON(typed_paths={"value": type_hint}).name == f"JSON(`value` {expected})"


@pytest.mark.parametrize(
    "type_hint, expected",
    [
        ("Decimal64(18)", "Decimal(18, 18)"),
        ("Dynamic(max_types=254)", "Dynamic(max_types=254)"),
    ],
)
def test_json_type_argument_metadata_accepts_boundary_values(type_hint, expected):
    assert JSON(typed_paths={"value": type_hint}).name == f"JSON(`value` {expected})"


@pytest.mark.parametrize("type_hint", ["Decimal64(19)", "Dynamic(max_types=255)"])
def test_json_type_argument_metadata_rejects_out_of_range_values(type_hint):
    with pytest.raises(ArgumentError):
        JSON(typed_paths={"value": type_hint})


@pytest.mark.parametrize(
    "type_name",
    [
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "Int128",
        "Int256",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "UInt128",
        "UInt256",
    ],
)
def test_json_raw_integers_accept_empty_parentheses(type_name):
    assert JSON(typed_paths={"value": f"{type_name}()"}).name == f"JSON(`value` {type_name})"


def test_json_raw_identifier_quotes_are_syntax_but_mapping_quotes_are_data():
    assert JSON(typed_paths={"doc": 'JSON("child" UInt32)'}).name == "JSON(`doc` JSON(`child` UInt32))"
    assert JSON(typed_paths={'"child"': UInt32}).name == 'JSON(`"child"` UInt32)'


@pytest.mark.parametrize("regexp_hint", ["JSON(SKIP REGEXP 'a''b')", "JSON(SKIP REGEXP 'a\\'b')"])
def test_json_raw_regexp_accepts_clickhouse_quote_escapes(regexp_hint):
    json_type = JSON(typed_paths={"value": regexp_hint})

    assert json_type.ch_type.typed_types[0].skip_regexps == ["a'b"]


@pytest.mark.parametrize(
    "type_hint, expected_path",
    [
        ("JSON(SKIP string)", "string"),
        ("JSON(SKIP array)", "array"),
        ("JSON(SKIP uint32)", "uint32"),
        ("Array(JSON(SKIP string))", "string"),
        ("JSON(`doc` JSON(SKIP array))", "array"),
    ],
)
def test_json_raw_skip_paths_preserve_type_like_identifier_case(type_hint, expected_path):
    json_type = JSON(typed_paths={"value": type_hint})
    nested_type = json_type.ch_type.typed_types[0]
    while getattr(nested_type, "skip_paths", None) != [expected_path]:
        if hasattr(nested_type, "element_type"):
            nested_type = nested_type.element_type
        elif getattr(nested_type, "typed_types", None):
            nested_type = nested_type.typed_types[0]
        else:
            pytest.fail(f"Could not find nested JSON skip path in {type_hint}")

    assert nested_type.skip_paths == [expected_path]


@pytest.mark.parametrize(
    "regexp_hint",
    ["JSON(SKIP REGEXP'x')", "JSON(SKIP REGEXP\t'x')", "JSON(SKIP   REGEXP 'x')"],
)
def test_json_raw_regexp_accepts_sql_separator_variants(regexp_hint):
    json_type = JSON(typed_paths={"value": regexp_hint})

    assert json_type.ch_type.typed_types[0].skip_regexps == ["x"]


def test_json_raw_skip_accepts_quoted_spaces_and_repeated_separator_whitespace():
    json_type = JSON(typed_paths={"value": "JSON(SKIP  `a garbage`, SKIP   REGEXP 'x')"})
    nested = json_type.ch_type.typed_types[0]

    assert nested.skip_paths == ["a garbage"]
    assert nested.skip_regexps == ["x"]


def test_json_constructor_canonicalizes_arguments():
    json_type = JSON(
        typed_paths={
            "event.id": UInt32,
            "a b": "String",
            "a`b": "UInt64",
            "literal%2Edot": "Dynamic",
            "SKIP": "Nullable(String)",
            "max_dynamic_paths": "Array(UInt32)",
        },
        max_dynamic_paths=13,
        max_dynamic_types=79,
        skip_paths=["z", "a", "z"],
        skip_regexps=["z.*", "a'\\d", "z.*"],
    )

    assert json_type.name == (
        "JSON(max_dynamic_types = 79, max_dynamic_paths = 13, `SKIP` Nullable(String), "
        "`a b` String, `a\\`b` UInt64, `event.id` UInt32, `literal%2Edot` Dynamic, "
        "`max_dynamic_paths` Array(UInt32), SKIP `a`, SKIP `z`, "
        "SKIP REGEXP 'a\\'\\\\d', SKIP REGEXP 'z.*', SKIP REGEXP 'z.*')"
    )
    assert JSON(user_id=UInt64).name == "JSON(`user_id` UInt64)"
    assert JSON(max_dynamic_paths=1024, max_dynamic_types=32).name == "JSON"


@pytest.mark.parametrize(
    "type_hint, match",
    [
        ("Tuple(a UInt32, String)", "all named or all unnamed"),
        ("tuple(UInt32, b String)", "all named or all unnamed"),
        ("Array(Tuple(a UInt32, String))", "all named or all unnamed"),
        ("Nested(UInt32)", "Nested requires"),
        ("nested(UInt32, String)", "Nested requires"),
        ("Nested(a UInt32, String)", "Nested requires"),
        ("Map(String, Nested(a UInt32, String))", "Nested requires"),
        ("Variant(label UInt32)", "cannot be named"),
        ("variant(label UInt32, String)", "cannot be named"),
        ("Nullable(Variant(label UInt32))", "cannot be named"),
    ],
)
def test_json_typed_path_rejects_bad_element_names(type_hint, match):
    with pytest.raises(ArgumentError, match=match):
        JSON(typed_paths={"p": type_hint})


def test_json_typed_path_preserves_enum_doubled_quote_label():
    assert JSON(typed_paths={"v": "Enum8('a''b' = 1)"}).name == "JSON(`v` Enum8('a\\'b' = 1))"


def test_json_typed_path_tuple_literal_backtick_field_round_trips():
    json_type = JSON(typed_paths={"p": r"Tuple(`\`field\`` UInt32)"})
    assert json_type.name == r"JSON(`p` Tuple(`\`field\`` UInt32))"
    assert get_from_name(json_type.name).name == json_type.name


def test_json_constructor_preserves_literal_boundary_quotes_in_paths():
    typed_paths = {"`typed`": UInt32, '"typed"': String}
    skip_paths = ["`skipped`", '"skipped"']
    json_type = JSON(typed_paths=typed_paths, skip_paths=skip_paths)

    assert json_type.ch_type.typed_paths == ['"typed"', "`typed`"]
    assert json_type.ch_type.skip_paths == ['"skipped"', "`skipped`"]
    for path in [*typed_paths, *skip_paths]:
        assert _format_identifier(path) in json_type.name


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_dynamic_paths": -1},
        {"max_dynamic_paths": 10001},
        {"max_dynamic_types": -1},
        {"max_dynamic_types": 255},
        {"max_dynamic_types": True},
        {"max_dynamic_paths": 13.0},
    ],
)
def test_json_constructor_rejects_invalid_limits(kwargs):
    with pytest.raises(ArgumentError):
        JSON(**kwargs)


def test_json_constructor_rejects_invalid_paths_and_types():
    with pytest.raises(ArgumentError, match="more than once"):
        JSON(typed_paths={"user_id": UInt32}, user_id=UInt64)
    with pytest.raises(ArgumentError, match="mapping"):
        JSON(typed_paths=[("user_id", UInt32)])
    with pytest.raises(ArgumentError, match="path names"):
        JSON(typed_paths={13: UInt32})
    with pytest.raises(ArgumentError, match="type name strings"):
        JSON(typed_paths={"user_id": object()})
    with pytest.raises(ArgumentError, match="Invalid ClickHouse type hint"):
        JSON(typed_paths={"user_id": "Array("})
    for type_name in (
        "UInt32 garbage",
        "Array(UInt32) garbage",
        "Array(UInt32 garbage)",
        "Nullable(UInt32 garbage)",
        "Map(String, Tuple(id UInt32, value Array(UInt64 garbage)))",
    ):
        with pytest.raises(ArgumentError, match="trailing text"):
            JSON(typed_paths={"user_id": type_name})
    for type_name in (
        "Array(UInt32, String)",
        "Nullable(UInt32, String)",
        "LowCardinality(String, UInt32)",
        "Map(String, UInt32, String)",
        "Array(Map(String))",
    ):
        with pytest.raises(ArgumentError, match="requires exactly"):
            JSON(typed_paths={"user_id": type_name})
    for type_name in (
        "JSON(foo)",
        "JSON(`child` UInt32, garbage)",
        "Array(JSON(`child` UInt32, garbage))",
    ):
        with pytest.raises(ArgumentError, match="must be named"):
            JSON(typed_paths={"user_id": type_name})
    for type_class in (Array, Map, FixedString, Nested, Tuple, AggregateFunction, SimpleAggregateFunction):
        with pytest.raises(ArgumentError, match="requires constructor arguments"):
            JSON(typed_paths={"user_id": type_class})
    for type_name in (
        "FixedString(-1)",
        "AggregateFunction",
        "SimpleAggregateFunction",
        "Variant",
        "Nested",
        "Tuple",
        "Tuple()",
        "Array(Tuple())",
        "JSON(max_dynamic_paths = 10001)",
        "json(max_dynamic_types = 255)",
        "Array(JSON(max_dynamic_paths = 10001))",
        "JSON(`` UInt32)",
        "JSON(SKIP ``)",
        "JSON(SKIP `REGEXP`)",
    ):
        with pytest.raises(ArgumentError):
            JSON(typed_paths={"user_id": type_name})
    for type_name in (
        "String('x')",
        "String(0)",
        "String(13)",
        "String(+13)",
        "String(-1)",
        "UInt32(0)",
        "UInt32(13)",
        "Int8(1.5)",
        "Int8(-1)",
        "Float32(13)",
        "Float64(13)",
        "BFloat16(13)",
        "Array(UInt32(13))",
        "Map(String, Float32(13))",
        "UUID(foo)",
        "Dynamic(foo)",
        "Dynamic(max_types=-1)",
        "Dynamic(max_types=255)",
        "Bool(13)",
        "IPv4(13)",
        "Date(13)",
        "Nothing(foo)",
        "DateTime64(3, 'UTC', 9)",
        "DateTime(10)",
        "DateTime64(3, 'Not/A-Timezone')",
        "Time64(3, 9)",
        "Time64(foo)",
        "Time64(-1)",
        "Time64(10)",
        "Decimal(10, 2, 3)",
        "Decimal(77, 2)",
        "Decimal(10, 11)",
        "Decimal32(2, 3)",
        "Decimal64(2, 3)",
        "Decimal128(2, 3)",
        "Decimal256(2, 3)",
        "Decimal256(77)",
        "QBit(Float32, 13, 9)",
        "SimpleAggregateFunction(any, UInt32, String)",
        "AggregateFunction(any, UInt32)",
        "Array(AggregateFunction(any, UInt32))",
        "SimpleAggregateFunction(any, AggregateFunction(any, UInt32))",
        "Enum('low' = -32769, 'high' = 32767)",
        "Enum('low' = -32768, 'high' = 32768)",
        "Enum8('low' = -129)",
        "Enum8('high' = 128)",
        "Enum16('low' = -32769)",
        "Enum16('high' = 32768)",
        "Enum8('user_1' = 1, 'user_1' = 2)",
        "Enum8('user_1' = 1, 'user_2' = 1)",
        "SharedDataString",
        "SharedVariant",
    ):
        with pytest.raises(ArgumentError):
            JSON(typed_paths={"user_id": type_name})
    for type_name in (
        "JSON(SKIP REGEXP 'a'garbage)",
        "JSON(SKIP REGEXP garbage)",
        "JSON(SKIP REGEXP 'a' 'b')",
        "JSON(SKIP REGEXP'x'garbage)",
        "JSON(SKIP REGEXP\t'x'garbage)",
    ):
        with pytest.raises(ArgumentError, match="REGEXP"):
            JSON(typed_paths={"user_id": type_name})
    for type_name in ("JSON(SKIP `a` garbage)", "JSON(SKIP a garbage)", "JSON(SKIP 'a')"):
        with pytest.raises(ArgumentError, match="one path identifier"):
            JSON(typed_paths={"user_id": type_name})
    assert JSON(typed_paths={"user_id": "JSON(SKIP REGEXPfoo, SKIP REGEXP_foo)"}).name == (
        "JSON(`user_id` JSON(SKIP `REGEXP_foo`, SKIP `REGEXPfoo`))"
    )
    for kwargs in (
        {"typed_paths": {"": UInt32}},
        {"skip_paths": [""]},
        {"skip_paths": ["REGEXP"]},
        {"skip_paths": ["regexp"]},
    ):
        with pytest.raises(ArgumentError):
            JSON(**kwargs)
    with pytest.raises(ArgumentError, match="TypeDef"):
        JSON(type_def="garbage")
    with pytest.raises(ArgumentError, match="sequence of strings"):
        JSON(skip_paths="user_id")
    with pytest.raises(ArgumentError, match="at most 1000"):
        JSON(typed_paths={f"path_{index}": UInt32 for index in range(1001)})


@pytest.mark.parametrize(
    "operand, expected_path",
    [
        ("a.b", "a.b"),
        ("`a.b`", "a.b"),
        ("`a`.`b`", "a.b"),
        ('"a"."b"', "a.b"),
        ("_a13", "_a13"),
        ("a_b13", "a_b13"),
        ("a$", "a$"),
        ("`REGEXP`.`foo`", "REGEXP.foo"),
        ("REGEXP_foo.foo", "REGEXP_foo.foo"),
        ("`a\\`b`.`c`", "a`b.c"),
        ("`a``b`.c", "a`b.c"),
    ],
)
def test_json_raw_skip_accepts_compound_identifiers(operand, expected_path):
    hint = JSON(typed_paths={"value": f"JSON(SKIP {operand})"})

    assert hint.ch_type.typed_types[0].skip_paths == [expected_path]


@pytest.mark.parametrize("wrapper", ["JSON(SKIP {})", "Array(JSON(SKIP {}))"])
@pytest.mark.parametrize(
    "operand",
    ["a+b", "a-b", "a/b", "a%2Eb", "a:b", "13", "a.13", "a..b", ".a", "a.", "*", "REGEXP.foo"],
)
def test_json_raw_skip_rejects_non_identifier_expressions(wrapper, operand):
    with pytest.raises(ArgumentError, match="one path identifier"):
        JSON(typed_paths={"value": wrapper.format(operand)})


def test_json_constructor_quotes_regexp_compound_skip_path_data():
    assert JSON(skip_paths=["REGEXP.foo"]).name == "JSON(SKIP `REGEXP.foo`)"


@pytest.mark.parametrize("separator", ["\t", "\n", "\r\n"])
def test_json_raw_skip_accepts_sql_whitespace(separator):
    hint = JSON(typed_paths={"value": f"JSON(SKIP{separator}path, SKIP{separator}REGEXP 'x')"})

    nested = hint.ch_type.typed_types[0]
    assert nested.skip_paths == ["path"]
    assert nested.skip_regexps == ["x"]


@pytest.mark.parametrize(
    "type_hint, expected_key",
    [
        ("Tuple(`path one` UInt32)", "path one"),
        ("Tuple(`path,one` UInt32)", "path,one"),
        ("Tuple(`path)one` UInt32)", "path)one"),
        ("Tuple(`path\\`one` UInt32)", "path`one"),
        ('Tuple("path one" UInt32)', "path one"),
        ("Nested(`path one` UInt32)", "path one"),
        ("Nested(`path,one` UInt32)", "path,one"),
        ("Nested(`path)one` UInt32)", "path)one"),
        ("Nested(`path\\`one` UInt32)", "path`one"),
        ('Nested("path one" UInt32)', "path one"),
        ("Array(Tuple(`path one` UInt32))", "path one"),
        ('Map(String, Nested("path one" UInt32))', "path one"),
    ],
)
def test_json_raw_named_container_hints_preserve_quoted_names(type_hint, expected_key):
    hint = JSON(typed_paths={"value": type_hint})

    assert _format_identifier(expected_key) in hint.name
    assert sqla_type_from_name(hint.name).name == hint.name


def test_json_sqlalchemy_copy_cache_and_ddl_preserve_configuration():
    source = JSON(
        typed_paths={"event.id": "Array(Tuple(id UInt32, value String))"},
        max_dynamic_paths=13,
        skip_paths=["ignored"],
    )
    expected = source.name

    assert source.copy().name == expected
    assert source.adapt(JSON).name == expected
    assert source.dialect_impl(ClickHouseDialect()).name == expected
    assert (
        source._static_cache_key
        == JSON(
            typed_paths={"event.id": "Array(Tuple(id UInt32, value String))"},
            max_dynamic_paths=13,
            skip_paths=["ignored"],
        )._static_cache_key
    )
    assert source._static_cache_key != JSON(typed_paths={"event.id": UInt64})._static_cache_key

    table = Table(
        "events",
        MetaData(),
        Column("id", UInt32),
        Column("payload", source),
        MergeTree(order_by="id"),
    )
    ddl = str(CreateTable(table).compile(dialect=ClickHouseDialect()))
    assert f"`payload` {expected}" in ddl


# One representative per SQLAlchemy base family; Float and Interval bases return a live
# result_processor that ChSqlaType must shadow to None via the MRO (issue #847).
_PASSTHROUGH_TYPES = [
    Int64(),
    Float64(),
    Decimal(18, 4),
    Bool(),
    Date(),
    ChDateTime(),
    Time(),
    Time64(),
    Array(String),
    Enum(keys=["a", "b"], values=[1, 2]),
    String(),
]


@pytest.mark.parametrize("ch_type", _PASSTHROUGH_TYPES, ids=lambda t: type(t).__name__)
def test_result_processor_returns_none(ch_type):
    """result_processor honors the TypeEngine(self, dialect, coltype) contract and returns None."""
    assert ch_type.result_processor(ClickHouseDialect(), None) is None


@pytest.mark.parametrize("ch_type", _PASSTHROUGH_TYPES, ids=lambda t: type(t).__name__)
def test_type_decorator_result_processor(ch_type):
    """Wrapping a ClickHouse type in a TypeDecorator must not crash on result_processor (issue #847)."""

    class Wrapped(TypeDecorator):
        impl = ch_type
        cache_ok = True

    assert Wrapped().result_processor(ClickHouseDialect(), None) is None
