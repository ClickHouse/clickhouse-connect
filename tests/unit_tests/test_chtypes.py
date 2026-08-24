import struct
from datetime import timedelta, timezone

import pytest

from clickhouse_connect.datatypes.container import Nested
from clickhouse_connect.datatypes.registry import get_from_name as gfn
from clickhouse_connect.driver.buffer import ResponseBuffer
from clickhouse_connect.driver.query import QueryContext


def test_enum_parse():
    enum_type = gfn("Enum8('OZC|8;' = -125, '6MQ4v-t' = -114, 'As7]sEg\\'' = 40, 'v~l$PR5' = 84)")
    assert "OZC|8;" in enum_type._name_map
    enum_type = gfn("Enum8('\\'\"2Af' = 93,'KG;+\\' = -114,'j0' = -40)")
    assert "'\"2Af" in enum_type._name_map
    enum_type = gfn("Enum8('value1' = 7, 'value2'=5)")
    assert enum_type.name == "Enum8('value2' = 5, 'value1' = 7)"
    assert 7 in enum_type._int_map
    assert 5 in enum_type._int_map
    enum_type = gfn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
    assert r"alpha'" == enum_type._int_map[3822]
    assert -3 == enum_type._name_map["beta&&"]


def test_names():
    array_type = gfn("Array(Nullable(FixedString(50)))")
    assert array_type.name == "Array(Nullable(FixedString(50)))"
    array_type = gfn("Array(Enum8('user_name' = 1, 'ip_address' = -2, 'forwarded_ip_address' = 3, 'client_key' = 4))")
    assert array_type.name == ("Array(Enum8('ip_address' = -2, 'user_name' = 1, 'forwarded_ip_address' = 3, 'client_key' = 4))")


def test_nested_parse():
    nested_type = gfn("Nested(str1 String, int32 UInt32)")
    assert nested_type.name == "Nested(str1 String, int32 UInt32)"
    assert isinstance(nested_type, Nested)
    nested_type = gfn("Nested(id Int64, data Nested(inner_key String, inner_map Map(String, UUID)))")
    assert nested_type.name == "Nested(id Int64, data Nested(inner_key String, inner_map Map(String, UUID)))"
    nest = "key_0 Enum16('[m(X*' = -18773, '_9as' = 11854, '&e$LE' = 27685), key_1 Nullable(Decimal(62, 38))"
    nested_name = f"Nested({nest})"
    nested_type = gfn(nested_name)
    assert nested_type.name == nested_name


def test_named_tuple():
    tuple_type = gfn("Tuple(Int64, String)")
    assert tuple_type.name == "Tuple(Int64, String)"
    tuple_type = gfn("Tuple(`key` Int64, `value` String)")
    assert tuple_type.name == "Tuple(`key` Int64, `value` String)"


def test_datetime_fixed_offset_timezone():
    """DateTime('Fixed/UTC+05:30:00') is emitted by ClickHouse servers without IANA tzdb."""
    dt_type = gfn("DateTime('Fixed/UTC+05:30:00')")
    assert dt_type.tzinfo == timezone(timedelta(hours=5, minutes=30))


def test_datetime_fixed_offset_negative_timezone():
    dt_type = gfn("DateTime('Fixed/UTC-03:00:00')")
    assert dt_type.tzinfo == timezone(timedelta(hours=-3))


def test_datetime64_fixed_offset_timezone():
    dt64_type = gfn("DateTime64(3, 'Fixed/UTC+05:30:00')")
    assert dt64_type.tzinfo == timezone(timedelta(hours=5, minutes=30))


def _read_column(type_name: str, payload: bytes, query_formats=None):
    """Read a single row of `type_name` out of its native bytes."""
    ch_type = gfn(type_name)
    ctx = QueryContext(query_formats=query_formats)
    source = ResponseBuffer(type("_Src", (), {"gen": iter([payload])})())
    state = ch_type.read_column_prefix(source, ctx)
    return ch_type.read_column_data(source, 1, ctx, state)[0]


def _lstr(text: str) -> bytes:
    encoded = text.encode()
    return bytes([len(encoded)]) + encoded


# One map holding two pairs that share a key, which ClickHouse permits: server
# side a Map is an Array(Tuple(key, value)) and is never deduplicated.
_DUP_MAP = struct.pack("Q", 2) + _lstr("k") + _lstr("k") + _lstr("1") + _lstr("2")
_DUP_PAIRS = [("k", "1"), ("k", "2")]

MAP_SHAPES = [
    ("Map(String, String)", _DUP_MAP, {"k": "2"}, _DUP_PAIRS),
    ("Array(Map(String, String))", struct.pack("Q", 1) + _DUP_MAP, [{"k": "2"}], [_DUP_PAIRS]),
    (
        "Map(String, Map(String, String))",
        struct.pack("Q", 1) + _lstr("a") + _DUP_MAP,
        {"a": {"k": "2"}},
        [("a", _DUP_PAIRS)],
    ),
]


@pytest.mark.parametrize("type_name, payload, native, tuples", MAP_SHAPES)
def test_map_native_format_collapses_duplicate_keys(type_name, payload, native, tuples):
    """The default stays a dict, so existing behavior is unchanged."""
    assert _read_column(type_name, payload) == native


@pytest.mark.parametrize("type_name, payload, native, tuples", MAP_SHAPES)
def test_map_tuple_format_keeps_duplicate_keys(type_name, payload, native, tuples):
    assert _read_column(type_name, payload, {"Map": "tuple"}) == tuples


def test_map_tuple_format_is_declared_valid():
    assert "tuple" in gfn("Map(String, String)").valid_formats
