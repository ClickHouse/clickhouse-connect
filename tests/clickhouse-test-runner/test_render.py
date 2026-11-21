"""Unit tests of the TabSeparated renderer. No server needed. The expected text is what ClickHouse 26.10 writes
for FORMAT TSV."""

import datetime as dt
import decimal
import ipaddress
import math
import uuid
from collections import namedtuple
from zoneinfo import ZoneInfo

import pytest
from clickhouse_test_runner.chtype import TypeParseError, parse_type
from clickhouse_test_runner.render import (
    ColumnWriter,
    RenderError,
    UnsupportedTypeError,
    format_float,
    format_ipv6,
    json_object,
    renderer_for,
)

TypedVariant = namedtuple("TypedVariant", "value type_name")


@pytest.mark.parametrize(
    "value, bits, expected",
    [
        (1e15, 64, "1000000000000000"),
        (1e20, 64, "100000000000000000000"),
        (1e21, 64, "1e21"),
        (1.2345e21, 64, "1.2345e21"),
        (1.2345e20, 64, "123450000000000000000"),
        (0.0001, 64, "0.0001"),
        (1e-6, 64, "0.000001"),
        (1.5e-6, 64, "0.0000015"),
        (1e-7, 64, "1e-7"),
        (1.5e-7, 64, "1.5e-7"),
        (1.5e300, 64, "1.5e300"),
        (0.1 + 0.2, 64, "0.30000000000000004"),
        (5e-324, 64, "5e-324"),
        (-12345678.9, 64, "-12345678.9"),
        (100.0, 64, "100"),
        (math.nan, 64, "nan"),
        (math.inf, 64, "inf"),
        (-math.inf, 64, "-inf"),
        (-0.0, 64, "-0"),
        (0.0, 64, "0"),
        (0.10000000149011612, 32, "0.1"),
        (3.4028234663852886e38, 32, "3.4028235e38"),
        (16777216.0, 32, "16777216"),
        (1.401298464324817e-45, 32, "1e-45"),
    ],
)
def test_format_float(value, bits, expected):
    assert format_float(value, bits) == expected


@pytest.mark.parametrize(
    "address, expected",
    [
        ("::ffff:1.2.3.4", "::ffff:1.2.3.4"),
        ("::1.2.3.4", "::1.2.3.4"),
        ("::", "::"),
        ("::1", "::1"),
        ("2001:db8::1", "2001:db8::1"),
        ("1::", "1::"),
        ("2001:db8:0:1::1", "2001:db8:0:1::1"),
        ("1:0:0:2:0:0:0:3", "1:0:0:2::3"),
        ("1:2:3:4:5:6:7:8", "1:2:3:4:5:6:7:8"),
    ],
)
def test_format_ipv6(address, expected):
    assert format_ipv6(ipaddress.IPv6Address(address).packed) == expected


@pytest.mark.parametrize(
    "name",
    [
        "UInt8",
        "Nullable(String)",
        "Array(Nullable(DateTime64(3, 'UTC')))",
        "Map(String, Array(Tuple(UInt8, String)))",
        "Tuple(a Int32, b String)",
        "Enum8('a' = 1, 'b,c' = 2)",
        "Decimal(9, 2)",
        "LowCardinality(Nullable(String))",
        "SimpleAggregateFunction(sum, UInt64)",
        "AggregateFunction(quantiles(0.5, 0.9), UInt64)",
    ],
)
def test_parse_type_round_trip(name):
    assert str(parse_type(name)) == name


def test_parse_type_named_elements():
    t = parse_type("Tuple(`a b` Int32, c Array(String), `x``y` UInt8)")
    assert [a.name for a in t.args] == ["a b", "c", "x`y"]
    assert t.args[1].type is not None and t.args[1].type.base == "Array"
    with pytest.raises(TypeParseError):
        parse_type("Array(String")


TOKYO = ZoneInfo("Asia/Tokyo")


@pytest.mark.parametrize(
    "type_name, value, expected",
    [
        ("String", "a\tb\\c\nd'e\x00\b\f\x01\v", "a\\tb\\\\c\\nd\\'e\\0\\b\\f\x01\v"),
        ("String", b"\xff\xfe", "\udcff\udcfe"),
        ("FixedString(3)", b"a\x00\x00", "a\\0\\0"),
        ("Nullable(String)", None, "\\N"),
        ("Nullable(String)", "x", "x"),
        ("Array(Nullable(String))", ["a'b", None], "['a\\'b',NULL]"),
        ("Tuple(UInt8, String, Nullable(UInt8))", (1, "x", None), "(1,'x',NULL)"),
        ("Tuple(a UInt8, b String)", {"b": "x", "a": 1}, "(1,'x')"),
        ("Tuple(`a b` UInt8)", {"a b": 1}, "(1)"),
        ("Tuple()", (), "()"),
        ("Nested(a UInt8, b String)", [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], "[(1,'x'),(2,'y')]"),
        ("Map(String, UInt8)", {"k": 1, "a": 2}, "{'k':1,'a':2}"),
        ("Map(UInt8, Map(String, UInt8))", {1: {"b": 2, "a": 3}}, "{1:{'b':2,'a':3}}"),
        ("Int128", -5, "-5"),
        ("UInt256", 2**256 - 1, str(2**256 - 1)),
        ("Decimal(9, 2)", decimal.Decimal("1.50"), "1.5"),
        ("Decimal(9, 2)", decimal.Decimal("0.00"), "0"),
        ("Decimal(9, 2)", decimal.Decimal("100.00"), "100"),
        ("Decimal(38, 10)", decimal.Decimal("-1E-10"), "-0.0000000001"),
        ("DateTime64(3)", dt.datetime(2020, 1, 1, 0, 0, 0, 120000), "2020-01-01 00:00:00.120"),
        ("DateTime64(9, 'Asia/Tokyo')", dt.datetime(2020, 1, 1, 9, 0, 0, 1, tzinfo=TOKYO), "2020-01-01 09:00:00.000001000"),
        ("DateTime('Asia/Tokyo')", dt.datetime(2020, 1, 1, 9, 0, 0, tzinfo=TOKYO), "2020-01-01 09:00:00"),
        ("DateTime", dt.datetime(1970, 1, 1, 0, 0, 0), "1970-01-01 00:00:00"),
        ("Array(Date)", [dt.date(2020, 1, 2)], "['2020-01-02']"),
        ("Date32", dt.date(1900, 1, 1), "1900-01-01"),
        ("Time", -dt.timedelta(hours=1, minutes=2, seconds=3), "-01:02:03"),
        ("Time64(3)", dt.timedelta(hours=100, milliseconds=500), "100:00:00.500"),
        # The server writes a value above 999:59:59 as 999:59:59.
        ("Time64(2)", dt.timedelta(hours=2160, seconds=12), "999:59:59.00"),
        ("Time", -dt.timedelta(hours=2000), "-999:59:59"),
        ("UUID", uuid.UUID("61f0c404-5cb3-11e7-907b-a6006ad3dba0"), "61f0c404-5cb3-11e7-907b-a6006ad3dba0"),
        ("IPv4", ipaddress.IPv4Address("1.2.3.4"), "1.2.3.4"),
        ("Array(IPv6)", [ipaddress.IPv6Address("::ffff:1.2.3.4")], "['::ffff:1.2.3.4']"),
        ("Bool", True, "true"),
        ("Array(Float64)", [math.nan, math.inf], "[nan,inf]"),
        ("Float32", 0.10000000149011612, "0.1"),
        ("IntervalDay", 3, "3"),
        ("LowCardinality(String)", "a", "a"),
        ("Array(LowCardinality(Nullable(String)))", [None, "a"], "[NULL,'a']"),
        ("SimpleAggregateFunction(sum, UInt64)", 7, "7"),
        ("SimpleAggregateFunction(anyLast, JSON)", {"a": 1}, '{"a":1}'),
        ("SimpleAggregateFunction(anyLast, Dynamic)", "a", "a"),
        ("SimpleAggregateFunction(anyLast, IntervalSecond)", 5, "5"),
        ("SimpleAggregateFunction(groupArrayArray, Array(Variant(String, UInt64)))", [TypedVariant(1, "UInt64")], "[1]"),
        ("Enum8('a' = 1, 'b' = 2)", "b", "b"),
        ("Point", (1.5, 2.0), "(1.5,2)"),
        ("Ring", [(1.0, 2.0), (3.0, 4.0)], "[(1,2),(3,4)]"),
        ("MultiPolygon", [[[(1.0, 2.0)]]], "[[[(1,2)]]]"),
        ("Nothing", None, "\\N"),
        ("Nullable(Nothing)", None, "\\N"),
        ("Variant(String, UInt64)", TypedVariant(1, "UInt64"), "1"),
        ("Variant(String, UInt64)", TypedVariant("a'", "String"), "a\\'"),
        ("Array(Variant(String, UInt64))", [TypedVariant("a", "String"), None], "['a',NULL]"),
        ("Dynamic", [1, "a", None], "[1,'a',NULL]"),
        ("JSON", {"a": 'x/"\t', "b": {"c": [1.0, None]}}, '{"a":"x\\\\/\\\\"\\\\t","b":{"c":[1,null]}}'),
        ("Array(JSON)", [{"a": "x'"}], '[\'{"a":"x\\\'"}\']'),
        (
            "JSON(`a.b` DateTime64(3), c Float32, max_dynamic_paths = 2, SKIP d)",
            {"a": {"b": dt.datetime(2020, 1, 1)}, "c": 0.10000000149011612},
            '{"a":{"b":"2020-01-01 00:00:00.000"},"c":0.1}',
        ),
        ("Dynamic", dt.datetime(2020, 1, 1, 0, 0, 0, 500000), "2020-01-01 00:00:00.500"),
        # The server writes the JSON paths in sorted order; the client gives the typed paths first.
        ("JSON(a UInt32, d UInt32)", {"a": 3, "d": 1, "c": 2}, '{"a":3,"c":2,"d":1}'),
        ("JSON(`b.z` UInt8)", {"b": {"z": 1, "y": 2}, "a": {"x": 3}}, '{"a":{"x":3},"b":{"y":2,"z":1}}'),
        ("JSON", {"arr": [{"y": 1, "x": 2}], "a": {}}, '{"a":{},"arr":[{"x":2,"y":1}]}'),
        (
            "JSON(m Map(String, UInt8), t Tuple(z UInt8, y UInt8))",
            {"t": {"z": 1, "y": 2}, "m": {"b": 1, "a": 2}},
            '{"m":{"b":1,"a":2},"t":{"z":1,"y":2}}',
        ),
        ("Array(JSON(b UInt8))", [{"b": 1, "a": 2}], '[\'{"a":2,"b":1}\']'),
    ],
)
def test_render(type_name, value, expected):
    assert renderer_for(type_name)(value) == expected


def test_raw_string():
    w = ColumnWriter("Nullable(String)")
    assert w.write("a\tb", raw=True) == "a\tb"
    assert w.write("a\tb", raw=False) == "a\\tb"
    assert ColumnWriter("UInt8").write(1, raw=True) == "1"


@pytest.mark.parametrize(
    "type_name, value",
    [
        # A value of another Python type than the client should give.
        ("UInt8", "1"),
        ("UInt8", True),
        ("String", None),
        ("Tuple(a UInt8, b UInt8)", {"a": 1}),
        ("Variant(String, UInt64)", 1),
    ],
)
def test_render_mismatch(type_name, value):
    with pytest.raises(RenderError):
        renderer_for(type_name)(value)


@pytest.mark.parametrize("type_name", ["AggregateFunction(sum, UInt64)", "Object('json')"])
def test_render_unsupported(type_name):
    with pytest.raises(UnsupportedTypeError):
        renderer_for(type_name)


def test_json_object_key_with_a_dot():
    # With json_type_escape_dots_in_keys = 1 a key can hold a dot. The server sorts the paths with the dot
    # escaped (a%2Eb < a.c < a_b): '{"a.b":1,"a":{"c":5},"a_b":4}'.
    assert json_object({"a": {"c": 5}, "a_b": 4, "a.b": 1}, {}) == '{"a.b":1,"a":{"c":5},"a_b":4}'
