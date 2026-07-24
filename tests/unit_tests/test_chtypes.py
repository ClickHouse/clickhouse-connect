from datetime import timedelta, timezone

import pytest

from clickhouse_connect.datatypes.container import Nested
from clickhouse_connect.datatypes.registry import get_from_name as gfn
from clickhouse_connect.driver.query import QueryContext

INTERVAL_TYPES = (
    "IntervalYear",
    "IntervalQuarter",
    "IntervalMonth",
    "IntervalWeek",
    "IntervalDay",
    "IntervalHour",
    "IntervalMinute",
    "IntervalSecond",
    "IntervalMillisecond",
    "IntervalMicrosecond",
    "IntervalNanosecond",
)


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


def test_intervals_use_signed_i64_storage():
    for type_name in INTERVAL_TYPES:
        interval_type = gfn(type_name)
        assert interval_type.byte_size == 8
        assert interval_type.np_type == "<i8"


def test_nullable_interval_finalizes_to_pandas_int64():
    pd = pytest.importorskip("pandas")
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    for type_name in INTERVAL_TYPES:
        result = gfn(f"Nullable({type_name})")._finalize_column([-13, None, 79], ctx)
        assert str(result.dtype) == "Int64"
        assert list(result) == [-13, pd.NA, 79]
