import struct

import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.options import pd, pd_time_test
from clickhouse_connect.driver.query import QueryContext
from tests.helpers import bytes_source

pytestmark = pytest.mark.skipif(pd is None, reason="Pandas package not installed")


def test_pd_time_test_datetime64_ns():
    series = pd.Series([1, 2, 3], dtype="datetime64[ns]")
    assert pd_time_test(series) is True


def test_pd_time_test_datetime64_s():
    series = pd.Series([1, 2, 3], dtype="datetime64[s]")
    assert pd_time_test(series) is True


def test_pd_time_test_timedelta64_ns():
    series = pd.Series([1, 2, 3], dtype="timedelta64[ns]")
    assert pd_time_test(series) is True


def test_pd_time_test_int64():
    series = pd.Series([1, 2, 3], dtype="int64")
    assert pd_time_test(series) is False


def test_pd_time_test_float64():
    series = pd.Series([1.0, 2.0, 3.0], dtype="float64")
    assert pd_time_test(series) is False


def test_pd_time_test_object():
    series = pd.Series(["a", "b", "c"], dtype="object")
    assert pd_time_test(series) is False


def test_pd_time_test_empty_object_series():
    series = pd.Series([], dtype="object")
    assert pd_time_test(series) is False


def test_pd_time_test_empty_datetime64_series():
    series = pd.Series([], dtype="datetime64[ns]")
    assert pd_time_test(series) is True


def test_pd_time_test_empty_timedelta64_series():
    series = pd.Series([], dtype="timedelta64[ns]")
    assert pd_time_test(series) is True


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
@pytest.mark.parametrize(
    "values,null_map",
    [
        ((2**63 - 1025, 2**63), b"\x00\x00"),
        ((9007199254740993, 2**63), b"\x00\x00"),
        ((9007199254740993, 2**63), b"\x00\x01"),
        ((2**63, 0), b"\x00\x01"),
        ((2**63 - 1, 9007199254740993), b"\x00\x00"),
    ],
)
def test_nullable_uint64_df_values_around_int64_max(buffer_cls, values, null_map):
    """query_df keeps the values pandas infers from the list of Python ints, rounding included"""
    uint64_type = registry.get_from_name("Nullable(UInt64)")
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    source = bytes_source(null_map + struct.pack("<QQ", *values), cls=buffer_cls)
    expected = pd.array([pd.NA if is_null else value for value, is_null in zip(values, null_map)], dtype="UInt64")

    pd.testing.assert_extension_array_equal(uint64_type.read_column(source, 2, ctx), expected, check_exact=True)


@pytest.mark.parametrize("buffer_cls", [CResponseBuffer, PyResponseBuffer], ids=["cython", "python"])
@pytest.mark.parametrize(
    "type_name,ticks,query_tz",
    [
        ("Nullable(DateTime64(3, 'Asia/Tokyo'))", 9223372036854, "Asia/Tokyo"),
        ("Nullable(DateTime64(6, 'Asia/Tokyo'))", 9223372036854775, "Asia/Tokyo"),
        ("Nullable(DateTime64(3, 'America/New_York'))", -9223372036854, None),
        ("Nullable(DateTime64(6, 'America/New_York'))", -9223372036854775, None),
        ("Nullable(DateTime64(3, 'Asia/Tokyo'))", 2**63 - 1, "Asia/Tokyo"),
        ("Nullable(DateTime64(3, 'America/New_York'))", -(2**63) + 1, None),
    ],
)
def test_nullable_datetime64_df_near_nanosecond_bounds(buffer_cls, type_name, ticks, query_tz):
    """Instants whose UTC or local wall time is near or past the nanosecond range keep the per-row path,
    so pandas 2 raises the same error there"""
    dt_type = registry.get_from_name(type_name)
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True, query_tz=query_tz)
    payload = b"\x00\x01" + struct.pack("<qq", ticks, 0)

    def decode(read_nullable_column):
        try:
            column = read_nullable_column(bytes_source(payload, cls=buffer_cls), 2, ctx, None)
            return type(column), dt_type._finalize_column(column, ctx).tolist()
        except (OverflowError, ValueError) as ex:
            return type(ex)

    def generic_read(source, num_rows, read_ctx, read_state):
        return ClickHouseType._read_nullable_column(dt_type, source, num_rows, read_ctx, read_state)

    assert decode(dt_type._read_nullable_column) == decode(generic_read)


def test_map_df_unboxable_elements_fail_in_row_order():
    """A Map whose first row holds an out-of-range value and whose second row holds an out-of-range key raises the
    same error as building each row from slices of the key and value arrays"""
    map_type = registry.get_from_name("Map(DateTime64(3, 'Asia/Tokyo'), DateTime64(3, 'Asia/Tokyo'))")
    element_type = registry.get_from_name("DateTime64(3, 'Asia/Tokyo')")
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    payload = struct.pack("<QQ", 1, 2) + struct.pack("<qq", 0, 2**63 - 1) + struct.pack("<qq", -(2**63) + 1, 0)

    def outcome(read):
        try:
            return read(bytes_source(payload))
        except ValueError as ex:
            return str(ex)

    def read_rows_from_slices(source):
        offsets = source.read_array("Q", 2)
        keys = element_type.read_column_data(source, 2, ctx, None)
        values = element_type.read_column_data(source, 2, ctx, None)
        return [dict(zip(keys[start:end], values[start:end])) for start, end in zip([0, *offsets[:-1]], offsets)]

    assert outcome(lambda source: map_type.read_column(source, 2, ctx)) == outcome(read_rows_from_slices)
