import struct

import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.datatypes import registry
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
        ((2**64 - 1, 9007199254740993), b"\x00\x00"),
    ],
)
def test_nullable_uint64_df_values_around_int64_max(buffer_cls, values, null_map):
    """Nullable UInt64 values retain every bit, with and without NULLs."""
    uint64_type = registry.get_from_name("Nullable(UInt64)")
    ctx = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    source = bytes_source(null_map + struct.pack("<QQ", *values), cls=buffer_cls)
    result = uint64_type.read_column(source, 2, ctx)
    assert str(result.dtype) == "UInt64"
    assert [None if pd.isna(value) else int(value) for value in result] == [
        None if is_null else value for value, is_null in zip(values, null_map)
    ]
