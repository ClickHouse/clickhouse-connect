import pytest

from clickhouse_connect.driver.options import pd, pd_time_test  # pylint: disable=no-name-in-module

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
