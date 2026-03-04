import numpy as np
import pytest

from clickhouse_connect.driver.options import pd, pd_time_test

pytestmark = pytest.mark.skipif(pd is None, reason="Pandas package not installed")


def test_pd_time_test_datetime64_series():
    s = pd.Series(pd.to_datetime(["2024-01-01"]))
    assert pd_time_test(s) is True


def test_pd_time_test_datetime64_various_resolutions():
    for unit in ("s", "ms", "us", "ns"):
        s = pd.Series(pd.to_datetime(["2024-01-01"]), dtype=f"datetime64[{unit}]")
        assert pd_time_test(s) is True, f"Failed for datetime64[{unit}]"


def test_pd_time_test_tz_aware_datetime():
    s = pd.Series(pd.to_datetime(["2024-01-01"]).tz_localize("US/Eastern"))
    assert pd_time_test(s) is True


def test_pd_time_test_timedelta_series():
    s = pd.Series(pd.to_timedelta(["1 day", "2 hours"]))
    assert pd_time_test(s) is True


def test_pd_time_test_int_series_rejected():
    assert pd_time_test(pd.Series([1, 2, 3])) is False


def test_pd_time_test_string_series_rejected():
    assert pd_time_test(pd.Series(["a", "b"])) is False


def test_pd_time_test_float_series_rejected():
    assert pd_time_test(pd.Series([1.0, 2.0])) is False


def test_pd_time_test_bool_series_rejected():
    assert pd_time_test(pd.Series([True, False])) is False


def test_pd_time_test_raw_dtype():
    assert pd_time_test(np.dtype("datetime64[ns]")) is True
    assert pd_time_test(np.dtype("int64")) is False
