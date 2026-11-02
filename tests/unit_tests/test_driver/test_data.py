from datetime import date

import pytest
from clickhouse_connect.driverc.dataconv import epoch_days_to_date as c_date
from clickhouse_connect.driverc.dataconv import pivot as c_pivot

from clickhouse_connect.driver.dataconv import epoch_days_to_date as py_date
from clickhouse_connect.driver.dataconv import pivot as py_pivot

EPOCH = date(1970, 1, 1)
EXTENDED_DATES = (
    [
        date(1, 1, 1),
        date(1, 1, 2),
        date(9999, 12, 31),
        # Python's calendar has no gap between October 4 and October 15, 1582.
        date(1582, 10, 4),
        date(1582, 10, 10),
        date(1582, 10, 15),
        # C fast-path boundaries: days -1, 0, 1 and 47481, 47482.
        date(1969, 12, 31),
        date(1970, 1, 1),
        date(1970, 1, 2),
        date(2099, 12, 31),
        date(2100, 1, 1),
        date(2149, 6, 6),  # UInt16 Date maximum: day 65535.
    ]
    + [date(year, month, day) for year in range(400, 10000, 400) for month, day in ((2, 28), (2, 29), (3, 1), (12, 30), (12, 31))]
    + [date(year + 1, 1, 1) for year in range(400, 10000, 400)]
    + [date(year, month, day) for year in (1500, 1700, 1800, 1900, 2100) for month, day in ((2, 28), (3, 1))]
)
OUT_OF_RANGE_DAYS = {
    "before_min_date": -719163,
    "after_max_date": 2932897,
    "int32_min": -(1 << 31),
    "int32_max": (1 << 31) - 1,
    "year_zero": -719528,
}


def test_date_conv():
    for date_func in (c_date, py_date):
        assert date_func(11322) == date(2000, 12, 31)
        assert date_func(47579) == date(2100, 4, 8)
        assert date_func(0) == date(1970, 1, 1)
        assert date_func(364) == date(1970, 12, 31)
        assert date_func(365) == date(1971, 1, 1)
        assert date_func(500) == date(1971, 5, 16)
        assert date_func(729) == date(1971, 12, 31)
        assert date_func(730) == date(1972, 1, 1)
        assert date_func(1096) == date(1973, 1, 1)
        assert date_func(2250) == date(1976, 2, 29)
        assert date_func(10957) == date(2000, 1, 1)
        assert date_func(11323) == date(2001, 1, 1)
        assert date_func(15941) == date(2013, 8, 24)
        assert date_func(12477) == date(2004, 2, 29)
        assert date_func(12478) == date(2004, 3, 1)
        assert date_func(12783) == date(2004, 12, 31)
        assert date_func(13148) == date(2005, 12, 31)
        assert date_func(19378) == date(2023, 1, 21)
        assert date_func(19378) == date(2023, 1, 21)
        assert date_func(47847) == date(2101, 1, 1)
        assert date_func(54727) == date(2119, 11, 3)
        assert date_func(-18165) == date(1920, 4, 8)


@pytest.mark.parametrize("date_func", [py_date, c_date], ids=["python", "cython"])
@pytest.mark.parametrize("expected", EXTENDED_DATES, ids=date.isoformat)
def test_extended_date_conv(date_func, expected):
    assert date_func((expected - EPOCH).days) == expected


@pytest.mark.parametrize("date_func", [py_date, c_date], ids=["python", "cython"])
@pytest.mark.parametrize("days", OUT_OF_RANGE_DAYS.values(), ids=OUT_OF_RANGE_DAYS)
def test_date_conv_out_of_range(date_func, days):
    with pytest.raises(ValueError):
        date_func(days)


def test_pivot():
    data = [[1, 2, 3], [4, 5, 6]]
    for pivot in (c_pivot, py_pivot):
        result = pivot(data, 0, 2)
        assert result == ((1, 4), (2, 5), (3, 6))
