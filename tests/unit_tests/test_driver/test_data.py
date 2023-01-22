from datetime import date
from clickhouse_connect.driver.dataconv import epoch_days_to_date as py_date
# pylint: disable=no-name-in-module
from clickhouse_connect.driverc.dataconv import epoch_days_to_date as c_date


def test_date_conv():
    for date_func in (c_date, py_date):
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
