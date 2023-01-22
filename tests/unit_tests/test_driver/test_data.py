from datetime import date
from clickhouse_connect.driver.dataconv import epoch_days_to_date


def test_date_conv():
    assert epoch_days_to_date(47579) == date(2100, 4, 8)
    assert epoch_days_to_date(0) == date(1970, 1, 1)
    assert epoch_days_to_date(364) == date(1970, 12, 31)
    assert epoch_days_to_date(365) == date(1971, 1, 1)
    assert epoch_days_to_date(500) == date(1971, 5, 16)
    assert epoch_days_to_date(729) == date(1971, 12, 31)
    assert epoch_days_to_date(730) == date(1972, 1, 1)
    assert epoch_days_to_date(1096) == date(1973, 1, 1)
    assert epoch_days_to_date(2250) == date(1976, 2, 29)
    assert epoch_days_to_date(10957) == date(2000, 1, 1)
    assert epoch_days_to_date(15941) == date(2013, 8, 24)
    assert epoch_days_to_date(12477) == date(2004, 2, 29)
    assert epoch_days_to_date(12478) == date(2004, 3, 1)
    assert epoch_days_to_date(12783) == date(2004, 12, 31)
    assert epoch_days_to_date(13148) == date(2005, 12, 31)
    assert epoch_days_to_date(19378) == date(2023, 1, 21)
    assert epoch_days_to_date(19378) == date(2023, 1, 21)
    assert epoch_days_to_date(47847) == date(2101, 1, 1)
    assert epoch_days_to_date(54727) == date(2119, 11, 3)
