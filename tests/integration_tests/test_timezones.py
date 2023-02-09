from datetime import datetime

import pytz

from clickhouse_connect.driver import Client


def test_basic_timezones(test_client: Client):
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago')",
                            query_tz='America/Chicago').first_row

    # We have to localize a datetime from a timezone to get a current, sensible timezone object for testing.  See
    # https://stackoverflow.com/questions/35462876/python-pytz-timezone-function-returns-a-timezone-that-is-off-by-9-minutes
    test_tz = pytz.timezone('America/Chicago').localize(datetime(2020, 8, 8, 10, 5, 5)).tzinfo
    assert row[0].tzinfo == test_tz
    assert row[0].hour == 10
    assert row[0].day == 25

    row = test_client.query("SELECT toDateTime64('2022-10-25 10:55:22.789123', 6, 'America/Chicago')",
                            query_tz='America/Chicago').first_row
    assert row[0].tzinfo == test_tz
    assert row[0].hour == 10
    assert row[0].day == 25
    assert row[0].microsecond == 789123


def test_column_timezones(test_client: Client):
    column_tzs = {'chicago': 'America/Chicago', 'china': 'Asia/Shanghai'}
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                            "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai') as china",
                            column_tzs=column_tzs).first_row
    chicago_tz = pytz.timezone('America/Chicago').localize(datetime(2020, 8, 8, 10, 5, 5)).tzinfo
    china_tz = pytz.timezone('Asia/Shanghai').localize(datetime(2024, 12, 4, 10, 5, 5)).tzinfo
    assert row[0].tzinfo == chicago_tz
    assert row[1].tzinfo == china_tz
