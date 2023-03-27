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

    if test_client.min_version('20'):
        row = test_client.query("SELECT toDateTime64('2022-10-25 10:55:22.789123', 6, 'America/Chicago')",
                                query_tz='America/Chicago').first_row
        assert row[0].tzinfo == test_tz
        assert row[0].hour == 10
        assert row[0].day == 25
        assert row[0].microsecond == 789123


def test_server_timezone(test_client: Client):
    #  This test is really for manual testing since changing the timezone on the test ClickHouse server
    #  still requires a restart.  Other tests will depend on https://github.com/ClickHouse/ClickHouse/pull/44149
    test_client.apply_server_timezone = True
    try:
        date = test_client.query("SELECT toDateTime('2023-03-18 16:04:25') as st").first_row[0]
        if test_client.server_tz == pytz.UTC:
            assert date.tzinfo is None
            assert date == datetime(2023, 3, 18, 16, 4, 25, tzinfo=None)
            assert date.timestamp() == 1679155465
        else:
            den_tz = pytz.timezone('America/Denver').localize(datetime(2020, 8, 8)).tzinfo
            assert date == datetime(2023, 3, 18, 16, 4, 25, tzinfo=den_tz)
            assert date.tzinfo == den_tz
            assert date.timestamp() == 1679177065
    finally:
        test_client.apply_server_timezone = False


def test_column_timezones(test_client: Client):
    date_tz64 = "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai')"
    if not test_client.min_version('20'):
        date_tz64 = "toDateTime('2023-01-02 15:44:22', 'Asia/Shanghai')"
    column_tzs = {'chicago': 'America/Chicago', 'china': 'Asia/Shanghai'}
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                            f'{date_tz64} as china',
                            column_tzs=column_tzs).first_row
    chicago_tz = pytz.timezone('America/Chicago').localize(datetime(2020, 8, 8, 10, 5, 5)).tzinfo
    china_tz = pytz.timezone('Asia/Shanghai').localize(datetime(2024, 12, 4, 10, 5, 5)).tzinfo
    assert row[0].tzinfo == chicago_tz
    assert row[1].tzinfo == china_tz

    if test_client.min_version('20'):
        row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                                "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai') as china").first_row
        if test_client.protocol_version:
            assert row[0].tzinfo == chicago_tz
        else:
            assert row[0].tzinfo is None
        assert row[1].tzinfo == china_tz  # DateTime64 columns work correctly
