import os
import time
import warnings
from datetime import datetime

import pytz
import pytest

from clickhouse_connect.driver import Client, tzutil
from clickhouse_connect.driver.exceptions import ProgrammingError

# We have to localize a datetime from a timezone to get a current, sensible timezone object for testing.  See
# https://stackoverflow.com/questions/35462876/python-pytz-timezone-function-returns-a-timezone-that-is-off-by-9-minutes
chicago_tz = pytz.timezone('America/Chicago').localize(datetime(2020, 8, 8, 10, 5, 5)).tzinfo

# pylint:disable=protected-access

def test_basic_timezones(test_client: Client):
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                            "toDateTime('2023-07-05 15:10:40') as utc",
                            query_tz='America/Chicago').first_row

    assert row[0].tzinfo == chicago_tz
    assert row[0].hour == 10
    assert row[0].day == 25

    assert row[1].tzinfo == chicago_tz
    assert row[1].hour == 10
    assert row[1].day == 5

    if test_client.min_version('20'):
        row = test_client.query("SELECT toDateTime64('2022-10-25 10:55:22.789123', 6, 'America/Chicago')",
                                query_tz='America/Chicago').first_row
        assert row[0].tzinfo == chicago_tz
        assert row[0].hour == 10
        assert row[0].day == 25
        assert row[0].microsecond == 789123


def test_server_timezone(test_client: Client):
    #  This test is really for manual testing since changing the timezone on the test ClickHouse server
    #  still requires a restart.  Other tests will depend on https://github.com/ClickHouse/ClickHouse/pull/44149
    test_client.tz_source = "server"
    test_datetime = datetime(2023, 3, 18, 16, 4, 25)
    try:
        date = test_client.query('SELECT toDateTime(%s) as st', parameters=[test_datetime]).first_row[0]
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
        test_client.tz_source = "auto"


def test_column_timezones(test_client: Client):
    date_tz64 = "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai')"
    if not test_client.min_version('20'):
        date_tz64 = "toDateTime('2023-01-02 15:44:22', 'Asia/Shanghai')"
    column_tzs = {'chicago': 'America/Chicago', 'china': 'Asia/Shanghai'}
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                            f'{date_tz64} as china,' +
                            "toDateTime('2023-07-05 15:10:40') as utc",
                            column_tzs=column_tzs).first_row
    china_tz = pytz.timezone('Asia/Shanghai').localize(datetime(2024, 12, 4, 10, 5, 5)).tzinfo
    assert row[0].tzinfo == chicago_tz
    assert row[1].tzinfo == china_tz
    assert row[2].tzinfo is None

    if test_client.min_version('20'):
        row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                                "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai') as china").first_row
        if test_client.protocol_version:
            assert row[0].tzinfo == chicago_tz
        else:
            assert row[0].tzinfo is None
        assert row[1].tzinfo == china_tz  # DateTime64 columns work correctly


def test_local_timezones(test_client: Client):
    denver_tz = pytz.timezone('America/Denver')
    tzutil.local_tz = denver_tz
    test_client.tz_source = "local"
    try:
        row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22'," +
                                "'America/Chicago') as chicago," +
                                "toDateTime('2023-07-05 15:10:40') as raw_utc_dst," +
                                "toDateTime('2023-07-05 12:44:22', 'UTC') as forced_utc," +
                                "toDateTime('2023-12-31 17:00:55') as raw_utc_std").first_row
        if test_client.protocol_version:
            assert row[0].tzinfo.tzname(None) == chicago_tz.tzname(None)
        else:
            assert row[0].tzinfo.tzname(None) == denver_tz.tzname(None)
        assert row[1].tzinfo.tzname(None) == denver_tz.tzname(None)
        assert row[2].tzinfo is None
        assert row[3].tzinfo.tzname(None) == denver_tz.tzname(None)
    finally:
        tzutil.local_tz = pytz.UTC
        test_client.tz_source = "auto"


def test_naive_timezones(test_client: Client):
    row = test_client.query("SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," +
                            "toDateTime('2023-07-05 15:10:40') as utc").first_row

    if test_client.protocol_version:
        assert row[0].tzinfo == chicago_tz
    else:
        assert row[0].tzinfo is None
    assert row[1].tzinfo is None


def test_timezone_binding_client(test_client: Client):
    os.environ['TZ'] = 'America/Denver'
    time.tzset()
    denver_tz = pytz.timezone('America/Denver')
    tzutil.local_tz = denver_tz
    test_client.tz_source = "local"
    denver_time = datetime(2023, 3, 18, 16, 4, 25, tzinfo=denver_tz)
    try:
        server_time = test_client.query(
            'SELECT toDateTime(%(dt)s) as dt', parameters={'dt': denver_time}).first_row[0]
        assert server_time == denver_time
    finally:
        os.environ['TZ'] = 'UTC'
        tzutil.local_tz = pytz.UTC
        time.tzset()
        test_client.tz_source = "auto"

    naive_time = datetime(2023, 3, 18, 16, 4, 25)
    server_time = test_client.query(
        'SELECT toDateTime(%(dt)s) as dt', parameters={'dt': naive_time}).first_row[0]
    assert server_time.astimezone(pytz.UTC) == naive_time.astimezone(pytz.UTC)

    utc_time = datetime(2023, 3, 18, 16, 4, 25, tzinfo=pytz.UTC)
    server_time = test_client.query(
        'SELECT toDateTime(%(dt)s) as dt', parameters={'dt': utc_time}).first_row[0]
    assert server_time.astimezone(pytz.UTC) == utc_time


def test_timezone_binding_server(test_client: Client):
    os.environ['TZ'] = 'America/Denver'
    time.tzset()
    denver_tz = pytz.timezone('America/Denver')
    tzutil.local_tz = denver_tz
    test_client.tz_source = "local"
    denver_time = datetime(2022, 3, 18, 16, 4, 25, tzinfo=denver_tz)
    try:
        server_time = test_client.query(
            'SELECT toDateTime({dt:DateTime}) as dt', parameters={'dt': denver_time}).first_row[0]
        assert server_time == denver_time
    finally:
        os.environ['TZ'] = 'UTC'
        time.tzset()
        tzutil.local_tz = pytz.UTC
        test_client.tz_source = "auto"

    naive_time = datetime(2022, 3, 18, 16, 4, 25)
    server_time = test_client.query(
        'SELECT toDateTime({dt:DateTime}) as dt', parameters={'dt': naive_time}).first_row[0]
    assert naive_time.astimezone(pytz.UTC) == server_time.astimezone(pytz.UTC)

    utc_time = datetime(2020, 3, 18, 16, 4, 25, tzinfo=pytz.UTC)
    server_time = test_client.query(
        'SELECT toDateTime({dt:DateTime}) as dt', parameters={'dt': utc_time}).first_row[0]
    assert server_time.astimezone(pytz.UTC) == utc_time


def test_tz_mode(test_client: Client):
    row = test_client.query("SELECT toDateTime('2023-07-05 15:10:40') as dt," +
                            "toDateTime('2023-07-05 15:10:40', 'UTC') as dt_utc",
                            query_tz='UTC').first_row
    assert row[0].tzinfo is None
    assert row[1].tzinfo is None

    row = test_client.query("SELECT toDateTime('2023-07-05 15:10:40') as dt," +
                            "toDateTime('2023-07-05 15:10:40', 'UTC') as dt_utc",
                            query_tz='UTC', tz_mode="aware").first_row
    assert row[0].tzinfo == pytz.UTC
    assert row[1].tzinfo == pytz.UTC

    if test_client.min_version('20'):
        row = test_client.query("SELECT toDateTime64('2023-07-05 15:10:40.123456', 6) as dt64," +
                                "toDateTime64('2023-07-05 15:10:40.123456', 6, 'UTC') as dt64_utc",
                                query_tz='UTC').first_row
        assert row[0].tzinfo is None
        assert row[1].tzinfo is None
        assert row[0].microsecond == 123456

        row = test_client.query("SELECT toDateTime64('2023-07-05 15:10:40.123456', 6) as dt64," +
                                "toDateTime64('2023-07-05 15:10:40.123456', 6, 'UTC') as dt64_utc",
                                query_tz='UTC', tz_mode="aware").first_row
        assert row[0].tzinfo == pytz.UTC
        assert row[1].tzinfo == pytz.UTC
        assert row[0].microsecond == 123456


def test_apply_server_timezone_setter_deprecated(test_client: Client):
    """Setting client.apply_server_timezone should emit a DeprecationWarning and update state."""
    try:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            test_client.apply_server_timezone = True
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "apply_server_timezone is deprecated" in str(w[0].message)
        assert test_client.tz_source == "server"
        assert test_client._apply_server_tz is True

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            test_client.apply_server_timezone = False
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
        assert test_client.tz_source == "local"
        assert test_client._apply_server_tz is False
    finally:
        test_client.tz_source = "auto"


def test_apply_server_timezone_getter_deprecated(test_client: Client):
    """Reading client.apply_server_timezone should emit a DeprecationWarning."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = test_client.apply_server_timezone
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


def test_tz_source_setter_validates(test_client: Client):
    """Setting client.tz_source to an invalid value should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match='tz_source must be'):
        test_client.tz_source = "serer"


def test_tz_source_setter_auto_restores_dst_safe(test_client: Client):
    """Setting tz_source back to 'auto' should re-resolve based on server DST safety."""
    original = test_client._apply_server_tz
    try:
        test_client.tz_source = "local"
        assert test_client._apply_server_tz is False
        test_client.tz_source = "auto"
        assert test_client._apply_server_tz == original
    finally:
        test_client.tz_source = "auto"
