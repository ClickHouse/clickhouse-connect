import os
import time
import zoneinfo
from datetime import datetime, timezone

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver import Client, tzutil
from clickhouse_connect.driver.exceptions import ProgrammingError

chicago_tz = zoneinfo.ZoneInfo("America/Chicago")
HAS_TZSET = hasattr(time, "tzset")


def _force_process_timezone(monkeypatch, tz_name: str) -> str | None:
    original_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", tz_name)
    time.tzset()
    return original_tz


def _restore_process_timezone(monkeypatch, original_tz: str | None) -> None:
    if original_tz is None:
        monkeypatch.delenv("TZ", raising=False)
    else:
        monkeypatch.setenv("TZ", original_tz)
    time.tzset()


def test_basic_timezones(param_client: Client, call):
    row = call(
        param_client.query,
        "SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," + "toDateTime('2023-07-05 15:10:40') as utc",
        query_tz="America/Chicago",
    ).first_row

    assert row[0].tzinfo == chicago_tz
    assert row[0].hour == 10
    assert row[0].day == 25

    assert row[1].tzinfo == chicago_tz
    assert row[1].hour == 10
    assert row[1].day == 5

    row = call(
        param_client.query, "SELECT toDateTime64('2022-10-25 10:55:22.789123', 6, 'America/Chicago')", query_tz="America/Chicago"
    ).first_row
    assert row[0].tzinfo == chicago_tz
    assert row[0].hour == 10
    assert row[0].day == 25
    assert row[0].microsecond == 789123


def test_server_timezone(param_client: Client, call):
    #  This test is really for manual testing since changing the timezone on the test ClickHouse server
    #  still requires a restart.  Other tests will depend on https://github.com/ClickHouse/ClickHouse/pull/44149
    param_client.tz_source = "server"
    test_datetime = datetime(2023, 3, 18, 16, 4, 25)
    try:
        date = call(param_client.query, "SELECT toDateTime(%s) as st", parameters=[test_datetime]).first_row[0]
        if tzutil.is_utc_timezone(param_client.server_tz):
            assert date.tzinfo is None
            assert date == datetime(2023, 3, 18, 16, 4, 25, tzinfo=None)
            assert date.timestamp() == 1679155465
        else:
            den_tz = zoneinfo.ZoneInfo("America/Denver")
            assert date == datetime(2023, 3, 18, 16, 4, 25, tzinfo=den_tz)
            assert date.tzinfo == den_tz
            assert date.timestamp() == 1679177065
    finally:
        param_client.tz_source = "auto"


def test_column_timezones(param_client: Client, call):
    date_tz64 = "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai')"
    column_tzs = {"chicago": "America/Chicago", "china": "Asia/Shanghai"}
    row = call(
        param_client.query,
        "SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago,"
        + f"{date_tz64} as china,"
        + "toDateTime('2023-07-05 15:10:40') as utc",
        column_tzs=column_tzs,
    ).first_row
    china_tz = zoneinfo.ZoneInfo("Asia/Shanghai")
    assert row[0].tzinfo == chicago_tz
    assert row[1].tzinfo == china_tz
    assert row[2].tzinfo is None

    row = call(
        param_client.query,
        "SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago,"
        + "toDateTime64('2023-01-02 15:44:22.7832', 6, 'Asia/Shanghai') as china",
    ).first_row
    if param_client.protocol_version:
        assert row[0].tzinfo == chicago_tz
    else:
        assert row[0].tzinfo is None
    assert row[1].tzinfo == china_tz  # DateTime64 columns work correctly


def test_local_timezones(param_client: Client, call):
    denver_tz = zoneinfo.ZoneInfo("America/Denver")
    tzutil.local_tz = denver_tz
    param_client.tz_source = "local"
    try:
        row = call(
            param_client.query,
            "SELECT toDateTime('2022-10-25 10:55:22',"
            + "'America/Chicago') as chicago,"
            + "toDateTime('2023-07-05 15:10:40') as raw_utc_dst,"
            + "toDateTime('2023-07-05 12:44:22', 'UTC') as forced_utc,"
            + "toDateTime('2023-12-31 17:00:55') as raw_utc_std",
        ).first_row
        if param_client.protocol_version:
            assert row[0].tzinfo.tzname(None) == chicago_tz.tzname(None)
        else:
            assert row[0].tzinfo.tzname(None) == denver_tz.tzname(None)
        assert row[1].tzinfo.tzname(None) == denver_tz.tzname(None)
        assert row[2].tzinfo is None
        assert row[3].tzinfo.tzname(None) == denver_tz.tzname(None)
    finally:
        tzutil.local_tz = timezone.utc
        param_client.tz_source = "auto"


def test_naive_timezones(param_client: Client, call):
    row = call(
        param_client.query,
        "SELECT toDateTime('2022-10-25 10:55:22', 'America/Chicago') as chicago," + "toDateTime('2023-07-05 15:10:40') as utc",
    ).first_row

    if param_client.protocol_version:
        assert row[0].tzinfo == chicago_tz
    else:
        assert row[0].tzinfo is None
    assert row[1].tzinfo is None


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
def test_timezone_binding_client(param_client: Client, call, monkeypatch):
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "wall")
    try:
        original_tz = _force_process_timezone(monkeypatch, "America/Denver")
        original_local_tz = tzutil.local_tz
        original_tz_source = param_client.tz_source
        denver_tz = zoneinfo.ZoneInfo("America/Denver")
        tzutil.local_tz = denver_tz
        param_client.tz_source = "local"
        denver_time = datetime(2023, 3, 18, 16, 4, 25, tzinfo=denver_tz)
        try:
            server_time = call(param_client.query, "SELECT toDateTime(%(dt)s) as dt", parameters={"dt": denver_time}).first_row[0]
            assert server_time == denver_time
        finally:
            tzutil.local_tz = original_local_tz
            param_client.tz_source = original_tz_source
            _restore_process_timezone(monkeypatch, original_tz)

        naive_time = datetime(2023, 3, 18, 16, 4, 25)
        server_time = call(
            param_client.query,
            "SELECT toString(toDateTime(%(dt)s))",
            parameters={"dt": naive_time},
        ).first_row[0]
        assert server_time == naive_time.strftime("%Y-%m-%d %H:%M:%S")

        utc_time = datetime(2023, 3, 18, 16, 4, 25, tzinfo=timezone.utc)
        server_time = call(param_client.query, "SELECT toDateTime(%(dt)s) as dt", parameters={"dt": utc_time}).first_row[0]
        assert server_time.astimezone(timezone.utc) == utc_time
    finally:
        common.set_setting("naive_datetime_binding", original_setting)


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
def test_timezone_binding_server(param_client: Client, call, monkeypatch):
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "wall")
    try:
        original_tz = _force_process_timezone(monkeypatch, "America/Denver")
        original_local_tz = tzutil.local_tz
        original_tz_source = param_client.tz_source
        denver_tz = zoneinfo.ZoneInfo("America/Denver")
        tzutil.local_tz = denver_tz
        param_client.tz_source = "local"
        denver_time = datetime(2022, 3, 18, 16, 4, 25, tzinfo=denver_tz)
        try:
            server_time = call(param_client.query, "SELECT toDateTime({dt:DateTime}) as dt", parameters={"dt": denver_time}).first_row[0]
            assert server_time == denver_time

            # Asserted while the process timezone is still Denver so that legacy
            # host-local conversion would visibly shift the wall fields.
            naive_time = datetime(2022, 3, 18, 16, 4, 25)
            server_time = call(
                param_client.query,
                "SELECT toString(toDateTime({dt:DateTime}))",
                parameters={"dt": naive_time},
            ).first_row[0]
            assert server_time == naive_time.strftime("%Y-%m-%d %H:%M:%S")
        finally:
            tzutil.local_tz = original_local_tz
            param_client.tz_source = original_tz_source
            _restore_process_timezone(monkeypatch, original_tz)

        utc_time = datetime(2020, 3, 18, 16, 4, 25, tzinfo=timezone.utc)
        server_time = call(param_client.query, "SELECT toDateTime({dt:DateTime}) as dt", parameters={"dt": utc_time}).first_row[0]
        assert server_time.astimezone(timezone.utc) == utc_time
    finally:
        common.set_setting("naive_datetime_binding", original_setting)


def test_tz_mode(param_client: Client, call):
    row = call(
        param_client.query,
        "SELECT toDateTime('2023-07-05 15:10:40') as dt," + "toDateTime('2023-07-05 15:10:40', 'UTC') as dt_utc",
        query_tz="UTC",
    ).first_row
    assert row[0].tzinfo is None
    assert row[1].tzinfo is None

    row = call(
        param_client.query,
        "SELECT toDateTime('2023-07-05 15:10:40') as dt," + "toDateTime('2023-07-05 15:10:40', 'UTC') as dt_utc",
        query_tz="UTC",
        tz_mode="aware",
    ).first_row
    assert tzutil.is_utc_timezone(row[0].tzinfo)
    assert tzutil.is_utc_timezone(row[1].tzinfo)

    row = call(
        param_client.query,
        "SELECT toDateTime64('2023-07-05 15:10:40.123456', 6) as dt64,"
        + "toDateTime64('2023-07-05 15:10:40.123456', 6, 'UTC') as dt64_utc",
        query_tz="UTC",
    ).first_row
    assert row[0].tzinfo is None
    assert row[1].tzinfo is None
    assert row[0].microsecond == 123456

    row = call(
        param_client.query,
        "SELECT toDateTime64('2023-07-05 15:10:40.123456', 6) as dt64,"
        + "toDateTime64('2023-07-05 15:10:40.123456', 6, 'UTC') as dt64_utc",
        query_tz="UTC",
        tz_mode="aware",
    ).first_row
    assert tzutil.is_utc_timezone(row[0].tzinfo)
    assert tzutil.is_utc_timezone(row[1].tzinfo)
    assert row[0].microsecond == 123456


def test_tz_source_setter_validates(param_client: Client):
    """Setting client.tz_source to an invalid value should raise ProgrammingError."""
    with pytest.raises(ProgrammingError, match="tz_source must be"):
        param_client.tz_source = "serer"


def test_tz_source_setter_auto_restores_dst_safe(param_client: Client):
    """Setting tz_source back to 'auto' should re-resolve based on server DST safety."""
    original = param_client._apply_server_tz
    try:
        param_client.tz_source = "local"
        assert param_client._apply_server_tz is False
        param_client.tz_source = "auto"
        assert param_client._apply_server_tz == original
    finally:
        param_client.tz_source = "auto"
