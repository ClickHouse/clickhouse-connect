import os
import time
import zoneinfo
from datetime import datetime, timedelta, timezone, tzinfo
from math import floor
from unittest.mock import patch

import pytest

from clickhouse_connect import common
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.insert import InsertContext

HAS_TZSET = hasattr(time, "tzset")
HOST_TZ_NAME = "America/New_York"
SERVER_TZ = zoneinfo.ZoneInfo("America/Denver")
COLUMN_TZ_NAME = "America/Chicago"
COLUMN_TZ = zoneinfo.ZoneInfo(COLUMN_TZ_NAME)


class NoOffsetTZ(tzinfo):
    def utcoffset(self, dt):
        return None

    def dst(self, dt):
        return None


@pytest.fixture
def non_utc_process_timezone(monkeypatch):
    original_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", HOST_TZ_NAME)
    time.tzset()
    try:
        yield
    finally:
        if original_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", original_tz)
        time.tzset()


@pytest.fixture
def restore_insert_setting():
    original = common.get_setting("naive_datetime_insert")
    try:
        yield
    finally:
        common.set_setting("naive_datetime_insert", original)


def _type_names() -> list[str]:
    type_names = ["DateTime", f"DateTime('{COLUMN_TZ_NAME}')"]
    for precision in range(10):
        type_names.extend(
            (
                f"DateTime64({precision})",
                f"DateTime64({precision}, '{COLUMN_TZ_NAME}')",
            )
        )
    return type_names


def _serialize_datetime(type_name: str, value: datetime | str, server_tz: tzinfo = SERVER_TZ) -> int:
    ch_type = get_from_name(type_name)
    ctx = InsertContext("test_table", ["value"], [ch_type])
    ctx.server_tz = server_tz
    ctx.start_column("value")
    dest = bytearray()
    ch_type._write_column_binary([value], dest, ctx)
    return int.from_bytes(dest, "little", signed="DateTime64" in type_name)


def _expected_epoch(type_name: str, value: datetime, mode: str) -> int:
    if mode == "server":
        target_tz = COLUMN_TZ if COLUMN_TZ_NAME in type_name else SERVER_TZ
        value = value.replace(tzinfo=target_tz)
    seconds = floor(value.timestamp())
    if not type_name.startswith("DateTime64"):
        return seconds
    precision = int(type_name.removeprefix("DateTime64(").split(",", maxsplit=1)[0].rstrip(")"))
    scale = 10**precision
    return ((seconds * 1_000_000 + value.microsecond) * scale) // 1_000_000


def test_naive_datetime_insert_setting_validation(restore_insert_setting):
    assert common.get_setting("naive_datetime_insert") == "local"
    common.set_setting("naive_datetime_insert", "server")
    assert common.get_setting("naive_datetime_insert") == "server"
    with pytest.raises(ProgrammingError, match="naive_datetime_insert"):
        common.set_setting("naive_datetime_insert", "wall")


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
@pytest.mark.parametrize("mode", ["local", "server"])
@pytest.mark.parametrize("value", [datetime(2025, 1, 15, 12, 34, 56, 250306), datetime(2025, 7, 15, 12, 34, 56, 250306)])
@pytest.mark.parametrize("type_name", _type_names())
def test_naive_datetime_epoch_math_all_precisions(
    type_name,
    value,
    mode,
    non_utc_process_timezone,
    restore_insert_setting,
):
    common.set_setting("naive_datetime_insert", mode)

    assert _serialize_datetime(type_name, value) == _expected_epoch(type_name, value, mode)


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
@pytest.mark.parametrize("mode", ["local", "server"])
@pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(6)", f"DateTime('{COLUMN_TZ_NAME}')", f"DateTime64(6, '{COLUMN_TZ_NAME}')"])
def test_aware_datetime_insert_is_unchanged(type_name, mode, non_utc_process_timezone, restore_insert_setting):
    common.set_setting("naive_datetime_insert", mode)
    value = datetime(2025, 7, 15, 12, 34, 56, 250306, tzinfo=zoneinfo.ZoneInfo("Asia/Tokyo"))

    assert _serialize_datetime(type_name, value) == _expected_epoch(type_name, value, "local")


def test_none_offset_tzinfo_is_naive_for_server_insert(restore_insert_setting):
    common.set_setting("naive_datetime_insert", "server")
    value = datetime(2025, 1, 15, 12, 34, 56, 250306, tzinfo=NoOffsetTZ())
    expected = value.replace(tzinfo=SERVER_TZ)

    assert _serialize_datetime("DateTime64(6)", value) == _expected_epoch("DateTime64(6)", expected, "local")


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
@pytest.mark.parametrize("mode", ["local", "server"])
@pytest.mark.parametrize("type_name", ["DateTime64(6)", f"DateTime64(6, '{COLUMN_TZ_NAME}')"])
def test_naive_datetime64_iso_string_epoch_math(
    type_name,
    mode,
    non_utc_process_timezone,
    restore_insert_setting,
):
    common.set_setting("naive_datetime_insert", mode)
    value = "2025-07-15 12:34:56.250306"
    parsed = datetime.fromisoformat(value)

    assert _serialize_datetime(type_name, value) == _expected_epoch(type_name, parsed, mode)


@pytest.mark.parametrize("mode", ["local", "server"])
@pytest.mark.parametrize("type_name", ["DateTime64(6)", "Nullable(DateTime64(6))"])
@pytest.mark.parametrize(
    "value",
    [
        datetime.fromisoformat("1969-12-31 23:59:59.250000+00:00"),
        "1969-12-31 23:59:59.250000+00:00",
    ],
)
def test_datetime64_fractional_value_before_epoch(value, type_name, mode, restore_insert_setting):
    common.set_setting("naive_datetime_insert", mode)

    assert _serialize_datetime(type_name, value) == -750_000


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
@pytest.mark.parametrize("mode", ["local", "server"])
@pytest.mark.parametrize("type_name", ["DateTime64(6)", "Nullable(DateTime64(6))"])
def test_naive_datetime64_fractional_value_before_epoch(
    type_name,
    mode,
    non_utc_process_timezone,
    restore_insert_setting,
):
    common.set_setting("naive_datetime_insert", mode)
    value = datetime(1969, 6, 1, 12, 0, 0, 300000)
    target_tz = SERVER_TZ if mode == "server" else zoneinfo.ZoneInfo(HOST_TZ_NAME)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    expected = (value.replace(tzinfo=target_tz) - epoch) // timedelta(microseconds=1)

    assert _serialize_datetime(type_name, value) == expected


@pytest.mark.parametrize(
    "type_name, values",
    [
        ("DateTime", [datetime(2025, 1, 15, 12, 34, 56), datetime(2025, 7, 15, 12, 34, 56)]),
        ("DateTime64(6)", [datetime(2025, 1, 15, 12, 34, 56), datetime(2025, 7, 15, 12, 34, 56)]),
        ("DateTime64(6)", ["2025-01-15 12:34:56", "2025-07-15 12:34:56"]),
    ],
    ids=["datetime", "datetime64", "datetime64_iso_string"],
)
def test_insert_setting_is_read_once_per_column(type_name, values):
    ch_type = get_from_name(type_name)
    ctx = InsertContext("test_table", ["value"], [ch_type], server_tz=SERVER_TZ)
    ctx.start_column("value")

    with patch("clickhouse_connect.datatypes.temporal.common.get_setting", return_value="server") as get_setting:
        ch_type._write_column_binary(values, bytearray(), ctx)

    get_setting.assert_called_once_with("naive_datetime_insert")


@pytest.mark.parametrize(
    "value",
    [
        datetime(2025, 3, 9, 2, 30),
        datetime(2025, 11, 2, 1, 30),
        datetime(2025, 11, 2, 1, 30, fold=1),
    ],
)
@pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(6)"])
def test_server_insert_uses_zoneinfo_gap_and_fold_defaults(type_name, value, restore_insert_setting):
    common.set_setting("naive_datetime_insert", "server")
    new_york = zoneinfo.ZoneInfo(HOST_TZ_NAME)
    expected = value.replace(tzinfo=new_york)

    assert _serialize_datetime(type_name, value, new_york) == _expected_epoch(type_name, expected, "local")
