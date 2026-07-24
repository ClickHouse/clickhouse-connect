import ipaddress
import uuid
import zoneinfo
from datetime import date, datetime, timezone

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import (
    DT64Param,
    _extract_tz_from_type,
    bind_query,
    finalize_query,
    format_bind_value,
    format_query_value,
)


def test_finalize():
    hash_id = "0x772"
    timestamp = datetime.fromtimestamp(1661447719)
    parameters = {"hash_id": hash_id, "dt": timestamp}
    expected = "SELECT hash_id FROM db.mytable WHERE hash_id = '0x772' AND dt = '2022-08-25 17:15:19'"
    query = finalize_query("SELECT hash_id FROM db.mytable WHERE hash_id = %(hash_id)s AND dt = %(dt)s", parameters)
    assert query == expected

    parameters = [hash_id, timestamp]
    query = finalize_query("SELECT hash_id FROM db.mytable WHERE hash_id = %s AND dt = %s", parameters)
    assert query == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("a", "a"),
        ("a'", r"a\'"),
        ("'a'", r"\'a\'"),
        ("''a'", r"\'\'a\'"),
        ([], "[]"),
        ([1], "[1]"),
        (["a"], "['a']"),
        (["a'"], r"['a\'']"),
        ([["a"]], "[['a']]"),
        (date(2023, 6, 1), "2023-06-01"),
        (datetime(2023, 6, 1, 20, 4, 5), "2023-06-01 20:04:05"),
        ([date(2023, 6, 1), date(2023, 8, 5)], "['2023-06-01', '2023-08-05']"),
        (b"AB", r"\x41\x42"),
        (b"\x00\xf8'", r"\x00\xf8\x27"),
        ([b"AB"], r"['\x41\x42']"),
        ((b"AB", "x"), r"('\x41\x42', 'x')"),
        (uuid.UUID("019e1780-3b41-7673-a645-17f9b60fe8ec"), "019e1780-3b41-7673-a645-17f9b60fe8ec"),
        (
            [uuid.UUID("019e1780-3b41-7673-a645-17f9b60fe8ec"), uuid.UUID("019e1780-3b41-7673-a645-17f9b60fe8ed")],
            "['019e1780-3b41-7673-a645-17f9b60fe8ec', '019e1780-3b41-7673-a645-17f9b60fe8ed']",
        ),
        ((uuid.UUID("019e1780-3b41-7673-a645-17f9b60fe8ec"), "user_1"), "('019e1780-3b41-7673-a645-17f9b60fe8ec', 'user_1')"),
        (ipaddress.IPv4Address("10.13.79.1"), "10.13.79.1"),
        ([ipaddress.IPv4Address("10.13.79.1")], "['10.13.79.1']"),
        (ipaddress.IPv6Address("2001:db8::79"), "2001:db8::79"),
        ([ipaddress.IPv6Address("2001:db8::79")], "['2001:db8::79']"),
        (None, "\\N"),
        (["user_1", None], "['user_1', NULL]"),
        (("user_1", None, 79), "('user_1', NULL, 79)"),
        (("user_1", ("user_2", None)), "('user_1', ('user_2', NULL))"),
        ([("user_1", None)], "[('user_1', NULL)]"),
    ],
)
def test_format_bind_value(value, expected):
    assert format_bind_value(value) == expected


def test_format_bind_value_map_null():
    original = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        assert format_bind_value({"user_1": None}) == "{'user_1':NULL}"
        assert format_bind_value({"user_1": "user_2", "user_3": None}) == "{'user_1':'user_2', 'user_3':NULL}"
    finally:
        common.set_setting("dict_parameter_format", original)


@pytest.mark.parametrize(
    "value, expected",
    [
        (b"AB", r"'\x41\x42'"),
        (bytearray(b"AB"), r"'\x41\x42'"),
        (b"j!lUA\xf8\x93q;ky\x00", r"'\x6a\x21\x6c\x55\x41\xf8\x93\x71\x3b\x6b\x79\x00'"),
        ([b"AB", b"\x00"], r"['\x41\x42', '\x00']"),
        ((b"AB", 1), r"('\x41\x42', 1)"),
    ],
)
def test_format_query_value_bytes(value, expected):
    assert format_query_value(value) == expected


def test_finalize_bytes():
    query = finalize_query("INSERT INTO t (id) VALUES (%(id)s)", {"id": b"j!lUA\xf8\x93q;ky\x00"})
    assert query == r"INSERT INTO t (id) VALUES ('\x6a\x21\x6c\x55\x41\xf8\x93\x71\x3b\x6b\x79\x00')"


class TestBindQueryTimezoneHint:
    """Type hint timezone in {param:Type} should override server_tz."""

    berlin_tz = zoneinfo.ZoneInfo("Europe/Berlin")
    dt_utc = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_datetime64_utc_hint(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime64(6, 'UTC')}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00.000000"

    def test_datetime_utc_hint(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime('UTC')}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00"

    def test_dt64param_utc_hint(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime64(6, 'UTC')}"
        _, params = bind_query(query, {"dt_64": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00.000000"

    def test_no_hint_tz_falls_back_to_server_tz(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime64(6)}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 13:00:00.000000"

    def test_nullable_wrapper(self):
        query = "SELECT * FROM t WHERE dt >= {dt:Nullable(DateTime64(6, 'UTC'))}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00.000000"

    def test_lowcardinality_nullable_wrapper(self):
        query = "SELECT * FROM t WHERE dt >= {dt:LowCardinality(Nullable(DateTime('UTC')))}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00"

    def test_array_container(self):
        query = "SELECT * FROM t WHERE dt IN {dts:Array(DateTime64(6, 'UTC'))}"
        _, params = bind_query(query, {"dts": [self.dt_utc]}, server_tz=self.berlin_tz)
        assert "2025-01-01 12:00:00" in params["param_dts"]

    def test_tuple_container(self):
        query = "SELECT * FROM t WHERE x = {val:Tuple(DateTime('UTC'), String)}"
        _, params = bind_query(query, {"val": (self.dt_utc, "test")}, server_tz=self.berlin_tz)
        assert "2025-01-01 12:00:00" in params["param_val"]

    def test_map_type_hint_extraction(self):
        tz = _extract_tz_from_type("Map(String, DateTime64(6, 'UTC'))")
        assert tzutil.is_utc_timezone(tz)

    def test_non_utc_hint(self):
        tokyo_tz = zoneinfo.ZoneInfo("Asia/Tokyo")
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime('Asia/Tokyo')}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        # 12:00 UTC = 21:00 Tokyo
        expected = self.dt_utc.astimezone(tokyo_tz).strftime("%Y-%m-%d %H:%M:%S")
        assert params["param_dt"] == expected

    def test_unknown_tz_falls_back_to_server_tz(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime('FakeZone/Nowhere')}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 13:00:00"

    def test_malformed_hint_falls_back_to_server_tz(self):
        query = "SELECT * FROM t WHERE dt >= {dt:NotAType!!!}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 13:00:00"


class TestBindQueryDateTime64Precision:
    """A DateTime64 type hint preserves sub-second precision without a _64 suffix or DT64Param."""

    utc = timezone.utc
    dt = datetime(2025, 1, 1, 12, 0, 0, 250306, tzinfo=timezone.utc)

    def test_scalar(self):
        query = "SELECT {dt:DateTime64(6)}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_plain_datetime_truncates(self):
        query = "SELECT {dt:DateTime}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00"

    def test_lowercase_spelling(self):
        query = "SELECT {dt:Datetime64(6)}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_nullable_wrapper(self):
        query = "SELECT {dt:Nullable(DateTime64(9))}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_lowcardinality_nullable_wrapper(self):
        query = "SELECT {dt:LowCardinality(Nullable(DateTime64(6)))}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_none_nullable(self):
        query = "SELECT {dt:Nullable(DateTime64(6))}"
        _, params = bind_query(query, {"dt": None}, server_tz=self.utc)
        assert params["param_dt"] == "\\N"

    def test_array(self):
        query = "SELECT {dts:Array(DateTime64(6))}"
        dts = [self.dt, self.dt.replace(microsecond=777722)]
        _, params = bind_query(query, {"dts": dts}, server_tz=self.utc)
        assert params["param_dts"] == "['2025-01-01 12:00:00.250306', '2025-01-01 12:00:00.777722']"

    def test_tuple(self):
        query = "SELECT {val:Tuple(DateTime64(6), String)}"
        _, params = bind_query(query, {"val": (self.dt, "user_1")}, server_tz=self.utc)
        assert params["param_val"] == "('2025-01-01 12:00:00.250306', 'user_1')"

    def test_array_of_tuple(self):
        query = "SELECT {vals:Array(Tuple(DateTime64(6), String))}"
        _, params = bind_query(query, {"vals": [(self.dt, "user_1")]}, server_tz=self.utc)
        assert params["param_vals"] == "[('2025-01-01 12:00:00.250306', 'user_1')]"

    def test_tz_hint_with_precision(self):
        berlin = zoneinfo.ZoneInfo("Europe/Berlin")
        query = "SELECT {dt:DateTime64(6, 'Europe/Berlin')}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert params["param_dt"] == self.dt.astimezone(berlin).strftime("%Y-%m-%d %H:%M:%S.%f")

    def test_lowercase_tz_hint_preserved(self):
        query = "SELECT {dt:Datetime64(6, 'UTC')}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=zoneinfo.ZoneInfo("Europe/Berlin"))
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_already_dt64param_not_double_wrapped(self):
        query = "SELECT {dt:DateTime64(6)}"
        _, params = bind_query(query, {"dt": DT64Param(self.dt)}, server_tz=self.utc)
        assert params["param_dt"] == "2025-01-01 12:00:00.250306"

    def test_malformed_hint_does_not_crash(self):
        query = "SELECT {dt:DateTime64(((}"
        _, params = bind_query(query, {"dt": self.dt}, server_tz=self.utc)
        assert "param_dt" in params


class TestBindQuerySuffixCollision:
    """A param whose real name ends in _64 keeps its name when the query binds the full name."""

    utc = timezone.utc
    dt = datetime(2026, 1, 1, 12, 0, 0, 250306, tzinfo=timezone.utc)

    def test_scalar(self):
        query = "SELECT 1 WHERE t >= {param_64:DateTime64(6, 'UTC')}"
        _, params = bind_query(query, {"param_64": self.dt}, server_tz=self.utc)
        assert params == {"param_param_64": "2026-01-01 12:00:00.250306"}

    def test_array(self):
        query = "SELECT {dts_64:Array(DateTime64(6))}"
        _, params = bind_query(query, {"dts_64": [self.dt]}, server_tz=self.utc)
        assert params == {"param_dts_64": "['2026-01-01 12:00:00.250306']"}

    def test_suffix_strips_when_stripped_name_is_bound(self):
        query = "SELECT {dt:DateTime64(6)}"
        _, params = bind_query(query, {"dt_64": self.dt}, server_tz=self.utc)
        assert params == {"param_dt": "2026-01-01 12:00:00.250306"}

    def test_suffix_strips_without_placeholders(self):
        query = "SELECT %(dt)s"
        q, params = bind_query(query, {"dt_64": self.dt}, server_tz=self.utc)
        assert q == "SELECT '2026-01-01 12:00:00.250306'"
        assert params == {}
