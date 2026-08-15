import ipaddress
import os
import time
import uuid
import zoneinfo
from array import array
from datetime import date, datetime, timedelta, timezone, tzinfo
from datetime import time as dt_time

import numpy as np
import pandas as pd
import pytest

from clickhouse_connect import common
from clickhouse_connect.driver import tzutil
from clickhouse_connect.driver.binding import (
    DT64Param,
    _binary_bind_value,
    _extract_tz_from_type,
    _is_valid_bind_name,
    bind_query,
    finalize_query,
    format_bind_value,
    format_query_value,
)
from clickhouse_connect.driver.exceptions import ProgrammingError

HAS_TZSET = hasattr(time, "tzset")
NON_UTC_HOST_TZ = "America/New_York"


class NoOffsetTZ(tzinfo):
    def utcoffset(self, dt):
        return None

    def dst(self, dt):
        return None


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


TIME_OF_DAY_CASES = [
    (dt_time(14, 30, 0), "14:30:00"),
    (dt_time(1, 2, 3, 456789), "01:02:03.456789"),
    (dt_time(9, 5, 7, tzinfo=timezone.utc), "09:05:07"),
    (dt_time(9, 5, 7, 250306, tzinfo=zoneinfo.ZoneInfo("Europe/Berlin")), "09:05:07.250306"),
    (timedelta(0), "00:00:00"),
    (timedelta(hours=1, minutes=2, seconds=3), "01:02:03"),
    (timedelta(seconds=5, microseconds=250306), "00:00:05.250306"),
    (timedelta(seconds=-2), "-00:00:02"),
    (timedelta(microseconds=-1), "-00:00:00.000001"),
    (timedelta(days=2, hours=3, seconds=13), "51:00:13"),
    (timedelta(days=-1, hours=-1, minutes=-30), "-25:30:00"),
    (pd.Timedelta("13s 500ns"), "00:00:13.000000500"),
    (pd.Timedelta("-1ns"), "-00:00:00.000000001"),
    (pd.Timedelta("1s 250306us"), "00:00:01.250306"),
]


@pytest.mark.parametrize("value, expected", TIME_OF_DAY_CASES)
def test_format_query_value_time(value, expected):
    assert format_query_value(value) == f"'{expected}'"


@pytest.mark.parametrize("value, expected", TIME_OF_DAY_CASES)
def test_format_bind_value_time(value, expected):
    assert format_bind_value(value) == expected
    assert format_bind_value(value, top_level=False) == f"'{expected}'"


@pytest.mark.parametrize(
    "value, expected",
    [
        ([dt_time(1, 2, 3)], "['01:02:03']"),
        ([timedelta(seconds=-2), timedelta(hours=1)], "['-00:00:02', '01:00:00']"),
        ((dt_time(1, 2, 3, 456789), None), "('01:02:03.456789', NULL)"),
        ((timedelta(seconds=13), "user_1"), "('00:00:13', 'user_1')"),
        ([(timedelta(seconds=13), dt_time(1, 2, 3))], "[('00:00:13', '01:02:03')]"),
    ],
)
def test_time_value_nesting(value, expected):
    assert format_query_value(value) == expected
    assert format_bind_value(value) == expected


def test_time_value_map_format():
    original = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        value = {"start": dt_time(1, 2, 3), "gap": timedelta(seconds=-2), "end": None}
        expected = "{'start':'01:02:03', 'gap':'-00:00:02', 'end':NULL}"
        assert format_query_value(value) == expected
        assert format_bind_value(value) == expected
    finally:
        common.set_setting("dict_parameter_format", original)


def test_finalize_time_values():
    parameters = {"t": dt_time(14, 30, 0), "td": timedelta(hours=-1, seconds=-13)}
    query = finalize_query("SELECT * FROM t1 WHERE t = %(t)s AND td = %(td)s", parameters)
    assert query == "SELECT * FROM t1 WHERE t = '14:30:00' AND td = '-01:00:13'"


def test_bind_query_time_params():
    _, params = bind_query(
        "SELECT {t:Time}, {arr:Array(Time64(6))}",
        {"t": timedelta(seconds=-2), "arr": [dt_time(1, 2, 3, 250306)]},
    )
    assert params == {"param_t": "-00:00:02", "param_arr": "['01:02:03.250306']"}


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


class TestBindQueryDollarInParamName:
    """ClickHouse accepts dollar signs at several positions in ASCII BareWord names."""

    utc = timezone.utc
    dt = datetime(2026, 1, 1, 12, 0, 0, 250306, tzinfo=timezone.utc)

    @pytest.mark.parametrize("name", ["id$x", "$x", "id$", "a$$b", "$1", "13_", "$x$"])
    def test_binds_server_side(self, name):
        assert _is_valid_bind_name(name)
        query = f"SELECT {{{name}:Int32}} AS v"
        q, params = bind_query(query, {name: 13}, server_tz=self.utc)
        assert q == query
        assert params == {f"param_{name}": "13"}

    @pytest.mark.parametrize("name", ["$", "$$", "$$a", "13$"])
    def test_invalid_bareword_does_not_select_server_side_binding(self, name):
        assert not _is_valid_bind_name(name)
        query = f"SELECT {{{name}:Int32}} AS v"
        if name == "$$":
            with pytest.raises(ProgrammingError, match="also appears elsewhere"):
                bind_query(query, {name: 13}, server_tz=self.utc)
            return
        q, params = bind_query(query, {name: 13}, server_tz=self.utc)
        assert q == query
        assert params == {}

    @pytest.mark.parametrize("name", ["13", "id\N{LATIN SMALL LETTER E WITH ACUTE}"])
    def test_word_name_keeps_shipped_routing(self, name):
        # Not server-valid barewords, but shipped 1.x routing matched any \w+ name.
        assert not _is_valid_bind_name(name)
        query = f"SELECT {{{name}:Int32}} AS v"
        q, params = bind_query(query, {name: 13}, server_tz=self.utc)
        assert q == query
        assert params == {f"param_{name}": "13"}

    @pytest.mark.parametrize(
        "type_str,expected",
        [
            ("DateTime64(6)", "2026-01-01 12:00:00.250306"),
            ("Nullable(DateTime64(6))", "2026-01-01 12:00:00.250306"),
            ("Array(DateTime64(6))", "['2026-01-01 12:00:00.250306']"),
            ("DateTime", "2026-01-01 12:00:00"),
        ],
    )
    def test_type_hint_applies(self, type_str, expected):
        # The hint is keyed by the captured name, so a missed capture silently drops precision.
        value = [self.dt] if type_str.startswith("Array") else self.dt
        query = f"SELECT {{t$x:{type_str}}} AS t"
        _, params = bind_query(query, {"t$x": value}, server_tz=self.utc)
        assert params["param_t$x"] == expected

    def test_type_hint_applies_in_mixed_query(self):
        query = "SELECT {t$x:DateTime64(6)} AS t, {a:Int32} AS a"
        _, params = bind_query(query, {"t$x": self.dt, "a": 79}, server_tz=self.utc)
        assert params == {"param_t$x": "2026-01-01 12:00:00.250306", "param_a": "79"}

    def test_timezone_hint_applies(self):
        berlin = zoneinfo.ZoneInfo("Europe/Berlin")
        query = "SELECT {t$x:DateTime64(6, 'Europe/Berlin')} AS t"
        _, params = bind_query(query, {"t$x": self.dt}, server_tz=self.utc)
        assert params["param_t$x"] == self.dt.astimezone(berlin).strftime("%Y-%m-%d %H:%M:%S.%f")

    @pytest.mark.parametrize("value", [b"user_1", bytearray(b"user_1"), memoryview(b"user_1"), array("B", b"user_1")])
    def test_binary_bind_sentinel_still_wins(self, value):
        query = "SELECT $x$ AS v"
        q, params = bind_query(query, {"$x$": value})
        assert q == b"SELECT $x$user_1$x$ AS v"
        assert params == {}

    def test_binary_bind_value_bytes_passthrough(self):
        value = b"user_1"
        assert _binary_bind_value(value) is value

    def test_numpy_scalar_uses_native_buffer_bytes(self):
        value = np.int64(13)
        q, params = bind_query("SELECT $x$ AS v", {"$x$": value})
        assert q == b"SELECT $x$" + memoryview(value).tobytes() + b"$x$ AS v"
        assert params == {}

    def test_binary_bind_alongside_dollar_name(self):
        query = "SELECT $x$ AS v, {id$y:Int32} AS n"
        q, params = bind_query(query, {"$x$": b"user_2", "id$y": 79}, server_tz=self.utc)
        assert q == b"SELECT $x$user_2$x$ AS v, {id$y:Int32} AS n"
        assert params == {"param_id$y": "79"}

    @pytest.mark.parametrize("type_str,value,expected", [("Int32", 13, "13"), ("String", "user_1", "user_1")])
    def test_non_binary_sentinel_binds_server_side(self, type_str, value, expected):
        query = f"SELECT {{$x$:{type_str}}} AS v"
        q, params = bind_query(query, {"$x$": value})
        assert q == query
        assert params == {"param_$x$": expected}

    @pytest.mark.parametrize(
        "query,key",
        [
            ("SELECT $x$ AS v", "$x$"),
            ("SELECT $a-b$ AS v", "$a-b$"),
            ("SELECT $x$tail AS v", "$x$"),
            ("SELECT 13a$x$ AS raw", "$x$"),
        ],
    )
    def test_non_binary_sentinel_used_as_raw_marker_raises(self, query, key):
        with pytest.raises(ProgrammingError, match="must be a buffer value"):
            bind_query(query, {key: 13})

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT '$x$', %(a)s",
            "SELECT 1 -- $x$\n, %(a)s",
            "SELECT 1 // $x$\n, %(a)s",
            "SELECT 1 # $x$\n, %(a)s",
            "SELECT 1 #!$x$\n, %(a)s",
            "SELECT 1 /* outer /* $x$ */ outer */ + %(a)s",
            "SELECT $tag$$x$$tag$, %(a)s",
            "SELECT foo$x$bar, %(a)s",
        ],
    )
    def test_non_raw_marker_without_placeholder_is_ignored(self, query):
        q, params = bind_query(query, {"$x$": 13, "a": 79})
        assert q == query.replace("%(a)s", "79")
        assert params == {}

    def test_non_binary_sentinel_used_as_placeholder_and_raw_marker_raises(self):
        with pytest.raises(ProgrammingError, match="also appears elsewhere"):
            bind_query("SELECT {$x$:Int32} AS v, $x$ AS raw", {"$x$": 13})

    @pytest.mark.parametrize(
        "prefix",
        [
            "'$x$', ",
            "-- $x$\n",
            "/* outer /* $x$ */ outer */ ",
            "$tag$$x$$tag$, ",
        ],
    )
    def test_non_executable_marker_before_placeholder_is_allowed(self, prefix):
        query = f"SELECT {prefix}" + "{$x$:Int32}"
        q, params = bind_query(query, {"$x$": 13})
        assert q == query
        assert params == {"param_$x$": "13"}

    @pytest.mark.parametrize("suffix", ["'$x$'", "/* $x$ */ 1", "{foo$x$bar:Int32}"])
    def test_exact_marker_after_placeholder_raises(self, suffix):
        query = "SELECT {$x$:Int32}, " + suffix
        params = {"$x$": 13, "foo$x$bar": 79}
        with pytest.raises(ProgrammingError, match="also appears elsewhere"):
            bind_query(query, params)

    def test_executable_marker_before_placeholder_raises(self):
        # The pair of names parses as one heredoc that swallows the written placeholder.
        with pytest.raises(ProgrammingError, match="also appears elsewhere"):
            bind_query("SELECT $x$, {$x$:Int32}", {"$x$": 13})

    def test_dollar_run_retries_heredoc_at_next_dollar(self):
        # The server emits a one-char DollarSign token at the first `$`, then opens the
        # `$x$` heredoc at position 1, which closes at the placeholder name and swallows it.
        with pytest.raises(ProgrammingError, match="also appears elsewhere"):
            bind_query("SELECT $$x$ ok {$x$:String}", {"$x$": "user_1"})

    def test_adjacent_heredoc_does_not_hide_raw_marker(self):
        query = "SELECT $tag$user_1$tag$$x$"
        with pytest.raises(ProgrammingError, match="must be a buffer value"):
            bind_query(query, {"$x$": 13})

    @pytest.mark.parametrize("token", ["$a$a$", "$other$x$"])
    def test_overlapping_tag_shape_stays_one_bareword(self, token):
        query = f"SELECT {token}, %(a)s"
        q, params = bind_query(query, {"$x$": 13, "a": 79})
        assert q == query.replace("%(a)s", "79")
        assert params == {}

    def test_many_distinct_unmatched_tags_keep_client_side_binding(self):
        tags = "+".join(f"$t{index}$" for index in range(256))
        query = f"SELECT {tags}, %(a)s"
        q, params = bind_query(query, {"a": 79})
        assert q == query.replace("%(a)s", "79")
        assert params == {}

    def test_repeated_non_binary_sentinel_placeholder_raises(self):
        with pytest.raises(ProgrammingError, match="can appear only once"):
            bind_query("SELECT {$x$:Int32} + {$x$:Int32}", {"$x$": 13})

    def test_unused_non_binary_sentinel_does_not_raise(self):
        q, params = bind_query("SELECT %(a)s", {"$x$": 13, "a": 79})
        assert q == "SELECT 79"
        assert params == {}

    @pytest.mark.parametrize(
        "query,params",
        [
            # The `$x$` in the type opens a server heredoc that swallows the second placeholder.
            ("SELECT {a:Array($x$)} AS a, {$x$:String} AS v", {"a": [13], "$x$": "user_1"}),
            # A name starting with the marker tag gets a server heredoc check at the name start.
            ("SELECT {$x$y:Int32}, {$x$:Int32}", {"$x$y": 79, "$x$": 13}),
        ],
    )
    def test_marker_inside_other_placeholder_raises(self, query, params):
        with pytest.raises(ProgrammingError, match="also appears elsewhere"):
            bind_query(query, params)

    def test_marker_mid_name_in_other_placeholder_binds(self):
        # `foo$x$bar` lexes as one bareword, so the embedded tag never opens a heredoc.
        query = "SELECT {foo$x$bar:Int32}, {$x$:Int32}"
        q, params = bind_query(query, {"foo$x$bar": 79, "$x$": 13})
        assert q == query
        assert params == {"param_foo$x$bar": "79", "param_$x$": "13"}

    def test_non_binary_sentinel_after_number_is_a_raw_marker(self):
        with pytest.raises(ProgrammingError, match="must be a buffer value"):
            bind_query("SELECT 13$x$ AS raw", {"$x$": 13})

    @pytest.mark.parametrize("name", ["13a$", "13a$x", "13a$x$bar"])
    def test_digit_led_name_never_absorbs_dollar(self, name):
        # Digit-led tokens promote to a bareword only through [A-Za-z0-9_], never `$`.
        assert not _is_valid_bind_name(name)
        query = f"SELECT {{{name}:Int32}} AS v"
        q, params = bind_query(query, {name: 13}, server_tz=self.utc)
        assert q == query
        assert params == {}

    def test_non_binary_sentinel_after_split_bareword_is_not_raw_marker(self):
        query = "SELECT 13$abc$x$tail, {$x$:Int32}"
        q, params = bind_query(query, {"$x$": 13})
        assert q == query
        assert params == {"param_$x$": "13"}

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT '{id$x:Int32}'",
            'SELECT "{id$x:Int32}"',
            "SELECT `{id$x:Int32}`",
            "SELECT 'it\\'s {id$x:Int32}'",
            "SELECT 'it''s {id$x:Int32}'",
            "SELECT 1 -- {id$x:Int32}",
            "SELECT 1 /* {id$x:Int32} */",
            "SELECT $tag${id$x:Int32}$tag$",
            "SELECT \u2018{id$x:Int32}\u2019",
        ],
    )
    def test_decoy_placeholder_routes_server_side(self, query):
        # Detection uses the regex over the raw query text, matching shipped 1.x semantics,
        # so decoys inside strings and comments still route server-side.
        q, params = bind_query(query, {"id$x": 13})
        assert q == query
        assert params == {"param_id$x": "13"}

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT '{a:String}', '100%$'",
            "SELECT '{13:Int32}', '100%'",
            "SELECT '{idé:Int32}', '100%'",
        ],
    )
    def test_decoy_placeholder_with_percent_literal_routes_server_side(self, query):
        # Any shipped-1.x decoy match must keep the query off the client-side `%` path.
        q, params = bind_query(query, {"a": "user_1"})
        assert q == query
        assert params == {"param_a": "user_1"}

    def test_decoy_type_hint_applies_alongside_real_placeholder(self):
        query = "SELECT '{fake$x:DateTime64(6)}', {real$x:Int32}"
        q, params = bind_query(query, {"fake$x": self.dt, "real$x": 13}, server_tz=self.utc)
        assert q == query
        assert params == {"param_fake$x": "2026-01-01 12:00:00.250306", "param_real$x": "13"}

    def test_hash_without_required_suffix_does_not_hide_placeholder(self):
        query = "SELECT 1 #x {id$x:Int32}, %(a)s"
        q, params = bind_query(query, {"id$x": 79, "a": 13})
        assert q == query
        assert params == {"param_id$x": "79", "param_a": "13"}

    def test_binary_placeholder_match_does_not_change_client_side_classification(self):
        query = "SELECT {$x$:String} AS v, %(a)s AS n"
        q, params = bind_query(query, {"$x$": b"user_1", "a": 13})
        assert q == b"SELECT {$x$user_1$x$:String} AS v, 13 AS n"
        assert params == {}

    def test_no_placeholder_still_uses_client_side_path(self):
        query = "SELECT %(a$b)s"
        q, params = bind_query(query, {"a$b": 79}, server_tz=self.utc)
        assert q == "SELECT 79"
        assert params == {}


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
def test_naive_datetime_wall_binding_behavior(monkeypatch):
    original_tz = _force_process_timezone(monkeypatch, NON_UTC_HOST_TZ)
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "wall")
    try:
        berlin = zoneinfo.ZoneInfo("Europe/Berlin")
        naive = datetime(2025, 1, 1, 12, 0, 0, 250306)

        assert format_query_value(naive, server_tz=berlin) == "'2025-01-01 12:00:00'"
        assert format_bind_value(naive, server_tz=berlin) == "2025-01-01 12:00:00"
        assert format_bind_value(naive, server_tz=berlin, top_level=False) == "'2025-01-01 12:00:00'"
        assert format_bind_value(DT64Param(naive), server_tz=berlin) == "2025-01-01 12:00:00.250306"
        assert format_query_value(DT64Param(naive), server_tz=berlin) == "'2025-01-01 12:00:00.250306'"

        _, params = bind_query(
            "SELECT {dt:DateTime}, {dt_berlin:DateTime('Europe/Berlin')}",
            {"dt": naive, "dt_berlin": naive},
            server_tz=timezone.utc,
        )
        assert params == {"param_dt": "2025-01-01 12:00:00", "param_dt_berlin": "2025-01-01 12:00:00"}

        query, params = bind_query(
            "SELECT %(items)s, %(pair)s",
            {"items": [naive], "pair": (naive, "user_1")},
            server_tz=berlin,
        )
        assert query == "SELECT ['2025-01-01 12:00:00'], ('2025-01-01 12:00:00', 'user_1')"
        assert params == {}

        _, params = bind_query(
            "SELECT {items:Array(DateTime)}, {berlin:Datetime('Europe/Berlin')}, "
            "{nullable:Nullable(DateTime64(6))}, {pair:Tuple(DateTime64(6), String)}, "
            "{vals:Array(Tuple(DateTime64(6), String))}",
            {
                "items": [naive],
                "berlin": naive,
                "nullable": naive,
                "pair": (naive, "user_1"),
                "vals": [(naive, "user_2")],
            },
            server_tz=berlin,
        )
        assert params == {
            "param_items": "['2025-01-01 12:00:00']",
            "param_berlin": "2025-01-01 12:00:00",
            "param_nullable": "2025-01-01 12:00:00.250306",
            "param_pair": "('2025-01-01 12:00:00.250306', 'user_1')",
            "param_vals": "[('2025-01-01 12:00:00.250306', 'user_2')]",
        }

        aware = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert format_query_value(aware, server_tz=berlin) == "'2025-01-01 13:00:00'"
        assert format_bind_value(aware, server_tz=berlin) == "2025-01-01 13:00:00"
        assert format_bind_value(DT64Param(aware), server_tz=berlin) == "2025-01-01 13:00:00.000000"
    finally:
        common.set_setting("naive_datetime_binding", original_setting)
        _restore_process_timezone(monkeypatch, original_tz)


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
@pytest.mark.parametrize(
    "host_tz, query_utc, bind_utc, berlin_value",
    [
        ("UTC", "2025-01-01 12:00:00", "2025-01-01 12:00:00", "2025-01-01 13:00:00"),
        (NON_UTC_HOST_TZ, "2025-01-01 12:00:00", "2025-01-01 17:00:00", "2025-01-01 18:00:00"),
    ],
)
def test_naive_datetime_legacy_binding_behavior(
    monkeypatch,
    host_tz,
    query_utc,
    bind_utc,
    berlin_value,
):
    original_tz = _force_process_timezone(monkeypatch, host_tz)
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "legacy")
    try:
        berlin = zoneinfo.ZoneInfo("Europe/Berlin")
        naive = datetime(2025, 1, 1, 12, 0, 0, 250306)

        assert format_query_value(naive, server_tz=timezone.utc) == f"'{query_utc}'"
        assert format_query_value(naive, server_tz=berlin) == f"'{berlin_value}'"
        assert format_bind_value(naive, server_tz=timezone.utc) == bind_utc
        assert format_bind_value(naive, server_tz=berlin) == berlin_value
        assert format_bind_value(DT64Param(naive), server_tz=timezone.utc) == f"{bind_utc}.250306"
        assert format_bind_value(DT64Param(naive), server_tz=berlin) == f"{berlin_value}.250306"

        _, params = bind_query(
            "SELECT {dt:DateTime}, {dt_berlin:DateTime('Europe/Berlin')}",
            {"dt": naive, "dt_berlin": naive},
            server_tz=timezone.utc,
        )
        assert params == {"param_dt": bind_utc, "param_dt_berlin": berlin_value}

        _, params = bind_query(
            "SELECT {dt:DateTime64(6, 'UTC')}, {items:Array(DateTime64(6, 'UTC'))}",
            {"dt": naive, "items": [naive]},
            server_tz=timezone.utc,
        )
        assert params == {
            "param_dt": f"{bind_utc}.250306",
            "param_items": f"['{bind_utc}.250306']",
        }

        # Aware values must format identically in legacy and wall modes.
        aware = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        assert format_query_value(aware, server_tz=berlin) == "'2025-01-01 13:00:00'"
        assert format_bind_value(aware, server_tz=berlin) == "2025-01-01 13:00:00"
        assert format_bind_value(DT64Param(aware), server_tz=berlin) == "2025-01-01 13:00:00.000000"
    finally:
        common.set_setting("naive_datetime_binding", original_setting)
        _restore_process_timezone(monkeypatch, original_tz)


def test_none_offset_tzinfo_is_naive_for_wall_binding():
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "wall")
    try:
        berlin = zoneinfo.ZoneInfo("Europe/Berlin")
        value = datetime(2025, 1, 1, 12, 0, 0, 250306, tzinfo=NoOffsetTZ())

        assert format_query_value(value, server_tz=berlin) == "'2025-01-01 12:00:00'"
        assert format_bind_value(value, server_tz=berlin) == "2025-01-01 12:00:00"
        assert DT64Param(value).format(berlin, top_level=True) == "2025-01-01 12:00:00.250306"
    finally:
        common.set_setting("naive_datetime_binding", original_setting)
