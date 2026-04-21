import zoneinfo
from datetime import date, datetime, timezone

import pytest

from clickhouse_connect.driver.binding import _extract_tz_from_type, bind_query, finalize_query, format_bind_value


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
    ],
)
def test_format_bind_value(value, expected):
    assert format_bind_value(value) == expected


class TestBindQueryTimezoneHint:
    """Type hint timezone in {param:Type} should override server_tz."""

    berlin_tz = zoneinfo.ZoneInfo("Europe/Berlin")
    dt_utc = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    def test_datetime64_utc_hint(self):
        query = "SELECT * FROM t WHERE dt >= {dt:DateTime64(6, 'UTC')}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00"

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
        assert params["param_dt"] == "2025-01-01 13:00:00"

    def test_nullable_wrapper(self):
        query = "SELECT * FROM t WHERE dt >= {dt:Nullable(DateTime64(6, 'UTC'))}"
        _, params = bind_query(query, {"dt": self.dt_utc}, server_tz=self.berlin_tz)
        assert params["param_dt"] == "2025-01-01 12:00:00"

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
        assert tz == zoneinfo.ZoneInfo("UTC")

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
