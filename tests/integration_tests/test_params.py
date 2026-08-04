import os
import time
from collections.abc import Callable
from datetime import date, datetime

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.binding import DT64Param

HAS_TZSET = hasattr(time, "tzset")
NON_UTC_HOST_TZ = "America/New_York"


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


def test_params(param_client: Client, call, table_context: Callable):
    result = call(param_client.query, "SELECT name, database FROM system.tables WHERE database = {db:String}", parameters={"db": "system"})
    assert result.first_item["database"] == "system"
    if param_client.min_version("21"):
        result = call(
            param_client.query,
            "SELECT name, {col:String} FROM system.tables WHERE table ILIKE {t:String}",
            parameters={"t": "%rr%", "col": "database"},
        )
        assert "rr" in result.first_item["name"]

    first_date = datetime.strptime("Jun 1 2005  1:33PM", "%b %d %Y %I:%M%p").replace(tzinfo=param_client.server_tz)
    second_date = datetime.strptime("Dec 25 2022  5:00AM", "%b %d %Y %I:%M%p").replace(tzinfo=param_client.server_tz)
    with table_context("test_bind_params", ["key UInt64", "dt DateTime", "value String", "t Tuple(String, String)"]):
        call(
            param_client.insert,
            "test_bind_params",
            [
                [1, first_date, "v11", ("one", "two")],
                [2, second_date, "v21", ("t1", "t2")],
                [3, datetime.now(), "v31", ("str1", "str2")],
            ],
        )
        result = call(param_client.query, "SELECT * FROM test_bind_params WHERE dt = {dt:DateTime}", parameters={"dt": second_date})
        assert result.first_item["key"] == 2
        result = call(param_client.query, "SELECT * FROM test_bind_params WHERE dt = %(dt)s", parameters={"dt": first_date})
        assert result.first_item["key"] == 1
        result = call(
            param_client.query, "SELECT * FROM test_bind_params WHERE value != %(v)s AND value like '%%1'", parameters={"v": "v11"}
        )
        assert result.row_count == 2
        result = call(param_client.query, "SELECT * FROM test_bind_params WHERE value IN %(tp)s", parameters={"tp": ("v18", "v31")})
        assert result.first_item["key"] == 3

    result = call(
        param_client.query, "SELECT number FROM numbers(10) WHERE {n:Nullable(String)} IS NULL", parameters={"n": None}
    ).result_rows
    assert len(result) == 10

    date_params = [date(2023, 6, 1), date(2023, 8, 5)]
    result = call(param_client.query, "SELECT {l:Array(Date)}", parameters={"l": date_params}).first_row
    assert date_params == result[0]

    dt_params = [datetime(2023, 6, 1, 7, 40, 2), datetime(2023, 8, 17, 20, 0, 10)]
    result = call(param_client.query, "SELECT {l:Array(DateTime)}", parameters={"l": dt_params}).first_row
    assert dt_params == result[0]

    num_array_params = [2.5, 5.3, 7.4]
    result = call(param_client.query, "SELECT {l:Array(Float64)}", parameters={"l": num_array_params}).first_row
    assert num_array_params == result[0]
    result = call(param_client.query, "SELECT %(l)s", parameters={"l": num_array_params}).first_row
    assert num_array_params == result[0]

    tp_params = ("str1", "str2")
    result = call(param_client.query, "SELECT %(tp)s", parameters={"tp": tp_params}).first_row
    assert tp_params == result[0]

    num_params = {"p_0": 2, "p_1": 100523.55}
    result = call(
        param_client.query, "SELECT count() FROM system.tables WHERE total_rows > %(p_0)d and total_rows < %(p_1)f", parameters=num_params
    )
    assert result.first_row[0] > 0


def test_datetime_64_params(param_client: Client, call):
    dt_values = [datetime(2023, 6, 1, 7, 40, 2, 250306), datetime(2023, 8, 17, 20, 0, 10, 777722)]
    dt_params = {f"d{ix}": DT64Param(v) for ix, v in enumerate(dt_values)}
    result = call(param_client.query, "SELECT {d0:DateTime64(3)}, {d1:Datetime64(9)}", parameters=dt_params).first_row
    assert result[0] == dt_values[0].replace(microsecond=250000)
    assert result[1] == dt_values[1]

    result = call(param_client.query, "SELECT {a1:Array(DateTime64(6))}", parameters={"a1": [dt_params["d0"], dt_params["d1"]]}).first_row
    assert result[0] == dt_values

    dt_params = {f"d{ix}_64": v for ix, v in enumerate(dt_values)}
    result = call(param_client.query, "SELECT {d0:DateTime64(3)}, {d1:Datetime64(9)}", parameters=dt_params).first_row
    assert result[0] == dt_values[0].replace(microsecond=250000)
    assert result[1] == dt_values[1]

    result = call(param_client.query, "SELECT {a1:Array(DateTime64(6))}", parameters={"a1_64": dt_values}).first_row
    assert result[0] == dt_values

    dt_params = [DT64Param(v) for v in dt_values]
    result = call(param_client.query, "SELECT %s as string, toDateTime64(%s,6) as dateTime", parameters=dt_params).first_row
    assert result == ("2023-06-01 07:40:02.250306", dt_values[1])

    result = call(
        param_client.query, "SELECT {d0:DateTime64(6)}, {d1:DateTime64(9)}", parameters={"d0": dt_values[0], "d1": dt_values[1]}
    ).first_row
    assert result[0] == dt_values[0]
    assert result[1] == dt_values[1]

    result = call(param_client.query, "SELECT {a1:Array(DateTime64(6))}", parameters={"a1": dt_values}).first_row
    assert result[0] == dt_values


@pytest.mark.skipif(not HAS_TZSET, reason="time.tzset is required")
def test_naive_datetime_wall_binding_live(param_client: Client, call, monkeypatch):
    original_tz = _force_process_timezone(monkeypatch, NON_UTC_HOST_TZ)
    original_setting = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", "wall")
    try:
        naive = datetime(2025, 1, 1, 12, 0, 0, 250306)
        dt = naive.replace(microsecond=0)
        query = "SELECT {dt:DateTime('UTC')}, {dt64:DateTime64(6, 'UTC')}, {items:Array(Tuple(DateTime('UTC'), DateTime64(6, 'UTC')))}"
        row = call(
            param_client.query,
            query,
            parameters={"dt": dt, "dt64": naive, "items": [(dt, naive)]},
            query_tz="UTC",
        ).first_row
        assert row == (dt, naive, [(dt, naive)])

        row = call(
            param_client.query,
            "SELECT toString({dt:DateTime}), formatDateTime({dt_berlin:DateTime('Europe/Berlin')}, '%F %T', 'Europe/Berlin')",
            parameters={"dt": dt, "dt_berlin": dt},
            query_tz="UTC",
        ).first_row
        assert row == ("2025-01-01 12:00:00", "2025-01-01 12:00:00")
    finally:
        common.set_setting("naive_datetime_binding", original_setting)
        _restore_process_timezone(monkeypatch, original_tz)


def test_null_in_containers(param_client: Client, call):
    result = call(
        param_client.query,
        "SELECT {t:Tuple(String, Nullable(String), Int32)}",
        parameters={"t": ("user_1", None, 79)},
    ).first_row
    assert result[0] == ("user_1", None, 79)

    result = call(
        param_client.query,
        "SELECT {a:Array(Nullable(String))}",
        parameters={"a": ["user_1", None]},
    ).first_row
    assert result[0] == ["user_1", None]

    result = call(
        param_client.query,
        "SELECT {a:Array(Tuple(String, Nullable(String)))}",
        parameters={"a": [("user_1", None)]},
    ).first_row
    assert result[0] == [("user_1", None)]

    original = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        result = call(
            param_client.query,
            "SELECT {m:Map(String, Nullable(String))}",
            parameters={"m": {"user_1": None}},
        ).first_row
        assert result[0] == {"user_1": None}
    finally:
        common.set_setting("dict_parameter_format", original)
