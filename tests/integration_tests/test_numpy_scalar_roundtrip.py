import json
from datetime import datetime, timezone

import numpy as np
import pytest

from clickhouse_connect import common
from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.driver.exceptions import DataError


@pytest.mark.parametrize("type_name", ["JSON", "JSON(a Int64)", "JSON(max_dynamic_paths=0)", "JSON(d Nullable(Int64))"])
@pytest.mark.parametrize("output", ["query_np", "df_extended", "df_plain"])
def test_numpy_json_string(param_client, call, test_config, type_name, output):
    if test_config.native_codec == "rust_strict":
        pytest.skip("rust_strict does not support query_formats")
    document = {"a": 13, "f": 1.5, "b": False, "s": "user_1", "arr": [1, 2], "nested": {"n": 79}}
    if type_name == "JSON(d Nullable(Int64))":
        document = {"d": None}
    query = f"SELECT {{v:String}}::{type_name} AS j"
    kwargs = {"parameters": {"v": json.dumps(document)}, "query_formats": {"JSON": "string"}}
    expected = call(param_client.query, query, **kwargs).first_row[0]
    if output == "query_np":
        actual = call(param_client.query_np, query, **kwargs)[0][0]
    else:
        actual = call(param_client.query_df, query, use_extended_dtypes=output == "df_extended", **kwargs).iloc[0, 0]
    assert actual == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT CAST((13, false, [79]), 'Tuple(n Int64, b Bool, a Array(Int64))') AS v",
        "SELECT CAST((13, NULL), 'Tuple(n Int64, z Nullable(Int8))') AS v",
    ],
)
@pytest.mark.parametrize("output", ["query_np", "df_extended", "df_plain"])
def test_numpy_named_tuple_json(param_client, call, test_config, query, output):
    if test_config.native_codec == "rust_strict":
        pytest.skip("rust_strict does not support query_formats")
    kwargs = {"query_formats": {"Tuple": "json"}}
    expected = call(param_client.query, query, **kwargs).first_row[0]
    if output == "query_np":
        actual = call(param_client.query_np, query, **kwargs)[0][0]
    else:
        actual = call(param_client.query_df, query, use_extended_dtypes=output == "df_extended", **kwargs).iloc[0, 0]
    assert actual == expected


@pytest.mark.parametrize("type_name", ["JSON", pytest.param("Nullable(JSON)", marks=pytest.mark.local_only)])
def test_numpy_json_insert(param_client, call, table_context, type_name):
    value = {
        "n": np.int64(13),
        "f": np.float64(1.5),
        "b": np.bool_(True),
        "s": np.str_("user_1"),
        "arr": np.array([13, 79]),
    }
    expected = {"n": 13, "f": 1.5, "b": True, "s": "user_1", "arr": [13, 79]}
    with table_context("numpy_json_insert", ["id UInt8", f"v {type_name}"]):
        rows = [[1, value], [2, expected]]
        if type_name.startswith("Nullable"):
            rows.append([3, None])
        call(param_client.insert, "numpy_json_insert", rows)
        result = call(param_client.query, "SELECT v FROM numpy_json_insert ORDER BY id").result_rows
        assert result[0][0] == result[1][0] == expected
        if len(rows) == 3:
            assert result[2][0] is None


DATETIME_TYPES = ["DateTime", "DateTime('Asia/Tokyo')", "DateTime64(3)", "DateTime64(6, 'UTC')", "DateTime64(9)", "DateTime64(2)"]
DATETIME_UNITS = ["Y", "M", "W", "D", "h", "m", "s", "ms", "us", "ns", "ps", "fs", "as", "3ms"]
SHAPE_CLOUD_CASES = {("DateTime", "s"), ("DateTime64(3)", "ms")}


def _ticks(unit, scale):
    if unit == "Y":
        seconds = int(datetime(1983, 1, 1, tzinfo=timezone.utc).timestamp())
        return seconds * 10**scale
    if unit == "M":
        seconds = int(datetime(1971, 2, 1, tzinfo=timezone.utc).timestamp())
        return seconds * 10**scale
    numerator, denominator = {
        "W": (604800, 1),
        "D": (86400, 1),
        "h": (3600, 1),
        "m": (60, 1),
        "s": (1, 1),
        "ms": (1, 1000),
        "us": (1, 1000000),
        "ns": (1, 1000000000),
        "ps": (1, 10**12),
        "fs": (1, 10**15),
        "as": (1, 10**18),
        "3ms": (3, 1000),
    }[unit]
    return 13 * numerator * 10**scale // denominator


@pytest.mark.parametrize(
    "type_name,unit",
    [
        pytest.param(t, u, id=f"{u}-{t}", marks=() if (t, u) in SHAPE_CLOUD_CASES else pytest.mark.local_only)
        for u in DATETIME_UNITS
        for t in DATETIME_TYPES
    ],
)
def test_numpy_datetime_scalar_shapes(param_client, call, table_context, type_name, unit):
    scale = int(type_name.split("(")[1].split(",")[0].rstrip(")")) if type_name.startswith("DateTime64") else 0
    value = np.datetime64(13, unit)
    expected = _ticks(unit, scale)
    columns = [
        "id UInt8",
        f"v {type_name}",
        f"n Nullable({type_name})",
        f"a Array({type_name})",
        f"t Tuple(String, {type_name})",
        f"at Array(Tuple(String, {type_name}))",
        f"m Map(String, {type_name})",
    ]
    with table_context("numpy_datetime_shapes", columns):

        def row(key, v):
            return [key, v, v, [v], ("user_1", v), [("user_1", v)], {"user_1": v}]

        actual = row(1, value)
        actual[2] = None
        reference = row(2, expected)
        reference[2] = None
        call(param_client.insert, "numpy_datetime_shapes", [actual, row(3, value)])
        call(param_client.insert, "numpy_datetime_shapes", [reference, row(4, expected)])
        assert (
            call(
                param_client.command,
                "SELECT (v,a,t,at,m) = "
                "(SELECT (v,a,t,at,m) FROM numpy_datetime_shapes WHERE id=2) "
                "AND isNull(n) FROM numpy_datetime_shapes WHERE id=1",
            )
            == 1
        )
        assert (
            call(
                param_client.command,
                "SELECT n = (SELECT n FROM numpy_datetime_shapes WHERE id=4) FROM numpy_datetime_shapes WHERE id=3",
            )
            == 1
        )


@pytest.mark.parametrize(
    "scale",
    [
        2,
        pytest.param(3, marks=pytest.mark.local_only),
        pytest.param(6, marks=pytest.mark.local_only),
        pytest.param(9, marks=pytest.mark.local_only),
    ],
)
@pytest.mark.parametrize("value", [np.datetime64(-13, "ns"), np.datetime64("2500-01-02T03:04:05.123456", "us")])
def test_numpy_datetime64_floor_and_range(param_client, call, table_context, scale, value):
    if scale == 9 and str(value).startswith("2500"):
        pytest.skip("Year 2500 is outside DateTime64(9)")
    if int(value.astype("int64")) == -13:
        ticks = -13 * 10**scale // 10**9
    else:
        seconds = int(datetime(2500, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp())
        ticks = seconds * 10**scale + 123456 * 10**scale // 10**6
    with table_context("numpy_datetime_floor", ["id UInt8", f"v DateTime64({scale})"]):
        call(param_client.insert, "numpy_datetime_floor", [[1, value]])
        call(param_client.insert, "numpy_datetime_floor", [[2, ticks]])
        assert (
            call(
                param_client.command,
                "SELECT v = (SELECT v FROM numpy_datetime_floor WHERE id=2) FROM numpy_datetime_floor WHERE id=1",
            )
            == 1
        )


@pytest.mark.local_only
@pytest.mark.parametrize("type_name", ["DateTime", "Nullable(DateTime64(3))"])
def test_numpy_datetime_nat(param_client, call, table_context, type_name):
    with table_context("numpy_datetime_nat", ["id UInt8", f"v {type_name}"]):
        rows = [[1, np.datetime64("NaT")]]
        if type_name.startswith("Nullable"):
            rows.insert(0, [0, None])
        with pytest.raises(DataError, match="column.*v.*NaT"):
            call(param_client.insert, "numpy_datetime_nat", rows)


@pytest.mark.local_only
def test_numpy_datetime_array_roundtrip(param_client, call, table_context):
    with (
        table_context("numpy_datetime_source", ["id UInt8", "v Array(DateTime)"]),
        table_context("numpy_datetime_copy", ["id UInt8", "v Array(DateTime)"]),
    ):
        call(param_client.insert, "numpy_datetime_source", [[1, [13, 79]], [2, []]])
        values = call(param_client.query_np, "SELECT v FROM numpy_datetime_source ORDER BY id")
        call(param_client.insert, "numpy_datetime_copy", [[i + 1, row[0]] for i, row in enumerate(values)])
        assert (
            call(param_client.query, "SELECT * FROM numpy_datetime_source ORDER BY id").result_rows
            == call(param_client.query, "SELECT * FROM numpy_datetime_copy ORDER BY id").result_rows
        )


@pytest.mark.parametrize("type_name", ["Variant(String, DateTime64(3))"])
def test_numpy_datetime_typed_variant(param_client, call, table_context, type_name):
    with table_context("numpy_datetime_variant", ["id UInt8", f"v {type_name}"]):
        call(
            param_client.insert,
            "numpy_datetime_variant",
            [[1, typed_variant(np.datetime64(1379, "ms"), "DateTime64(3)")]],
        )
        call(param_client.insert, "numpy_datetime_variant", [[2, typed_variant(1379, "DateTime64(3)")]])
        assert (
            call(
                param_client.command,
                "SELECT v = (SELECT v FROM numpy_datetime_variant WHERE id=2) FROM numpy_datetime_variant WHERE id=1",
            )
            == 1
        )


@pytest.mark.parametrize("nullable", [False, pytest.param(True, marks=pytest.mark.local_only)])
def test_numpy_datetime_low_cardinality(param_client, call, table_context, nullable):
    type_name = "Nullable(DateTime)" if nullable else "DateTime"
    with table_context("numpy_datetime_lc", ["id UInt8", f"v LowCardinality({type_name})"]):
        values = [[1, np.datetime64(1379, "ms")], [2, np.datetime64(1379, "ms")]]
        if nullable:
            values.insert(0, [0, None])
        call(param_client.insert, "numpy_datetime_lc", values)
        result = call(param_client.query, "SELECT id, toUnixTimestamp(v) FROM numpy_datetime_lc ORDER BY id").result_rows
        assert result == ([(0, None)] if nullable else []) + [(1, 1), (2, 1)]


@pytest.mark.parametrize("setting", ["local", pytest.param("server", marks=pytest.mark.local_only)])
def test_numpy_datetime_utc_epoch(param_client, call, table_context, test_config, setting):
    if setting == "server" and test_config.native_codec == "rust_strict":
        pytest.skip("rust_strict does not support naive_datetime_insert=server")
    previous = common.get_setting("naive_datetime_insert")
    try:
        common.set_setting("naive_datetime_insert", setting)
        with table_context("numpy_datetime_epoch", ["id UInt8", "v DateTime('Asia/Tokyo')"]):
            call(param_client.insert, "numpy_datetime_epoch", [[1, np.datetime64("2024-01-02T03:04:05", "s")]])
            assert call(param_client.command, "SELECT toUnixTimestamp(v) FROM numpy_datetime_epoch") == 1704164645
    finally:
        common.set_setting("naive_datetime_insert", previous)


@pytest.mark.local_only
@pytest.mark.parametrize("ticks", [-1, 2**32])
def test_numpy_datetime_out_of_range(param_client, call, table_context, ticks):
    with table_context("numpy_datetime_range", ["id UInt8", "v DateTime"]):
        with pytest.raises(DataError):
            call(param_client.insert, "numpy_datetime_range", [[1, ticks]])
        with pytest.raises(DataError):
            call(param_client.insert, "numpy_datetime_range", [[1, np.datetime64(ticks, "s")]])


@pytest.mark.local_only
@pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(0)", "DateTime64(9)"])
def test_numpy_datetime_calendar_overflow(param_client, call, table_context, type_name):
    with table_context("numpy_datetime_calendar_overflow", ["id UInt8", f"v {type_name}"]):
        with pytest.raises(DataError, match="column.*v.*out.*range"):
            call(
                param_client.insert,
                "numpy_datetime_calendar_overflow",
                [[1, np.datetime64(-1111120336821728400, "Y")]],
            )
