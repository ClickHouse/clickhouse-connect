import threading
import time
from datetime import date, timedelta

import pytest

from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.driver.exceptions import DataError, NotSupportedError, ProgrammingError, StreamFailureError
from tests.integration_tests.conftest import type_available

pytest.importorskip("_ch_core")

# These tests require a UTC-configured server. On a non-UTC server the eligibility gate reports
# "ambient timezone"/"server timezone header" and rust_strict raises instead of exercising the codec.

SCALAR_QUERY = (
    "SELECT number AS n, toString(number) AS s, toDateTime(number) AS dt, "
    "toDate(number) AS d, toInt32(number) - 5 AS i, CAST(number AS Bool) AS b FROM numbers(100)"
)

# Decoder-supported type shapes, including the higher-risk timezone-materialization paths.
DECODE_MATRIX = {
    "nullable_nothing": "NULL",
    "array_nothing": "[]",
    "array_nullable_nothing": "[NULL]",
    "tuple_nullable_nothing": "tuple(NULL, toUInt8(number))",
    "array_tuple_nullable_nothing": "[tuple(NULL, toUInt8(number))]",
    "tuple_array_nothing": "tuple([], toUInt8(number))",
    "map_nothing": "map()",
    "map_nullable_nothing_value": "mapFromArrays([toUInt8(number)], [NULL])",
    "int128": "toInt128(number) - toInt128('170141183460469231731687303715884105000')",
    "uint128": "toUInt128('340282366920938463463374607431768211000') + number",
    "int256": "toInt256(number) - toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819000')",
    "uint256": "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639000') + number",
    "nullable_wide_int": "CAST(if(number % 3 = 0, NULL, toInt256(number) - 2) AS Nullable(Int256))",
    "array_wide_int": "arrayMap(x -> toUInt128(x) + toUInt128('18446744073709551616'), range(number % 4))",
    "tuple_wide_int": "tuple(toInt128(number) - 2, toUInt256(number) + toUInt256('340282366920938463463374607431768211456'))",
    "array_tuple_wide_int": ("arrayMap(x -> tuple(toInt256(x) - 2, toUInt128(x) + toUInt128('18446744073709551616')), range(number % 4))"),
    "nullable_int": "CAST(if(number % 3 = 0, NULL, toInt32(number)) AS Nullable(Int32))",
    "low_card_string": "CAST(toString(number % 3) AS LowCardinality(String))",
    "low_card_nullable_string": "CAST(if(number % 2 = 0, NULL, toString(number)) AS LowCardinality(Nullable(String)))",
    "enum8": "CAST(if(number % 2 = 0, 'a', 'b') AS Enum8('a' = 1, 'b' = 2))",
    "enum16": "CAST(if(number % 2 = 0, 'x', 'y') AS Enum16('x' = 100, 'y' = 200))",
    "datetime64": "toDateTime64(number, 3)",
    "datetime64_utc": "toDateTime64(number, 3, 'UTC')",
    "fixed_string": "CAST(leftPad(toString(number), 6, '0') AS FixedString(6))",
    "datetime_utc": "toDateTime(number, 'UTC')",
    "datetime_named_tz": "toDateTime(number, 'America/New_York')",
    "float64": "toFloat64(number) / 2",
    "uuid": "toUUID(concat(leftPad(lower(hex(number)), 8, '0'), '-1122-3344-5566-778899aabbcc'))",
    "nullable_uuid": (
        "CAST(if(number % 3 = 0, NULL, toUUID(concat(leftPad(lower(hex(number)), 8, '0'), "
        "'-1122-3344-5566-778899aabbcc'))) AS Nullable(UUID))"
    ),
    "decimal32": "toDecimal32(number / 3 - 1, 2)",
    "decimal64": "toDecimal64(number / 3 - 2, 4)",
    "decimal128": "toDecimal128(number / 3 - 2, 10)",
    "decimal256": "toDecimal256(number / 3 - 2, 20)",
    "nullable_decimal": "CAST(if(number % 3 = 0, NULL, toDecimal64(number / 3, 4)) AS Nullable(Decimal(18, 4)))",
    "ipv4": "toIPv4(toUInt32(number * 16909060))",
    "nullable_ipv4": "CAST(if(number % 3 = 0, NULL, toIPv4(toUInt32(number * 16909060))) AS Nullable(IPv4))",
    "ipv6": "toIPv6(concat('2001:db8::', lower(hex(toUInt16(number + 1)))))",
    "ipv6_v4_mapped": "toIPv6(toIPv4(toUInt32(number + 1)))",
    "array_int": "range(number % 4)",
    "array_string": "arrayMap(x -> toString(x), range(number % 4))",
    "array_nullable_int": "arrayMap(x -> if(x % 2 = 0, NULL, toInt64(x)), range(number % 4))",
    "array_low_card_string": "CAST(arrayMap(x -> toString(x % 3), range(number % 4)) AS Array(LowCardinality(String)))",
    "array_nested": "arrayMap(x -> range(x % 3), range(number % 4))",
    "array_uuid": "arrayMap(x -> toUUID(concat(leftPad(lower(hex(x)), 8, '0'), '-1122-3344-5566-778899aabbcc')), range(number % 4))",
    "array_datetime": "arrayMap(x -> toDateTime(x), range(number % 4))",
    "array_decimal": "arrayMap(x -> toDecimal64(x, 4), range(number % 4))",
    "tuple_unnamed": "tuple(number, toString(number))",
    "tuple_named": "CAST((toInt64(number), toString(number)), 'Tuple(a Int64, b String)')",
    "tuple_low_card": "CAST((toString(number % 3), number), 'Tuple(LowCardinality(String), UInt64)')",
    "tuple_nullable_element": "tuple(if(number % 2 = 0, NULL, toString(number)))",
    "map_string_int": "mapFromArrays(arrayMap(x -> concat('k', toString(x)), range(number % 4)), range(number % 4))",
    "map_array_value": "CAST(map('a', range(number % 4)), 'Map(String, Array(UInt64))')",
    "map_low_card_key": "CAST(map(toString(number % 3), number), 'Map(LowCardinality(String), UInt64)')",
    "array_of_tuple": "arrayMap(x -> (x, toString(x)), range(number % 4))",
    "map_of_tuple_value": "CAST(map('a', (number, toString(number))), 'Map(String, Tuple(UInt64, String))')",
    "point": "(toFloat64(number), toFloat64(number) / 2)::Point",
    "ring": "[(toFloat64(number), toFloat64(number) / 2), (toFloat64(number) + 1, toFloat64(number))]::Ring",
    "linestring": "[(toFloat64(number), toFloat64(number)), (toFloat64(number) + 1, 0.)]::LineString",
    "polygon": "[[(toFloat64(number), 0.), (0., toFloat64(number)), (1., 1.)]]::Polygon",
    "multilinestring": "[[(toFloat64(number), 0.), (1., toFloat64(number))]]::MultiLineString",
    "multipolygon": "[[[(toFloat64(number), 0.), (0., 1.), (1., 0.)]]]::MultiPolygon",
    "simple_agg_uint64": "CAST(number AS SimpleAggregateFunction(sum, UInt64))",
    "simple_agg_string": "CAST(toString(number) AS SimpleAggregateFunction(anyLast, String))",
    "simple_agg_low_card": "CAST(toString(number % 3) AS SimpleAggregateFunction(anyLast, LowCardinality(String)))",
    "simple_agg_array": "CAST(range(number % 4) AS SimpleAggregateFunction(groupArrayArray, Array(UInt64)))",
}


def test_rust_codec_ab_parity(client_factory, call):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    rust_result = call(rust_client.query, SCALAR_QUERY)
    python_result = call(python_client.query, SCALAR_QUERY)

    assert rust_result.result_rows == python_result.result_rows
    assert rust_result.column_names == python_result.column_names
    assert [t.name for t in rust_result.column_types] == [t.name for t in python_result.column_types]


def test_rust_codec_variant_round_trip_parity(client_factory, call, client_mode):
    pytest.importorskip("pandas")
    probe = client_factory(native_codec="python")
    type_available(probe, "variant")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    rust_table = f"rc_variant_rust_{client_mode}"
    python_table = f"rc_variant_python_{client_mode}"
    schema = (
        "id UInt8, v Variant(String, UInt64), "
        "a Array(Variant(Bool, Int32, String)), "
        "m Map(String, Variant(String, UInt64)), "
        "va Variant(Array(String), Array(UInt32))"
    )
    rows = [
        [
            0,
            None,
            [True, 13, "a", None],
            {"a": "x", "b": 13, "c": "y"},
            typed_variant([13, 79], "Array(UInt32)"),
        ],
        [
            1,
            "user_1",
            [],
            {},
            typed_variant(["a", "b"], "Array(String)"),
        ],
        [
            2,
            79,
            [False, -13, "b"],
            {"d": 79, "e": "z"},
            typed_variant([], "Array(UInt32)"),
        ],
    ]
    names = ["id", "v", "a", "m", "va"]
    expected = [
        (0, None, [True, 13, "a", None], {"a": "x", "b": 13, "c": "y"}, [13, 79]),
        (1, "user_1", [], {}, ["a", "b"]),
        (2, 79, [False, -13, "b"], {"d": 79, "e": "z"}, []),
    ]

    def round_trip(insert_client, table):
        call(insert_client.command, f"DROP TABLE IF EXISTS {table}")
        call(insert_client.command, f"CREATE TABLE {table} ({schema}) ENGINE Memory")
        call(insert_client.insert, table, rows, column_names=names)
        query = f"SELECT * FROM {table} ORDER BY id"
        rust_result = call(rust_client.query, query)
        python_result = call(python_client.query, query)
        assert rust_result.result_rows == python_result.result_rows == expected
        assert [ch_type.name for ch_type in rust_result.column_types] == [ch_type.name for ch_type in python_result.column_types]
        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        assert rust_np.tolist() == python_np.tolist()
        rust_df = call(rust_client.query_df, query)
        python_df = call(python_client.query_df, query)
        # Cells are objects; cell-type parity is a known divergence, so compare values only.
        assert list(rust_df.dtypes) == list(python_df.dtypes)
        assert rust_df.equals(python_df)

    try:
        round_trip(rust_client, rust_table)
        round_trip(python_client, python_table)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {python_table}")


DYNAMIC_QUERY = """
    SELECT id, d, s, [d, CAST('tail', 'Dynamic')] AS a,
           tuple(d, id) AS t, map('v', d) AS m
    FROM
    (
        SELECT toUInt8(0) AS id, CAST(NULL, 'Dynamic') AS d,
               CAST(NULL, 'Dynamic(max_types=0)') AS s
        UNION ALL
        SELECT 1, CAST('user_1', 'Dynamic'),
               CAST(toDate('2024-01-02'), 'Dynamic(max_types=0)')
        UNION ALL
        SELECT 2, CAST(toUInt64(79), 'Dynamic'),
               CAST([toUInt8(1), 2, 3], 'Dynamic(max_types=0)')
        UNION ALL
        SELECT 3, CAST(toInt32(-13), 'Dynamic'),
               CAST('hello', 'Dynamic(max_types=0)')
        UNION ALL
        SELECT 4, CAST(toFloat64(2.5), 'Dynamic'),
               CAST(toUInt64(79), 'Dynamic(max_types=0)')
        UNION ALL
        SELECT 5, CAST(true, 'Dynamic'),
               CAST(toFloat64(-0.5), 'Dynamic(max_types=0)')
        UNION ALL
        SELECT 6, CAST(toInt64(7), 'Dynamic'),
               CAST(true, 'Dynamic(max_types=0)')
    )
    ORDER BY id
"""

# Typed SharedVariant decode under rust: date/list/str/int/float/bool.
DYNAMIC_SHARED_TYPED = [None, date(2024, 1, 2), [1, 2, 3], "hello", 79, -0.5, True]
# Known divergence: the python codec's shared-cell heuristic only decodes
# int/float/str/bool and returns raw wire bytes for Date and Array cells
# (FINDINGS.md finding 4).
DYNAMIC_SHARED_PYTHON = [None, b"\x0f\x0cM", b"\x1e\x01\x03\x01\x02\x03", "hello", 79, -0.5, True]
DYNAMIC_DIRECT = [None, "user_1", 79, -13, 2.5, True, 7]


def test_rust_codec_dynamic_decode_parity(client_factory, call, consume_stream):
    np = pytest.importorskip("numpy")
    pytest.importorskip("pandas")
    probe = client_factory(native_codec="python")
    type_available(probe, "dynamic")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    rust_result = call(rust_client.query, DYNAMIC_QUERY, settings={"max_block_size": 1})
    python_result = call(python_client.query, DYNAMIC_QUERY)
    rust_rows = rust_result.result_rows
    python_rows = python_result.result_rows
    assert [t.name for t in rust_result.column_types] == [t.name for t in python_result.column_types]

    # The direct Dynamic column and every container built from it are full parity.
    for ix in (0, 1, 3, 4, 5):  # id, d, a, t, m
        assert [row[ix] for row in rust_rows] == [row[ix] for row in python_rows]
    assert [row[1] for row in rust_rows] == DYNAMIC_DIRECT
    assert [row[3] for row in rust_rows] == [[value, "tail"] for value in DYNAMIC_DIRECT]
    assert [row[4] for row in rust_rows] == list(zip(DYNAMIC_DIRECT, range(7)))
    assert [row[5] for row in rust_rows] == [{"v": value} for value in DYNAMIC_DIRECT]

    assert [row[2] for row in rust_rows] == DYNAMIC_SHARED_TYPED
    assert [row[2] for row in python_rows] == DYNAMIC_SHARED_PYTHON

    # max_block_size=1 forces one row per block, exercising block-local child unification.
    for settings in (None, {"max_block_size": 1}):
        rust_np = call(rust_client.query_np, DYNAMIC_QUERY, settings=settings)
        python_np = call(python_client.query_np, DYNAMIC_QUERY, settings=settings)
        assert rust_np.dtype == python_np.dtype
        for name in ("id", "d", "a", "t", "m"):
            assert rust_np[name].tolist() == python_np[name].tolist()
        assert rust_np["s"].tolist() == DYNAMIC_SHARED_TYPED
        assert python_np["s"].tolist() == DYNAMIC_SHARED_PYTHON

        # np-scalar residue (FINDINGS.md finding 4): rust yields python-native
        # scalars in object cells where python yields value-equal numpy scalars.
        assert type(rust_np["d"].tolist()[2]) is int
        assert isinstance(python_np["d"].tolist()[2], np.unsignedinteger)

        rust_df = call(rust_client.query_df, DYNAMIC_QUERY, settings=settings)
        python_df = call(python_client.query_df, DYNAMIC_QUERY, settings=settings)
        assert list(rust_df.dtypes) == list(python_df.dtypes)
        for name in ("id", "d", "a", "t", "m"):
            assert rust_df[name].tolist() == python_df[name].tolist()
        assert rust_df["s"].tolist() == DYNAMIC_SHARED_TYPED
        # Known divergence: the python codec's pandas exit stringifies every
        # non-null shared cell, including the heuristically decoded ones.
        assert python_df["s"].tolist() == [None, "\x0f\x0cM", "\x1e\x01\x03\x01\x02\x03", "hello", "79", "-0.5", "True"]

        # np-scalar residue inside a container cell (FINDINGS.md finding 4).
        assert type(rust_df["a"].tolist()[2][0]) is int
        assert isinstance(python_df["a"].tolist()[2][0], np.unsignedinteger)

    streamed = []
    consume_stream(call(rust_client.query_rows_stream, DYNAMIC_QUERY, settings={"max_block_size": 1}), streamed.append)
    assert [row[1] for row in streamed] == DYNAMIC_DIRECT
    assert [row[2] for row in streamed] == DYNAMIC_SHARED_TYPED


def test_rust_codec_json_round_trip_parity(client_factory, call, consume_stream, client_mode):
    pytest.importorskip("numpy")
    pytest.importorskip("pandas")
    probe = client_factory(native_codec="python")
    type_available(probe, "json")
    if not probe.min_version("25.3"):
        pytest.skip("Nullable(JSON) requires ClickHouse 25.3+")

    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    rust_table = f"rc_json_rust_{client_mode}"
    python_table = f"rc_json_python_{client_mode}"
    schema = (
        "id UInt8, "
        "payload json(max_dynamic_paths=2, `typed.value` Int64), "
        "nullable_payload Nullable(JSON), "
        "items Array(JSON), "
        "wrapped Tuple(payload JSON)"
    )
    names = ["id", "payload", "nullable_payload", "items", "wrapped"]
    rows = [
        [
            0,
            {"typed": {"value": 13}, "name": "user_1", "active": True, "score": 17},
            None,
            [{"kind": "first"}, {"count": 13}],
            ({"nested": {"value": "a"}},),
        ],
        [
            1,
            {"typed": {"value": 79}, "ratio": 2.5, "deep": {"value": "x"}, "extra": -13},
            {"nullable": "present"},
            [],
            ({"nested": {"value": "b"}},),
        ],
        [
            2,
            {"typed": {"value": 5}, "other": 79, "flag": False, "tail": "shared"},
            {"array": [1, None, 3]},
            [{"kind": "last"}],
            ({"nested": {"value": "c"}},),
        ],
    ]
    expected = [
        (
            0,
            {"typed": {"value": 13}, "name": "user_1", "active": True, "score": 17},
            None,
            [{"kind": "first"}, {"count": 13}],
            {"payload": {"nested": {"value": "a"}}},
        ),
        (
            1,
            {"typed": {"value": 79}, "ratio": 2.5, "deep": {"value": "x"}, "extra": -13},
            {"nullable": "present"},
            [],
            {"payload": {"nested": {"value": "b"}}},
        ),
        (
            2,
            {"typed": {"value": 5}, "other": 79, "flag": False, "tail": "shared"},
            {"array": [1, None, 3]},
            [{"kind": "last"}],
            {"payload": {"nested": {"value": "c"}}},
        ),
    ]

    def create_and_insert(client, table):
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")
        call(python_client.command, f"CREATE TABLE {table} ({schema}) ENGINE Memory")
        call(client.insert, table, rows, column_names=names)

    try:
        create_and_insert(rust_client, rust_table)
        create_and_insert(python_client, python_table)
        rust_query = f"SELECT * FROM {rust_table} ORDER BY id"
        python_query = f"SELECT * FROM {python_table} ORDER BY id"

        # max_block_size=1 varies the block-local dynamic path set and forces
        # shared-data overflow under max_dynamic_paths=2.
        settings = {"max_block_size": 1}
        rust_result = call(rust_client.query, rust_query, settings=settings)
        python_result = call(python_client.query, python_query, settings=settings)
        assert rust_result.result_rows == python_result.result_rows == expected
        assert [ch_type.name for ch_type in rust_result.column_types] == [ch_type.name for ch_type in python_result.column_types]

        rust_np = call(rust_client.query_np, rust_query, settings=settings)
        python_np = call(python_client.query_np, python_query, settings=settings)
        assert rust_np.dtype == python_np.dtype
        assert rust_np.tolist() == rust_result.result_rows

        rust_df = call(rust_client.query_df, rust_query, settings=settings)
        assert list(rust_df.itertuples(index=False, name=None)) == rust_result.result_rows

        streamed = []
        consume_stream(
            call(rust_client.query_rows_stream, rust_query, settings=settings),
            streamed.append,
        )
        assert streamed == rust_result.result_rows
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {python_table}")


def test_rust_codec_json_skip_regexp_round_trip(client_factory, call, client_mode):
    probe = client_factory(native_codec="python")
    type_available(probe, "json")

    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    table = f"rc_json_skip_regexp_{client_mode}"
    # SQL literal renders the regex skip'me\..* with both a single quote and a backslash.
    schema = "id UInt8, payload JSON(SKIP REGEXP 'skip\\'me\\\\..*')"
    rows = [
        [0, {"kept": 13, "skip'me": {"inner": 79}}],
        [1, {"kept": "user_1", "skip'me": {"inner": "x"}, "other": 2.5}],
    ]
    expected = [
        (0, {"kept": 13}),
        (1, {"kept": "user_1", "other": 2.5}),
    ]
    try:
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")
        call(python_client.command, f"CREATE TABLE {table} ({schema}) ENGINE Memory")
        call(rust_client.insert, table, rows, column_names=["id", "payload"])
        query = f"SELECT * FROM {table} ORDER BY id"
        rust_result = call(rust_client.query, query)
        python_result = call(python_client.query, query)
        assert rust_result.result_rows == python_result.result_rows == expected
        assert [ch_type.name for ch_type in rust_result.column_types] == [ch_type.name for ch_type in python_result.column_types]
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize("native_codec", ["rust", "rust_strict"])
def test_rust_codec_dynamic_insert_parity(client_factory, call, client_mode, native_codec):
    probe = client_factory(native_codec="python")
    type_available(probe, "dynamic")
    python_client = client_factory(native_codec="python")
    rows = [[0, None], [1, True], [2, 13], [3, 2.5], [4, "user_1"], [5, [1, 2]]]

    def roundtrip(codec):
        client = client_factory(native_codec=codec)
        table = f"rc_ins_dynamic_{codec}_{client_mode}"
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")
        try:
            call(python_client.command, f"CREATE TABLE {table} (id UInt8, d Dynamic) ENGINE Memory")
            call(client.insert, table, rows, column_names=["id", "d"])
            return call(python_client.query, f"SELECT id, d, dynamicType(d) FROM {table} ORDER BY id").result_rows
        finally:
            call(python_client.command, f"DROP TABLE IF EXISTS {table}")

    # Dynamic inserts send str(value) with None as the literal "NULL"; the rust
    # encoder now builds that String column natively with exact wire parity, so
    # both rust and rust_strict insert without the python fallback.
    expected = roundtrip("python")
    assert roundtrip(native_codec) == expected
    assert [row[1] for row in expected] == ["NULL", "True", "13", "2.5", "user_1", "[1, 2]"]


def test_rust_codec_time_parity(client_factory, call, test_config):
    if test_config.cloud:
        pytest.skip("Time/Time64 settings are locked in ClickHouse Cloud")

    version_client = client_factory(native_codec="python")
    if not version_client.min_version("25.6"):
        pytest.skip("Time and Time64 require ClickHouse 25.6+")

    settings = {"allow_suspicious_low_cardinality_types": 1, "enable_time_time64_type": 1}
    rust_client = client_factory(native_codec="rust_strict", settings=settings)
    python_client = client_factory(native_codec="python", settings=settings)

    cases = [
        (
            "CAST(v AS Time)",
            ["-000:00:05", "000:00:00", "030:00:00"],
            [timedelta(seconds=-5), timedelta(0), timedelta(hours=30)],
        ),
        (
            "CAST(v AS Time64(9))",
            ["-000:00:05.500000000", "000:00:00.000000001", "001:02:03.123456789"],
            [timedelta(seconds=-5, microseconds=-500_000), timedelta(0), timedelta(seconds=3_723, microseconds=123_456)],
        ),
    ]
    for expression, values, expected in cases:
        rows = ", ".join(f"('{value}')" for value in values)
        query = f"SELECT {expression} AS c FROM values('v String', {rows})"

        rust_result = call(rust_client.query, query)
        python_result = call(python_client.query, query)
        assert rust_result.result_rows == python_result.result_rows == [(value,) for value in expected]

        np = pytest.importorskip("numpy")
        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)

    pd = pytest.importorskip("pandas")
    nullable_cases = [
        ("Time", ["000:00:13", None, "-000:00:05"]),
        ("Time64(6)", ["000:00:13.000079", None, "-000:00:05.500000"]),
        ("Time64(9)", ["-000:00:00.000000001", None, "000:00:00.000000001"]),
    ]
    for type_name, values in nullable_cases:
        rows = ", ".join("(NULL)" if value is None else f"('{value}')" for value in values)
        query = f"SELECT CAST(v AS Nullable({type_name})) AS c FROM values('v Nullable(String)', {rows})"

        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)

        for use_extended_dtypes in (False, True):
            rust_df = call(rust_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            python_df = call(python_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            assert rust_df["c"].dtype == python_df["c"].dtype
            pd.testing.assert_frame_equal(rust_df, python_df)

    low_card_queries = [
        ("SELECT CAST(v AS LowCardinality(Time)) AS c FROM values('v String', ('000:00:13'), ('-000:00:05'), ('000:00:13'))"),
        ("SELECT CAST(v AS LowCardinality(Nullable(Time))) AS c FROM values('v Nullable(String)', ('000:00:13'), (NULL), ('-000:00:05'))"),
    ]
    for query in low_card_queries:
        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)
        rust_df = call(rust_client.query_df, query)
        python_df = call(python_client.query_df, query)
        assert rust_df["c"].dtype == python_df["c"].dtype
        pd.testing.assert_frame_equal(rust_df, python_df)

    nested_queries = [
        ("SELECT [CAST('-000:00:00.000000001' AS Time64(9)), CAST('000:00:00.000000001' AS Time64(9))] AS c"),
        ("SELECT tuple(CAST('-000:00:05' AS Time), CAST('000:00:00.000000001' AS Time64(9))) AS c"),
        (
            "SELECT [tuple(CAST('-000:00:05' AS Time), "
            "CAST('-000:00:00.000000001' AS Time64(9))), "
            "tuple(CAST('000:00:13' AS Time), "
            "CAST('000:00:00.000000001' AS Time64(9)))] AS c"
        ),
        ("SELECT map('key', CAST('000:00:00.000000001' AS Time64(9)), 'key', CAST('000:00:00.000000002' AS Time64(9))) AS c"),
        ("SELECT [CAST(NULL AS Nullable(Time64(9))), CAST('000:00:00.000000001' AS Nullable(Time64(9)))] AS c"),
    ]
    for query in nested_queries:
        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)

        for use_extended_dtypes in (False, True):
            rust_df = call(rust_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            python_df = call(python_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            pd.testing.assert_frame_equal(rust_df, python_df)


def test_rust_codec_interval_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = (
        "SELECT toIntervalYear(-13), toIntervalQuarter(79), toIntervalMonth(-13), "
        "toIntervalWeek(79), toIntervalDay(-13), toIntervalHour(79), "
        "toIntervalMinute(-13), toIntervalSecond(79), toIntervalMillisecond(-13), "
        "toIntervalMicrosecond(79), toIntervalNanosecond(-13), 'sentinel'"
    )

    rust_result = call(rust_client.query, query)
    python_result = call(python_client.query, query)
    assert rust_result.result_rows == python_result.result_rows
    assert [ch_type.name for ch_type in rust_result.column_types] == [ch_type.name for ch_type in python_result.column_types]

    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    parity_queries = [
        "SELECT toIntervalDay(v) AS c FROM values('v Int64', (-13), (0), (79))",
        "SELECT toIntervalSecond(v) AS c FROM values('v Int64', (-13), (0), (79))",
        "SELECT toIntervalDay(v) AS c FROM values('v Nullable(Int64)', (-13), (NULL), (79))",
        "SELECT toIntervalNanosecond(v) AS c FROM values('v Nullable(Int64)', (-13), (NULL), (79))",
        "SELECT [toIntervalMinute(-13), toIntervalMinute(79)] AS c",
        "SELECT tuple(toIntervalSecond(-13), 'x') AS c",
        "SELECT [CAST(NULL AS Nullable(IntervalHour)), toIntervalHour(79)] AS c",
        "SELECT map(toIntervalDay(-13), 'x') AS c",
    ]
    for parity_query in parity_queries:
        rust_np = call(rust_client.query_np, parity_query)
        python_np = call(python_client.query_np, parity_query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)

        for use_extended_dtypes in (False, True):
            rust_df = call(rust_client.query_df, parity_query, use_extended_dtypes=use_extended_dtypes)
            python_df = call(python_client.query_df, parity_query, use_extended_dtypes=use_extended_dtypes)
            assert rust_df["c"].dtype == python_df["c"].dtype
            pd.testing.assert_frame_equal(rust_df, python_df)

    lc_query = "SELECT toLowCardinality(toIntervalHour(v)) AS c FROM values('v Int64', (13), (79), (13))"
    rust_df = call(rust_client.query_df, lc_query)
    python_df = call(python_client.query_df, lc_query)
    pd.testing.assert_frame_equal(rust_df, python_df)

    table = f"rc_ins_interval_{client_mode}"
    schema = (
        "id UInt32, d IntervalDay, n Nullable(IntervalHour), a Array(IntervalMinute), "
        "t Tuple(IntervalSecond, String), "
        "at Array(Tuple(IntervalMillisecond, IntervalMonth)), m Map(IntervalDay, String)"
    )
    rows = [
        [0, -13, None, [-13, 79], (-13, "x"), [(-13, 1), (79, -2)], {-13: "x"}],
        [1, 79, 13, [], (79, "y"), [], {}],
    ]
    try:
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")
        call(python_client.command, f"CREATE TABLE {table} ({schema}) ENGINE Memory")
        call(rust_client.insert, table, rows, column_names=["id", "d", "n", "a", "t", "at", "m"])

        expected = [tuple(row) for row in rows]
        assert call(rust_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows == expected
        assert call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows == expected
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {table}")


@pytest.mark.parametrize(
    "expr",
    [
        "CAST((CAST('000:00:13' AS Time), if(number = 0, NULL, toInt64(13))), 'Tuple(t Time, v Nullable(Int64))')",
        "CAST((CAST('000:00:13' AS Time), toDate(13)), 'Tuple(t Time, d Date)')",
    ],
    ids=["nullable_int", "date"],
)
def test_rust_codec_nested_time_sibling_df_parity(client_factory, call, test_config, expr):
    if test_config.cloud:
        pytest.skip("Time/Time64 settings are locked in ClickHouse Cloud")

    version_client = client_factory(native_codec="python")
    if not version_client.min_version("25.6"):
        pytest.skip("Time and Time64 require ClickHouse 25.6+")

    pd = pytest.importorskip("pandas")
    settings = {"enable_time_time64_type": 1}
    rust_client = client_factory(native_codec="rust_strict", settings=settings)
    python_client = client_factory(native_codec="python", settings=settings)
    query = f"SELECT {expr} AS c FROM numbers(2)"

    rust_df = call(rust_client.query_df, query, use_extended_dtypes=True)
    python_df = call(python_client.query_df, query, use_extended_dtypes=True)
    pd.testing.assert_frame_equal(rust_df, python_df)
    if "Nullable(Int64)" in expr:
        assert rust_df.iloc[0, 0]["v"] is pd.NA


@pytest.mark.parametrize("expr", DECODE_MATRIX.values(), ids=list(DECODE_MATRIX))
def test_rust_codec_decode_matrix_parity(client_factory, call, expr):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = f"SELECT {expr} AS c FROM numbers(13)"

    rust_result = call(rust_client.query, query)
    python_result = call(python_client.query, query)

    assert rust_result.result_rows == python_result.result_rows
    assert [t.name for t in rust_result.column_types] == [t.name for t in python_result.column_types]


def test_rust_codec_nothing_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = "id UInt8, tn Tuple(Nullable(Nothing), UInt8), at Array(Tuple(Nullable(Nothing), UInt8)), an Tuple(Array(Nothing), UInt8)"
    rows = [
        [0, (None, 13), [(None, 5), (None, 7)], ([], 79)],
        [1, (None, 79), [], ([], 13)],
    ]
    names = ["id", "tn", "at", "an"]
    rust_table = f"rc_ins_nothing_rust_{client_mode}"
    py_table = f"rc_ins_nothing_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE Memory")
        call(client.insert, table, rows, column_names=names)
        select = f"SELECT * FROM {table} ORDER BY id"
        assert call(rust_client.query, select).result_rows == call(python_client.query, select).result_rows
        return call(rust_client.query, select).result_rows

    try:
        expected = [tuple(row) for row in rows]
        assert roundtrip(rust_client, rust_table) == expected
        assert roundtrip(python_client, py_table) == expected
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_nothing_np_df_parity(client_factory, call):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")

    # Sibling scalars inside tuples/maps are the accepted value-equal residue
    # (rust python int vs numpy scalar); assert_array_equal and
    # assert_frame_equal compare by value.
    parity_queries = [
        "SELECT NULL AS c FROM numbers(3)",
        "SELECT [NULL] AS c FROM numbers(3)",
        "SELECT tuple(NULL, toUInt8(number)) AS c FROM numbers(3)",
        "SELECT map() AS c FROM numbers(3)",
        "SELECT mapFromArrays([toUInt8(number)], [NULL]) AS c FROM numbers(3)",
    ]
    for query in parity_queries:
        rust_np = call(rust_client.query_np, query)
        python_np = call(python_client.query_np, query)
        assert rust_np.dtype == python_np.dtype
        np.testing.assert_array_equal(rust_np, python_np)

        for use_extended_dtypes in (False, True):
            rust_df = call(rust_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            python_df = call(python_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            assert rust_df["c"].dtype == python_df["c"].dtype
            pd.testing.assert_frame_equal(rust_df, python_df)

    # Empty flat Array runs break query_np identically in both codecs, so these
    # shapes are df-only.
    df_only_queries = [
        "SELECT array() AS c FROM numbers(3)",
        "SELECT tuple([], toUInt8(number)) AS c FROM numbers(3)",
    ]
    for query in df_only_queries:
        for use_extended_dtypes in (False, True):
            rust_df = call(rust_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            python_df = call(python_client.query_df, query, use_extended_dtypes=use_extended_dtypes)
            assert rust_df["c"].dtype == python_df["c"].dtype
            pd.testing.assert_frame_equal(rust_df, python_df)


def test_rust_codec_streaming_parity(client_factory, call, consume_stream):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT number AS n, toString(number) AS s FROM numbers(5000)"

    def totals(client):
        block_rows = 0

        def on_block(block):
            nonlocal block_rows
            block_rows += len(block[0])

        consume_stream(call(client.query_column_block_stream, query, settings={"max_block_size": 1000}), on_block)

        row_count = 0

        def on_row(_row):
            nonlocal row_count
            row_count += 1

        consume_stream(call(client.query_rows_stream, query), on_row)
        return block_rows, row_count

    rust_totals = totals(rust_client)
    assert rust_totals == totals(python_client)
    assert rust_totals == (5000, 5000)


@pytest.mark.parametrize("native_codec", ["rust", "rust_strict"])
def test_rust_codec_aggregate_function_decode(client_factory, call, native_codec):
    client = client_factory(native_codec=native_codec)
    result = call(
        client.query,
        "SELECT countState() AS count_state, "
        "sumState(toUInt64(number)) AS sum_state, "
        "sumState(CAST(number, 'Nullable(UInt64)')) AS nullable_sum_state, "
        "sumState(CAST(NULL, 'Nullable(UInt64)')) AS empty_nullable_sum_state "
        "FROM numbers(13)",
    )

    assert result.result_rows == [
        (
            b"\x0d",
            (78).to_bytes(8, "little"),
            b"\x01" + (78).to_bytes(8, "little"),
            b"\x00",
        )
    ]
    assert [column_type.name for column_type in result.column_types] == [
        "AggregateFunction(count)",
        "AggregateFunction(sum, UInt64)",
        "AggregateFunction(sum, Nullable(UInt64))",
        "AggregateFunction(sum, Nullable(UInt64))",
    ]


def test_rust_codec_aggregate_function_streaming(client_factory, call, consume_stream):
    client = client_factory(native_codec="rust_strict")
    query = (
        "SELECT number, sumState(toUInt64(number)) AS state, "
        "sumState(CAST(number, 'Nullable(UInt64)')) AS nullable_state "
        "FROM numbers(3) GROUP BY number ORDER BY number"
    )
    expected = [
        (
            number,
            number.to_bytes(8, "little"),
            b"\x01" + number.to_bytes(8, "little"),
        )
        for number in range(3)
    ]
    rows = []
    consume_stream(call(client.query_rows_stream, query), rows.append)

    assert rows == expected


def test_rust_codec_aggregate_function_np_df(client_factory, call):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    client = client_factory(native_codec="rust_strict")
    query = (
        "SELECT number, sumState(toUInt64(number)) AS state, "
        "sumState(CAST(number, 'Nullable(UInt64)')) AS nullable_state "
        "FROM numbers(3) GROUP BY number ORDER BY number"
    )
    expected_states = [number.to_bytes(8, "little") for number in range(3)]
    expected_nullable_states = [b"\x01" + state for state in expected_states]

    array = call(client.query_np, query)
    assert array.dtype["state"] == np.dtype("O")
    assert array.dtype["nullable_state"] == np.dtype("O")
    assert array["state"].tolist() == expected_states
    assert array["nullable_state"].tolist() == expected_nullable_states

    frame = call(client.query_df, query)
    assert frame["state"].dtype == np.dtype("O")
    assert frame["nullable_state"].dtype == np.dtype("O")
    pd.testing.assert_series_equal(
        frame["state"],
        pd.Series(expected_states, name="state", dtype=object),
    )
    pd.testing.assert_series_equal(
        frame["nullable_state"],
        pd.Series(expected_nullable_states, name="nullable_state", dtype=object),
    )


@pytest.mark.parametrize("native_codec", ["rust", "rust_strict"])
def test_rust_codec_unsupported_aggregate_function_decode(client_factory, call, native_codec):
    client = client_factory(native_codec=native_codec)
    with pytest.raises(NotSupportedError):
        call(client.query, "SELECT avgState(number) AS agg FROM numbers(3)")


def test_rust_codec_nullable_tuple_decode(client_factory, call):
    # The python codec cannot parse Nullable(Tuple), so the rust path is the
    # reference here rather than a parity target.
    client = client_factory(native_codec="rust_strict")
    query = "SELECT if(number % 2 = 0, CAST((number, 'x'), 'Nullable(Tuple(UInt64, String))'), NULL) AS t FROM numbers(4)"
    result = call(client.query, query, settings={"enable_nullable_tuple_type": 1})
    assert result.result_rows == [((0, "x"),), (None,), ((2, "x"),), (None,)]


def test_rust_codec_nullable_tuple_aggregate_function_decode(client_factory, call):
    # Null rows carry server-written placeholder states under the null mask,
    # so boundary recovery must stay correct across the masked rows.
    client = client_factory(native_codec="rust_strict")
    query = "SELECT if(number % 2 = 0, tuple(initializeAggregation('countState', number)), NULL) AS t FROM numbers(4)"
    result = call(client.query, query, settings={"enable_nullable_tuple_type": 1})
    assert result.result_rows == [((b"\x01",),), (None,), ((b"\x01",),), (None,)]


def test_rust_codec_eligibility_routing(client_factory, call):
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust")
    strict_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    python_df = call(python_client.query_df, "SELECT number FROM numbers(10)")
    # numpy/pandas output is now served by the rust codec, so both rust and rust_strict route through it.
    pd.testing.assert_frame_equal(call(rust_client.query_df, "SELECT number FROM numbers(10)"), python_df)
    pd.testing.assert_frame_equal(call(strict_client.query_df, "SELECT number FROM numbers(10)"), python_df)

    # Timezone contexts remain a Python fallback until type/tz coverage lands.
    with pytest.raises(NotSupportedError):
        call(strict_client.query, "SELECT toDateTime(number) FROM numbers(3)", query_tz="America/New_York")


def test_rust_codec_midstream_error_parity(client_factory, call, consume_stream):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT number, throwIf(number = 100000) FROM numbers(200000)"

    def run(client):
        consume_stream(call(client.query_row_block_stream, query), lambda _block: None)

    with pytest.raises(StreamFailureError):
        run(rust_client)
    with pytest.raises(StreamFailureError):
        run(python_client)


def test_rust_codec_columns_only_limit_zero(client_factory, call):
    strict_client = client_factory(native_codec="rust_strict")

    result = call(strict_client.query, "SELECT number AS n, toString(number) AS s FROM numbers(10) LIMIT 0")

    assert result.column_names == ("n", "s")
    assert [t.name for t in result.column_types] == ["UInt64", "String"]
    assert result.result_rows == []


# numpy/pandas dtype parity matrix. Values chosen to exercise nulls, tz materialization, and dtype width.
NP_DF_MATRIX = {
    "int128": "toInt128(number) - toInt128('170141183460469231731687303715884105000')",
    "uint256": "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639000') + number",
    "int32": "toInt32(number) - 5",
    "uint64": "number",
    "float64": "toFloat64(number) / 2",
    "bool": "CAST(number % 2 AS Bool)",
    "string": "toString(number)",
    "fixed_string": "CAST(leftPad(toString(number), 6, '0') AS FixedString(6))",
    "date": "toDate(number)",
    "date32": "toDate32(number)",
    "datetime": "toDateTime(number)",
    "datetime_utc": "toDateTime(number, 'UTC')",
    "datetime_named_tz": "toDateTime(number, 'America/New_York')",
    "datetime64_3": "toDateTime64(number, 3)",
    "datetime64_6": "toDateTime64(number, 6)",
    "datetime64_9": "toDateTime64(number, 9)",
    "datetime64_named_tz": "toDateTime64(number, 3, 'America/New_York')",
    "nullable_int": "CAST(if(number % 3 = 0, NULL, toInt32(number)) AS Nullable(Int32))",
    "nullable_uint64": "CAST(if(number % 3 = 0, NULL, number) AS Nullable(UInt64))",
    "nullable_float": "CAST(if(number % 3 = 0, NULL, toFloat64(number) / 2) AS Nullable(Float64))",
    "nullable_string": "CAST(if(number % 3 = 0, NULL, toString(number)) AS Nullable(String))",
    "nullable_datetime": "CAST(if(number % 3 = 0, NULL, toDateTime(number)) AS Nullable(DateTime))",
    "nullable_datetime64": "CAST(if(number % 3 = 0, NULL, toDateTime64(number, 3)) AS Nullable(DateTime64(3)))",
    "low_card_string": "CAST(toString(number % 3) AS LowCardinality(String))",
    "low_card_nullable_string": "CAST(if(number % 2 = 0, NULL, toString(number)) AS LowCardinality(Nullable(String)))",
    "enum8": "CAST(if(number % 2 = 0, 'a', 'b') AS Enum8('a' = 1, 'b' = 2))",
    "enum16": "CAST(if(number % 2 = 0, 'x', 'y') AS Enum16('x' = 100, 'y' = 200))",
    "uuid": "toUUID(concat(leftPad(lower(hex(number)), 8, '0'), '-1122-3344-5566-778899aabbcc'))",
    "nullable_uuid": (
        "CAST(if(number % 3 = 0, NULL, toUUID(concat(leftPad(lower(hex(number)), 8, '0'), "
        "'-1122-3344-5566-778899aabbcc'))) AS Nullable(UUID))"
    ),
    "decimal64": "toDecimal64(number / 3 - 2, 4)",
    "decimal128": "toDecimal128(number / 3 - 2, 10)",
    "ipv4": "toIPv4(toUInt32(number * 16909060))",
    "ipv6": "toIPv6(concat('2001:db8::', lower(hex(toUInt16(number + 1)))))",
    # Non-null nested scalars stay python-native under rust (value-equal to python's numpy scalars).
    "array_int": "range(number % 4)",
    "array_string": "arrayMap(x -> toString(x), range(number % 4))",
    # Nested nulls are refinalized to match the python codec per leaf type (pd.NA, NaN, NaT, numpy scalars).
    "array_nullable_int": "arrayMap(x -> if(x % 2 = 0, NULL, toInt64(x)), range(number % 4))",
    "array_nullable_string": "arrayMap(x -> if(x % 2 = 0, NULL, toString(x)), range(number % 4))",
    "array_nullable_float": "arrayMap(x -> if(x % 2 = 0, NULL, toFloat64(x) / 2), range(number % 4))",
    "array_nullable_date": "arrayMap(x -> if(x % 2 = 0, NULL, toDate(x)), range(number % 4))",
    "array_nullable_datetime": "arrayMap(x -> if(x % 2 = 0, NULL, toDateTime(x, 'UTC')), range(number % 4))",
    "tuple_unnamed": "tuple(number, toString(number))",
    "tuple_named": "CAST((toInt64(number), toString(number)), 'Tuple(a Int64, b String)')",
    "tuple_nullable_int": "tuple(if(number % 2 = 0, NULL, toInt64(number)), toString(number))",
    "map_string_int": "mapFromArrays(arrayMap(x -> concat('k', toString(x)), range(number % 4)), range(number % 4))",
    "map_nullable_int": "CAST(map('k', if(number % 2 = 0, NULL, toInt64(number))), 'Map(String, Nullable(Int64))')",
    "array_tuple_nullable": "arrayMap(x -> tuple(if(x % 2 = 0, NULL, toInt64(x)), toString(x)), range(number % 4))",
    # Non-nullable and null-free temporal leaves are still rewrapped to numpy datetime64 to match python.
    "array_date_nonnull": "arrayMap(x -> toDate(x), range(number % 4))",
    "tuple_nullable_int_date": "CAST((if(number % 2 = 0, NULL, toInt64(number)), toDate(number)), 'Tuple(a Nullable(Int64), b Date)')",
    # SimpleAggregateFunction converts as its element type. Geo aliases take the object exit on both codecs.
    "simple_agg_uint64": "CAST(number AS SimpleAggregateFunction(sum, UInt64))",
    "simple_agg_string": "CAST(toString(number) AS SimpleAggregateFunction(anyLast, String))",
    "simple_agg_date": "CAST(toDate(number + 13) AS SimpleAggregateFunction(anyLast, Date))",
    "simple_agg_datetime": "CAST(toDateTime(number, 'UTC') AS SimpleAggregateFunction(anyLast, DateTime('UTC')))",
    "point": "(toFloat64(number), toFloat64(number) / 2)::Point",
    "ring": "[(toFloat64(number), toFloat64(number) / 2), (toFloat64(number) + 1, toFloat64(number))]::Ring",
}


@pytest.mark.parametrize("expr", NP_DF_MATRIX.values(), ids=list(NP_DF_MATRIX))
def test_rust_codec_np_df_parity(client_factory, call, expr):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = f"SELECT {expr} AS c FROM numbers(13)"

    rust_np = call(rust_client.query_np, query)
    python_np = call(python_client.query_np, query)
    assert rust_np.dtype == python_np.dtype
    np.testing.assert_array_equal(rust_np, python_np)

    rust_df = call(rust_client.query_df, query)
    python_df = call(python_client.query_df, query)
    assert rust_df["c"].dtype == python_df["c"].dtype
    pd.testing.assert_frame_equal(rust_df, python_df)


def test_rust_codec_bfloat16_parity(client_factory, call, client_mode):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    settings = {"allow_suspicious_low_cardinality_types": 1}
    rust_client = client_factory(native_codec="rust_strict", settings=settings)
    python_client = client_factory(native_codec="python", settings=settings)
    if not python_client.min_version("24.11"):
        pytest.skip("BFloat16 requires ClickHouse 24.11+")

    query = (
        "SELECT "
        "CAST(toFloat32(number) + 0.1 AS BFloat16) AS bf, "
        "CAST(if(number = 1, NULL, toFloat32(number) + 0.1) AS Nullable(BFloat16)) AS nbf, "
        "[CAST(toFloat32(number) + 0.1 AS BFloat16)] AS abf, "
        "tuple(CAST(toFloat32(number) + 0.1 AS BFloat16), toString(number)) AS tbf, "
        "[tuple(CAST(toFloat32(number) + 0.1 AS BFloat16), toUInt8(number))] AS atbf, "
        "CAST(toFloat32(number % 2) + 0.1 AS LowCardinality(BFloat16)) AS lcbf "
        "FROM numbers(3)"
    )
    rust_result = call(rust_client.query, query)
    python_result = call(python_client.query, query)
    assert rust_result.result_rows == python_result.result_rows
    assert [ch_type.name for ch_type in rust_result.column_types] == [ch_type.name for ch_type in python_result.column_types]

    numpy_query = (
        "SELECT "
        "CAST(toFloat32(number) + 0.1 AS BFloat16) AS bf, "
        "CAST(if(number = 1, NULL, toFloat32(number) + 0.1) AS Nullable(BFloat16)) AS nbf "
        "FROM numbers(3)"
    )
    rust_np = call(rust_client.query_np, numpy_query)
    python_np = call(python_client.query_np, numpy_query)
    assert rust_np.dtype == python_np.dtype
    np.testing.assert_equal(rust_np, python_np)

    for use_extended_dtypes in (False, True):
        rust_df = call(rust_client.query_df, numpy_query, use_extended_dtypes=use_extended_dtypes)
        python_df = call(python_client.query_df, numpy_query, use_extended_dtypes=use_extended_dtypes)
        pd.testing.assert_frame_equal(rust_df, python_df)

    # Nested nullable BFloat16 leaves densify to np.float32 with NaN in numpy output and pd.NA in
    # extended output. The Nullable(Float32) tuple sibling keeps python float/None in numpy output.
    nested_query = (
        "SELECT "
        "CAST(multiIf(number = 0, [toFloat32(1.5), NULL, toFloat32(-0.0)], number = 1, [NULL], number = 2, [], "
        "[toFloat32(nan), toFloat32(79)]), 'Array(Nullable(BFloat16))') AS anbf, "
        "CAST((if(number = 0, NULL, toFloat32(1.5)), if(number = 1, NULL, toFloat32(2.5))), "
        "'Tuple(Nullable(BFloat16), Nullable(Float32))') AS tnbf "
        "FROM numbers(4)"
    )

    def assert_nested_equal(rust_value, python_value):
        assert type(rust_value) is type(python_value)
        if isinstance(rust_value, (list, tuple)):
            assert len(rust_value) == len(python_value)
            for rust_item, python_item in zip(rust_value, python_value):
                assert_nested_equal(rust_item, python_item)
        elif rust_value is python_value:
            pass
        elif isinstance(rust_value, (float, np.floating)) and np.isnan(rust_value):
            assert np.isnan(python_value)
        else:
            assert rust_value == python_value

    rust_nested_np = call(rust_client.query_np, nested_query)
    python_nested_np = call(python_client.query_np, nested_query)
    assert_nested_equal(rust_nested_np.tolist(), python_nested_np.tolist())
    assert [type(leaf) for leaf in rust_nested_np[0, 0]] == [np.float32, np.float32, np.float32]
    assert np.isnan(rust_nested_np[0, 0][1])
    assert isinstance(rust_nested_np[0, 1][0], np.float32) and np.isnan(rust_nested_np[0, 1][0])
    assert rust_nested_np[1, 1][1] is None

    for use_extended_dtypes in (False, True):
        rust_df = call(rust_client.query_df, nested_query, use_extended_dtypes=use_extended_dtypes)
        python_df = call(python_client.query_df, nested_query, use_extended_dtypes=use_extended_dtypes)
        pd.testing.assert_frame_equal(rust_df, python_df)
        cell = rust_df["anbf"].iloc[0]
        if use_extended_dtypes:
            assert cell[1] is pd.NA
        else:
            assert np.isnan(cell[1])

    schema = (
        "id UInt8, bf BFloat16, nbf Nullable(BFloat16), abf Array(BFloat16), "
        "tbf Tuple(BFloat16, String), atbf Array(Tuple(BFloat16, UInt8)), "
        "lcbf LowCardinality(BFloat16)"
    )
    rows = [
        [0, 1.1, None, [1.1, -1.1], (1.1, "user_1"), [(1.1, 13)], 1.1],
        [1, -1.1, -1.1, [], (-1.1, "user_2"), [], -1.1],
        [2, 13.0, 79.0, [13.0], (13.0, "user_3"), [(13.0, 79)], 1.1],
    ]
    names = ["id", "bf", "nbf", "abf", "tbf", "atbf", "lcbf"]
    rust_table = f"rc_ins_bf16_rust_{client_mode}"
    python_table = f"rc_ins_bf16_python_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, python_table)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {python_table}")


@pytest.mark.parametrize(
    "expr",
    [
        "arrayMap(x -> if(x % 2 = 0, NULL, toDateTime(x, 'America/Denver')), range(number % 4))",
        "tuple(if(number % 2 = 0, NULL, toDateTime64(number, 3, 'America/Denver')), toString(number))",
    ],
    ids=["array_nullable_datetime_named_tz", "tuple_nullable_datetime64_named_tz"],
)
def test_rust_codec_nested_named_tz_df_parity(client_factory, call, expr):
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = f"SELECT {expr} AS c FROM numbers(13)"

    rust_df = call(rust_client.query_df, query, use_extended_dtypes=True)
    python_df = call(python_client.query_df, query, use_extended_dtypes=True)
    pd.testing.assert_frame_equal(rust_df, python_df)


def test_rust_codec_np_df_stream_parity(client_factory, call, consume_stream):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT number AS n, toString(number) AS s, toDateTime64(number, 3) AS dt FROM numbers(5000)"

    def np_blocks(client):
        blocks = []
        consume_stream(call(client.query_np_stream, query, settings={"max_block_size": 1000}), blocks.append)
        return blocks

    def df_blocks(client):
        parts = []
        consume_stream(call(client.query_df_stream, query, settings={"max_block_size": 1000}), parts.append)
        return parts

    rust_np, python_np = np_blocks(rust_client), np_blocks(python_client)
    assert len(rust_np) > 1  # max_block_size forced multiple blocks
    for name in ("n", "s", "dt"):
        np.testing.assert_array_equal(np.concatenate([b[name] for b in rust_np]), np.concatenate([b[name] for b in python_np]))

    rust_df = pd.concat(df_blocks(rust_client), ignore_index=True)
    python_df = pd.concat(df_blocks(python_client), ignore_index=True)
    pd.testing.assert_frame_equal(rust_df, python_df)


def test_rust_codec_empty_df_parity(client_factory, call):
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT number AS n, toString(number) AS s FROM numbers(0)"
    pd.testing.assert_frame_equal(call(rust_client.query_df, query), call(python_client.query_df, query))


def test_rust_codec_dt64_unsupported_precision_parity(client_factory, call):
    pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT toDateTime64(number, 1) AS c FROM numbers(3)"
    with pytest.raises(ProgrammingError):
        call(rust_client.query_df, query)
    with pytest.raises(ProgrammingError):
        call(python_client.query_df, query)


def test_rust_codec_uuid_df_parity(client_factory, call):
    pd = pytest.importorskip("pandas")
    python_client = client_factory(native_codec="python")
    query = "SELECT toUUID(concat(leftPad(lower(hex(number)), 8, '0'), '-1122-3344-5566-778899aabbcc')) AS u FROM numbers(3)"
    python_df = call(python_client.query_df, query)
    for codec in ("rust", "rust_strict"):
        client = client_factory(native_codec=codec)
        pd.testing.assert_frame_equal(call(client.query_df, query), python_df)


def test_rust_codec_abandoned_stream_no_read_ahead_thread(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    # The result must exceed the read-ahead queue capacity so the producer thread is still blocked at
    # abandonment. A small result the producer fully buffers would exit on its own and hide a close() leak.
    stream = call(rust_client.query_column_block_stream, "SELECT number FROM numbers(20000000)")

    if client_mode == "sync":
        with stream as blocks:
            for _ in blocks:
                break
    else:

        async def abandon():
            async with stream as blocks:
                async for _ in blocks:
                    break

        call(abandon)

    def read_ahead_threads():
        return [t for t in threading.enumerate() if t.name == "clickhouse-read-ahead" and t.is_alive()]

    deadline = time.time() + 2.0
    while time.time() < deadline and read_ahead_threads():
        time.sleep(0.05)
    assert not read_ahead_threads()


def _insert_df_roundtrip(client, python_client, call, table, schema, df):
    call(client.command, f"DROP TABLE IF EXISTS {table}")
    call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
    call(client.insert_df, table, df)
    return call(python_client.query_df, f"SELECT * FROM {table} ORDER BY id")


def test_rust_codec_insert_df_parity(client_factory, call, client_mode):
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = "id UInt32, i Int64, f Float64, b Bool, s String, dt DateTime"
    # Exercises _convert_pandas shapes: int list, float to_numpy array, np.bool_ array, string object array,
    # and the datetime "int" tick list injected post-init.
    df = pd.DataFrame(
        {
            "id": pd.Series([0, 1, 2], dtype="uint32"),
            "i": pd.Series([13, 79, -5], dtype="int64"),
            "f": pd.Series([1.5, 2.5, 3.5], dtype="float64"),
            "b": pd.Series([True, False, True], dtype="bool"),
            "s": pd.Series(["user_1", "user_2", "user_3"]),
            "dt": pd.to_datetime(["2020-01-01 00:00:13", "2021-06-15 12:00:00", "2022-12-31 23:59:59"]),
        }
    )
    rust_table = f"rc_ins_basic_rust_{client_mode}"
    py_table = f"rc_ins_basic_py_{client_mode}"
    try:
        rust_back = _insert_df_roundtrip(rust_client, python_client, call, rust_table, schema, df)
        python_back = _insert_df_roundtrip(python_client, python_client, call, py_table, schema, df)
        pd.testing.assert_frame_equal(rust_back, python_back)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_insert_df_nullable_parity(client_factory, call, client_mode):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = "id UInt32, ni Nullable(Int64), nf Nullable(Float64), ns Nullable(String), ndt Nullable(DateTime)"
    # Exercises nullable object arrays, NaN->None float lists, and NaT->None datetime tick lists.
    df = pd.DataFrame(
        {
            "id": pd.Series([0, 1, 2], dtype="uint32"),
            "ni": pd.array([13, None, 79], dtype="Int64"),
            "nf": pd.Series([1.5, np.nan, 3.5]),
            "ns": pd.Series(["user_1", None, "user_2"]),
            "ndt": pd.to_datetime(["2020-01-01 00:00:13", None, "2022-12-31 23:59:59"]),
        }
    )
    rust_table = f"rc_ins_nul_rust_{client_mode}"
    py_table = f"rc_ins_nul_py_{client_mode}"
    try:
        rust_back = _insert_df_roundtrip(rust_client, python_client, call, rust_table, schema, df)
        python_back = _insert_df_roundtrip(python_client, python_client, call, py_table, schema, df)
        pd.testing.assert_frame_equal(rust_back, python_back)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_wide_integer_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = (
        "id UInt8, i128 Int128, u128 UInt128, i256 Int256, u256 UInt256, "
        "ni Nullable(Int256), a Array(UInt128), t Tuple(Int128, UInt256), "
        "at Array(Tuple(Int256, UInt128)), m Map(UInt8, Int256)"
    )
    i128_min, i128_max = -(2**127), 2**127 - 1
    u128_max = 2**128 - 1
    i256_min, i256_max = -(2**255), 2**255 - 1
    u256_max = 2**256 - 1
    rows = [
        [0, i128_min, 0, i256_min, 0, None, [], (i128_min, u256_max), [], {}],
        [
            1,
            -1,
            2**127,
            -1,
            2**255,
            i256_max,
            [0, u128_max],
            (-1, 2**255),
            [(i256_min, 0), (-1, u128_max)],
            {1: i256_min, 2: -1},
        ],
        [
            2,
            i128_max,
            u128_max,
            i256_max,
            u256_max,
            i256_min,
            [79],
            (i128_max, 79),
            [(i256_max, 79)],
            {13: i256_max},
        ],
    ]
    names = ["id", "i128", "u128", "i256", "u256", "ni", "a", "t", "at", "m"]
    rust_table = f"rc_ins_wide_rust_{client_mode}"
    py_table = f"rc_ins_wide_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, py_table)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_wide_integer_string_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = (
        "id UInt8, i128 Int128, u128 UInt128, i256 Int256, u256 UInt256, "
        "ni Nullable(Int256), a Array(UInt128), t Tuple(Int128, UInt256), "
        "m Map(UInt128, Int256)"
    )
    rows = [
        [
            0,
            str(-13),
            str(2**128 - 1),
            str(-(2**255)),
            str(2**256 - 1),
            str(-79),
            [str(0), str(2**128 - 1)],
            (str(-1), str(2**256 - 1)),
            {str(2**127): str(-(2**255))},
        ],
        [1, str(13), str(0), str(79), str(0), None, [], (str(13), str(79)), {}],
    ]
    names = ["id", "i128", "u128", "i256", "u256", "ni", "a", "t", "m"]
    rust_table = f"rc_ins_wide_str_rust_{client_mode}"
    py_table = f"rc_ins_wide_str_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, py_table)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_tuple_map_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = (
        "id UInt32, tu Tuple(Int64, String), tn Tuple(a Int64, b Nullable(String)), "
        "ms Map(String, Int64), ma Map(String, Array(Int64)), at Array(Tuple(Int64, String))"
    )
    rows = [
        [0, (1, "x"), {"a": 5, "b": "named"}, {"k1": 1, "k2": 2}, {"m": [1, 2]}, [(1, "a"), (2, "b")]],
        [1, (2, "y"), {"a": 6}, {}, {"n": []}, []],
        [2, (3, "z"), {"a": 7, "b": None}, {"k": -1}, {"p": [3]}, [(3, "c")]],
    ]
    names = ["id", "tu", "tn", "ms", "ma", "at"]
    rust_table = f"rc_ins_nested_rust_{client_mode}"
    py_table = f"rc_ins_nested_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, py_table)
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_nullable_tuple_insert(client_factory, call, client_mode):
    # The python codec cannot insert Nullable(Tuple) at all, so the rust path is
    # the reference. Requires the true type name to reach the encoder because
    # Tuple.insert_name drops the Nullable wrapper.
    client = client_factory(native_codec="rust_strict")
    table = f"rc_ins_ntup_{client_mode}"
    call(client.command, f"DROP TABLE IF EXISTS {table}")
    try:
        call(
            client.command,
            f"CREATE TABLE {table} (id UInt32, t Nullable(Tuple(a Int64, b String))) ENGINE Memory",
            settings={"enable_nullable_tuple_type": 1},
        )
        call(client.insert, table, [[0, (1, "x")], [1, None], [2, (3, "z")]], column_names=["id", "t"])
        result = call(client.query, f"SELECT * FROM {table} ORDER BY id")
        assert result.result_rows == [(0, {"a": 1, "b": "x"}), (1, None), (2, {"a": 3, "b": "z"})]
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


def test_rust_codec_geo_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = "id UInt32, pt Point, rg Ring, ls LineString, pg Polygon, mls MultiLineString, mpg MultiPolygon"
    rows = [
        [
            0,
            (3.55, 3.55),
            [(5.522, 58.472), (3.55, 3.55)],
            [(1.0, 2.0), (3.0, 4.0)],
            [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]],
            [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]],
            [[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]],
        ],
        [1, (4.55, 4.55), [(4.55, 4.55)], [(7.0, 8.0)], [[(2.0, 2.0)]], [[(9.0, 9.0)]], [[[(5.0, 5.0)]]]],
    ]
    names = ["id", "pt", "rg", "ls", "pg", "mls", "mpg"]
    rust_table = f"rc_ins_geo_rust_{client_mode}"
    py_table = f"rc_ins_geo_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, py_table) == [tuple(r) for r in rows]
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_nested_insert_parity(client_factory, call, client_mode):
    # A single Nested(...) typed column only appears with flatten_nested=0; the
    # default splits it into sibling Array columns. Nested reads as a list of
    # dicts keyed by the field names in both codecs.
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = "id UInt32, n Nested(sku String, qty UInt32)"
    rows = [
        [0, []],
        [1, [{"sku": "sku_1", "qty": 5}, {"sku": "sku_2", "qty": 77}]],
        [2, [{"sku": "sku_3", "qty": 13}]],
    ]
    expected = [
        (0, []),
        (1, [{"sku": "sku_1", "qty": 5}, {"sku": "sku_2", "qty": 77}]),
        (2, [{"sku": "sku_3", "qty": 13}]),
    ]
    rust_table = f"rc_ins_nested_col_rust_{client_mode}"
    py_table = f"rc_ins_nested_col_py_{client_mode}"

    def roundtrip(client, read_client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id", settings={"flatten_nested": 0})
        call(client.insert, table, rows, column_names=["id", "n"])
        return call(read_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        # rust->rust exercises encode and decode end to end; python->python is the reference.
        assert roundtrip(rust_client, rust_client, rust_table) == expected
        assert roundtrip(python_client, python_client, py_table) == expected
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_simple_agg_insert_parity(client_factory, call, client_mode):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    schema = (
        "id UInt32, s SimpleAggregateFunction(anyLast, String), "
        "lc SimpleAggregateFunction(anyLast, LowCardinality(String)), "
        "n SimpleAggregateFunction(sum, UInt64), f SimpleAggregateFunction(max, Float64), "
        "arr SimpleAggregateFunction(groupArrayArray, Array(UInt64))"
    )
    rows = [
        [0, "first", "lc_1", 100, 3.5, [1, 2, 3]],
        [1, "second", "lc_2", 79, -1.5, []],
    ]
    names = ["id", "s", "lc", "n", "f", "arr"]
    rust_table = f"rc_ins_saf_rust_{client_mode}"
    py_table = f"rc_ins_saf_py_{client_mode}"

    def roundtrip(client, table):
        call(client.command, f"DROP TABLE IF EXISTS {table}")
        call(client.command, f"CREATE TABLE {table} ({schema}) ENGINE MergeTree ORDER BY id")
        call(client.insert, table, rows, column_names=names)
        return call(python_client.query, f"SELECT * FROM {table} ORDER BY id").result_rows

    try:
        assert roundtrip(rust_client, rust_table) == roundtrip(python_client, py_table) == [tuple(r) for r in rows]
    finally:
        call(python_client.command, f"DROP TABLE IF EXISTS {rust_table}")
        call(python_client.command, f"DROP TABLE IF EXISTS {py_table}")


def test_rust_codec_aggregate_function_insert(client_factory, call, client_mode):
    client = client_factory(native_codec="rust_strict")
    table = f"rc_ins_aggregate_function_{client_mode}"
    rows = [
        [0, b"\x00", (13).to_bytes(8, "little"), b"\x00"],
        [
            1,
            b"\x0d",
            (79).to_bytes(8, "little"),
            b"\x01" + (-13).to_bytes(8, "little", signed=True),
        ],
        [
            2,
            b"\x80\x01",
            (258).to_bytes(8, "little"),
            b"\x80" + (79).to_bytes(8, "little"),
        ],
    ]
    call(client.command, f"DROP TABLE IF EXISTS {table}")
    try:
        call(
            client.command,
            f"CREATE TABLE {table} ("
            "id UInt8, count_state AggregateFunction(count), "
            "sum_state AggregateFunction(sum, UInt64), "
            "nullable_sum_state AggregateFunction(sum, Nullable(Int32))) ENGINE Memory",
        )
        call(
            client.insert,
            table,
            rows,
            column_names=["id", "count_state", "sum_state", "nullable_sum_state"],
        )
        result = call(
            client.query,
            f"SELECT id, finalizeAggregation(count_state), finalizeAggregation(sum_state), "
            f"finalizeAggregation(nullable_sum_state) FROM {table} ORDER BY id",
        )
        assert result.result_rows == [
            (0, 0, 13, None),
            (1, 13, 79, -13),
            (2, 128, 258, 79),
        ]
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


def test_rust_codec_aggregate_function_insert_failure(client_factory, call, client_mode):
    client = client_factory(native_codec="rust_strict")
    table = f"rc_ins_aggregate_function_bad_{client_mode}"
    call(client.command, f"DROP TABLE IF EXISTS {table}")
    try:
        call(
            client.command,
            f"CREATE TABLE {table} (s AggregateFunction(sum, UInt64)) ENGINE Memory",
        )
        with pytest.raises(DataError, match="not exactly one valid serialized"):
            call(client.insert, table, [[b"\x00" * 7]], column_names=["s"])
        assert call(client.query, f"SELECT count() FROM {table}").result_rows == [(0,)]
    finally:
        call(client.command, f"DROP TABLE IF EXISTS {table}")


def test_rust_codec_midstream_error_df_parity(client_factory, call, consume_stream):
    pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = "SELECT number, throwIf(number = 100000) FROM numbers(200000)"

    def run(client):
        consume_stream(call(client.query_df_stream, query), lambda _df: None)

    with pytest.raises(StreamFailureError):
        run(rust_client)
    with pytest.raises(StreamFailureError):
        run(python_client)
