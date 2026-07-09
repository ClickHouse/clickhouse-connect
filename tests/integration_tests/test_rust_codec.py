import threading
import time

import pytest

from clickhouse_connect.driver.exceptions import NotSupportedError, ProgrammingError, StreamFailureError

pytest.importorskip("_ch_core")

# These tests require a UTC-configured server. On a non-UTC server the eligibility gate reports
# "ambient timezone"/"server timezone header" and rust_strict raises instead of exercising the codec.

SCALAR_QUERY = (
    "SELECT number AS n, toString(number) AS s, toDateTime(number) AS dt, "
    "toDate(number) AS d, toInt32(number) - 5 AS i, CAST(number AS Bool) AS b FROM numbers(100)"
)

# Decoder-supported type shapes, including the higher-risk timezone-materialization paths.
DECODE_MATRIX = {
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
}


def test_rust_codec_ab_parity(client_factory, call):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    rust_result = call(rust_client.query, SCALAR_QUERY)
    python_result = call(python_client.query, SCALAR_QUERY)

    assert rust_result.result_rows == python_result.result_rows
    assert rust_result.column_names == python_result.column_names
    assert [t.name for t in rust_result.column_types] == [t.name for t in python_result.column_types]


@pytest.mark.parametrize("expr", DECODE_MATRIX.values(), ids=list(DECODE_MATRIX))
def test_rust_codec_decode_matrix_parity(client_factory, call, expr):
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")
    query = f"SELECT {expr} AS c FROM numbers(13)"

    rust_result = call(rust_client.query, query)
    python_result = call(python_client.query, query)

    assert rust_result.result_rows == python_result.result_rows
    assert [t.name for t in rust_result.column_types] == [t.name for t in python_result.column_types]


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
def test_rust_codec_unsupported_decode(client_factory, call, native_codec):
    client = client_factory(native_codec=native_codec)
    with pytest.raises(NotSupportedError):
        call(client.query, "SELECT (1., 2.)::Point AS p")


def test_rust_codec_nullable_tuple_decode(client_factory, call):
    # The python codec cannot parse Nullable(Tuple), so the rust path is the
    # reference here rather than a parity target.
    client = client_factory(native_codec="rust_strict")
    query = "SELECT if(number % 2 = 0, CAST((number, 'x'), 'Nullable(Tuple(UInt64, String))'), NULL) AS t FROM numbers(4)"
    result = call(client.query, query, settings={"enable_nullable_tuple_type": 1})
    assert result.result_rows == [((0, "x"),), (None,), ((2, "x"),), (None,)]


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
    # Array cells compare by value; element scalars are python-native under rust vs
    # numpy scalars under python (documented df-parity gap pending a decision).
    "array_int": "range(number % 4)",
    "array_string": "arrayMap(x -> toString(x), range(number % 4))",
    "array_nullable_int": "arrayMap(x -> if(x % 2 = 0, NULL, toInt64(x)), range(number % 4))",
    # Tuple/Map cells share the same element-scalar gap as Array.
    "tuple_unnamed": "tuple(number, toString(number))",
    "tuple_named": "CAST((toInt64(number), toString(number)), 'Tuple(a Int64, b String)')",
    "map_string_int": "mapFromArrays(arrayMap(x -> concat('k', toString(x)), range(number % 4)), range(number % 4))",
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
