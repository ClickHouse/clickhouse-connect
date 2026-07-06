import pytest

from clickhouse_connect.driver.exceptions import NotSupportedError, StreamFailureError

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
        call(client.query, "SELECT [1, 2, 3] AS arr")


def test_rust_codec_eligibility_routing(client_factory, call):
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust")
    strict_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    rust_df = call(rust_client.query_df, "SELECT number FROM numbers(10)")
    python_df = call(python_client.query_df, "SELECT number FROM numbers(10)")
    pd.testing.assert_frame_equal(rust_df, python_df)

    with pytest.raises(NotSupportedError):
        call(strict_client.query_df, "SELECT number FROM numbers(10)")
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
