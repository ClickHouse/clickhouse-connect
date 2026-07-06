from collections.abc import Callable
from datetime import date
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address
from uuid import UUID

import pytest

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DataError


def test_insert(param_client: Client, call, test_table_engine: str):
    if param_client.min_version("19"):
        call(param_client.command, "DROP TABLE IF EXISTS test_system_insert")
    else:
        call(param_client.command, "DROP TABLE IF EXISTS test_system_insert SYNC")
    call(param_client.command, f"CREATE TABLE test_system_insert AS system.tables Engine {test_table_engine} ORDER BY name")
    tables_result = call(param_client.query, "SELECT * from system.tables")
    call(param_client.insert, table="test_system_insert", column_names="*", data=tables_result.result_set)
    copy_result = call(param_client.command, "SELECT count() from test_system_insert")
    assert tables_result.row_count == copy_result
    call(param_client.command, "DROP TABLE IF EXISTS test_system_insert")


def test_decimal_conv(param_client: Client, call, table_context: Callable):
    with table_context("test_num_conv", ["col1 UInt64", "col2 Int32", "f1 Float64"]):
        data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
        call(param_client.insert, "test_num_conv", data)
        result = call(param_client.query, "SELECT * FROM test_num_conv").result_set
        assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]


def test_float_decimal_conv(param_client: Client, call, table_context: Callable):
    with table_context("test_float_to_dec_conv", ["col1 Decimal32(6)", "col2 Decimal32(6)", "col3 Decimal128(6)", "col4 Decimal128(6)"]):
        data = [[0.492917, 0.49291700, 0.492917, 0.49291700]]
        call(param_client.insert, "test_float_to_dec_conv", data)
        result = call(param_client.query, "SELECT * FROM test_float_to_dec_conv").result_set
        assert result == [(Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"))]


def test_rust_codec_insert(client_factory, call, table_context: Callable):
    pytest.importorskip("_ch_core")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    columns = [
        "id UInt32",
        "b Bool",
        "i Int32",
        "u UInt64",
        "f Float64",
        "s String",
        "fs FixedString(4)",
        "n Nullable(Int32)",
        "d Date",
        "dt DateTime",
        "ts DateTime64(3)",
        "e Enum8('red' = 1, 'green' = 2)",
        "dec Decimal(18, 4)",
        "uuid_col UUID",
        "ip4 IPv4",
        "ip6 IPv6",
        "lc LowCardinality(String)",
        "lcn LowCardinality(Nullable(String))",
    ]
    data = [
        [
            1,
            True,
            -13,
            79,
            1.25,
            "user_1",
            "abcd",
            13,
            date(2024, 1, 15),
            1705322096,
            1705322096789,
            "red",
            Decimal("123.4567"),
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            "192.0.2.1",
            IPv6Address("2001:db8::1"),
            "x",
            "nx",
        ],
        [
            2,
            False,
            -79,
            500,
            -2.5,
            "user_2",
            "xy",
            None,
            19738,
            1705322097,
            1705322097790,
            2,
            "-1.5",
            "11111111-2222-3333-4444-555555555555",
            IPv4Address("198.51.100.7"),
            "2001:db8::2",
            "x",
            None,
        ],
    ]
    expected = [
        (
            1,
            True,
            -13,
            79,
            1.25,
            "user_1",
            b"abcd",
            13,
            19737,
            1705322096,
            1705322096789,
            "red",
            Decimal("123.4567"),
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            IPv4Address("192.0.2.1"),
            IPv6Address("2001:db8::1"),
            "x",
            "nx",
        ),
        (
            2,
            False,
            -79,
            500,
            -2.5,
            "user_2",
            b"xy\x00\x00",
            None,
            19738,
            1705322097,
            1705322097790,
            "green",
            Decimal("-1.5000"),
            UUID("11111111-2222-3333-4444-555555555555"),
            IPv4Address("198.51.100.7"),
            IPv6Address("2001:db8::2"),
            "x",
            None,
        ),
    ]

    with table_context("test_rust_native_insert", columns):
        call(
            rust_client.insert,
            "test_rust_native_insert",
            data,
        )
        result = call(
            python_client.query,
            "SELECT * FROM test_rust_native_insert ORDER BY id",
            query_formats={"Date": "int", "DateTime": "int", "DateTime64": "int"},
        ).result_rows
        assert result == expected


def test_rust_codec_insert_dataframe(client_factory, call, table_context: Callable):
    pytest.importorskip("_ch_core")
    pd = pytest.importorskip("pandas")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    data = pd.DataFrame(
        {
            "id": [13, 79],
            "name": ["user_1", "user_2"],
            "score": [Decimal("12.30"), Decimal("45.60")],
        }
    )

    with table_context("test_rust_native_insert_df", ["id Int32", "name String", "score Decimal(9, 2)"]):
        call(
            rust_client.insert_df,
            "test_rust_native_insert_df",
            data,
        )
        result = call(python_client.query, "SELECT * FROM test_rust_native_insert_df ORDER BY id").result_rows
        assert result == [(13, "user_1", Decimal("12.30")), (79, "user_2", Decimal("45.60"))]


def test_rust_codec_insert_numpy(client_factory, call, table_context: Callable):
    pytest.importorskip("_ch_core")
    np = pytest.importorskip("numpy")
    rust_client = client_factory(native_codec="rust_strict")
    python_client = client_factory(native_codec="python")

    data = np.array([(13, 1.25), (79, 2.5)], dtype=[("id", "<i4"), ("value", "<f8")])

    with table_context("test_rust_native_insert_numpy", ["id Int32", "value Float64"]):
        call(
            rust_client.insert,
            "test_rust_native_insert_numpy",
            data,
            column_names=["id", "value"],
        )
        result = call(python_client.query, "SELECT * FROM test_rust_native_insert_numpy ORDER BY id").result_rows
        assert result == [(13, 1.25), (79, 2.5)]


def test_bad_data_insert(param_client: Client, call, table_context: Callable):
    with table_context("test_bad_insert", ["key Int32", "float_col Float64"]):
        data = [[1, 3.22], [2, "nope"]]
        with pytest.raises(DataError, match="array"):
            call(param_client.insert, "test_bad_insert", data)


def test_bad_strings(param_client: Client, call, table_context: Callable):
    with table_context("test_bad_strings", "key Int32, fs FixedString(6), nsf Nullable(FixedString(4))"):
        try:
            call(param_client.insert, "test_bad_strings", [[1, b"\x0535", None]])
        except DataError as ex:
            assert "match" in str(ex)
        try:
            call(param_client.insert, "test_bad_strings", [[1, b"\x0535abc", "😀🙃"]])
        except DataError as ex:
            assert "encoded" in str(ex)


def test_low_card_dictionary_size(param_client: Client, call, table_context: Callable):
    with table_context("test_low_card_dict", "key Int32, lc LowCardinality(String)", settings={"index_granularity": 65536}):
        data = [[x, str(x)] for x in range(30000)]
        call(param_client.insert, "test_low_card_dict", data)
        assert 30000 == call(param_client.command, "SELECT count() FROM test_low_card_dict")


def test_column_names_spaces(param_client: Client, call, table_context: Callable):
    with table_context("test_column_spaces", columns=["key 1", "value 1"], column_types=["Int32", "String"]):
        data = [[1, "str 1"], [2, "str 2"]]
        call(param_client.insert, "test_column_spaces", data)
        result = call(param_client.query, "SELECT * FROM test_column_spaces").result_rows
        assert result[0][0] == 1
        assert result[1][1] == "str 2"


def test_numeric_conversion(param_client: Client, call, table_context: Callable):
    with table_context("test_numeric_convert", columns=["key Int32", "n_int Nullable(UInt64)", "n_flt Nullable(Float64)"]):
        data = [[1, None, None], [2, "2", "5.32"]]
        call(param_client.insert, "test_numeric_convert", data)
        result = call(param_client.query, "SELECT * FROM test_numeric_convert").result_rows
        assert result[1][1] == 2
        assert result[1][2] == float("5.32")
        call(param_client.command, "TRUNCATE TABLE test_numeric_convert")
        data = [[0, "55", "532.48"], [1, None, None], [2, "2", "5.32"]]
        call(param_client.insert, "test_numeric_convert", data)
        result = call(param_client.query, "SELECT * FROM test_numeric_convert").result_rows
        assert result[0][1] == 55
        assert result[0][2] == 532.48
        assert result[1][1] is None
        assert result[2][1] == 2
        assert result[2][2] == 5.32


def test_insert_table_name_with_unescaped_inner_backtick(param_client: Client, call, test_table_engine: str):
    # A table name wrapped in backticks but containing unescaped inner backticks must be re-escaped.
    raw_table = "`quote`insert`"
    quoted_table = "`\\`quote\\`insert\\``"
    call(param_client.command, f"DROP TABLE IF EXISTS {quoted_table}")
    try:
        call(param_client.command, f"CREATE TABLE {quoted_table} (id UInt32) ENGINE {test_table_engine} ORDER BY id")
        call(param_client.insert, raw_table, [[13]], column_names=["id"])
        assert call(param_client.command, f"SELECT count() FROM {quoted_table}") == 1
    finally:
        call(param_client.command, f"DROP TABLE IF EXISTS {quoted_table}")
