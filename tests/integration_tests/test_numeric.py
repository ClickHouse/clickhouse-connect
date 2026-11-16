from collections.abc import Callable
from decimal import Decimal

import pytest


def test_bfloat16_round_trip(param_client, call, table_context: Callable):
    """Test BFloat16 data type with precision loss on round trip."""
    with table_context("bf16_test", ["id UInt32", "bfloat16 BFloat16", "bfloat16_nullable Nullable(BFloat16)"], order_by="id"):
        input_data = [[0, 3.141592, -2.71828], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, -2.703125], [1, 3.140625, -2.703125]]
        call(param_client.insert, "bf16_test", input_data)

        result = call(param_client.query, "SELECT * FROM bf16_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)


def test_bfloat16_nullable_round_trip(param_client, call, table_context: Callable):
    """Test BFloat16 nullable column with precision loss."""
    with table_context("bf16_nullable_test", ["id UInt32", "bfloat16 BFloat16", "bfloat16_nullable Nullable(BFloat16)"], order_by="id"):
        input_data = [[0, 3.141592, None], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, None], [1, 3.140625, -2.703125]]
        call(param_client.insert, "bf16_nullable_test", input_data)

        result = call(param_client.query, "SELECT * FROM bf16_nullable_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)


def test_bfloat16_empty_and_all_null_inserts(param_client, call, table_context: Callable):
    """Test BFloat16 with empty inserts and all-null columns."""
    with table_context("bf16_empty_test", ["id UInt32", "bfloat16 BFloat16", "bfloat16_nullable Nullable(BFloat16)"], order_by="id"):
        # Test empty insert
        call(param_client.insert, "bf16_empty_test", [])
        result = call(param_client.query, "SELECT count() FROM bf16_empty_test")
        assert result.result_rows[0][0] == 0

        input_data = [[0, 3.141592, None], [1, -2.71828, None]]
        expected = [[0, 3.140625, None], [1, -2.703125, None]]
        call(param_client.insert, "bf16_empty_test", input_data)

        result = call(param_client.query, "SELECT * FROM bf16_empty_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row


def test_interval_selects(param_client, call):
    """Interval columns use signed Int64 bodies without corrupting following columns."""
    result = call(
        param_client.query,
        "SELECT toIntervalYear(-13), toIntervalQuarter(79), toIntervalMonth(-13), "
        "toIntervalWeek(79), toIntervalDay(-13), toIntervalHour(79), "
        "toIntervalMinute(-13), toIntervalSecond(79), toIntervalMillisecond(-13), "
        "toIntervalMicrosecond(79), toIntervalNanosecond(-13), 'sentinel'",
    )
    assert result.result_rows == [(-13, 79, -13, 79, -13, 79, -13, 79, -13, 79, -13, "sentinel")]


def test_interval_dataframe(param_client, call):
    """Nullable interval columns finalize to pandas Int64 instead of raising on the type name."""
    pd = pytest.importorskip("pandas")
    df = call(
        param_client.query_df,
        "SELECT toIntervalDay(v) AS c FROM values('v Nullable(Int64)', (-13), (NULL), (79))",
    )
    assert str(df["c"].dtype) == "Int64"
    assert list(df["c"]) == [-13, pd.NA, 79]

    nested = call(
        param_client.query_df,
        "SELECT [CAST(NULL AS Nullable(IntervalHour)), toIntervalHour(79)] AS c",
    )
    assert list(nested["c"][0]) == [pd.NA, 79]


@pytest.mark.parametrize(
    "type_name, value, expected",
    [
        ("Decimal(9, 2)", Decimal("9999999.999"), Decimal("9999999.99")),
        ("Decimal32(2)", "1234567.899", Decimal("1234567.89")),
        ("Decimal64(2)", "1234567890123456.899", Decimal("1234567890123456.89")),
        ("Decimal128(2)", "1" * 36 + ".899", Decimal("1" * 36 + ".89")),
        ("Decimal256(2)", "-" + "1" * 74 + ".899", Decimal("-" + "1" * 74 + ".89")),
        ("Nullable(Decimal(9, 2))", None, None),
        ("Nullable(Decimal128(2))", "1" * 36 + ".899", Decimal("1" * 36 + ".89")),
        ("Nullable(Decimal256(2))", "1" * 74 + ".899", Decimal("1" * 74 + ".89")),
        ("Array(Decimal(9, 2))", [9999999.999, -1234567.899], [Decimal("9999999.99"), Decimal("-1234567.89")]),
        ("Tuple(Decimal(9, 2))", (Decimal("1234567.899"),), (Decimal("1234567.89"),)),
        ("Array(Tuple(Decimal(9, 2)))", [("1234567.899",)], [(Decimal("1234567.89"),)]),
    ],
)
def test_decimal_truncates_extra_digits(param_client, call, table_context: Callable, type_name, value, expected):
    with table_context("decimal_truncation", [f"value {type_name}"], order_by="tuple()"):
        call(param_client.insert, "decimal_truncation", [[value]])
        assert call(param_client.query, "SELECT value FROM decimal_truncation").result_rows == [(expected,)]
