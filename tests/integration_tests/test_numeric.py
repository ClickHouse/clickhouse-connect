from collections.abc import Callable
from decimal import Decimal

import pytest


def test_bfloat16_round_trip(param_client, call, table_context: Callable):
    """Test BFloat16 data type with precision loss on round trip."""
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

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
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

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
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

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
    """Test that interval type selects work correctly."""
    result = call(param_client.query, "SELECT INTERVAL 30 DAY")
    assert result.result_rows[0][0] == 30


def test_decimal_query_parameter_precision(param_client, call, table_context: Callable):
    """A high-precision Decimal query parameter must not lose precision through Float64.

    A Decimal passed via %s substitution used to be inlined as a bare numeric literal, which
    the server parsed as Float64. For Decimal(38, 10) values around 20 digits this silently
    collapsed distinct values to the same float, so a strict comparison dropped matching rows.
    """
    stored = "12345678901234567001.1234567890"
    threshold = Decimal("12345678901234567008.1234567890")
    with table_context("decimal_param_precision", ["col_decimal Decimal(38, 10)"], order_by="tuple()"):
        call(param_client.command, f"INSERT INTO decimal_param_precision VALUES (toDecimal128('{stored}', 10))")

        result = call(
            param_client.query,
            "SELECT col_decimal FROM decimal_param_precision WHERE col_decimal < %s",
            parameters=[threshold],
        )

        assert result.row_count == 1
        assert result.result_rows[0][0] == Decimal(stored)
