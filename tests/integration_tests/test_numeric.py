from typing import Callable
import pytest


def test_bfloat16_round_trip(param_client, call, table_context: Callable):
    """Test BFloat16 data type with precision loss on round trip."""
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

    with table_context('bf16_test', ['id UInt32', 'bfloat16 BFloat16', 'bfloat16_nullable Nullable(BFloat16)'],
                       order_by='id'):
        input_data = [[0, 3.141592, -2.71828], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, -2.703125], [1, 3.140625, -2.703125]]
        call(param_client.insert, 'bf16_test', input_data)

        result = call(param_client.query, "SELECT * FROM bf16_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)

def test_bfloat16_nullable_round_trip(param_client, call, table_context: Callable):
    """Test BFloat16 nullable column with precision loss."""
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

    with table_context('bf16_nullable_test', ['id UInt32', 'bfloat16 BFloat16', 'bfloat16_nullable Nullable(BFloat16)'],
                       order_by='id'):
        input_data = [[0, 3.141592, None], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, None], [1, 3.140625, -2.703125]]
        call(param_client.insert, 'bf16_nullable_test', input_data)

        result = call(param_client.query, "SELECT * FROM bf16_nullable_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)

def test_bfloat16_empty_and_all_null_inserts(param_client, call, table_context: Callable):
    """Test BFloat16 with empty inserts and all-null columns."""
    if not param_client.min_version("24.11"):
        pytest.skip(f"BFloat16 type not supported in ClickHouse version {param_client.server_version}")

    with table_context('bf16_empty_test', ['id UInt32', 'bfloat16 BFloat16', 'bfloat16_nullable Nullable(BFloat16)'],
                       order_by='id'):
        # Test empty insert
        call(param_client.insert, 'bf16_empty_test', [])
        result = call(param_client.query, "SELECT count() FROM bf16_empty_test")
        assert result.result_rows[0][0] == 0

        input_data = [[0, 3.141592, None], [1, -2.71828, None]]
        expected = [[0, 3.140625, None], [1, -2.703125, None]]
        call(param_client.insert, 'bf16_empty_test', input_data)

        result = call(param_client.query, "SELECT * FROM bf16_empty_test ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row


def test_interval_selects(param_client, call):
    """Test that interval type selects work correctly."""
    result = call(param_client.query, "SELECT INTERVAL 30 DAY")
    assert result.result_rows[0][0] == 30
