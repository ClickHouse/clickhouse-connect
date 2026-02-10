import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import StreamFailureError


def test_mid_stream_exception(test_client: Client):
    """Test that mid-stream exceptions are properly detected and raised."""
    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"

    with pytest.raises(StreamFailureError) as exc_info:
        result = test_client.query(query, settings={"max_block_size": 1, "wait_end_of_query": 0})
        _ = result.result_set

    error_msg = str(exc_info.value)
    assert "Value passed to 'throwIf' function is non-zero" in error_msg
    assert test_client.command("SELECT 1") == 1


def test_mid_stream_exception_streaming(test_client: Client):
    """Test that mid-stream exceptions are properly detected in streaming mode."""
    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"

    with pytest.raises(StreamFailureError) as exc_info:
        with test_client.query_rows_stream(query, settings={"max_block_size": 1, "wait_end_of_query": 0}) as stream:
            for _ in stream:
                pass

    error_msg = str(exc_info.value)
    assert "Value passed to 'throwIf' function is non-zero" in error_msg
    assert test_client.command("SELECT 1") == 1
