import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import StreamFailureError


def test_mid_stream_exception(test_client: Client):
    """Test that mid-stream exceptions are properly detected and raised.

    This test works with both old (pre-25.11) and new (25.11+) ClickHouse servers:
    - Old servers: Exception text sent in response body, detected via fallback path
    - New servers: Exception sent with X-ClickHouse-Exception-Tag header and structured format
    """
    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"

    # We expect a StreamFailureError, which is a subclass of DatabaseError
    # specifically for errors that happen after the response status code is 200.
    # We force max_block_size=1 to ensure data is sent in small chunks
    # and wait_end_of_query=0 to ensure headers are sent immediately
    with pytest.raises(StreamFailureError) as exc_info:
        result = test_client.query(query, settings={"max_block_size": 1, "wait_end_of_query": 0})
        _ = result.result_set

    error_msg = str(exc_info.value)
    assert "Value passed to 'throwIf' function is non-zero" in error_msg
    assert test_client.command("SELECT 1") == 1


def test_mid_stream_exception_streaming(test_client: Client):
    """Test that mid-stream exceptions are properly detected in streaming mode.

    Works with both old and new server versions (see test_mid_stream_exception).
    """
    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"

    with pytest.raises(StreamFailureError) as exc_info:
        with test_client.query_rows_stream(query, settings={"max_block_size": 1, "wait_end_of_query": 0}) as stream:
            for _ in stream:
                pass

    error_msg = str(exc_info.value)
    assert "Value passed to 'throwIf' function is non-zero" in error_msg
    assert test_client.command("SELECT 1") == 1
