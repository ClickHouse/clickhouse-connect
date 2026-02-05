import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import StreamFailureError
from clickhouse_connect.driver.httpclient import HttpClient, ex_tag_header


def test_mid_stream_exception(test_client: Client):
    """Test that mid-stream exceptions are properly detected and raised."""
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
    """Test that mid-stream exceptions are properly detected in streaming mode."""
    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"

    with pytest.raises(StreamFailureError) as exc_info:
        with test_client.query_rows_stream(query, settings={"max_block_size": 1, "wait_end_of_query": 0}) as stream:
            for _ in stream:
                pass

    error_msg = str(exc_info.value)
    assert "Value passed to 'throwIf' function is non-zero" in error_msg
    assert test_client.command("SELECT 1") == 1


# pylint: disable=protected-access
def test_new_exception_format_on_25_11_plus(test_client: HttpClient):
    """Test that the new exception format is used on ClickHouse 25.11+."""
    if not test_client.min_version("25.11"):
        pytest.skip("Test requires ClickHouse 25.11+ for new exception format")

    query = "SELECT sleepEachRow(0.01), throwIf(number=100) FROM numbers(200)"
    exception_tag_seen = []
    original_raw_request = test_client._raw_request

    def capture_exception_tag(*args, **kwargs):
        response = original_raw_request(*args, **kwargs)
        tag = response.headers.get(ex_tag_header)
        if tag:
            exception_tag_seen.append(tag)
        return response

    test_client._raw_request = capture_exception_tag

    try:
        with pytest.raises(StreamFailureError) as exc_info:
            result = test_client.query(query, settings={"max_block_size": 1, "wait_end_of_query": 0})
            _ = result.result_set

        assert len(exception_tag_seen) > 0, "Expected X-ClickHouse-Exception-Tag header to be present on 25.11+"
        exception_tag = exception_tag_seen[0]

        assert len(exception_tag) == 16
        assert exception_tag.isalnum()
        assert exception_tag.islower()

        error_msg = str(exc_info.value)
        assert "Value passed to 'throwIf' function is non-zero" in error_msg
        assert "Code: 395" in error_msg

    finally:
        test_client._raw_request = original_raw_request

    assert test_client.command("SELECT 1") == 1
