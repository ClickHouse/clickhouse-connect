from unittest.mock import Mock

import pytest

from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httputil import ResponseSource


# pylint: disable=no-self-use, unused-argument, unreachable
class TestResponseSourceNetworkError:
    """Test ResponseSource handling of network errors"""

    def test_network_error_before_any_data_raises_exception(self):
        """Test that a network error before receiving any data raises OperationalError"""
        mock_response = Mock()
        mock_response.headers = {}

        def failing_stream(chunk_size, decompress):
            """Generator that raises an exception immediately (simulating network failure)"""
            raise ConnectionError("Connection reset by peer")
            yield

        mock_response.stream = failing_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        with pytest.raises(OperationalError) as excinfo:
            list(source.gen)

        assert "Failed to read response data from server" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ConnectionError)

    def test_network_error_after_data_received_does_not_raise(self):
        """Test that a network error after some data was received does not raise an exception"""
        mock_response = Mock()
        mock_response.headers = {}

        def partial_stream(chunk_size, decompress):
            """Generator that yields one chunk then fails"""
            yield b"first chunk of data"
            raise ConnectionError("Connection lost")

        mock_response.stream = partial_stream
        source = ResponseSource(mock_response, chunk_size=1024)
        chunks = list(source.gen)

        assert len(chunks) == 1
        assert chunks[0] == b"first chunk of data"

    def test_normal_empty_response_does_not_raise(self):
        """Test that a legitimately empty response (no error) does not raise an exception"""
        mock_response = Mock()
        mock_response.headers = {}

        def empty_stream(chunk_size, decompress):
            """Generator that returns no data (empty result set)"""
            return
            yield

        mock_response.stream = empty_stream
        source = ResponseSource(mock_response, chunk_size=1024)
        chunks = list(source.gen)

        assert len(chunks) == 0

    def test_network_error_with_compressed_response(self):
        """Test network error handling with compressed (lz4) response"""
        mock_response = Mock()
        mock_response.headers = {"content-encoding": "lz4"}

        def failing_stream(chunk_size, decompress):
            """Generator that raises an exception immediately"""
            raise ConnectionError("Network error during compressed transfer")
            yield

        mock_response.stream = failing_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        with pytest.raises(OperationalError) as excinfo:
            list(source.gen)

        assert "Failed to read response data from server" in str(excinfo.value)
