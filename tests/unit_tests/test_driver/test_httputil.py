from unittest.mock import Mock

import pytest

from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httputil import ResponseSource


class TestResponseSourceZstd:
    def test_zstd_response_decompressed_correctly(self):
        original = b"clickhouse row data " * 200
        compressed = _zstd_compress(original)
        chunk_size = len(compressed) // 3
        raw_chunks = [compressed[i : i + chunk_size] for i in range(0, len(compressed), chunk_size)]

        mock_response = Mock()
        mock_response.headers = {"content-encoding": "zstd"}

        def zstd_stream(chunk_size, decompress):
            yield from raw_chunks

        mock_response.stream = zstd_stream
        source = ResponseSource(mock_response, chunk_size=1024 * 1024)

        result = b"".join(source.gen)
        assert result == original


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

    def test_network_error_after_data_received_does_raise(self):
        """Test that a network error after some data was received raises an exception"""
        mock_response = Mock()
        mock_response.headers = {}

        def partial_stream(chunk_size, decompress):
            """Generator that yields one chunk then fails"""
            yield b"first chunk of data"
            raise ConnectionError("Connection lost")

        mock_response.stream = partial_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        received = []
        with pytest.raises(OperationalError) as excinfo:
            for chunk in source.gen:
                received.append(chunk)

        assert received == [b"first chunk of data"]
        assert "Failed to read response data from server" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ConnectionError)

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
