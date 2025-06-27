from unittest.mock import Mock, patch
import logging
import pytest

from clickhouse_connect.driver.httpclient import HttpClient, ex_header
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

# pylint: disable=protected-access
# pylint: disable=attribute-defined-outside-init


# Helper function to create mock response
def create_mock_response(status=500, headers=None, data=None):
    """Create a mock HTTP response with the specified attributes"""
    response = Mock()
    response.status = status
    response.headers = headers or {}
    response.data = data or b""
    response.close = Mock()  # Mock the close method
    return response


class TestHttpClientErrorHandler:
    """Test the error handling functionality of HttpClient"""

    def setup_method(self):
        """Set up common test fixtures"""
        # Create a minimal HttpClient instance
        self.client = HttpClient(
            interface="http",
            host="localhost",
            port=8123,
            username="default",
            password="",
            database="default",
        )
        self.client.url = "http://localhost:8123"

        # Always turn on show_clickhouse_error. Will disable in tests as needed.
        self.client.show_clickhouse_errors = True

    def test_error_handler_with_exception_code(self):
        """Test error handling when ClickHouse exception code is present"""

        # Create mock response with exception code
        response = create_mock_response(
            status=500,
            headers={ex_header: "99"},
            data=b"Error executing query",
        )

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify the error message contains all expected parts
        error_msg = str(excinfo.value)
        assert "Received ClickHouse exception, code: 99" in error_msg
        assert "server response: Error executing query" in error_msg
        assert self.client.url in error_msg
        response.close.assert_called_once()

    def test_error_handler_without_exception_code(self):
        """Test error handling when only HTTP status is available"""

        # Create mock response without exception code
        response = create_mock_response(status=503, data=b"Service unavailable")

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify the error message contains all expected parts
        error_msg = str(excinfo.value)
        assert "HTTP driver received HTTP status 503" in error_msg
        assert "server response: Service unavailable" in error_msg
        assert self.client.url in error_msg
        response.close.assert_called_once()

    def test_error_handler_with_empty_body(self):
        """Test error handling when response body is empty"""

        # Create mock response with empty body
        response = create_mock_response(status=400, headers={ex_header: "99"}, data=b"")

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify the error message contains expected parts but not empty body
        error_msg = str(excinfo.value)
        assert "Received ClickHouse exception, code: 99" in error_msg
        assert (
            "server response:" not in error_msg
        )  # No body, so no server response part
        assert self.client.url in error_msg
        response.close.assert_called_once()

    def test_error_handler_with_errors_disabled(self):
        """Test error handling when show_clickhouse_errors is disabled"""
        # Explicitly disable showing ClickHouse errors
        self.client.show_clickhouse_errors = False

        # Create mock response
        response = create_mock_response(
            status=400,
            headers={ex_header: "99"},
            data=b"Invalid query",
        )

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify the error message is generic
        error_msg = str(excinfo.value)
        assert (
            "The ClickHouse server returned an error (for url http://localhost:8123)"
            in error_msg
        )
        assert "Invalid query" not in error_msg  # Should not include the body
        assert "99" not in error_msg  # Should not include the exception code
        response.close.assert_called_once()

    def test_error_handler_with_unicode_decode_error(self):
        """Test error handling when the response body has invalid Unicode"""

        # Create response with invalid UTF-8 sequence
        response = create_mock_response(
            status=500, data=b"\xff\xfe Invalid UTF-8 sequence"
        )

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify error message contains the backslash-escaped bytes
        error_msg = str(excinfo.value)
        assert "HTTP driver received HTTP status 500" in error_msg
        assert "server response:" in error_msg  # Should have backslash-escaped data
        response.close.assert_called_once()

    def test_error_handler_with_retried_flag(self):
        """Test error handling with retried flag set to True"""
        # Create mock response
        response = create_mock_response(status=500, data=b"Server error")

        # Test the error handler with retried=True
        with pytest.raises(OperationalError) as excinfo:
            self.client._error_handler(response, retried=True)

        # Verify that OperationalError is raised instead of DatabaseError
        assert isinstance(excinfo.value, OperationalError)
        response.close.assert_called_once()

    @patch("clickhouse_connect.driver.httpclient.get_response_data")
    def test_error_handler_with_body_reading_exception(
        self, mock_get_response_data, caplog
    ):
        """Test error handling when reading the response body throws an exception"""
        # Set up the mock to raise an exception when reading the response body
        mock_get_response_data.side_effect = Exception("Error reading response data")

        # Swallow logging messages to prevent polluting pytest output
        caplog.set_level(logging.CRITICAL)

        # Create mock response
        response = create_mock_response(
            status=500,
            headers={"X-ClickHouse-Exception-Code": "99"},
            data=b"Some data",  # This won't be read due to the mocked exception
        )

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        # Verify the error message has the exception code but no body
        error_msg = str(excinfo.value)
        assert "Received ClickHouse exception, code: 99" in error_msg
        assert "server response:" not in error_msg  # No body due to exception
        assert self.client.url in error_msg

        # Verify the mock was called
        mock_get_response_data.assert_called_once_with(response)
        response.close.assert_called_once()
