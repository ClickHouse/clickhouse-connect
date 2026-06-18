import logging
import zoneinfo
from datetime import timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver import create_async_client, create_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.httpclient import HttpClient, ex_header
from clickhouse_connect.driver.query import QueryContext


# Helper function to create mock response
def create_mock_response(status=500, headers=None, data=None):
    """Create a mock HTTP response with the specified attributes"""
    response = Mock()
    response.status = status
    response.headers = headers or {}
    response.data = data or b""
    response.close = Mock()  # Mock the close method
    return response


class TestHttpClientHeaders:
    """Test client-level HTTP header configuration."""

    def test_headers_are_available_during_initialization(self):
        init_headers = {}

        def capture_headers(client, _tz_source):
            init_headers.update(client.headers)

        with patch.object(Client, "_init_common_settings", autospec=True, side_effect=capture_headers):
            HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
                headers={
                    "CF-Access-Client-Id": "test_client_id",
                    "CF-Access-Client-Secret": "test_client_secret",
                },
            )

        assert init_headers["CF-Access-Client-Id"] == "test_client_id"
        assert init_headers["CF-Access-Client-Secret"] == "test_client_secret"
        assert "Authorization" in init_headers
        assert "User-Agent" in init_headers

    def test_request_headers_override_client_headers(self):
        response = create_mock_response(status=200)
        pool_mgr = Mock()
        pool_mgr.request.return_value = response

        with patch.object(Client, "_init_common_settings", autospec=True):
            client = HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
                pool_mgr=pool_mgr,
                headers={"X-Trace": "client", "X-Gateway": "cloudflare"},
            )

        client._raw_request(b"", {}, headers={"X-Trace": "request"})

        request_headers = pool_mgr.request.call_args.kwargs["headers"]
        assert request_headers["X-Trace"] == "request"
        assert request_headers["X-Gateway"] == "cloudflare"
        assert request_headers["Authorization"] == client.headers["Authorization"]
        assert request_headers["User-Agent"] == client.headers["User-Agent"]

    def test_ping_uses_client_headers(self):
        response = create_mock_response(status=200)
        pool_mgr = Mock()
        pool_mgr.request.return_value = response

        with patch.object(Client, "_init_common_settings", autospec=True):
            client = HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
                pool_mgr=pool_mgr,
                server_host_name="clickhouse.example.com",
                headers={"X-Gateway": "cloudflare"},
            )

        assert client.ping() is True

        request_headers = pool_mgr.request.call_args.kwargs["headers"]
        assert request_headers["X-Gateway"] == "cloudflare"
        assert request_headers["Authorization"] == client.headers["Authorization"]
        assert request_headers["User-Agent"] == client.headers["User-Agent"]
        assert request_headers["Host"] == "clickhouse.example.com"
        assert pool_mgr.request.call_args.kwargs["assert_same_host"] is False

    def test_dsn_headers_query_param_must_be_dict(self):
        with pytest.raises(ProgrammingError, match="headers must be a dictionary"):
            create_client(dsn="http://localhost:8123/default?headers=not_a_dict")

    def test_explicit_headers_override_dsn_headers_query_param(self):
        init_headers = {}

        def capture_headers(client, _tz_source):
            init_headers.update(client.headers)

        with patch.object(Client, "_init_common_settings", autospec=True, side_effect=capture_headers):
            create_client(
                dsn="http://localhost:8123/default?headers=not_a_dict",
                headers={"X-Gateway": "cloudflare"},
            )

        assert init_headers["X-Gateway"] == "cloudflare"


class TestAsyncClientHeaders:
    """Test async client-level HTTP header configuration."""

    @pytest.mark.asyncio
    async def test_request_headers_override_client_headers(self):
        client = AsyncClient(
            interface="http",
            host="localhost",
            port=8123,
            username="default",
            password="",
            database="default",
            headers={"X-Trace": "client", "X-Gateway": "cloudflare"},
        )
        session = Mock()
        session.closed = False
        response = Mock()
        response.status = 200
        response.headers = {}
        session.request = AsyncMock(return_value=response)
        client._session = session

        await client._raw_request(None, {}, headers={"X-Trace": "request"})

        request_headers = session.request.call_args.kwargs["headers"]
        assert request_headers["X-Trace"] == "request"
        assert request_headers["X-Gateway"] == "cloudflare"
        assert request_headers["Authorization"] == client.headers["Authorization"]
        assert request_headers["User-Agent"] == client.headers["User-Agent"]
        assert request_headers["Accept-Encoding"] == client.headers["Accept-Encoding"]

    @pytest.mark.asyncio
    async def test_dsn_headers_query_param_must_be_dict(self):
        with pytest.raises(ProgrammingError, match="headers must be a dictionary"):
            await create_async_client(dsn="http://localhost:8123/default?headers=not_a_dict")

    @pytest.mark.asyncio
    async def test_explicit_headers_override_dsn_headers_query_param(self):
        with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
            client = await create_async_client(
                dsn="http://localhost:8123/default?headers=not_a_dict",
                headers={"X-Gateway": "cloudflare"},
            )

        assert client.headers["X-Gateway"] == "cloudflare"


class TestAsyncClientErrorHandler:
    """Test the error handling functionality of AsyncClient"""

    @staticmethod
    def make_client():
        client = AsyncClient(
            interface="http",
            host="localhost",
            port=8123,
            username="default",
            password="",
            database="default",
        )
        client.url = "http://localhost:8123"
        client.show_clickhouse_errors = True
        return client

    @staticmethod
    def make_response(status=500, headers=None, data=b""):
        response = Mock()
        response.status = status
        response.headers = headers or {}
        response.read = AsyncMock(return_value=data)
        response.close = Mock()
        return response

    @pytest.mark.asyncio
    async def test_error_handler_sets_structured_code_and_name(self):
        client = self.make_client()
        response = self.make_response(
            status=404,
            headers={ex_header: "60"},
            data=b"Code: 60. DB::Exception: Unknown table 'x'. (UNKNOWN_TABLE) (version 26.2.4.23)",
        )

        with pytest.raises(DatabaseError) as excinfo:
            await client._error_handler(response)

        assert excinfo.value.code == 60
        assert excinfo.value.name == "UNKNOWN_TABLE"
        assert "server response:" in str(excinfo.value)
        response.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handler_code_set_when_errors_disabled(self):
        client = self.make_client()
        client.show_clickhouse_errors = False
        response = self.make_response(
            status=404,
            headers={ex_header: "60"},
            data=b"Code: 60. DB::Exception: Unknown table 'x'. (UNKNOWN_TABLE)",
        )

        with pytest.raises(DatabaseError) as excinfo:
            await client._error_handler(response)

        assert excinfo.value.code == 60
        assert excinfo.value.name is None
        assert "UNKNOWN_TABLE" not in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_error_handler_retried_raises_operational_error(self):
        client = self.make_client()
        response = self.make_response(status=503, headers={ex_header: "159"}, data=b"timeout")

        with pytest.raises(OperationalError) as excinfo:
            await client._error_handler(response, retried=True)

        assert excinfo.value.code == 159


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
        assert excinfo.value.code == 99
        response.close.assert_called_once()

    def test_error_handler_sets_structured_code_and_name(self):
        """Code and name are exposed as attributes parsed from the header and body"""
        response = create_mock_response(
            status=404,
            headers={ex_header: "60"},
            data=b"Code: 60. DB::Exception: Unknown table 'x'. (UNKNOWN_TABLE) (version 26.2.4.23)",
        )

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        assert excinfo.value.code == 60
        assert excinfo.value.name == "UNKNOWN_TABLE"

    def test_error_handler_code_none_without_header(self):
        """Code is None when the server sends no exception header"""
        response = create_mock_response(status=503, data=b"Service unavailable")

        with pytest.raises(DatabaseError) as excinfo:
            self.client._error_handler(response)

        assert excinfo.value.code is None
        assert excinfo.value.name is None

    def test_error_handler_name_parsed_before_truncation(self):
        """name is parsed from the full body even when max_error_size truncates the message"""
        long_body = "Code: 62. DB::Exception: " + ("x" * 400) + " (SYNTAX_ERROR) (version 26.2.4.23)"
        response = create_mock_response(status=400, headers={ex_header: "62"}, data=long_body.encode())

        original = common.get_setting("max_error_size")
        common.set_setting("max_error_size", 100)
        try:
            with pytest.raises(DatabaseError) as excinfo:
                self.client._error_handler(response)
        finally:
            common.set_setting("max_error_size", original)

        assert excinfo.value.code == 62
        assert excinfo.value.name == "SYNTAX_ERROR"
        assert "SYNTAX_ERROR" not in str(excinfo.value)  # truncated out of the displayed message

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
        assert "server response:" not in error_msg  # No body, so no server response part
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
        assert "The ClickHouse server returned an error (for url http://localhost:8123)" in error_msg
        assert "Invalid query" not in error_msg  # Should not include the body
        assert "99" not in error_msg  # Should not include the exception code
        # Numeric code is still exposed structurally, but the body-derived name is suppressed
        assert excinfo.value.code == 99
        assert excinfo.value.name is None
        response.close.assert_called_once()

    def test_error_handler_with_unicode_decode_error(self):
        """Test error handling when the response body has invalid Unicode"""

        # Create response with invalid UTF-8 sequence
        response = create_mock_response(status=500, data=b"\xff\xfe Invalid UTF-8 sequence")

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
    def test_error_handler_with_body_reading_exception(self, mock_get_response_data, caplog):
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


class TestQuery:
    """Test the form encoding and external data handling in HttpClient"""

    def setup_method(self):
        """Set up common test fixtures"""
        self.client = HttpClient(
            interface="http",
            host="localhost",
            port=8123,
            username="default",
            password="",
            database="default",
        )

    # Helper methods

    @staticmethod
    def create_mock_external_data() -> Mock:
        """Create a mock ExternalData object with standard test data"""
        external_data = Mock(spec=ExternalData)
        external_data.query_params = {"_file1_format": "CSV", "_file1_structure": "id UInt32"}
        external_data.form_data = {"_file1": b"1\n2\n3\n"}
        return external_data

    @staticmethod
    def create_mock_query_context(
        query: str = "SELECT * FROM table", bind_params: dict[str, Any] | None = None, external_data: ExternalData | None = None
    ) -> Mock:
        """Create a mock QueryContext with common test values"""
        context = Mock(spec=QueryContext)
        context.final_query = f"{query}\n FORMAT Native"
        context.bind_params = bind_params or {}
        context.external_data = external_data
        context.is_insert = False
        context.uncommented_query = query
        context.settings = {}
        context.transport_settings = {}
        context.streaming = False
        return context

    @staticmethod
    def setup_mock_raw_request() -> MagicMock:
        """Create a mock for _raw_request with standard response"""
        mock_response = MagicMock()
        mock_response.headers = {}
        return mock_response

    @staticmethod
    def extract_raw_request_params(mock_raw_request: MagicMock) -> tuple[Any, dict, dict]:
        assert mock_raw_request.called
        call_args = mock_raw_request.call_args

        # Extract positional arguments
        body = call_args[0][0] if len(call_args[0]) > 0 else None
        params = call_args[0][1] if len(call_args[0]) > 1 else {}

        # Extract fields from keyword arguments
        fields = call_args[1].get("fields", {}) if call_args[1] else {}

        return body, params, fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query(self, mock_raw_request):
        """Test raw_query with neither form_encode_query_params nor external_data"""
        self.client.form_encode_query_params = False

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"test_result"
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table WHERE id = {id:UInt32}"
        parameters = {"id": 123}

        # Call raw_query
        result = self.client.raw_query(query, parameters=parameters)

        # Verify result
        assert result == b"test_result"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, str)
        assert body
        assert "SELECT * FROM table WHERE id =" in body
        assert "param_id" in params
        assert not fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_with_form_encode(self, mock_raw_request):
        """Test raw_query with form_encode_query_params=True"""
        self.client.form_encode_query_params = True

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"test_result"
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table WHERE id = {id:UInt32}"
        parameters = {"id": 123}

        # Call raw_query
        result = self.client.raw_query(query, parameters=parameters)

        # Verify result
        assert result == b"test_result"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" in fields
        assert isinstance(fields["query"], str)
        assert "SELECT * FROM table WHERE id =" in fields["query"]
        assert "param_id" in fields
        assert "param_id" not in params

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_auto_form_encode_large_params(self, mock_raw_request):
        """Large bind params auto-promote to form data even with form_encode_query_params=False"""
        self.client.form_encode_query_params = False

        mock_response = Mock()
        mock_response.data = b"test_result"
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table WHERE name = {name:String}"
        parameters = {"name": "x" * 5000}

        self.client.raw_query(query, parameters=parameters)

        body, params, fields = self.extract_raw_request_params(mock_raw_request)
        assert body == b""
        assert "param_name" in fields
        assert "param_name" not in params

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_with_external_data_only(self, mock_raw_request):
        """Test raw_query with external_data only (no form_encode)"""
        self.client.form_encode_query_params = False

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"100"
        mock_raw_request.return_value = mock_response

        external_data = self.create_mock_external_data()
        query = "SELECT COUNT() FROM file1"

        # Call raw_query
        result = self.client.raw_query(query, external_data=external_data)

        # Verify result
        assert result == b"100"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" in params
        assert isinstance(params["query"], str)
        assert params["query"] == query
        assert "_file1_format" in params
        assert "_file1" in fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_with_form_encode_and_external_data(self, mock_raw_request):
        """Test raw_query with both form_encode_query_params and external_data"""
        self.client.form_encode_query_params = True

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"150"
        mock_raw_request.return_value = mock_response

        external_data = self.create_mock_external_data()
        query = "SELECT COUNT() FROM file1 WHERE value > {min_val:UInt32}"
        parameters = {"min_val": 10}

        # Call raw_query
        result = self.client.raw_query(query, parameters=parameters, external_data=external_data)

        # Verify result
        assert result == b"150"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" not in params
        assert "query" in fields
        assert isinstance(fields["query"], str)
        assert "_file1_format" in params
        assert "_file1" in fields
        assert "param_min_val" in fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_form_encode_without_external_data(self, mock_raw_request):
        """Test that query goes to fields when form_encode is True but no external_data"""
        self.client.form_encode_query_params = True

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"50"
        mock_raw_request.return_value = mock_response

        query = "SELECT COUNT() FROM table"

        # Call raw_query
        result = self.client.raw_query(query)

        # Verify result
        assert result == b"50"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" not in params
        assert "query" in fields
        assert isinstance(fields["query"], str)
        assert fields["query"] == query

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_with_settings(self, mock_raw_request):
        """Test raw_query properly handles settings parameter"""
        self.client.form_encode_query_params = False

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"result_with_settings"
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table"
        settings = {"max_threads": 4, "max_memory_usage": 1000000}

        # Call raw_query
        result = self.client.raw_query(query, settings=settings)

        # Verify result
        assert result == b"result_with_settings"

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, str)
        assert "max_threads" in params
        assert params["max_threads"] == 4
        assert "max_memory_usage" in params
        assert params["max_memory_usage"] == 1000000
        assert not fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_with_format(self, mock_raw_request):
        """Test raw_query properly appends FORMAT clause"""
        self.client.form_encode_query_params = False

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b'{"data": "json_formatted"}'
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table"
        fmt = "JSONEachRow"

        # Call raw_query
        result = self.client.raw_query(query, fmt=fmt)

        # Verify result
        assert result == b'{"data": "json_formatted"}'

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, str)
        assert "FORMAT JSONEachRow" in body
        assert params is not None
        assert not fields

    @patch.object(HttpClient, "_raw_request")
    def test_raw_query_database_handling(self, mock_raw_request):
        """Test raw_query properly handles database parameter"""
        self.client.form_encode_query_params = False
        self.client.database = "test_db"

        # Setup mock response
        mock_response = Mock()
        mock_response.data = b"db_result"
        mock_raw_request.return_value = mock_response

        query = "SELECT * FROM table"

        # Test with use_database=True (default)
        result = self.client.raw_query(query, use_database=True)
        assert result == b"db_result"

        body, params, fields = self.extract_raw_request_params(mock_raw_request)
        assert isinstance(body, str)
        assert "database" in params
        assert params["database"] == "test_db"
        assert not fields

        # Reset mock for second test
        mock_raw_request.reset_mock()
        mock_raw_request.return_value = mock_response

        # Test with use_database=False
        result = self.client.raw_query(query, use_database=False)
        assert result == b"db_result"

        body, params, fields = self.extract_raw_request_params(mock_raw_request)
        assert isinstance(body, str)
        assert "database" not in params
        assert not fields

    @patch.object(HttpClient, "_raw_request")
    def test_query_with_context(self, mock_raw_request):
        """Test _query_with_context with neither form_encode_query_params nor external_data"""
        self.client.form_encode_query_params = False

        # Setup mocks
        mock_raw_request.return_value = self.setup_mock_raw_request()
        self.client._transform = Mock()
        self.client._transform.parse_response.return_value = Mock(summary=None)

        # Create context
        context = self.create_mock_query_context(query="SELECT * FROM table WHERE id = 123", bind_params={"param_id": 123})

        # Call the method
        self.client._query_with_context(context)

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, str)
        assert "SELECT * FROM table WHERE id =" in body
        assert "param_id" in params
        assert not fields

    @patch.object(HttpClient, "_raw_request")
    def test_query_with_context_form_encode(self, mock_raw_request):
        """Test _query_with_context with form_encode_query_params=True"""
        self.client.form_encode_query_params = True

        # Setup mocks
        mock_raw_request.return_value = self.setup_mock_raw_request()
        self.client._transform = Mock()
        self.client._transform.parse_response.return_value = Mock(summary=None)

        # Create context
        context = self.create_mock_query_context(query="SELECT * FROM table WHERE id = 123", bind_params={"param_id": 123})

        # Call the method
        self.client._query_with_context(context)

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" in fields
        assert "param_id" in fields
        assert "param_id" not in params

    @patch.object(HttpClient, "_raw_request")
    def test_query_with_context_auto_form_encode_large_params(self, mock_raw_request):
        """Large bind params auto-promote to form data even with form_encode_query_params=False"""
        self.client.form_encode_query_params = False

        mock_raw_request.return_value = self.setup_mock_raw_request()
        self.client._transform = Mock()
        self.client._transform.parse_response.return_value = Mock(summary=None)

        context = self.create_mock_query_context(query="SELECT * FROM table WHERE name = 'x'", bind_params={"param_name": "x" * 5000})

        self.client._query_with_context(context)

        body, params, fields = self.extract_raw_request_params(mock_raw_request)
        assert body == b""
        assert "param_name" in fields
        assert "param_name" not in params

    @patch.object(HttpClient, "_raw_request")
    def test_query_with_context_external_data(self, mock_raw_request):
        """Test _query_with_context with external_data only"""
        self.client.form_encode_query_params = False

        # Setup mocks
        mock_raw_request.return_value = self.setup_mock_raw_request()
        self.client._transform = Mock()
        self.client._transform.parse_response.return_value = Mock(summary=None)

        # Create external data and context
        external_data = self.create_mock_external_data()
        context = self.create_mock_query_context(query="SELECT * FROM file1", external_data=external_data)

        # Call the method
        self.client._query_with_context(context)

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" in params
        assert isinstance(params["query"], str)
        assert "_file1_format" in params
        assert "_file1" in fields

    @patch.object(HttpClient, "_raw_request")
    def test_query_with_context_with_form_encode_and_external_data(self, mock_raw_request):
        """Test _query_with_context with both form_encode_query_params and external_data"""
        self.client.form_encode_query_params = True

        # Setup mocks
        mock_raw_request.return_value = self.setup_mock_raw_request()
        self.client._transform = Mock()
        self.client._transform.parse_response.return_value = Mock(summary=None)

        # Create external data and context
        external_data = self.create_mock_external_data()
        context = self.create_mock_query_context(
            query="SELECT * FROM file1 WHERE value > 10", bind_params={"param_min_val": 10}, external_data=external_data
        )

        # Call the method
        self.client._query_with_context(context)

        # Check the call to _raw_request
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        assert isinstance(body, bytes)
        assert body == b""
        assert "query" not in params
        assert "query" in fields
        assert isinstance(fields["query"], str)
        assert "_file1_format" in params
        assert "_file1" in fields
        assert "param_min_val" in fields

    @patch.object(HttpClient, "_raw_request")
    @patch("clickhouse_connect.driver.httpclient.columns_only_re")
    def test_query_with_context_schema_probe_form_encode(self, mock_columns_re, mock_raw_request):
        """Test that schema-probe queries (LIMIT 0) work correctly with form_encode_query_params"""
        self.client.form_encode_query_params = True

        # Mock the columns_only_re to match LIMIT 0
        mock_columns_re.search.return_value = True

        # Setup mock response for schema probe
        mock_response = Mock()
        mock_response.data = b'{"meta": [{"name": "id", "type": "UInt32"}, {"name": "name", "type": "String"}]}'
        mock_response.headers = {}
        mock_raw_request.return_value = mock_response

        # Create query context
        context = self.create_mock_query_context(
            query="SELECT * FROM table WHERE id = {id:UInt32} LIMIT 0", bind_params={"param_id": "123"}
        )
        context.uncommented_query = "SELECT * FROM table WHERE id = {id:UInt32} LIMIT 0"
        context.is_insert = False
        context.final_query = "SELECT * FROM table WHERE id = {id:UInt32} LIMIT 0"
        context.settings = {}
        context.transport_settings = {}
        context.streaming = False
        context.block_info = False
        context.set_response_tz = Mock()
        context.column_renamer = None

        # Call _query_with_context
        self.client._query_with_context(context)

        # Extract parameters from the mock call
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        # Verify that form encoding was used for schema probe
        assert body == b""  # Body should be empty with form encoding
        assert fields is not None  # Fields should be populated
        assert "query" in fields
        assert "FORMAT JSON" in fields["query"]
        assert "param_id" in fields
        assert fields["param_id"] == "123"

        # Verify params dont contain the query or bind params
        assert "query" not in params
        assert "param_id" not in params

    @patch.object(HttpClient, "_raw_request")
    @patch("clickhouse_connect.driver.httpclient.columns_only_re")
    def test_query_with_context_schema_probe_external_data(self, mock_columns_re, mock_raw_request):
        """Test schema-probe queries (LIMIT 0) with external data but no form encoding"""
        self.client.form_encode_query_params = False

        # Mock the columns_only_re to match LIMIT 0
        mock_columns_re.search.return_value = True

        # Setup mock response for schema probe
        mock_response = Mock()
        mock_response.data = b'{"meta": [{"name": "id", "type": "UInt32"}, {"name": "count", "type": "UInt64"}]}'
        mock_response.headers = {}
        mock_raw_request.return_value = mock_response

        # Create external data and context
        external_data = self.create_mock_external_data()
        context = self.create_mock_query_context(query="SELECT * FROM file1 LIMIT 0", external_data=external_data)
        context.uncommented_query = "SELECT * FROM file1 LIMIT 0"
        context.is_insert = False
        context.final_query = "SELECT * FROM file1 LIMIT 0"
        context.settings = {}
        context.transport_settings = {}
        context.streaming = False
        context.block_info = False
        context.set_response_tz = Mock()
        context.column_renamer = None

        # Call _query_with_context
        self.client._query_with_context(context)

        # Extract parameters from the mock call
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        # Verify external data handling without form encoding
        assert body == b""  # Body should be empty when using external data
        assert "query" in params  # Query should be in params
        assert "FORMAT JSON" in params["query"]
        assert "_file1_format" in params  # External data query params
        assert "_file1" in fields  # External data form fields

    @patch.object(HttpClient, "_raw_request")
    @patch("clickhouse_connect.driver.httpclient.columns_only_re")
    def test_query_with_context_schema_probe_form_encode_external_data(self, mock_columns_re, mock_raw_request):
        """Test schema-probe queries (LIMIT 0) with both form encoding and external data"""
        self.client.form_encode_query_params = True

        # Mock the columns_only_re to match LIMIT 0
        mock_columns_re.search.return_value = True

        # Setup mock response for schema probe
        mock_response = Mock()
        mock_response.data = b'{"meta": [{"name": "id", "type": "UInt32"}, {"name": "value", "type": "Float64"}]}'
        mock_response.headers = {}
        mock_raw_request.return_value = mock_response

        # Create external data and context
        external_data = self.create_mock_external_data()
        context = self.create_mock_query_context(
            query="SELECT * FROM file1 WHERE value > 10",
            bind_params={"param_min_val": 10},
            external_data=external_data,
        )
        context.uncommented_query = "SELECT * FROM file1 WHERE value > 10"
        context.is_insert = False
        context.final_query = "SELECT * FROM file1 WHERE value > 10"
        context.settings = {}
        context.transport_settings = {}
        context.streaming = False
        context.block_info = False
        context.set_response_tz = Mock()
        context.column_renamer = None

        # Call _query_with_context
        self.client._query_with_context(context)

        # Extract parameters from the mock call
        body, params, fields = self.extract_raw_request_params(mock_raw_request)

        # Verify both form encoding and external data handling
        assert body == b""  # Body should be empty
        assert "query" in fields  # Query should be in fields (form encoding)
        assert "FORMAT JSON" in fields["query"]
        assert "param_min_val" in fields  # Bind params in fields
        assert "_file1_format" in params  # External data query params
        assert "_file1" in fields  # External data form fields
        assert "query" not in params  # Query should not be in params when form encoding


class TestResponseTimezone:
    """set_response_tz is called only when the server reports a timezone different from server_tz."""

    def setup_method(self):
        with patch.object(Client, "_init_common_settings", autospec=True):
            self.client = HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
            )
        self.client.server_tz = timezone.utc
        self.client._dst_safe = True

    @staticmethod
    def _make_context() -> Mock:
        context = Mock(spec=QueryContext)
        context.final_query = "SELECT 1"
        context.uncommented_query = "SELECT 1"
        context.bind_params = {}
        context.external_data = None
        context.is_insert = False
        context.settings = {}
        context.transport_settings = {}
        context.streaming = False
        context.block_info = False
        return context

    def _run(self, tz_header: str | None) -> Mock:
        """Invoke _query_with_context with a mocked response and return the context Mock."""
        mock_response = Mock()
        mock_response.headers = {} if tz_header is None else {"X-ClickHouse-Timezone": tz_header}
        context = self._make_context()

        with (
            patch.object(HttpClient, "_raw_request", return_value=mock_response),
            patch("clickhouse_connect.driver.httpclient.RespBuffCls"),
            patch("clickhouse_connect.driver.httpclient.ResponseSource"),
            patch.object(self.client._transform, "parse_response", return_value=Mock()),
        ):
            self.client._query_with_context(context)

        return context

    def test_set_response_tz_not_called_when_header_absent(self):
        """set_response_tz is not called when X-ClickHouse-Timezone header is missing."""
        context = self._run(tz_header=None)
        context.set_response_tz.assert_not_called()

    def test_set_response_tz_not_called_when_timezone_matches_server(self):
        """set_response_tz is not called when the response timezone matches server_tz."""
        context = self._run(tz_header="UTC")
        context.set_response_tz.assert_not_called()

    def test_set_response_tz_called_with_correct_tzinfo_when_timezone_differs(self):
        """set_response_tz is called with the resolved tzinfo when the response timezone differs."""
        context = self._run(tz_header="America/New_York")
        context.set_response_tz.assert_called_once()
        called_tz = context.set_response_tz.call_args[0][0]
        assert called_tz == zoneinfo.ZoneInfo("America/New_York")
