import logging

import aiohttp
import pytest
import urllib3.exceptions

from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from clickhouse_connect.driver.options import pd
from tests.integration_tests.conftest import TestConfig


def _client_os_error_from(cause: ConnectionError) -> aiohttp.ClientOSError:
    error = aiohttp.ClientOSError(cause.errno, cause.strerror)
    error.__cause__ = cause
    return error


def _client_connection_reset_error() -> aiohttp.ClientConnectionError:
    error_class = getattr(aiohttp, "ClientConnectionResetError", None)
    if error_class is None:
        pytest.skip("aiohttp.ClientConnectionResetError is not available")
    return error_class(104, "Connection reset by peer")


def test_wrong_port_error_message(client_factory, test_config: TestConfig):
    """
    Test that connecting to the wrong port properly propagates
    the error message from ClickHouse.
    """
    if test_config.cloud:
        pytest.skip("Skipping wrong port test in cloud environ.")
    wrong_port = 9000

    with pytest.raises((DatabaseError, OperationalError)) as excinfo:
        client_factory(port=wrong_port)

    error_message = str(excinfo.value)
    assert f"Port {wrong_port} is for clickhouse-client program" in error_message or "You must use port 8123 for HTTP" in error_message


def test_connection_refused_error(client_factory, test_config: TestConfig, caplog):
    """
    Test that connecting to a port where nothing is listening
    produces a clear error message.
    """
    if test_config.cloud:
        pytest.skip("Skipping connection refused test in cloud environ.")
    # Suppress urllib3 and aiohttp connection warnings
    urllib3_logger = logging.getLogger("urllib3.connectionpool")
    original_urllib3_level = urllib3_logger.level
    urllib3_logger.setLevel(logging.CRITICAL)

    # Swallow logging messages to prevent polluting pytest output
    caplog.set_level(logging.CRITICAL)

    try:
        # Use a port that shouldn't have anything listening
        unused_port = 45678

        # Try connecting to an unused port - should fail with connection refused
        with pytest.raises(OperationalError) as excinfo:
            client_factory(port=unused_port)

        error_message = str(excinfo.value)
        assert (
            "Connection refused" in error_message
            or "Failed to establish a new connection" in error_message
            or "Cannot connect to host" in error_message
            or "Connection aborted" in error_message  # Port occasionally occupied in CI, apparently
        )
    finally:
        # Restore the original logging level
        urllib3_logger.setLevel(original_urllib3_level)


def test_successful_connection(client_factory, call):
    """Verify that connecting to the correct port works properly."""
    # Connect to the correct HTTP port (uses defaults from test_config)
    client = client_factory()

    # Simple query to verify connection works
    result = call(client.command, "SELECT 1")
    assert result == 1


@pytest.mark.parametrize(
    "disconnect_exc",
    [aiohttp.ServerDisconnectedError(), aiohttp.ServerDisconnectedError("Connection reset by peer")],
    ids=["bare", "connection_reset"],
)
@pytest.mark.parametrize("disconnect_count", [1, 2])
@pytest.mark.asyncio
async def test_async_retry_on_server_disconnected(test_native_async_client, mocker, disconnect_count, disconnect_exc):
    """
    aiohttp raises ServerDisconnectedError when the server (or an upstream load
    balancer) closes pooled keep-alive connections between requests. A burst of
    drops can leave several stale sockets in the pool, so the first retry can
    still pick up a bad one. The async client must keep retrying up to
    query_retries so the query succeeds as long as a healthy connection
    eventually becomes available. Both the bare default message and the
    "Connection reset" variant must trigger the retry path.
    """
    real_request = test_native_async_client._session.request
    attempts = 0

    async def flaky_request(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= disconnect_count:
            raise disconnect_exc
        return await real_request(*args, **kwargs)

    mocker.patch.object(test_native_async_client._session, "request", side_effect=flaky_request)

    result = await test_native_async_client.query("SELECT 13")

    assert attempts == disconnect_count + 1
    assert result.result_rows[0][0] == 13


@pytest.mark.asyncio
async def test_async_server_disconnected_raises_after_retry(test_native_async_client, mocker):
    """
    If the disconnect is not transient and the retry also fails, the error must
    still surface as OperationalError so callers can react.
    """
    mocker.patch.object(
        test_native_async_client._session,
        "request",
        side_effect=aiohttp.ServerDisconnectedError(),
    )

    with pytest.raises(OperationalError) as excinfo:
        await test_native_async_client.query("SELECT 13")

    assert "Server disconnected" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, aiohttp.ServerDisconnectedError)


@pytest.mark.parametrize(
    "disconnect_factory",
    [
        pytest.param(
            lambda: _client_os_error_from(ConnectionResetError(104, "Connection reset by peer")),
            id="client_os_error_reset_cause",
        ),
        pytest.param(
            lambda: _client_os_error_from(BrokenPipeError(32, "Broken pipe")),
            id="client_os_error_broken_pipe_cause",
        ),
        pytest.param(
            _client_connection_reset_error,
            id="client_connection_reset_error",
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_retry_on_remote_close_client_connection_error(test_native_async_client, mocker, disconnect_factory):
    """Retries aiohttp remote-close connection errors when the body can be replayed."""
    real_request = test_native_async_client._session.request
    attempts = 0

    async def flaky_request(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise disconnect_factory()
        return await real_request(*args, **kwargs)

    mocker.patch.object(test_native_async_client._session, "request", side_effect=flaky_request)

    result = await test_native_async_client.query("SELECT 13")

    assert attempts == 2
    assert result.result_rows[0][0] == 13


@pytest.mark.asyncio
async def test_async_client_connection_error_without_remote_close_is_not_retried(test_native_async_client, mocker):
    """Does not retry generic aiohttp connection errors."""
    attempts = 0

    async def failing_request(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise aiohttp.ClientConnectionError("generic connection failure")

    mocker.patch.object(test_native_async_client._session, "request", side_effect=failing_request)

    with pytest.raises(OperationalError) as excinfo:
        await test_native_async_client.query("SELECT 13")

    assert attempts == 1
    assert "generic connection failure" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, aiohttp.ClientConnectionError)


@pytest.mark.parametrize("disconnect_count", [1, 2])
def test_sync_retry_on_connection_reset(test_client, mocker, disconnect_count):
    """
    urllib3 raises ProtocolError("Connection aborted.") with ConnectionResetError
    as __context__ when a pooled keep-alive connection has been closed by the
    server. The sync client must keep retrying up to query_retries so the query
    succeeds as long as a healthy connection eventually becomes available.
    """
    real_request = test_client.http.request
    attempts = 0

    def flaky_request(method, url, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= disconnect_count:
            try:
                raise ConnectionResetError("Connection reset by peer")
            except ConnectionResetError:
                raise urllib3.exceptions.ProtocolError("Connection aborted.")  # noqa: B904
        return real_request(method, url, **kwargs)

    mocker.patch.object(test_client.http, "request", side_effect=flaky_request)

    result = test_client.query("SELECT 13")

    assert attempts == disconnect_count + 1
    assert result.result_rows[0][0] == 13


@pytest.mark.parametrize("disconnect_count", [1, 2])
def test_sync_raw_query_retry_on_connection_reset(test_client, mocker, disconnect_count):
    """Sync raw_query drains query_retries on stale-connection bursts, same as the regular query path."""
    real_request = test_client.http.request
    attempts = 0

    def flaky_request(method, url, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts <= disconnect_count:
            try:
                raise ConnectionResetError("Connection reset by peer")
            except ConnectionResetError:
                raise urllib3.exceptions.ProtocolError("Connection aborted.")  # noqa: B904
        return real_request(method, url, **kwargs)

    mocker.patch.object(test_client.http, "request", side_effect=flaky_request)

    result = test_client.raw_query("SELECT 13")

    assert attempts == disconnect_count + 1
    assert result.strip() == b"13"


def _install_one_shot_reset_mock(client, client_mode, mocker):
    """Drain and reset the first generator-bodied request, then defer to the real transport."""
    attempts = [0]

    if client_mode == "sync":
        real_request = client.http.request

        def flaky_request(method, url, **kwargs):
            body = kwargs.get("body")
            if body is not None and not isinstance(body, (bytes, bytearray, str)):
                attempts[0] += 1
                if attempts[0] == 1:
                    for _ in body:
                        pass
                    try:
                        raise ConnectionResetError("Connection reset by peer")
                    except ConnectionResetError:
                        raise urllib3.exceptions.ProtocolError("Connection aborted.")  # noqa: B904
            return real_request(method, url, **kwargs)

        mocker.patch.object(client.http, "request", side_effect=flaky_request)
    else:
        real_request = client._session.request

        async def flaky_request(*args, **kwargs):
            data = kwargs.get("data")
            if data is not None and hasattr(data, "__aiter__"):
                attempts[0] += 1
                if attempts[0] == 1:
                    async for _ in data:
                        pass
                    raise _client_os_error_from(ConnectionResetError(104, "Connection reset by peer"))
            return await real_request(*args, **kwargs)

        mocker.patch.object(client._session, "request", side_effect=flaky_request)

    return lambda: attempts[0]


def test_insert_retry_on_connection_reset(param_client, call, client_mode, table_context, mocker):
    """Insert retries and succeeds when the first attempt is hit by a connection reset."""
    with table_context("insert_retry_reset", ["key Int32", "name String"]):
        attempts = _install_one_shot_reset_mock(param_client, client_mode, mocker)

        call(
            param_client.insert,
            "insert_retry_reset",
            [[13, "user_1"], [79, "user_2"]],
            column_names=["key", "name"],
        )
        assert attempts() == 2

        rows = call(param_client.query, "SELECT key, name FROM insert_retry_reset ORDER BY key").result_rows
        assert rows == [(13, "user_1"), (79, "user_2")]


def test_insert_df_retry_on_connection_reset(param_client, call, client_mode, table_context, mocker):
    """insert_df retries and succeeds when the first attempt is hit by a connection reset."""
    if pd is None:
        pytest.skip("pandas not available")

    with table_context("insert_df_retry_reset", ["key Int32", "name String"]):
        attempts = _install_one_shot_reset_mock(param_client, client_mode, mocker)

        df = pd.DataFrame({"key": [13, 79], "name": ["user_1", "user_2"]})
        call(param_client.insert_df, "insert_df_retry_reset", df)
        assert attempts() == 2

        rows = call(param_client.query, "SELECT key, name FROM insert_df_retry_reset ORDER BY key").result_rows
        assert rows == [(13, "user_1"), (79, "user_2")]
