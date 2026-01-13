import logging
import pytest

from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from tests.integration_tests.conftest import TestConfig


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
    assert (
        f"Port {wrong_port} is for clickhouse-client program" in error_message
        or "You must use port 8123 for HTTP" in error_message
    )

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
