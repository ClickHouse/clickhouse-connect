import logging
import pytest

from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from tests.integration_tests.conftest import TestConfig

# pylint: disable=attribute-defined-outside-init


class TestErrorHandling:
    """Tests for error handling in the ClickHouse Connect client"""

    @pytest.fixture(autouse=True)
    def setup(self, test_config: TestConfig):
        self.config = test_config

    def test_wrong_port_error_message(self):
        """
        Test that connecting to the wrong port properly propagates
        the error message from ClickHouse.
        """
        wrong_port = 9000

        with pytest.raises((DatabaseError, OperationalError)) as excinfo:
            create_client(
                host=self.config.host,
                port=wrong_port,
                username=self.config.username,
                password=self.config.password,
            )

        error_message = str(excinfo.value)
        assert (
            f"Port {wrong_port} is for clickhouse-client program" in error_message
            or "You must use port 8123 for HTTP" in error_message
        )

    def test_connection_refused_error(self, caplog):
        """
        Test that connecting to a port where nothing is listening
        produces a clear error message.
        """
        # Suppress urllib3 connection pool warnings
        urllib3_logger = logging.getLogger("urllib3.connectionpool")
        original_level = urllib3_logger.level
        urllib3_logger.setLevel(logging.CRITICAL)

        # Swallow logging messages to prevent polluting pytest output
        caplog.set_level(logging.CRITICAL)

        try:
            # Use a port that shouldn't have anything listening
            unused_port = 45678

            # Try connecting to an unused port - should fail with connection refused
            with pytest.raises(OperationalError) as excinfo:
                create_client(
                    host=self.config.host,
                    port=unused_port,
                    username=self.config.username,
                    password=self.config.password,
                )

            error_message = str(excinfo.value)
            assert (
                "Connection refused" in error_message
                or "Failed to establish a new connection" in error_message
            )
        finally:
            # Restore the original logging level
            urllib3_logger.setLevel(original_level)

    def test_successful_connection(self):
        """
        Verify that connecting to the correct port works properly.
        This serves as a sanity check that the test environment is configured correctly.
        """
        # Connect to the correct HTTP port
        client = create_client(
            host=self.config.host,
            port=self.config.port,  # Use the port from test config
            username=self.config.username,
            password=self.config.password,
        )

        # Simple query to verify connection works
        result = client.command("SELECT 1")
        assert result == 1

        client.close()
