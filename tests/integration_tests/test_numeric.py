import pytest

from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig


# pylint: disable=duplicate-code
# pylint: disable=attribute-defined-outside-init
class TestBFloat16:
    """Integration tests for ClickHouse BFloat16 data type handling."""

    client: Client
    table_name: str = "bf16_integration_test"

    # pylint: disable=no-self-use
    @pytest.fixture(scope="class", autouse=True)
    def check_version(self, test_client: Client):
        """Skips the entire class if the server version is too old."""
        if not test_client.min_version("24.11"):
            pytest.skip(
                f"BFloat16 type not supported in ClickHouse version {test_client.server_version}"
            )

    @pytest.fixture(autouse=True)
    def setup_teardown(self, test_config: TestConfig, test_client: Client):
        """Create the test table before each test and drop it after."""
        self.config = test_config
        self.client = test_client
        self.client.command(f"DROP TABLE IF EXISTS {self.table_name}")
        self.client.command(
            f"""
            CREATE TABLE {self.table_name} (
                id UInt32,
                bfloat16 BFloat16,
                bfloat16_nullable Nullable(BFloat16)
            ) ENGINE = MergeTree ORDER BY id
            """
        )
        yield
        self.client.command(f"DROP TABLE IF EXISTS {self.table_name}")

    def test_bf16_round_trip(self):
        """Basic round trip test with precision loss."""
        input_data = [[0, 3.141592, -2.71828], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, -2.703125], [1, 3.140625, -2.703125]]
        self.client.insert(self.table_name, input_data)

        result = self.client.query(f"SELECT * FROM {self.table_name} ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)

    def test_bf16_nullable_round_trip(self):
        """Basic round nullable trip test with precision loss."""
        input_data = [[0, 3.141592, None], [1, 3.141592, -2.71828]]
        expected = [[0, 3.140625, None], [1, 3.140625, -2.703125]]
        self.client.insert(self.table_name, input_data)

        result = self.client.query(f"SELECT * FROM {self.table_name} ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
            assert isinstance(result_row[1], float)

    def test_bf16_empty_and_all_null_inserts(self):
        """Tests inserting no rows, and inserting rows with all-null columns."""
        self.client.insert(self.table_name, [])
        result = self.client.query(f"SELECT count() FROM {self.table_name}")
        assert result.result_rows[0][0] == 0

        input_data = [[0, 3.141592, None], [1, -2.71828, None]]
        expected = [[0, 3.140625, None], [1, -2.703125, None]]
        self.client.insert(self.table_name, input_data)

        result = self.client.query(f"SELECT * FROM {self.table_name} ORDER BY id")

        assert result.row_count == len(input_data)
        for result_row, expected_row in zip(result.result_rows, expected):
            assert list(result_row) == expected_row
