from ipaddress import IPv4Address, IPv6Address
import pytest

from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig

# A collection of diverse IPv6 addresses for testing
IPV6_TEST_CASES = [
    IPv6Address("2001:db8:85a3::8a2e:370:7334"),  # Standard address
    IPv6Address("::1"),  # Loopback
    IPv6Address("::"),  # Unspecified
    IPv6Address("fe80::1ff:fe23:4567:890a"),  # Link-local
    IPv6Address("::ffff:192.0.2.128"),  # IPv4-mapped address
    IPv6Address("::ffff:0.0.0.0"),  # IPv4-mapped zero address
]


# pylint: disable=attribute-defined-outside-init
class TestIPv6:
    """Integration tests for ClickHouse IPv6 data type handling."""

    client: Client
    table_name: str = "ipv6_integration_test"

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
                ip_addr IPv6,
                ip_addr_nullable Nullable(IPv6)
            ) ENGINE = MergeTree ORDER BY id
            """
        )
        yield
        self.client.command(f"DROP TABLE IF EXISTS {self.table_name}")

    def test_ipv6_round_trip(self):
        """Tests that various IPv6 addresses can be inserted as objects and read back correctly."""
        data = [[i, ip, ip] for i, ip in enumerate(IPV6_TEST_CASES)]
        self.client.insert(self.table_name, data)

        result = self.client.query(f"SELECT * FROM {self.table_name} ORDER BY id")

        assert result.row_count == len(IPV6_TEST_CASES)
        for i, ip in enumerate(IPV6_TEST_CASES):
            assert result.result_rows[i][1] == ip
            assert result.result_rows[i][2] == ip
            assert isinstance(result.result_rows[i][1], IPv6Address)

    def test_ipv4_mapping_and_promotion(self):
        """Tests that plain IPv4 strings/objects are correctly promoted to IPv4-mapped
        IPv6 addresses on insertion and read back correctly."""
        test_ips = [
            "198.51.100.1",
            IPv4Address("203.0.113.255"),
            "::ffff:192.168.1.1",
        ]
        expected_ips = [
            IPv6Address("::ffff:198.51.100.1"),
            IPv6Address("::ffff:203.0.113.255"),
            IPv6Address("::ffff:192.168.1.1"),
        ]

        data = [[i, ip, None] for i, ip in enumerate(test_ips)]
        self.client.insert(self.table_name, data)

        result = self.client.query(
            f"SELECT id, ip_addr FROM {self.table_name} ORDER BY id"
        )

        assert result.row_count == len(test_ips)
        for i, ip in enumerate(expected_ips):
            assert isinstance(result.result_rows[i][1], IPv6Address)
            assert result.result_rows[i][1] == ip

    def test_null_handling(self):
        """Tests inserting and retrieving NULL values in an IPv6 column."""
        data = [[1, "::1", None], [2, "2001:db8::", "2001:db8::"]]
        self.client.insert(self.table_name, data)

        result = self.client.query(
            f"SELECT id, ip_addr_nullable FROM {self.table_name} ORDER BY id"
        )

        assert result.row_count == 2
        assert result.result_rows[0][1] is None
        assert result.result_rows[1][1] == IPv6Address("2001:db8::")

    def test_read_as_string(self):
        """Tests reading IPv6 values as strings using the toString() function."""
        ip = IPV6_TEST_CASES[0]
        self.client.insert(self.table_name, [[1, ip, None]])

        result = self.client.query(f"SELECT toString(ip_addr) FROM {self.table_name}")

        assert result.row_count == 1
        read_val = result.result_rows[0][0]
        assert isinstance(read_val, str)
        assert read_val == str(ip)

    def test_insert_invalid_ipv6_fails(self):
        """Tests that the client correctly rejects an invalid IPv6 string."""
        with pytest.raises(ValueError) as excinfo:
            self.client.insert(self.table_name, [[1, "not a valid ip address", None]])

        assert "Failed to parse 'not a valid ip address'" in str(excinfo.value)
