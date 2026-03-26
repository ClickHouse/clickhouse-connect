from ipaddress import IPv4Address, IPv6Address
from typing import Callable

import pytest

from clickhouse_connect.driver import Client

# A collection of diverse IPv6 addresses for testing
IPV6_TEST_CASES = [
    IPv6Address("2001:db8:85a3::8a2e:370:7334"),  # Standard address
    IPv6Address("::1"),  # Loopback
    IPv6Address("::"),  # Unspecified
    IPv6Address("fe80::1ff:fe23:4567:890a"),  # Link-local
    IPv6Address("::ffff:192.0.2.128"),  # IPv4-mapped address
    IPv6Address("::ffff:0.0.0.0"),  # IPv4-mapped zero address
]


def test_ipv6_round_trip(param_client: Client, call, table_context: Callable):
    """Test that various IPv6 addresses can be inserted as objects and read back correctly."""
    with table_context("ipv6_round_trip_test", ["id UInt32", "ip_addr IPv6", "ip_addr_nullable Nullable(IPv6)"], order_by="id"):
        data = [[i, ip, ip] for i, ip in enumerate(IPV6_TEST_CASES)]
        call(param_client.insert, "ipv6_round_trip_test", data)

        result = call(param_client.query, "SELECT * FROM ipv6_round_trip_test ORDER BY id")

        assert result.row_count == len(IPV6_TEST_CASES)
        for i, ip in enumerate(IPV6_TEST_CASES):
            assert result.result_rows[i][1] == ip
            assert result.result_rows[i][2] == ip
            assert isinstance(result.result_rows[i][1], IPv6Address)

def test_ipv4_mapping_and_promotion(param_client: Client, call, table_context: Callable):
    """Test that plain IPv4 strings/objects are correctly promoted to IPv4-mapped IPv6 addresses."""
    with table_context("ipv4_promotion_test", ["id UInt32", "ip_addr IPv6", "ip_addr_nullable Nullable(IPv6)"], order_by="id"):
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
        call(param_client.insert, "ipv4_promotion_test", data)

        result = call(param_client.query, "SELECT id, ip_addr FROM ipv4_promotion_test ORDER BY id")

        assert result.row_count == len(test_ips)
        for i, ip in enumerate(expected_ips):
            assert isinstance(result.result_rows[i][1], IPv6Address)
            assert result.result_rows[i][1] == ip

def test_ipv6_null_handling(param_client: Client, call, table_context: Callable):
    """Test inserting and retrieving NULL values in an IPv6 column."""
    with table_context("ipv6_null_test", ["id UInt32", "ip_addr IPv6", "ip_addr_nullable Nullable(IPv6)"], order_by="id"):
        data = [[1, "::1", None], [2, "2001:db8::", "2001:db8::"]]
        call(param_client.insert, "ipv6_null_test", data)

        result = call(param_client.query, "SELECT id, ip_addr_nullable FROM ipv6_null_test ORDER BY id")

        assert result.row_count == 2
        assert result.result_rows[0][1] is None
        assert result.result_rows[1][1] == IPv6Address("2001:db8::")

def test_ipv6_read_as_string(param_client: Client, call, table_context: Callable):
    """Test reading IPv6 values as strings using the toString() function."""
    with table_context("ipv6_string_test", ["id UInt32", "ip_addr IPv6", "ip_addr_nullable Nullable(IPv6)"], order_by="id"):
        ip = IPV6_TEST_CASES[0]
        call(param_client.insert, "ipv6_string_test", [[1, ip, None]])

        result = call(param_client.query, "SELECT toString(ip_addr) FROM ipv6_string_test")

        assert result.row_count == 1
        read_val = result.result_rows[0][0]
        assert isinstance(read_val, str)
        assert read_val == str(ip)

def test_ipv6_insert_invalid_fails(param_client: Client, call, table_context: Callable):
    """Test that the client correctly rejects an invalid IPv6 string."""
    with table_context("ipv6_invalid_test", ["id UInt32", "ip_addr IPv6", "ip_addr_nullable Nullable(IPv6)"], order_by="id"):
        with pytest.raises(ValueError) as excinfo:
            call(param_client.insert, "ipv6_invalid_test", [[1, "not a valid ip address", None]])

        assert "Failed to parse 'not a valid ip address'" in str(excinfo.value)
