from ipaddress import IPv4Address, IPv6Address
from unittest.mock import Mock, MagicMock, patch
import unittest
from clickhouse_connect.datatypes.base import TypeDef


from clickhouse_connect.datatypes.network import IPv6, IPV4_V6_MASK, V6_NULL


# pylint: disable=protected-access
class TestIPv6DataType(unittest.TestCase):

    def setUp(self):
        """Set up a reusable IPv6 type instance and a mock insert context."""
        self.ipv6_type = IPv6(TypeDef())
        self.mock_context = Mock()
        self.mock_context.column_name = "test_ipv6_col"

    def test_write_ipv4_mapped_ipv6_string(self):
        """Tests that a string for an IPv4-mapped IPv6 address is correctly
        serialized into its proper 16-byte binary format."""
        dest = bytearray()
        ip_string = "::ffff:192.168.1.1"
        column = [ip_string]

        # Expected binary: 12 bytes of 0s, then 0xffff, then the 4 bytes for IP
        expected_bytes = IPV4_V6_MASK + IPv4Address("192.168.1.1").packed
        self.ipv6_type._write_column_binary(column, dest, self.mock_context)
        self.assertEqual(dest, expected_bytes)

    def test_write_plain_ipv4_string_to_ipv6_column(self):
        """Verifies that a plain IPv4 string is correctly promoted to an
        IPv4-mapped IPv6 address during serialization for an IPv6 column."""
        dest = bytearray()
        ip_string = "192.168.1.1"
        column = [ip_string]

        # Expected binary should be the same IPv4-mapped format
        expected_bytes = IPV4_V6_MASK + IPv4Address(ip_string).packed
        self.ipv6_type._write_column_binary(column, dest, self.mock_context)
        self.assertEqual(dest, expected_bytes)

    def test_read_ipv4_mapped_ipv6_binary(self):
        """Tests that when reading 16 bytes representing an IPv4-mapped address,
        the result is a full IPv6Address object, not an IPv4Address object."""
        ip_string = "::ffff:192.168.1.1"

        source_ip = IPv6Address(ip_string)
        mock_source = MagicMock()
        mock_source.read_bytes.return_value = source_ip.packed

        result = self.ipv6_type._read_column_binary(
            mock_source,
            1,
            self.mock_context,
            None,
        )

        self.assertEqual(len(result), 1)
        retrieved_ip = result[0]

        self.assertIsInstance(retrieved_ip, IPv6Address)
        self.assertNotIsInstance(retrieved_ip, IPv4Address)
        self.assertEqual(retrieved_ip, source_ip)

    def test_round_trip_ipv4_mapped_address(self):
        """A full round-trip test to ensure serialization and deserialization work together."""
        ip_string = "::ffff:192.168.1.1"
        column_to_write = [ip_string]
        dest_buffer = bytearray()

        self.ipv6_type._write_column_binary(
            column_to_write, dest_buffer, self.mock_context
        )

        mock_source = MagicMock()
        mock_source.read_bytes.return_value = dest_buffer
        read_result = self.ipv6_type._read_column_binary(
            mock_source,
            1,
            self.mock_context,
            None,
        )
        final_ip = read_result[0]

        self.assertIsInstance(final_ip, IPv6Address)
        self.assertEqual(str(final_ip), ip_string)

    def test_round_trip_standard_ipv6(self):
        """Ensures a standard IPv6 address can be written and read back correctly."""
        ip_v6 = IPv6Address("2001:db8::dead:beef")
        column_to_write = [ip_v6]
        dest_buffer = bytearray()

        self.ipv6_type._write_column_binary(
            column_to_write,
            dest_buffer,
            self.mock_context,
        )
        self.assertEqual(dest_buffer, ip_v6.packed)

        self.mock_context.read_format.return_value = "native"
        mock_source = MagicMock()
        mock_source.read_bytes.return_value = dest_buffer
        read_result = self.ipv6_type._read_column_binary(
            mock_source, 1, self.mock_context, None
        )

        self.assertEqual(read_result[0], ip_v6)

    def test_read_binary_as_string_format(self):
        """Tests that binary IPv6 data is correctly read as a string when the
        context format is 'string'."""
        ip_string = "2001:db8::1"
        source_ip = IPv6Address(ip_string)
        mock_source = MagicMock()
        mock_source.read_bytes.return_value = source_ip.packed

        with patch.object(self.ipv6_type, "read_format", return_value="string"):
            result = self.ipv6_type._read_column_binary(
                mock_source, 1, self.mock_context, None
            )

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], str)
        self.assertEqual(result[0], ip_string)

    def test_write_native_ip_address_objects(self):
        """Tests that native IPv4Address and IPv6Address objects are serialized correctly."""
        dest = bytearray()
        ipv4_obj = IPv4Address("192.0.2.1")
        ipv6_obj = IPv6Address("2001:db8::1")
        column = [ipv4_obj, ipv6_obj]

        expected = bytearray()
        expected += IPV4_V6_MASK + ipv4_obj.packed
        expected += ipv6_obj.packed

        self.ipv6_type._write_column_binary(column, dest, self.mock_context)
        self.assertEqual(dest, expected)

    def test_write_none_value(self):
        """Tests that a None value in the column is serialized to the 16-byte null representation."""
        dest = bytearray()
        column = [None]

        expected_bytes = b"\x00" * 16

        self.ipv6_type._write_column_binary(column, dest, self.mock_context)
        self.assertEqual(dest, expected_bytes)
        self.assertEqual(V6_NULL, expected_bytes)

    def test_write_invalid_ip_string_raises_error(self):
        """Tests that a ValueError is raised when trying to write an invalid IP address string."""
        dest = bytearray()
        column = ["not an ip address"]

        with self.assertRaises(ValueError) as e:
            self.ipv6_type._write_column_binary(column, dest, self.mock_context)

        self.assertIn("Failed to parse", str(e.exception))
        self.assertIn("test_ipv6_col", str(e.exception))


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
