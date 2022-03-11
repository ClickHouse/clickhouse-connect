from struct import unpack_from as suf
from ipaddress import IPv4Address, IPv6Address
from typing import Union

from clickhouse_connect.datatypes.registry import ClickHouseType


def _byte_output(b: bytearray):
    return bytes(b)


def _ipv4_str_output(b: bytearray):
    return str(IPv4Address(b))


def _ipv4_ip_output(b: bytearray):
    return IPv4Address(b)


class IPv4(ClickHouseType):
    _from_output = staticmethod(_ipv4_ip_output)

    def _from_row_binary(self, source: bytearray, loc: int):
        return self._from_output(suf('<L', source, loc)[0]), loc + 4

    @staticmethod
    def _to_row_binary(value: Union[str, bytes, IPv4Address], dest: bytearray):
        if isinstance(value, str) or isinstance(value, bytes):
            dest += IPv4Address(value).packed
        else:
            dest += value.packed


def _ipv6_str_output(n: int):
    return str(IPv6Address(n))


def _ipv6_ip_output(n: int):
    return IPv6Address(n)


def _int_to_byte_output(n: int):
    return n.to_bytes(16, 'big')


ipv4_v6_mask = bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF])


class IPv6(ClickHouseType):
    _from_v6_output = staticmethod(_ipv6_ip_output)
    _from_v4_output = staticmethod(_ipv4_ip_output)

    def _from_row_binary(self, source: bytearray, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value & 0xFFFF00000000 == 0xFFFF00000000:
            return self._from_v4_output((int_value & 0xFFFFFFFF).to_bytes(4, 'big')), end
        return self._from_v6_output(int_value), end

    def _to_row_binary(self, value: Union[str, IPv4Address, IPv6Address, bytes, bytearray]) -> bytes:
        if isinstance(value, str):
            if '.' in value:
                return ipv4_v6_mask + IPv4Address(value).packed
            return IPv6Address(value).packed
        if isinstance(value, IPv4Address):
            return ipv4_v6_mask + value.packed
        if isinstance(value, IPv6Address):
            return value.packed
        if len(value) == 4:
            return ipv4_v6_mask + value
        assert len(value) == 16
        return value


def ip_format(fmt: str):
    if fmt.startswith('ip'):
        IPv4._from_output = staticmethod(_ipv4_ip_output)
        IPv6._from_v4_output = staticmethod(_ipv4_ip_output)
        IPv6._from_v6_output = staticmethod(_ipv6_ip_output)
    elif fmt.startswith('b'):
        IPv4._from_output = staticmethod(_byte_output)
        IPv6._from_v4_output = staticmethod(_byte_output)
        IPv6._from_v6_output = staticmethod(_int_to_byte_output)
    elif fmt.startswith('s'):
        IPv4._from_output = staticmethod(_ipv4_str_output)
        IPv6._from_v4_output = staticmethod(_ipv4_str_output)
        IPv6._from_v6_output = staticmethod(_ipv6_str_output)
