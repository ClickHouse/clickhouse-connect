from ipaddress import IPv4Address, IPv6Address
from typing import Collection, Union, MutableSequence, Sequence

from clickhouse_connect.datatypes.base import FixedType


class IPv4(FixedType):
    _array_type = 'I'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        ipv4 = IPv4Address.__new__(IPv4Address)
        ipv4._ip = int.from_bytes(source[loc: loc + 4], 'little')
        return ipv4, loc + 4

    @staticmethod
    def _to_row_binary(value: [int, IPv4Address, str], dest: bytearray):
        if isinstance(value, IPv4Address):
            dest += value._ip.to_bytes(4, 'little')
        elif isinstance(value, str):
            dest += bytes(reversed([int(b) for b in value.split('.')]))
        else:
            dest += value.to_bytes(4, 'little')

    @staticmethod
    def _to_python(column: Sequence) -> MutableSequence:
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        app = new_col.append
        for x in column:
            ipv4 = fast_ip_v4(IPv4Address)
            ipv4._ip = x
            app(ipv4)
        return new_col


ipv4_v6_mask = bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF])


class IPv6(FixedType):
    _byte_size = 16

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value & 0xFFFF00000000 == 0xFFFF00000000:
            ipv4 = IPv4Address.__new__(IPv4Address)
            ipv4._ip = int_value & 0xFFFFFFFF
            return ipv4, end
        return IPv6Address(int_value), end

    @staticmethod
    def _to_row_binary(value: Union[str, IPv4Address, IPv6Address, bytes, bytearray], dest: bytearray):
        if isinstance(value, str):
            if '.' in value:
                dest += ipv4_v6_mask + bytes(reversed([int(b) for b in value.split('.')]))
            else:
                dest += IPv6Address(value).packed
        elif isinstance(value, IPv4Address):
            dest += ipv4_v6_mask + value._ip.to_bytes(4, 'big')
        elif isinstance(value, IPv6Address):
            dest += value.packed
        elif len(value) == 4:
            dest += ipv4_v6_mask + value
        else:
            assert len(value) == 16
            dest += value

    @staticmethod
    def _to_python(column: Collection[bytes]):
        fast_ip_v6 = IPv6Address.__new__
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        for x in column:
            int_value = int.from_bytes(x, 'big')
            if int_value & 0xFFFF00000000 == 0xFFFF00000000:
                ipv4 = fast_ip_v4(IPv4Address)
                ipv4._ip = int_value & 0xFFFFFFFF
                new_col.append(ipv4)
            else:
                ipv6 = fast_ip_v6(IPv6Address)
                ipv6._ip = int_value
                ipv6._scope_id = None
                new_col.append(ipv6)
        return new_col


def ip_format(fmt):
    pass