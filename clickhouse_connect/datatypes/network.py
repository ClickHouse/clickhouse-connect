from ipaddress import IPv4Address, IPv6Address
from typing import Collection, Union

from clickhouse_connect.datatypes.registry import ClickHouseType
from clickhouse_connect.datatypes.standard import UInt32


class IPv4(UInt32):
    _array_type = 'I'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        ipv4 = IPv4Address.__new__(IPv4Address)
        ipv4._ip = int.from_bytes(source[loc: loc + 4], 'little')
        return ipv4, loc + 4

    @staticmethod
    def _to_row_binary(value: [int, IPv4Address, str], dest: bytearray):
        if isinstance(value, IPv4Address):
            dest += value.packed
        elif isinstance(value, str):
            dest += bytes(reversed([int(b) for b in value.split('.')]))
        else:
            dest += int.to_bytes(4, value, 'little')

    @staticmethod
    def to_python(column: Collection):
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        app = new_col.append
        for x in column:
            ipv4 = fast_ip_v4(IPv4Address)
            ipv4._ip = x
            app(ipv4)
        return new_col


ipv4_v6_mask = bytes([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF])


class IPv6(ClickHouseType):
    @staticmethod
    def _from_row_binary(self, source: bytearray, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value & 0xFFFF00000000 == 0xFFFF00000000:
            return self._from_v4_output((int_value & 0xFFFFFFFF).to_bytes(4, 'big')), end
        return self._from_v6_output(int_value), end

    @staticmethod
    def _to_row_binary(value: Union[str, IPv4Address, IPv6Address, bytes, bytearray]) -> bytes:
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


