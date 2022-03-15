from struct import unpack_from as suf
from ipaddress import IPv4Address, IPv6Address
from typing import Union

from clickhouse_connect.datatypes.registry import ClickHouseType
from clickhouse_connect.datatypes.standard import UInt32


class IPv4(UInt32):
    pass


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


