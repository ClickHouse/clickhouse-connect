import socket
from ipaddress import IPv4Address, IPv6Address
from typing import Union, MutableSequence, Sequence

from clickhouse_connect.datatypes.base import ArrayType, ClickHouseType
from clickhouse_connect.datatypes.common import write_array, array_column

ipv4_v6_mask = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'
v6_null = bytes(b'\x00' * 16)


class IPv4(ArrayType):
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

    def _from_native_ip(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        app = new_col.append
        for x in column:
            ipv4 = fast_ip_v4(IPv4Address)
            ipv4._ip = x
            app(ipv4)
        return new_col, loc

    def _from_native_str(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        return [socket.inet_ntoa(x.to_bytes(4, 'big')) for x in column], loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        first = self._first_value(column)
        if isinstance(first, str):
            fixed = 24, 16, 8, 0
            column = [(sum([int(b) << fixed[ix] for ix, b in enumerate(x.split('.'))])) if x else 0 for x in column]
        else:
            if self.nullable:
                column = [x._ip if x else 0 for x in column]
            else:
                column = [x._ip for x in column]
        write_array(self._array_type, column, dest)

    _from_native = _from_native_ip

    @classmethod
    def format(cls, fmt: str):
        if fmt == 'string':
            cls._from_native = cls._from_native_str
        else:
            cls._from_native = cls._from_native_ip


class IPv6(ClickHouseType):
    @property
    def ch_null(self):
        return v6_null

    @staticmethod
    def _from_row_binary(source: Sequence, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value & 0xFFFF00000000 == 0xFFFF00000000:
            ipv4 = IPv4Address.__new__(IPv4Address)
            ipv4._ip = int_value & 0xFFFFFFFF
            return ipv4, end
        return IPv6Address(int_value), end

    @staticmethod
    def _to_row_binary(value: Union[str, IPv4Address, IPv6Address, bytes, bytearray], dest: bytearray):
        v4mask = ipv4_v6_mask
        if isinstance(value, str):
            if '.' in value:
                dest += v4mask + bytes(int(b) for b in value.split('.'))
            else:
                dest += socket.inet_pton(socket.AF_INET6, value)
        elif isinstance(value, IPv4Address):
            dest += v4mask + value._ip.to_bytes(4, 'big')
        elif isinstance(value, IPv6Address):
            dest += value.packed
        elif len(value) == 4:
            dest += ipv4_v6_mask + value
        else:
            dest += value

    @staticmethod
    def _from_native_ip(source: Sequence, loc: int, num_rows: int, **_):
        fast_ip_v6 = IPv6Address.__new__
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        app = new_col.append
        ifb = int.from_bytes
        end = loc + (num_rows << 4)
        for ix in range(loc, end, 16):
            int_value = ifb(source[ix: ix + 16], 'big')
            if int_value & 0xFFFF00000000 == 0xFFFF00000000:
                ipv4 = fast_ip_v4(IPv4Address)
                ipv4._ip = int_value & 0xFFFFFFFF
                app(ipv4)
            else:
                ipv6 = fast_ip_v6(IPv6Address)
                ipv6._ip = int_value
                ipv6._scope_id = None
                app(ipv6)
        return new_col, end

    @staticmethod
    def _from_native_str(source: Sequence, loc: int, num_rows: int, **_):
        new_col = []
        app = new_col.append
        v4mask = ipv4_v6_mask
        tov4 = socket.inet_ntoa
        tov6 = socket.inet_ntop
        af6 = socket.AF_INET6
        end = loc + (num_rows << 4)
        for ix in range(loc, end, 16):
            x = source[ix: ix + 16]
            if x[:12] == v4mask:
                app(tov4(x[12:]))
            else:
                app(tov6(af6, x))
        return new_col, end

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        nv = v6_null
        first = self._first_value(column)
        v4mask = ipv4_v6_mask
        af6 = socket.AF_INET6
        tov6 = socket.inet_pton
        if isinstance(first, str):
            for x in column:
                if x is None:
                    dest += nv
                elif '.' in x:
                    dest += v4mask + bytes(int(b) for b in x.split('.'))
                else:
                    dest += tov6(af6, x)
        elif isinstance(first, (IPv4Address, IPv6Address)):
            for x in column:
                if x is None:
                    dest += nv
                else:
                    b = x.packed
                    dest += b if len(b) == 16 else (v4mask + b)

    _from_native = _from_native_ip

    @classmethod
    def format(cls, fmt: str):
        if fmt == 'string':
            cls._from_native = staticmethod(cls._from_native_str)
        else:
            cls._from_native = staticmethod(cls._from_native_ip)
