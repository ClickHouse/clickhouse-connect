import socket
from functools import partial
from ipaddress import IPv4Address, IPv6Address
from typing import Union, MutableSequence, Sequence

from clickhouse_connect.datatypes.base import FixedType

ipv4_v6_mask = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'


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
    def _to_python_ip(column: Sequence) -> MutableSequence:
        fast_ip_v4 = IPv4Address.__new__
        new_col = []
        app = new_col.append
        for x in column:
            ipv4 = fast_ip_v4(IPv4Address)
            ipv4._ip = x
            app(ipv4)
        return new_col

    @staticmethod
    def _to_python_str(column: Sequence) -> MutableSequence:
        return [socket.inet_ntoa(x.to_bytes(4, 'big')) for x in column]

    def _from_python(self, column: Sequence) -> Sequence:
        first = self._first_value(column)
        if first is None or isinstance(first, int):
            return column
        if isinstance(first, str):
            fixed = 24, 16, 8, 0
            return [sum(int(b) << fixed[ix] for ix, b in enumerate(v.split('.'))) for v in column]
        return [ip._ip for ip in column]

    _to_python = _to_python_ip

    @classmethod
    def format(cls, fmt: str):
        if fmt == 'string':
            cls._to_python = staticmethod(cls._to_python_str)
        else:
            cls._to_python = staticmethod(cls._to_python_ip)


class IPv6(FixedType):
    _byte_size = 16

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
        mask = ipv4_v6_mask
        if isinstance(value, str):
            if '.' in value:
                dest += mask + bytes(int(b) for b in value.split('.'))
            else:
                dest += socket.inet_pton(socket.AF_INET6, value)
        elif isinstance(value, IPv4Address):
            dest += mask + value._ip.to_bytes(4, 'big')
        elif isinstance(value, IPv6Address):
            dest += value.packed
        elif len(value) == 4:
            dest += ipv4_v6_mask + value
        else:
            dest += value

    @staticmethod
    def _to_python_ip(column: Sequence) -> MutableSequence:
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

    @staticmethod
    def _to_python_str(column: Sequence) -> MutableSequence:
        ipv4_v6_mask = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'
        tov4 = socket.inet_ntoa
        tov6 = partial(socket.inet_ntop, af = socket.AF_INET6)
        new_col = []
        app = new_col.append
        for x in column:
            if x[:12] == ipv4_v6_mask:
                app(tov4(x[12:]))
            else:
                app(tov6(x))
        return new_col

    def _from_python(self, column: Sequence) -> Sequence:
        first = self._first_value(column)
        mask = ipv4_v6_mask
        if isinstance(first, str):
            tov6 = partial(socket.inet_pton, af = socket.AF_INET6)
            new_col = []
            app = new_col.append
            for v in column:
                if '.' in v:
                    app(mask + bytes(int(b) for b in v.split('.')))
                else:
                    app(tov6(v))
            return new_col
        if isinstance(first, IPv4Address) or isinstance(first, IPv6Address):
            new_col = []
            app = new_col.append
            for v in column:
                b = v.packed
                app(b if len(b) == 16 else mask + b)
            return new_col
        return column

    _to_python = _to_python_ip

    @classmethod
    def format(cls, fmt: str):
        if fmt == 'string':
            cls._to_python = staticmethod(cls._to_python_str)
        else:
            cls._to_python = staticmethod(cls._to_python_ip)
