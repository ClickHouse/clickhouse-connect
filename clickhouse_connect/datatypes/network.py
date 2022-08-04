import socket
from ipaddress import IPv4Address, IPv6Address
from typing import Union, MutableSequence, Sequence

from clickhouse_connect.datatypes.base import ArrayType, ClickHouseType
from clickhouse_connect.driver.common import write_array, array_column

IPV4_V6_MASK = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff'
V6_NULL = bytes(b'\x00' * 16)
V4_NULL = IPv4Address(0)


# pylint: disable=protected-access
class IPv4(ArrayType):
    _array_type = 'I'
    valid_formats = 'string', 'native'

    @property
    def python_type(self):
        return str if self.read_format() == 'string' else IPv4Address

    @property
    def np_type(self):
        return 'U' if self.read_format() == 'string' else 'O'

    @property
    def python_null(self):
        return '' if self.read_format() == 'string' else V4_NULL

    def _from_row_binary(self, source: bytes, loc: int):
        ipv4 = IPv4Address.__new__(IPv4Address)
        ipv4._ip = int.from_bytes(source[loc: loc + 4], 'little')
        return ipv4, loc + 4

    def _to_row_binary(self, value: [int, IPv4Address, str], dest: bytearray):
        if isinstance(value, IPv4Address):
            dest += value._ip.to_bytes(4, 'little')
        elif isinstance(value, str):
            dest += bytes(reversed([int(b) for b in value.split('.')]))
        else:
            dest += value.to_bytes(4, 'little')

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        if self.read_format() == 'string':
            return self._from_native_str(source, loc, num_rows)
        return self._from_native_ip(source, loc, num_rows)

    def _from_native_ip(self, source: Sequence, loc: int, num_rows: int):
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

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        first = self._first_value(column)
        if isinstance(first, str):
            fixed = 24, 16, 8, 0
            # pylint: disable=consider-using-generator
            column = [(sum([int(b) << fixed[ix] for ix, b in enumerate(x.split('.'))])) if x else 0 for x in column]
        else:
            if self.nullable:
                column = [x._ip if x else 0 for x in column]
            else:
                column = [x._ip for x in column]
        write_array(self._array_type, column, dest)


# pylint: disable=protected-access
class IPv6(ClickHouseType):
    valid_formats = 'string', 'native'

    @property
    def python_type(self):
        return str if self.read_format() == 'string' else IPv6Address

    @property
    def np_type(self):
        return 'U' if self.read_format() == 'string' else 'O'

    @property
    def python_null(self):
        return '' if self.read_format() == 'string' else V6_NULL

    def _from_row_binary(self, source: Sequence, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value >> 32 == 0xFFFF:
            ipv4 = IPv4Address.__new__(IPv4Address)
            ipv4._ip = int_value & 0xFFFFFFFF
            return ipv4, end
        return IPv6Address(int_value), end

    def _to_row_binary(self, value: Union[str, IPv4Address, IPv6Address, bytes, bytearray], dest: bytearray):
        v4mask = IPV4_V6_MASK
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
            dest += IPV4_V6_MASK + value
        else:
            dest += value

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        if self.read_format() == 'string':
            return self._read_native_str(source, loc, num_rows)
        return self._read_native_ip(source, loc, num_rows)

    @staticmethod
    def _read_native_ip(source: Sequence, loc: int, num_rows: int):
        fast_ip_v6 = IPv6Address.__new__
        fast_ip_v4 = IPv4Address.__new__
        with_scope_id = '_scope_id' in IPv6Address.__slots__
        new_col = []
        app = new_col.append
        ifb = int.from_bytes
        end = loc + (num_rows << 4)
        for ix in range(loc, end, 16):
            int_value = ifb(source[ix: ix + 16], 'big')
            if int_value >> 32 == 0xFFFF:
                ipv4 = fast_ip_v4(IPv4Address)
                ipv4._ip = int_value & 0xFFFFFFFF
                app(ipv4)
            else:
                ipv6 = fast_ip_v6(IPv6Address)
                ipv6._ip = int_value
                if with_scope_id:
                    ipv6._scope_id = None
                app(ipv6)
        return new_col, end

    @staticmethod
    def _read_native_str(source: Sequence, loc: int, num_rows: int):
        new_col = []
        app = new_col.append
        v4mask = IPV4_V6_MASK
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

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        v = V6_NULL
        first = self._first_value(column)
        v4mask = IPV4_V6_MASK
        af6 = socket.AF_INET6
        tov6 = socket.inet_pton
        if isinstance(first, str):
            for x in column:
                if x is None:
                    dest += v
                elif '.' in x:
                    dest += v4mask + bytes(int(b) for b in x.split('.'))
                else:
                    dest += tov6(af6, x)
        else:
            for x in column:
                if x is None:
                    dest += v
                else:
                    b = x.packed
                    dest += b if len(b) == 16 else (v4mask + b)
