import struct
import uuid

from datetime import datetime, timezone
from ipaddress import IPv4Address, IPv6Address
from typing import List

from clickhouse_connect.driver.rowbinary import string_leb128, parse_leb128
from clickhouse_connect.datatypes.registry import ClickHouseType, TypeDef, get_from_name


class Int(ClickHouseType):
    __slots__ = 'size',
    signed = True

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.size
        self.size = type_def.size // 8

    def _from_row_binary(self, source: bytearray, loc: int):
        return int.from_bytes(source[loc:loc + self.size], 'little', signed=self.signed), loc + self.size


class UInt(Int):
    signed = False


class Float(ClickHouseType):
    __slots__ = 'size', 'pack'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.size
        self.size = type_def.size // 8
        self.pack = 'd' if self.size == 8 else 'f'

    def _from_row_binary(self, source: bytearray, loc: int):
        return struct.unpack(self.pack, source[loc:loc + self.size])[0], loc + self.size


class DateTime(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        epoch = int.from_bytes(source[loc:loc + 4], 'little', signed=False)
        return datetime.fromtimestamp(epoch, timezone.utc), loc + 4


class Date(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little', signed=False)
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2


class Enum(Int):
    __slots__ = '_name_map', '_int_map'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = {key: value for key, value in zip(type_def.keys, type_def.values)}
        self._int_map = {value: key for key, value in zip(type_def.keys, type_def.values)}
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self.name_suffix = f'{type_def.size}({val_str})'

    def _from_row_binary(self, source: bytearray, loc: int):
        value, loc = super()._from_row_binary(source, loc)
        return self._int_map[value], loc


class String(ClickHouseType):
    _from_row_binary = staticmethod(string_leb128)


class FixedString(ClickHouseType):
    __slots__ = 'size',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.size = type_def.values[0]
        self.name_suffix = f'({self.size})'

    # TODO:  Pick a configuration mechanism to control whether we return a str, bytes, or bytearray for FixedString
    #        value(s) in a query response.  For now make it a str
    def _from_row_binary(self, source: bytearray, loc: int):
        return source[loc:loc + self.size].decode(), loc + self.size


class UUID(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        int_high = int.from_bytes(source[loc:loc + 8], 'little')
        int_low = int.from_bytes(source[loc + 8:loc + 16], 'little')
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return uuid.UUID(bytes=byte_value), loc + 16


class Boolean(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return source[loc] > 0, loc + 1


class Bool(Boolean):
    pass


class IPv4(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return str(IPv4Address(int.from_bytes(source[loc:loc + 4], 'little'))), loc + 4


class IPv6(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        end = loc + 16
        int_value = int.from_bytes(source[loc:end], 'big')
        if int_value & 0xFFFF00000000 == 0xFFFF00000000:
            return str(IPv4Address(int_value & 0xFFFFFFFF)), end
        return str(IPv6Address(int.from_bytes(source[loc:end], 'big'))), end


class Array(ClickHouseType):
    __slots__ = 'nested',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nested = get_from_name(type_def.values[0])
        self.name_suffix = f'({self.nested.name})'

    def _from_row_binary(self, source: bytearray, loc: int):
        size, loc = parse_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.nested.from_row_binary(source, loc)
            values.append(value)
        return values, loc


class Tuple(ClickHouseType):
    _slots = 'nested',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nested: List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self.name_suffix = f"({','.join([t.name for t in self.nested])})"

    def _from_row_binary(self, source: bytearray, loc: int):
        values = []
        for t in self.nested:
            value, loc = t.from_row_binary(source, loc)
            values.append(value)
        return tuple(values), loc
