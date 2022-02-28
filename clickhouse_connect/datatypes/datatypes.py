import struct
import uuid
import decimal
import pytz

from typing import List
from datetime import date, datetime, timezone, timedelta
from ipaddress import IPv4Address, IPv6Address

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


class Float32(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return struct.unpack('f', source[loc:loc + 4])[0], loc + 4


class Float64(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return struct.unpack('d', source[loc:loc + 8])[0], loc + 8


class DateTime(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        epoch = int.from_bytes(source[loc:loc + 4], 'little')
        return datetime.fromtimestamp(epoch, timezone.utc), loc + 4


class Date(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little')
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2


class Date32(ClickHouseType):
    start_date = date(1970, 1, 1)

    def _from_row_binary(self, source, loc):
        days = int.from_bytes(source[loc:loc + 4], 'little', signed=True)
        return self.start_date + timedelta(days), loc + 4


class DateTime64(ClickHouseType):
    __slots__ = 'prec', 'tzinfo'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.arg_str
        self.prec = 10 ** type_def.values[0]
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1])

    def _from_row_binary(self, source, loc):
        ticks = int.from_bytes(source[loc:loc + 8], 'little', signed=True)
        seconds = ticks // self.prec
        dt_sec =  datetime.fromtimestamp(seconds, self.tzinfo)
        microseconds = ((ticks - seconds * self.prec) * 1000000) // self.prec
        return dt_sec + timedelta(microseconds=microseconds), loc + 8


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


class Decimal(ClickHouseType):
    __slots__ = 'size', 'prec'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        size = type_def.size
        if size == 0:
            self.name_suffix = type_def.arg_str
            prec = type_def.values[0]
            self.prec = type_def.values[1]
            if prec < 1 or prec > 79:
                raise ArithmeticError("Invalid precision {prec} for ClickHouse Decimal type")
            if prec < 10:
                size = 32
            elif prec < 19:
                size = 64
            elif prec < 39:
                size = 128
            else:
                size = 256
        else:
            self.prec = type_def.values[0]
            self.name_suffix = f'{type_def.size}({self.prec})'
        self.size = size // 8

    def _from_row_binary(self, source, loc):
        neg = ''
        unscaled = int.from_bytes(source[loc:loc + self.size], 'little')
        if unscaled <= 0:
            neg = '-'
            unscaled = -unscaled
        digits = str(unscaled)
        return decimal.Decimal(f'{neg}{digits[:-self.prec]}.{digits[-self.prec:]}'), loc + self.size


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


class Nothing(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return None, loc


class Array(ClickHouseType):
    __slots__ = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[0])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        size, loc = parse_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.element_type.from_row_binary(source, loc)
            values.append(value)
        return values, loc


class Tuple(ClickHouseType):
    _slots = 'member_types',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.member_types: List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        values = []
        for t in self.member_types:
            value, loc = t.from_row_binary(source, loc)
            values.append(value)
        return tuple(values), loc


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.key_type: ClickHouseType = get_from_name(type_def.values[0])
        self.value_type: ClickHouseType = get_from_name(type_def.values[1])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        size, loc = parse_leb128(source, loc)
        values = {}
        for x in range(size):
            key, loc = self.key_type.from_row_binary(source, loc)
            value, loc = self.value_type.from_row_binary(source, loc)
            values[key] = value
        return values, loc
