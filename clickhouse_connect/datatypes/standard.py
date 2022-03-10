import struct
import uuid
import decimal
import pytz

from typing import Any
from datetime import date, datetime, timezone, timedelta

from clickhouse_connect.driver.rowbinary import read_leb128, to_leb128
from clickhouse_connect.datatypes.registry import ClickHouseType, TypeDef


from_ts = datetime.utcfromtimestamp


class Int(ClickHouseType):
    __slots__ = 'size',
    signed = True

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.size
        self.size = type_def.size // 8

    def _from_row_binary(self, source: bytes, loc: int):
        return int.from_bytes(source[loc:loc + self.size], 'little', signed=self.signed), loc + self.size

    def _to_row_binary(self, value: int) -> bytes:
        return value.to_bytes(self.size, 'little', signed=self.signed)


class Int16(ClickHouseType):
    _from_row_binary = staticmethod(lambda source, loc: (int.from_bytes(source[loc: loc + 2], 'little', signed=True), loc + 2))
    _to_row_binary = staticmethod(lambda value: value.to_bytes(2, 'little', signed=True))


class UInt8(ClickHouseType):
    _from_row_binary = staticmethod(lambda source, loc: (source[loc], loc + 1))
    _to_row_binary = staticmethod(lambda value: [value])


class UInt16(ClickHouseType):
    _from_row_binary = staticmethod(lambda source, loc: (int.from_bytes(source[loc: loc + 2], 'little'), loc + 2))
    _to_row_binary = staticmethod(lambda value: value.to_bytes(2, 'little'))


class UInt32(ClickHouseType):
    _from_row_binary = staticmethod(lambda source, loc: (int.from_bytes(source[loc: loc + 4], 'little'), loc + 4))
    _to_row_binary = staticmethod(lambda value: value.to_bytes(4, 'little'))


class UInt(Int):
    signed = False


class UInt64(ClickHouseType):
    signed = False

    def _from_row_binary(self, source: bytearray, loc: int):
        return int.from_bytes(source[loc:loc + 8], 'little', signed=self.signed), loc + 8

    def _to_row_binary(self, value: int) -> bytes:
        return value.to_bytes(8, 'little', signed=self.signed)


class Float32(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return struct.unpack_from('f', source, loc)[0], loc + 4

    def _to_row_binary(self, value: float) -> bytes:
        return struct.pack('f', (value,))


class Float64(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return struct.unpack_from('d', source, loc)[0], loc + 8

    def _to_row_binary(self, value: float) -> bytes:
        return struct.pack('d', (value,))


class DateTime(ClickHouseType):
    __slots__ = 'tzinfo', '_from_row_binary'

    def __init__(self, type_def: TypeDef):
        if type_def.values:
            self.tzinfo = pytz.timezone(type_def.values[0][1:-1])
        else:
            self.tzinfo = None
            self._from_row_binary = lambda source, loc: (from_ts(struct.unpack_from('<I', source, loc)[0]), loc + 4)
        super().__init__(type_def)

    #def _from_row_binary(self, source: bytearray, loc: int):
        #epoch, = struct.unpack_from('<I', source, loc)
        #epoch = int.from_bytes(source[loc:loc + 4], 'little')
        #return datetime.fromtimestamp(epoch), loc + 4

    def _to_row_binary(self, value: datetime) -> bytes:
        return int(value.timestamp()).to_bytes(4, 'little', signed=True)


class Date(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little')
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2

    def _to_row_binary(self, value: datetime) -> bytes:
        return (int(value.timestamp()) // 86400).to_bytes(2, 'little', signed=True)


epoch_start_date = date(1970, 1, 1)


class Date32(ClickHouseType):
    def _from_row_binary(self, source, loc):
        days = int.from_bytes(source[loc:loc + 4], 'little', signed=True)
        return epoch_start_date + timedelta(days), loc + 4

    def _to_row_binary(self, value: date) -> bytes:
        return (value - epoch_start_date).days.to_bytes(4, 'little', signed=True)


class DateTime64(ClickHouseType):
    __slots__ = 'prec', 'tzinfo'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.arg_str
        self.prec = 10 ** type_def.values[0]
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
        else:
            self.tzinfo = timezone.utc

    def _from_row_binary(self, source, loc):
        ticks = int.from_bytes(source[loc:loc + 8], 'little', signed=True)
        seconds = ticks // self.prec
        dt_sec = datetime.fromtimestamp(seconds, self.tzinfo)
        microseconds = ((ticks - seconds * self.prec) * 1000000) // self.prec
        return dt_sec + timedelta(microseconds=microseconds), loc + 8

    def _to_row_binary(self, value: datetime) -> bytes:
        microseconds = int(value.timestamp()) * 1000000 + value.microsecond
        return (int(microseconds * 1000000) // self.prec).to_bytes(8, 'little', signed=True)


class String(ClickHouseType):
    _encoding = 'utf8'

    def _from_row_binary(self, source, loc):
        length, loc = read_leb128(source, loc)
        return source[loc:loc + length].decode(self._encoding), loc + length

    def _to_row_binary(self, value: str) -> bytes:
        value = bytes(value, self._encoding)
        return to_leb128(len(value)) + value


class UUID(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        int_high = int.from_bytes(source[loc:loc + 8], 'little')
        int_low = int.from_bytes(source[loc + 8:loc + 16], 'little')
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return uuid.UUID(bytes=byte_value), loc + 16

    def _to_row_binary(self, value: uuid.UUID) -> bytes:
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        return bytes_high + bytes_low


class Boolean(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return source[loc] > 0, loc + 1

    def _to_row_binary(self, value: bool) -> bytes:
        return b'\x01' if value else b'\x00'


class Bool(Boolean):
    pass


class Decimal(ClickHouseType):
    __slots__ = 'size', 'prec', 'zeros', 'mult'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        size = type_def.size
        if size == 0:
            self.name_suffix = type_def.arg_str
            prec = type_def.values[0]
            self.prec = type_def.values[1]
            if prec < 1 or prec > 79:
                raise ArithmeticError(f"Invalid precision {prec} for ClickHouse Decimal type")
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
        self.mult = 10 ** self.prec
        self.zeros = bytes([0] * self.size)

    def _from_row_binary(self, source, loc):
        neg = ''
        unscaled = int.from_bytes(source[loc:loc + self.size], 'little')
        if unscaled <= 0:
            neg = '-'
            unscaled = -unscaled
        digits = str(unscaled)
        return decimal.Decimal(f'{neg}{digits[:-self.prec]}.{digits[-self.prec:]}'), loc + self.size

    def _to_row_binary(self, value: Any) -> bytes:
        if isinstance(value, int) or isinstance(value, float) or (
                isinstance(value, decimal.Decimal) and value.is_finite()):
            return int(value * self.mult).to_bytes(self.size, 'little')
        return self.zeros
