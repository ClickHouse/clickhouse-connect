from collections.abc import Sequence
from datetime import date, timedelta, datetime, timezone
from struct import unpack_from as suf, pack as sp
from typing import Iterable

import pytz

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, FixedType

epoch_start_date = date(1970, 1, 1)


class Date(FixedType):
    _array_type = 'H'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return epoch_start_date + timedelta(suf('<H', source, loc)[0]), loc + 2

    @staticmethod
    def _to_row_binary(value: date, dest: bytearray):
        dest += sp('<H', (value - epoch_start_date).days,)

    @staticmethod
    def _to_python(column: Sequence):
        return [epoch_start_date + timedelta(days) for days in column]


class Date32(FixedType):
    _array_type = 'i'

    @staticmethod
    def _from_row_binary(source, loc):
        return epoch_start_date + timedelta(suf('<i', source, loc)[0]), loc + 4

    @staticmethod
    def _to_row_binary(value: date, dest: bytearray):
         dest += (value - epoch_start_date).days.to_bytes(4, 'little', signed=True)


from_ts_naive = datetime.utcfromtimestamp
from_ts_tz = datetime.fromtimestamp


class DateTime(FixedType):
    __slots__ = '_from_row_binary',
    _array_type = 'I'

    def __init__(self, type_def: TypeDef):
        if type_def.values:
            tzinfo = pytz.timezone(type_def.values[0][1:-1])
            self._from_row_binary = lambda source, loc: (from_ts_tz(suf('<L', source, loc)[0], tzinfo), loc + 4)
        else:
            self._from_row_binary = lambda source, loc: (from_ts_naive(suf('<L', source, loc)[0]), loc + 4)
        super().__init__(type_def)

    @staticmethod
    def _to_row_binary(value: datetime, dest: bytearray):
        dest += sp('<I', int(value.timestamp()),)

    @staticmethod
    def _to_python(column: Iterable):
        return [from_ts_naive(ts) for ts in column]


class DateTime64(ClickHouseType):
    __slots__ = 'prec', 'tzinfo'
    _array_type = 'Q'

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

    def _to_row_binary(self, value: datetime, dest: bytearray):
        microseconds = int(value.timestamp()) * 1000000 + value.microsecond
        dest += (int(microseconds * 1000000) // self.prec).to_bytes(8, 'little', signed=True)