from collections.abc import Sequence
from datetime import date, timedelta, datetime
from struct import unpack_from as suf, pack as sp

import pytz

from clickhouse_connect.datatypes.base import TypeDef, FixedType
from clickhouse_connect.datatypes.tools import read_uint64

epoch_start_date = date(1970, 1, 1)


class Date(FixedType):
    _array_type = 'H'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return epoch_start_date + timedelta(suf('<H', source, loc)[0]), loc + 2

    @staticmethod
    def _to_row_binary(value: date, dest: bytearray):
        dest += sp('<H', (value - epoch_start_date).days, )

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

    @staticmethod
    def _to_python(column: Sequence):
        return [epoch_start_date + timedelta(days) for days in column]


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
        dest += sp('<I', int(value.timestamp()), )

    @staticmethod
    def _to_python(column: Sequence):
        return [from_ts_naive(ts) for ts in column]


class DateTime64(FixedType):
    __slots__ = 'prec', 'tzinfo'
    _array_type = 'Q'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.prec = 10 ** type_def.values[0]
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
            self._to_python = self._to_python_tz
        else:
            self._to_python = self._to_python_naive
            self.tzinfo = None

    def _from_row_binary(self, source, loc):
        ticks, loc = read_uint64(source, loc)
        seconds = ticks // self.prec
        dt_sec = datetime.fromtimestamp(seconds, self.tzinfo)
        microseconds = ((ticks - seconds * self.prec) * 1000000) // self.prec
        return dt_sec + timedelta(microseconds=microseconds), loc + 8

    def _to_row_binary(self, value: datetime, dest: bytearray):
        microseconds = int(value.timestamp()) * 1000000 + value.microsecond
        dest += (int(microseconds * 1000000) // self.prec).to_bytes(8, 'little', signed=True)

    def _to_python_tz(self, column: Sequence):
        new_col = []
        app = new_col.append
        df = datetime.fromtimestamp
        prec = self.prec
        tz = self.tzinfo
        for ticks in column:
            seconds = ticks // prec
            dt_sec = df(seconds, tz)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _to_python_naive(self, column: Sequence):
        new_col = []
        app = new_col.append
        df = datetime.utcfromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = df(seconds)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col
