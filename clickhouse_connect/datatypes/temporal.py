import pytz

from datetime import date, timedelta, datetime
from typing import Union, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import TypeDef, ArrayType
from clickhouse_connect.driver.common import array_column, write_array

epoch_start_date = date(1970, 1, 1)


class Date(ArrayType):
    _array_type = 'H'
    python_null = epoch_start_date
    np_type = 'M8[D]'
    python_type = date

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        return [epoch_start_date + timedelta(days) for days in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        esd = epoch_start_date
        if self.nullable:
            write_array(self._array_type, [0 if x is None else (x - esd).days for x in column], dest)
        else:
            write_array(self._array_type, [(x - esd).days for x in column], dest)


class Date32(Date):
    _array_type = 'i'


from_ts_naive = datetime.utcfromtimestamp
from_ts_tz = datetime.fromtimestamp


# pylint: disable=abstract-method
class DateTime(ArrayType):
    _array_type = 'I'
    np_type = 'M8[us]'
    python_null = from_ts_naive(0)
    python_type = datetime

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        fts = from_ts_naive
        return [fts(ts) for ts in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        if self.nullable:
            column = [int(x.timestamp()) if x else 0 for x in column]
        else:
            column = [int(x.timestamp()) for x in column]
        write_array(self._array_type, column, dest)


class DateTime64(ArrayType):
    __slots__ = 'prec', 'tzinfo'
    _array_type = 'Q'
    python_null = epoch_start_date
    python_type = datetime

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.prec = 10 ** type_def.values[0]
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
            self._read_native_binary = self._read_native_tz
        else:
            self._read_native_binary = self._read_native_naive
            self.tzinfo = None

    def _read_native_tz(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        new_col = []
        app = new_col.append
        dt_from = datetime.fromtimestamp
        prec = self.prec
        tz_info = self.tzinfo
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds, tz_info)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col, loc

    def _read_native_naive(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        new_col = []
        app = new_col.append
        dt_from = datetime.utcfromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col, loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        prec = self.prec
        if self.nullable:
            column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 if x else 0 for x in column]
        else:
            column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
        write_array(self._array_type, column, dest)
