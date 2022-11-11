import pytz

from datetime import date, timedelta, datetime
from typing import Union, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import TypeDef, ArrayType
from clickhouse_connect.driver.common import array_column, write_array, np_date_types

epoch_start_date = date(1970, 1, 1)
epoch_start_datetime = datetime(1970, 1, 1)


class Date(ArrayType):
    _array_type = 'H'
    _np_type = 'datetime64[D]'
    nano_divisor = 86400 * 1000000000
    valid_formats = 'native', 'int'
    python_null = epoch_start_date
    python_type = date

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        if self.read_format() == 'int':
            return column, loc
        return [epoch_start_date + timedelta(days) for days in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        if self.write_format() == 'native':
            first = self._first_value(column)
            if isinstance(first, datetime):
                esd = epoch_start_datetime
            else:
                esd = epoch_start_date
            if self.nullable:
                column = [0 if x is None else (x - esd).days for x in column]
            else:
                column = [(x - esd).days for x in column]
        elif self.nullable:
            column = [x if x else 0 for x in column]
        write_array(self._array_type, column, dest)


class Date32(Date):
    _array_type = 'i'


from_ts_naive = datetime.utcfromtimestamp
from_ts_tz = datetime.fromtimestamp


# pylint: disable=abstract-method
class DateTime(ArrayType):
    _array_type = 'I'
    _np_type = 'datetime64[s]'
    valid_formats = 'native', 'int'
    python_null = from_ts_naive(0)
    python_type = datetime
    nano_divisor = 1000000000

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        if self.read_format() == 'int':
            return column, loc
        fts = from_ts_naive
        return [fts(ts) for ts in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        if self.write_format() == 'native':
            if self.nullable:
                column = [int(x.timestamp()) if x else 0 for x in column]
            else:
                column = [int(x.timestamp()) for x in column]
        elif self.nullable:
            column = [x if x else 0 for x in column]
        write_array(self._array_type, column, dest)


class DateTime64(ArrayType):
    __slots__ = 'scale', 'prec', 'tzinfo'
    _array_type = 'Q'
    valid_formats = 'native', 'int'
    python_null = epoch_start_date
    python_type = datetime

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        self.prec = 10 ** self.scale
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
            self._read_native_binary = self._read_native_tz
        else:
            self._read_native_binary = self._read_native_naive
            self.tzinfo = None

    def np_type(self, _str_len: int = 0):
        opt = np_date_types.get(self.scale)
        return f'datetime64{opt}' if opt else 'O'

    @property
    def nano_divisor(self):
        return 1000000000 // self.prec

    def _read_native_tz(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        if self.write_format() == 'int':
            return column, loc
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
        if self.write_format() == 'int':
            return column, loc
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
        if self.write_format() == 'native':
            prec = self.prec
            if self.nullable:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 if x else 0 for x in column]
            else:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
        elif self.nullable:
            column = [x if x else 0 for x in column]
        write_array(self._array_type, column, dest)
