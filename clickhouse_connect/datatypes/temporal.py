import pytz

from datetime import date, datetime
from typing import Union, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import TypeDef, ArrayType
from clickhouse_connect.driver.common import write_array, np_date_types
from clickhouse_connect.driver.ctypes import data_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np

epoch_start_date = date(1970, 1, 1)
epoch_start_datetime = datetime(1970, 1, 1)


class Date(ArrayType):
    _array_type = 'H'
    np_type = 'datetime64[D]'
    nano_divisor = 86400 * 1000000000
    valid_formats = 'native', 'int'
    python_null = epoch_start_date
    python_type = date

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_date_col(source, num_rows)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        first = self._first_value(column)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if isinstance(first, datetime):
                esd = epoch_start_datetime
            else:
                esd = epoch_start_date
            if self.nullable:
                column = [0 if x is None else (x - esd).days for x in column]
            else:
                column = [(x - esd).days for x in column]
        write_array(self._array_type, column, dest)

    def _python_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_date


class Date32(Date):
    _array_type = 'i'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_date32_col(source, num_rows)


from_ts_naive = datetime.utcfromtimestamp
from_ts_tz = datetime.fromtimestamp


# pylint: disable=abstract-method
class DateTime(ArrayType):
    _array_type = 'I'
    np_type = 'datetime64[s]'
    valid_formats = 'native', 'int'
    python_type = datetime
    nano_divisor = 1000000000

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy:
            return source.read_numpy_array('<u4', num_rows).astype(self.np_type)
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_datetime_col(source, num_rows)

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        first = self._first_value(column)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            if self.nullable:
                column = [int(x.timestamp()) if x else 0 for x in column]
            else:
                column = [int(x.timestamp()) for x in column]
        write_array(self._array_type, column, dest)

    def _python_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_datetime


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
            self._read_column_binary = self._read_binary_tz
        else:
            self._read_column_binary = self._read_binary_naive
            self.tzinfo = None

    @property
    def np_type(self):
        opt = np_date_types.get(self.scale)
        return f'datetime64{opt}' if opt else 'O'

    @property
    def nano_divisor(self):
        return 1000000000 // self.prec

    def _read_binary_tz(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        column = source.read_array(self._array_type, num_rows)
        if self.read_format(ctx) == 'int':
            return column
        new_col = []
        app = new_col.append
        dt_from = datetime.fromtimestamp
        prec = self.prec
        tz_info = self.tzinfo
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds, tz_info)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _read_binary_naive(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy:
            return source.read_numpy_array(self.np_type, num_rows)
        column = source.read_array(self._array_type, num_rows)
        if self.read_format(ctx) == 'int':
            return column
        new_col = []
        app = new_col.append
        dt_from = datetime.utcfromtimestamp
        prec = self.prec
        for ticks in column:
            seconds = ticks // prec
            dt_sec = dt_from(seconds)
            app(dt_sec.replace(microsecond=((ticks - seconds * prec) * 1000000) // prec))
        return new_col

    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        first = self._first_value(column)
        if isinstance(first, int) or self.write_format(ctx) == 'int':
            if self.nullable:
                column = [x if x else 0 for x in column]
        else:
            prec = self.prec
            if self.nullable:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 if x else 0
                          for x in column]
            else:
                column = [((int(x.timestamp()) * 1000000 + x.microsecond) * prec) // 1000000 for x in column]
        write_array(self._array_type, column, dest)

    def _python_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_datetime
