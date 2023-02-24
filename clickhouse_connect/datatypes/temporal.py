import pytz

from datetime import date, datetime, tzinfo
from typing import Union, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType
from clickhouse_connect.driver.common import write_array, np_date_types, int_size
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.ctypes import data_conv, numpy_conv
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np, pd

epoch_start_date = date(1970, 1, 1)
epoch_start_datetime = datetime(1970, 1, 1)


class Date(ClickHouseType):
    _array_type = 'H'
    np_type = 'datetime64[D]'
    nano_divisor = 86400 * 1000000000
    valid_formats = 'native', 'int'
    python_null = epoch_start_date
    python_type = date

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<i2', num_rows).astype(self.np_type)
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

    def _active_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_date


class Date32(Date):
    _array_type = 'l' if int_size == 2 else 'i'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy:
            return numpy_conv.read_numpy_array(source, '<u4', num_rows).astype(self.np_type)
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_date32_col(source, num_rows)


from_ts_naive = datetime.utcfromtimestamp
from_ts_tz = datetime.fromtimestamp


class DateTime(ClickHouseType):
    __slots__ = ('tzinfo',)
    _array_type = 'L' if int_size == 2 else 'I'
    np_type = 'datetime64[s]'
    valid_formats = 'native', 'int'
    python_type = datetime
    nano_divisor = 1000000000

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        if len(type_def.values) > 0:
            self.tzinfo = pytz.timezone(type_def.values[0][1:-1])
        else:
            self.tzinfo = None

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, '<u4', num_rows).astype(self.np_type)
            if ctx.as_pandas:
                tz_info = ctx.active_tz or self.tzinfo
                if tz_info:
                    return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(tz_info)
            return np_array
        if self.read_format(ctx) == 'int':
            return source.read_array(self._array_type, num_rows)
        return data_conv.read_datetime_col(source, num_rows, ctx.active_tz or self.tzinfo)

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

    def _active_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_datetime


class DateTime64(ClickHouseType):
    __slots__ = 'scale', 'prec', 'tzinfo', 'unit'
    valid_formats = 'native', 'int'
    python_null = epoch_start_date
    python_type = datetime

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
        self.scale = type_def.values[0]
        self.prec = 10 ** self.scale
        self.unit = np_date_types.get(self.scale)
        if len(type_def.values) > 1:
            self.tzinfo = pytz.timezone(type_def.values[1][1:-1])
        else:
            self.tzinfo = None

    @property
    def np_type(self):
        if self.unit:
            return f'datetime64{self.unit}'
        raise ProgrammingError(f'Cannot use {self.name} as a numpy or Pandas datatype. Only milliseconds(3), ' +
                               'microseconds(6), or nanoseconds(9) are supported for numpy based queries.')

    @property
    def nano_divisor(self):
        return 1000000000 // self.prec

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy:
            np_array = numpy_conv.read_numpy_array(source, self.np_type, num_rows)
            if ctx.as_pandas:
                tz_info = ctx.active_tz or self.tzinfo
                if tz_info:
                    return pd.DatetimeIndex(np_array, tz='UTC').tz_convert(tz_info)
            return np_array
        column = source.read_array('Q', num_rows)
        if self.read_format(ctx) == 'int':
            return column
        tz_info = ctx.active_tz or self.tzinfo
        if tz_info:
            return self._read_binary_tz(column, tz_info)
        return self._read_binary_naive(column)

    def _read_binary_tz(self, column: Sequence, tz_info: tzinfo):
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

    def _read_binary_naive(self, column: Sequence):
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
        write_array('Q', column, dest)

    def _active_null(self, ctx: QueryContext):
        if ctx.use_numpy:
            return np.datetime64(0)
        if self.read_format(ctx) == 'int':
            return 0
        return epoch_start_datetime
