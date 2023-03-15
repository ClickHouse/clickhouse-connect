from typing import Sequence, MutableSequence, Union

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np, pd


class String(ClickHouseType):
    valid_formats = 'bytes', 'native'

    def _active_encoding(self, ctx):
        if self.read_format(ctx) == 'bytes':
            return None
        if ctx.encoding:
            return ctx.encoding
        return self.encoding

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        return source.read_str_col(num_rows, self._active_encoding(ctx))

    def _read_nullable_column(self, source: ByteSource, num_rows: int, ctx: QueryContext) -> Sequence:
        return source.read_str_col(num_rows, self._active_encoding(ctx), True, self._active_null(ctx))

    def _finalize_column(self, column: Sequence, ctx: QueryContext) -> Sequence:
        if ctx.use_na_values and self.read_format(ctx) == 'native':
            return pd.array(column, dtype=pd.StringDtype())
        if ctx.use_numpy and ctx.max_str_len:
            return np.array(column, dtype=f'<U{ctx.max_str_len}')
        return column

    # pylint: disable=duplicate-code,too-many-nested-blocks,too-many-branches
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        encoding = ctx.encoding or self.encoding
        app = dest.append
        first = self._first_value(column)
        if isinstance(first, str):
            if self.nullable:
                for x in column:
                    if x is None:
                        app(0)
                    else:
                        y = x.encode(encoding)
                        sz = len(y)
                        while True:
                            b = sz & 0x7f
                            sz >>= 7
                            if sz == 0:
                                app(b)
                                break
                            app(0x80 | b)
                        dest += y
            else:
                for x in column:
                    y = x.encode(encoding)
                    sz = len(y)
                    while True:
                        b = sz & 0x7f
                        sz >>= 7
                        if sz == 0:
                            app(b)
                            break
                        app(0x80 | b)
                    dest += y
        else:
            if self.nullable:
                for x in column:
                    if x is None:
                        app(0)
                    else:
                        sz = len(x)
                        while True:
                            b = sz & 0x7f
                            sz >>= 7
                            if sz == 0:
                                app(b)
                                break
                            app(0x80 | b)
                        dest += x
            else:
                for x in column:
                    sz = len(x)
                    while True:
                        b = sz & 0x7f
                        sz >>= 7
                        if sz == 0:
                            app(b)
                            break
                        app(0x80 | b)
                    dest += x

    def _active_null(self, ctx):
        if ctx.use_none:
            return None
        if self.read_format(ctx) == 'bytes':
            return bytes()
        return ''


class FixedString(ClickHouseType):
    valid_formats = 'string', 'native'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.byte_size = type_def.values[0]
        self._name_suffix = type_def.arg_str
        self._empty_bytes = bytes(b'\x00' * self.byte_size)

    def _active_null(self, ctx: QueryContext):
        if ctx.use_none:
            return None
        return self._empty_bytes if self.read_format(ctx) == 'native' else ''

    @property
    def np_type(self):
        return f'<U{self.byte_size}'

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if self.read_format(ctx) == 'string':
            return source.read_fixed_str_col(self.byte_size, num_rows, ctx.encoding or self.encoding )
        return source.read_bytes_col(self.byte_size, num_rows)

    # pylint: disable=too-many-branches
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        ext = dest.extend
        sz = self.byte_size
        empty = bytes((0,) * sz)
        str_enc = str.encode
        enc = ctx.encoding or self.encoding
        first = self._first_value(column)
        if isinstance(first, str) or self.write_format(ctx) == 'string':
            if self.nullable:
                for x in column:
                    if x is None:
                        ext(empty)
                    else:
                        try:
                            b = str_enc(x, enc)
                        except UnicodeEncodeError:
                            b = empty
                        ext(b)
                        if len(b) < sz:
                            ext(empty[:-len(b)])
            else:
                for x in column:
                    try:
                        b = str_enc(x, enc)
                    except UnicodeEncodeError:
                        b = empty
                    ext(b)
                    if len(b) < sz:
                        ext(empty[:-len(b)])
        elif self.nullable:
            for x in column:
                if not x:
                    ext(empty)
                else:
                    ext(x)
        else:
            for x in column:
                ext(x)
