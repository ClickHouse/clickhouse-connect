from typing import Sequence, MutableSequence, Union

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource
from clickhouse_connect.driver.options import np


class String(ClickHouseType):

    def _read_column_binary(self, source: ByteSource, num_rows: int, ctx: QueryContext):
        if ctx.use_numpy and ctx.max_str_len:
            return np.array(source.read_str_col(num_rows, ctx.encoding or self.encoding), dtype=f'<U{ctx.max_str_len}')
        return source.read_str_col(num_rows, ctx.encoding or self.encoding)

    def _read_nullable_column(self, source: ByteSource, num_rows: int, ctx: QueryContext) -> Sequence:
        if ctx.use_numpy:
            return super()._read_nullable_column(source, num_rows, ctx)
        return source.read_str_col(num_rows, ctx.encoding or self.encoding, nullable=True, use_none=ctx.use_none)

    # pylint: disable=duplicate-code
    def _write_column_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, ctx: InsertContext):
        encoding = ctx.encoding or self.encoding
        app = dest.append
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

    def _python_null(self, _ctx):
        return ''


class FixedString(ClickHouseType):
    valid_formats = 'string', 'native'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.byte_size = type_def.values[0]
        self._name_suffix = type_def.arg_str
        self._empty_bytes = bytes(b'\x00' * self.byte_size)

    def python_null(self, ctx: QueryContext):
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
