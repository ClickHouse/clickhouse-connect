from typing import Sequence, MutableSequence, Union

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.types import ByteSource


class String(ClickHouseType):
    python_null = ''

    def _read_native_binary(self, source: ByteSource, num_rows: int):
        return source.read_str_col(num_rows, self.encoding)

    def np_type(self, str_len: int = 0):
        return f'<U{str_len}' if str_len else 'O'

    # pylint: disable=duplicate-code
    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        encoding = self.encoding
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


class FixedString(ClickHouseType):
    valid_formats = 'string', 'native'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.byte_size = type_def.values[0]
        self._name_suffix = type_def.arg_str
        self._empty_bytes = bytes(b'\x00' * self.byte_size)

    @property
    def python_null(self):
        return self._empty_bytes if self.read_format() == 'native' else ''

    def np_type(self, _str_len: int = 0):
        return f'<U{self.byte_size}'

    def _read_native_binary(self, source: ByteSource, num_rows: int):
        if self.read_format() == 'string':
            return source.read_fixed_str_col(self.byte_size, num_rows, self.encoding)
        return source.read_bytes_col(self.byte_size, num_rows)

    # pylint: disable=too-many-branches
    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        ext = dest.extend
        sz = self.byte_size
        empty = bytes((0,) * sz)
        str_enc = str.encode
        enc = self.encoding
        first = self._first_value(column)
        if isinstance(first, str) or self.write_format() == 'string':
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
