from typing import Sequence, MutableSequence

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.common import read_leb128, to_leb128


class String(ClickHouseType):
    encoding = 'utf8'
    python_null = ''

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        try:
            self.encoding = type_def.values[0]
        except IndexError:
            pass

    def _from_row_binary(self, source, loc):
        length, loc = read_leb128(source, loc)
        return str(source[loc:loc + length], self.encoding), loc + length

    def _to_row_binary(self, value: str, dest: bytearray):
        value = bytes(value, self.encoding)
        dest += to_leb128(len(value)) + value

    def _from_native(self, source, loc, num_rows, **_):
        return self._from_native_impl(source, loc, num_rows, self.encoding)

    @staticmethod
    def _from_native_python(source, loc, num_rows, encoding: str):
        column = []
        app = column.append
        for _ in range(num_rows):
            length = 0
            shift = 0
            while True:
                b = source[loc]
                length += ((b & 0x7f) << shift)
                loc += 1
                if (b & 0x80) == 0:
                    break
                shift += 7
            app(str(source[loc: loc + length], encoding))
            loc += length
        return column, loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **_) -> None:
        encoding = self.encoding
        app = dest.append
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
                    dest += x.encode(encoding)
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
                dest += x.encode(encoding)

    _from_native_impl = _from_native_python


class FixedString(ClickHouseType):
    encoding = 'utf8'
    format = 'bytes'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._byte_size = type_def.values[0]
        try:
            self.encoding = type_def.values[1]
        except IndexError:
            pass
        self._name_suffix = type_def.arg_str
        self._python_null = self._ch_null = bytes(b'\x00' * self._byte_size)
        if self.format == 'bytes':
            self._to_row_binary = self._to_row_binary_bytes
        else:
            self._to_row_binary = self._to_row_binary_str

    @property
    def python_null(self):
        return self._python_null if self.format == 'bytes' else ''

    @property
    def ch_null(self):
        return self._ch_null

    def _to_row_binary(self, value, dest):
        pass  # Overridden anyway on instance creation

    def _from_row_binary(self, source: Sequence, loc: int):
        return bytes(source[loc:loc + self._byte_size]), loc + self._byte_size

    @staticmethod
    def _to_row_binary_bytes(value: Sequence, dest: MutableSequence):
        dest += value

    def _to_row_binary_str(self, value, dest: bytearray):
        value = str.encode(value, self.encoding)
        dest += value
        if len(value) < self._byte_size:
            dest += bytes((0,) * (self._byte_size - len(value)))

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        if self.format == 'string':
            return self._from_native_str(source, loc, num_rows, self._byte_size, self.encoding)
        return self._from_native_bytes(source, loc, num_rows, self._byte_size)

    @staticmethod
    def _from_native_str_python(source: Sequence, loc: int, num_rows: int, sz: int, encoding: str):
        column = []
        app = column.append
        end = loc + sz * num_rows
        for ix in range(loc, end, sz):
            try:
                app(str(source[ix: ix + sz], encoding).rstrip('\x00'))
            except UnicodeDecodeError:
                app(source[ix: ix + sz].hex())
        return column, end

    @staticmethod
    def _from_native_bytes_python(source: Sequence, loc: int, num_rows: int, sz: int):
        end = loc + sz * num_rows
        return [bytes(source[ix: ix + sz]) for ix in range(loc, end, sz)], end

    # pylint: disable=too-many-branches
    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        ext = dest.extend
        sz = self._byte_size
        empty = bytes((0,) * sz)
        str_enc = str.encode
        enc = self.encoding
        first = self._first_value(column)
        if isinstance(first, str):
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

    _from_native_str = _from_native_str_python
    _from_native_bytes = _from_native_bytes_python
