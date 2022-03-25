from typing import Union, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.datatypes.common import read_leb128, to_leb128


class String(ClickHouseType):
    _encoding = 'utf8'

    def _from_row_binary(self, source, loc):
        length, loc = read_leb128(source, loc)
        return str(source[loc:loc + length], self._encoding), loc + length

    def _to_row_binary(self, value: str, dest: bytearray):
        value = bytes(value, self._encoding)
        dest += to_leb128(len(value)) + value

    def _from_native(self, source, loc, num_rows, **_):
        encoding = self._encoding
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

    def _to_native(self, column:Sequence, dest: MutableSequence, **_):
        encoding = self._encoding
        app = dest.append
        if self.nullable:
            for x in column:
                if x is None:
                    app(0)
                else:
                    l = len(x)
                    while True:
                        b = l & 0x7f
                        l = l >> 7
                        if l == 0:
                            app(b)
                            break
                        app(0x80 | b)
                    dest += x.encode(encoding)
        else:
            for x in column:
                l = len(x)
                while True:
                    b = l & 0x7f
                    l = l >> 7
                    if l == 0:
                        app(b)
                        break
                    app(0x80 | b)
                dest += x.encode(encoding)


class FixedString(ClickHouseType):
    _encoding = 'utf8'
    _format = 'bytes'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._byte_size = type_def.values[0]
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        return bytes(source[loc:loc + self._byte_size]), loc + self._byte_size

    @staticmethod
    def _to_row_binary_bytes(value: Union[bytes, bytearray], dest: bytearray):
        dest += value

    def _to_row_binary_str(self, value, dest: bytearray):
        value = str.encode(value, self._encoding)
        dest += value
        if len(value) < self._byte_size:
            dest += bytes((0,) * (self._byte_size - len(value)))

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        encoding = self._encoding
        column = []
        app = column.append
        sz = self._byte_size
        end = loc + sz * num_rows
        if self._format == 'string':
            for ix in range(loc, end, sz):
                try:
                    app(str(source[ix: ix + sz], encoding).rstrip('\x00'))
                except UnicodeDecodeError:
                    app(source[ix: ix + sz].hex())
        else:
            for ix in range(loc, end, sz):
                app(bytes(source[ix: ix + sz]))
        return column, end

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        ext = dest.extend
        sz = self._byte_size
        empty = bytes((0,) * sz)
        e = str.encode
        enc = self._encoding
        first = self._first_value(column)
        if isinstance(first, str):
            if self.nullable:
                for x in column:
                    if x is None:
                        ext(empty)
                    else:
                        try:
                            sb = e(x, enc)
                        except UnicodeEncodeError:
                            sb = empty
                        ext(sb)
                        if len(sb) < sz:
                            ext(empty[:-len(sb)])
            else:
                for x in column:
                    try:
                        sb = e(x, enc)
                    except UnicodeEncodeError:
                        sb = empty
                    ext(sb)
                    if len(sb) < sz:
                        ext(empty[:-len(sb)])
        elif self.nullable:
            for x in column:
                if not x:
                    ext(empty)
                else:
                    ext(x)
        else:
            for x in column:
                ext(x)

    _to_row_binary = _to_row_binary_bytes

    @classmethod
    def format(cls, fmt: str, encoding: str = 'utf8'):
        fmt = fmt.lower()
        if fmt.lower().startswith('str'):
            cls._format = 'string'
            cls._encoding = encoding
            cls._to_row_binary = cls._to_row_binary_str
        else:
            cls._format = 'raw'
            cls._to_row_binary = cls._to_row_binary_bytes