from typing import Union, Sequence

from clickhouse_connect.datatypes.base import ClickHouseType, FixedType, TypeDef
from clickhouse_connect.datatypes.tools import read_leb128, to_leb128


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


class FixedString(FixedType):
    _encoding = 'utf8'

    def __init__(self, type_def: TypeDef):
        self._byte_size = type_def.values[0]
        self._name_suffix = f'({self._byte_size})'
        super().__init__(type_def)

    def _from_row_binary(self, source: bytearray, loc: int):
        return bytes(source[loc:loc + self._byte_size]), loc + self._byte_size

    @staticmethod
    def _to_row_binary(value: Union[str, bytes, bytearray], dest: bytearray):
        dest += value

    def _to_python_str(self, column: Sequence):
        encoding = self._encoding
        new_col = []
        app = new_col.append
        for x in column:
            try:
                app(str(x, encoding).rstrip('\x00'))
            except UnicodeDecodeError:
                app(x.hex())
        return new_col

    @classmethod
    def format(cls, fmt: str, encoding: str = 'utf8'):
        fmt = fmt.lower()
        if fmt.lower().startswith('str'):
            cls._to_python = cls._to_python_str
            cls._encoding = encoding
        elif fmt.startswith('raw') or fmt.startswith('byte'):
            cls._to_python = None
        else:
            raise ValueError("Unrecognized FixedString output format")