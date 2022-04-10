from collections.abc import Sequence, MutableSequence
from typing import Any
from uuid import UUID as PYUUID, SafeUUID

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, ArrayType, UnsupportedType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.common import read_uint64, array_column

empty_uuid_b = bytes(b'\x00' * 16)


class UUID(ClickHouseType):
    python_null = PYUUID(int=0)
    format = 'uuid'

    @property
    def ch_null(self):
        return empty_uuid_b

    def _from_row_binary(self, source: bytearray, loc: int):
        int_high, loc = read_uint64(source, loc)
        int_low, loc = read_uint64(source, loc)
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return PYUUID(bytes=byte_value), loc

    def _to_row_binary(self, value: PYUUID, dest: bytearray):
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        dest += bytes_high + bytes_low

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        if self.format == 'string':
            return self._from_native_str(source, loc, num_rows)
        return self._from_native_uuid(source, loc, num_rows)

    # pylint: disable=too-many-locals
    @staticmethod
    def _from_native_uuid(source: Sequence, loc: int, num_rows: int):
        v, end = array_column('Q', source, loc, num_rows * 2)
        empty_uuid = PYUUID(int=0)
        new_uuid = PYUUID.__new__
        unsafe = SafeUUID.unsafe
        oset = object.__setattr__
        column = []
        app = column.append
        for i in range(num_rows):
            ix = i << 1
            int_value = v[ix] << 64 | v[ix + 1]
            if int_value == 0:
                app(empty_uuid)
            else:
                fast_uuid = new_uuid(PYUUID)
                oset(fast_uuid, 'int', int_value)
                oset(fast_uuid, 'is_safe', unsafe)
                app(fast_uuid)
        return column, end

    @staticmethod
    def _from_native_str(source: Sequence, loc: int, num_rows: int):
        v, end = array_column('Q', source, loc, num_rows * 2)
        column = []
        app = column.append
        for i in range(num_rows):
            ix = i << 1
            x = f'{(v[ix] << 64 | v[ix + 1]):032x}'
            app(f'{x[:8]}-{x[8:12]}-{x[12:16]}-{x[16:20]}-{x[20:]}')
        return column, end

    # pylint: disable=too-many-branches
    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        first = self._first_value(column)
        empty = empty_uuid_b
        if isinstance(first, str):
            for v in column:
                if v:
                    x = int(v, 16)
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, int):
            for x in column:
                if x:
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, PYUUID):
            for v in column:
                if v:
                    x = v.int
                    dest += (x >> 64).to_bytes(8, 'little') + (x & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, (bytes, bytearray, memoryview)):
            for v in column:
                if v:
                    dest += bytes(reversed(v[:8])) + bytes(reversed(v[8:]))
                else:
                    dest += empty
        else:
            dest += empty * len(column)


class Nothing(ArrayType):
    _array_type = 'b'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nullable = True

    def _from_row_binary(self, source: Sequence, loc: int):
        return None, loc + 1

    def _to_row_binary(self, value: Any, dest: bytearray):
        dest.append(0x30)

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        dest += bytes(0x30 for _ in range(len(column)))


class SimpleAggregateFunction(ClickHouseType):
    _slots = ('element_type',)

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str
        self._ch_null = self.element_type.ch_null

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

    def _to_row_binary(self, value: Any, dest: MutableSequence):
        dest += self.element_type.to_row_binary(value, dest)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        return self.element_type.from_native(source, loc, num_rows, **kwargs)

    # pylint: disable=protected-access
    def _to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        self.element_type._to_native(column, dest, **kwargs)


class AggregateFunction(UnsupportedType):
    pass
