from collections.abc import Sequence, MutableSequence
from struct import unpack_from as suf
from typing import Any
from uuid import UUID as PYUUID, SafeUUID

from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, ArrayType, UnsupportedType
from clickhouse_connect.datatypes.common import read_uint64
from clickhouse_connect.datatypes.registry import get_from_name

empty_uuid_b = bytes(b'\x00' * 16)


class UUID(ClickHouseType):

    @property
    def ch_null(self):
        return empty_uuid_b

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        int_high, loc = read_uint64(source, loc)
        int_low, loc = read_uint64(source, loc)
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return PYUUID(bytes=byte_value), loc

    @staticmethod
    def _to_row_binary(value: PYUUID, dest: bytearray):
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        dest += bytes_high + bytes_low

    @staticmethod
    def _from_native_uuid(source: Sequence, loc: int, num_rows: int, **_):
        v = suf(f'<{num_rows * 2}Q', source, loc)
        empty_uuid = PYUUID(int=0)
        new_uuid = PYUUID.__new__
        unsafe = SafeUUID.unsafe
        oset = object.__setattr__
        column = []
        app = column.append
        for ix in range(num_rows):
            s = ix << 1
            int_value = v[s] << 64 | v[s + 1]
            if int_value == 0:
                app(empty_uuid)
            else:
                fast_uuid = new_uuid(PYUUID)
                oset(fast_uuid, 'int', int_value)
                oset(fast_uuid, 'is_safe', unsafe)
                app(fast_uuid)
        return column, loc + (num_rows << 4)

    @staticmethod
    def _from_native_str(source: Sequence, loc: int, num_rows: int, **_):
        v = suf(f'<{num_rows * 2}Q', source, loc)
        column = []
        app = column.append
        for ix in range(num_rows):
            s = ix << 1
            hs = f'{(v[s] << 64 | v[s + 1]):032x}'
            app(f'{hs[:8]}-{hs[8:12]}-{hs[12:16]}-{hs[16:20]}-{hs[20:]}')
        return column, loc + num_rows << 4

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        first = self._first_value(column)
        empty = empty_uuid_b
        if isinstance(first, str):
            for v in column:
                if v:
                    iv = int(v, 16)
                    dest += (iv >> 64).to_bytes(8, 'little') + (iv & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, int):
            for iv in column:
                if iv:
                    dest += (iv >> 64).to_bytes(8, 'little') + (iv & 0xffffffffffffffff).to_bytes(8, 'little')
                else:
                    dest += empty
        elif isinstance(first, PYUUID):
            for v in column:
                if v:
                    iv = v.int
                    dest += (iv >> 64).to_bytes(8, 'little') + (iv & 0xffffffffffffffff).to_bytes(8, 'little')
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

    _from_native = _from_native_uuid

    @classmethod
    def format(cls, fmt: str):
        fmt = fmt.lower()
        if fmt.startswith('str'):
            cls._from_native = staticmethod(cls._from_native_str)
        else:
            cls._from_native = staticmethod(cls._from_native_uuid)


class Nothing(ArrayType):
    _array_type = 'b'

    def __init(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nullable = True

    @staticmethod
    def _from_row_binary(_: bytes, loc: int):
        return None, loc + 1

    @staticmethod
    def _to_row_binary(_: Any, dest: bytearray):
        dest.append(0x30)

    @staticmethod
    def _to_native(column: Sequence, dest: MutableSequence, **_):
        dest += bytes(0x30 for _ in range(len(column)))


class SimpleAggregateFunction(ClickHouseType):
    _slots = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str
        self._ch_null = self.element_type.ch_null

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

    def _to_row_binary(self, value: Any) -> bytes:
        return self.element_type.to_row_binary(value)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        return self.element_type.from_native(source, loc, num_rows, **kwargs)

    def _to_native(self, source: Sequence, dest: MutableSequence):
        self.element_type._to_native(source, dest)


class AggregateFunction(UnsupportedType):
    pass
