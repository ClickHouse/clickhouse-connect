from collections.abc import Sequence, MutableSequence
from uuid import UUID as PyUUID, SafeUUID
from struct import unpack_from as suf

from typing import Any, Collection, Dict

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, FixedType, UnsupportedType
from clickhouse_connect.datatypes.tools import array_column, read_leb128, to_leb128, read_uint64
from clickhouse_connect.driver import DriverError


class UUID(ClickHouseType):
    _output = 'uuid'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        int_high, loc = read_uint64(source, loc)
        int_low, loc = read_uint64(source, loc)
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return PyUUID(bytes=byte_value), loc

    @staticmethod
    def _to_row_binary(value: PyUUID, dest: bytearray):
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        dest += bytes_high + bytes_low

    @classmethod
    def _from_native(cls, source: Sequence, loc: int, num_rows: int, **_):
        v = suf(f'<{num_rows * 2}Q', source, loc)
        empty_uuid = PyUUID(int=0)
        new_uuid = PyUUID.__new__
        unsafe_uuid = SafeUUID.unsafe
        oset = object.__setattr__
        column = []
        app = column.append
        as_str = cls._output == 'string'
        for ix in range(num_rows):
            s = ix << 1
            int_value = v[s] << 64 | v[s + 1]
            if int_value == 0:
                app('::' if as_str else empty_uuid)
            else:
                fast_uuid = new_uuid(PyUUID)
                oset(fast_uuid, 'int', int_value)
                oset(fast_uuid, 'is_safe', unsafe_uuid)
                app(str(fast_uuid) if as_str else fast_uuid)
        return column, loc + num_rows * 16

    @classmethod
    def _to_native(cls, column: Sequence, dest:MutableSequence):
        if not column:
            return
        if isinstance(column[0], str):
            cls._to_native_str(column, dest)
        elif isinstance(column[0], int):
            cls._to_native_int(column, dest)

    @classmethod
    def _to_native_str(cls, column: Sequence, dest: MutableSequence):
        pass

    @classmethod
    def format(cls, fmt: str):
        fmt = fmt.lower()
        if fmt.startswith('uuid'):
            cls._output = 'uuid'
        elif fmt.startswith('str'):
            cls._output = 'string'
        else:
            raise ValueError('Unrecognized Output Format for UUID')


class Nothing(FixedType):
    _array_type = 'b'

    def __init(self, type_def: TypeDef):
        super().__init__(type_def)
        self.nullable = True

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return None, loc + 1

    @staticmethod
    def _to_row_binary(value: Any, dest: bytearray):
        dest.append(0x30)

    @staticmethod
    def _to_native(column:Sequence, dest: MutableSequence):
        dest += bytes(0x30 for _ in range(len(column)))


class Array(ClickHouseType):
    __slots__ = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[0])
        if isinstance(self.element_type, Array):
            raise DriverError("Nested arrays not supported")
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        sz, loc = read_leb128(source, loc)
        values = []
        for x in range(sz):
            value, loc = self.element_type.from_row_binary(source, loc)
            values.append(value)
        return values, loc

    def _to_row_binary(self, values: Collection[Any], dest: bytearray):
        dest += to_leb128(len(values))
        conv = self.element_type.to_row_binary
        for value in values:
            conv(value, dest)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        lc_version = kwargs.pop('lc_version', None)
        conv = self.element_type.from_native
        if self.element_type.low_card:
            lc_version, loc = read_uint64(source, loc)
        offsets, loc = array_column('Q', source, loc, num_rows)
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            cnt = offset - last
            last = offset
            val_list, loc = conv(source, loc, cnt, lc_version=lc_version, **kwargs)
            app(tuple(val_list))
        return column, loc


class Tuple(ClickHouseType):
    _slots = 'from_rb_funcs', 'to_rb_funcs', 'from_native_funcs'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        element_types = [get_from_name(name) for name in type_def.values]
        self.from_rb_funcs = tuple((t.from_row_binary for t in element_types))
        self.to_rb_funcs = tuple((t.to_row_binary for t in element_types))
        self.from_native_funcs = tuple((t.from_native for t in element_types))
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytes, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, values: Sequence, dest: bytearray):
        for value, conv in zip(values, self.to_rb_funcs):
            conv(value, dest)

    def _from_native(self, source, loc, num_rows, **kwargs):
        columns = []
        for conv in self.from_native_funcs:
            column, loc = conv(source, loc, num_rows, **kwargs)
            columns.append(tuple(column))
        return list(zip(*columns)), loc


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type', 'key_from_rb', 'key_to_rb', 'value_from_rb', 'value_to_rb'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.key_type = get_from_name(type_def.values[0])
        self.key_from_rb, self.key_to_rb = self.key_type.from_row_binary, self.key_type.to_row_binary
        self.value_type = get_from_name(type_def.values[1])
        self.value_from_rb, self.value_to_rb = self.value_type.from_row_binary, self.value_type.to_row_binary
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: Sequence, loc: int):
        size, loc = read_leb128(source, loc)
        values = {}
        key_from = self.key_from_rb
        value_from = self.value_from_rb
        for x in range(size):
            key, loc = key_from(source, loc)
            value, loc = value_from(source, loc)
            values[key] = value
        return values, loc

    def _to_row_binary(self, values: Dict, dest: bytearray):
        key_to = self.key_to_rb
        value_to = self.value_to_rb
        for key, value in values.items():
            dest += key_to(key)
            dest += value_to(key)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        kwargs.pop('lc_version', None)
        key_version = None
        value_version = None
        if self.key_type.low_card:
            key_version, loc = read_uint64(source, loc)
        if self.value_type.low_card:
            value_version = read_uint64(source, loc)
        offsets, loc = array_column('Q', source, loc, num_rows)
        total_rows = offsets[-1]
        keys, loc = self.key_type.from_native(source, loc, total_rows, lc_version=key_version, **kwargs)
        values, loc = self.value_type.from_native(source, loc, total_rows, lc_version=value_version, **kwargs)
        all_pairs = tuple(zip(keys, values))
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            app({key: value for key, value in all_pairs[last: offset]})
            last = offset
        return column, loc


class SimpleAggregateFunction(ClickHouseType):
    _slots = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

    def _to_row_binary(self, value: Any) -> bytes:
        return self.element_type.to_row_binary(value)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        return self.element_type.from_native(source, loc, num_rows, **kwargs)


class AggregateFunction(UnsupportedType):
    pass
