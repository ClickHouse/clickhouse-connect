import array
from collections.abc import Sequence
from uuid import UUID as PyUUID, SafeUUID
from struct import unpack_from as suf, pack as sp

from typing import Union, Any, Collection, Dict

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, FixedType
from clickhouse_connect.driver.common import must_swap, read_leb128, to_leb128
from clickhouse_connect.driver.exceptions import NotSupportedError, DriverError

empty_uuid = PyUUID(int = 0, is_safe=SafeUUID.unknown)


class UUID(ClickHouseType):
    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        int_high = int.from_bytes(source[loc:loc + 8], 'little')
        int_low = int.from_bytes(source[loc + 8:loc + 16], 'little')
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return PyUUID(bytes=byte_value), loc + 16

    @staticmethod
    def _to_row_binary(value: PyUUID, dest: bytearray):
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        dest += bytes_high + bytes_low

    @staticmethod
    def _from_native(source: Sequence, loc: int, num_rows: int):
        new_uuid = PyUUID.__new__
        unsafe_uuid = SafeUUID.unsafe
        oset = object.__setattr__
        v = suf(f'<{num_rows * 2}Q', source, loc)
        column = []
        app = column.append
        for ix in range(num_rows):
            s = ix << 1
            int_value = v[s] << 64 | v[s + 1]
            if int_value == 0:
                app(empty_uuid)
            else:
                fast_uuid = new_uuid(PyUUID)
                oset(fast_uuid, 'int', int_value)
                oset(fast_uuid, 'is_safe', unsafe_uuid)
                app(fast_uuid)
        return column, loc + num_rows * 16


class FixedString(ClickHouseType):
    __slots__ = 'size', 'encoding', '_to_python'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.size = type_def.values[0]
        self.name_suffix = f'({self.size})'
        self._to_python = None

    def _from_row_binary(self, source: bytearray, loc: int):
        return bytes(source[loc:loc + self.size]), loc + self.size

    @staticmethod
    def _to_row_binary(value: Union[str, bytes, bytearray], dest: bytearray):
        dest += value


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
        dest += b'\x30'


class Array(ClickHouseType):
    __slots__ = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[0])
        if isinstance(self.element_type, Array):
            raise DriverError("Nested arrays not supported")
        self.name_suffix = type_def.arg_str

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

    def _from_native(self, source: Sequence, loc: int, num_rows: int):
        conv = self.element_type.from_native
        offsets = array.array('Q')
        sz = num_rows * 8
        offsets.frombytes(source[loc: loc + sz])
        loc += sz
        if must_swap:
            offsets.byteswap()
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            cnt = offset - last
            last = offset
            val_list, loc = conv(source, loc, cnt)
            app(val_list)
        return column, loc


class Tuple(ClickHouseType):
    _slots = 'from_rb_funcs', 'to_rb_funcs', 'from_native_funcs'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        element_types = [get_from_name(name) for name in type_def.values]
        self.from_rb_funcs = tuple((t.from_row_binary for t in element_types))
        self.to_rb_funcs = tuple((t.to_row_binary for t in element_types))
        self.from_native_funcs = tuple((t.from_native for t in element_types))
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytes, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, values: Collection, dest: bytearray):
        for value, conv in zip(values, self.to_rb_funcs):
            conv(value, dest)

    def _from_native(self, source, loc, num_rows):
        columns = []
        for conv in self.from_native_funcs:
            column, loc = conv(source, loc, num_rows)
            columns.append(column)
        return list(zip(*columns)), loc


class Map(ClickHouseType):
    _slots = 'key_from_rb', 'key_to_rb', 'value_from_rb', 'value_to_rb'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        ch_type = get_from_name(type_def.values[0])
        self.key_from_rb, self.key_to_rb = ch_type.from_row_binary, ch_type.to_row_binary
        ch_type = get_from_name(type_def.values[1])
        self.value_from_rb, self.value_to_rb = ch_type.from_row_binary, ch_type.to_row_binary
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        size, loc = read_leb128(source, loc)
        values = {}
        key_from = self.key_from_rb
        value_from = self.value_from_rb
        for x in range(size):
            key, loc = key_from(source, loc)
            value, loc = value_from(source, loc)
            values[key] = value
        return values, loc

    def _to_row_binary(self, values: Dict) -> bytearray:
        key_to = self.key_to_rb
        value_to = self.value_to_rb
        ret = bytearray()
        for key, value in values.items():
            ret.extend(key_to(key))
            ret.extend(value_to(key))
        return ret


class AggregateFunction(ClickHouseType):
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        raise NotSupportedError("Aggregate function deserialization not supported")

    def _to_row_binary(self, value: Any) -> bytes:
        raise NotSupportedError("Aggregate function serialization not supported")


class SimpleAggregateFunction(ClickHouseType):
    _slots = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

    def _to_row_binary(self, value: Any) -> bytes:
        return self.element_type.to_row_binary(value)
