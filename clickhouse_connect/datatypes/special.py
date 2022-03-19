import array
from collections.abc import Sequence
from uuid import UUID, SafeUUID
from struct import unpack_from as suf

from typing import Union, Any, Collection, Dict

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, FixedType
from clickhouse_connect.driver.common import must_swap, read_leb128, to_leb128
from clickhouse_connect.driver.exceptions import NotSupportedError, DriverError

empty_uuid = UUID(int=0)


class ChUUID(ClickHouseType, registry_name='UUID'):
    _output = 'uuid'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        int_high = int.from_bytes(source[loc:loc + 8], 'little')
        int_low = int.from_bytes(source[loc + 8:loc + 16], 'little')
        byte_value = int_high.to_bytes(8, 'big') + int_low.to_bytes(8, 'big')
        return UUID(bytes=byte_value), loc + 16

    @staticmethod
    def _to_row_binary(value: UUID, dest: bytearray):
        source = value.bytes
        bytes_high, bytes_low = bytearray(source[:8]), bytearray(source[8:])
        bytes_high.reverse()
        bytes_low.reverse()
        dest += bytes_high + bytes_low

    @classmethod
    def _from_native(cls, source: Sequence, loc: int, num_rows: int):
        v = suf(f'<{num_rows * 2}Q', source, loc)
        new_uuid = UUID.__new__
        unsafe_uuid = SafeUUID.unsafe
        oset = object.__setattr__
        column = []
        app = column.append
        as_str = cls._output == 'string'
        for ix in range(num_rows):
            s = ix << 1
            int_value = v[s] << 64 | v[s + 1]
            if int_value == 0:
                app(empty_uuid)
            else:
                fast_uuid = new_uuid(UUID)
                oset(fast_uuid, 'int', int_value)
                oset(fast_uuid, 'is_safe', unsafe_uuid)
                app(str(fast_uuid) if as_str else fast_uuid)
        return column, loc + num_rows * 16

    @classmethod
    def format(cls, fmt:str):
        fmt = fmt.lower()
        if fmt.startswith('uuid'):
            cls._output = 'uuid'
        elif fmt.startswith('str'):
            cls._output = 'string'
        else:
            raise ValueError('Unrecognized Output Format for UUID')


class FixedString(FixedType):
    _encoding = 'utf8'

    def __init__(self, type_def: TypeDef):
        self._byte_size = type_def.values[0]
        self.name_suffix = f'({self._byte_size})'
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
                app(str(x, encoding))
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
