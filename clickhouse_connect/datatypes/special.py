from typing import Union, Any, Collection, Dict

from clickhouse_connect.datatypes.registry import ClickHouseType, get_from_name, TypeDef
from clickhouse_connect.datatypes.standard import Int
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.rowbinary import read_leb128, write_leb128


class Enum(Int):
    __slots__ = '_name_map', '_int_map'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = {key: value for key, value in zip(type_def.keys, type_def.values)}
        self._int_map = {value: key for key, value in zip(type_def.keys, type_def.values)}
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self.name_suffix = f'{type_def.size}({val_str})'

    def _from_row_binary(self, source: bytearray, loc: int):
        value, loc = super()._from_row_binary(source, loc)
        return self._int_map[value], loc

    def _to_row_binary(self, value: Union [str, int], dest: bytearray):
        if isinstance(value, str):
            value = self._name_map[value]
        return super().to_row_binary(value, dest)


class Nothing(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return None, loc

    def _to_row_binary(self, value: Any, dest: bytearray) -> None:
        pass


class Array(ClickHouseType):
    __slots__ = 'element_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[0])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        size, loc = read_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.element_type.from_row_binary(source, loc)
            values.append(value)
        return values, loc

    def _to_row_binary(self, values: Collection[Any], dest: bytearray) -> None:
        write_leb128(len(values), dest)
        conv = self.element_type.to_row_binary
        for value in values:
            conv(value, dest)


class Tuple(ClickHouseType):
    _slots = 'from_rb_funcs', 'to_rb_funcs'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.from_rb_funcs = tuple([get_from_name(name).from_row_binary for name in type_def.values])
        self.to_rb_funcs = tuple([get_from_name(name).to_row_binary for name in type_def.values])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, values: Collection, dest: bytearray) -> None:
        for value, conv in zip(values, self.to_rb_funcs):
            dest += conv(value)


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

    def _to_row_binary(self, values: Dict, dest: bytearray) -> None:
        key_to = self.key_to_rb
        value_to = self.value_to_rb
        for key, value in values.items():
            key_to(key, dest)
            value_to(value, dest)


class AggregateFunction(ClickHouseType):
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        raise NotSupportedError("Aggregate function serialization not supported")

    def _to_row_binary(self, source, loc):
        raise NotSupportedError("Aggregate function serialization not supported")


class SimpleAggregateFunction(ClickHouseType):
    _slots = 'element_type',

    def __init__(self, type_def:TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

    def _to_row_binary(self, value: Any, dest: bytearray) -> None:
        self.element_type.to_row_binary(value, dest)

