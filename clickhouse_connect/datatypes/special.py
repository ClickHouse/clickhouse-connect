from clickhouse_connect.datatypes.registry import ClickHouseType, get_from_name, TypeDef
from clickhouse_connect.datatypes.standard import Int
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.rowbinary import read_leb128


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


class Nothing(ClickHouseType):
    def _from_row_binary(self, source: bytearray, loc: int):
        return None, loc


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


class Tuple(ClickHouseType):
    _slots = 'member_types',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.member_types: List[ClickHouseType] = [get_from_name(name) for name in type_def.values]
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytearray, loc: int):
        values = []
        for t in self.member_types:
            value, loc = t.from_row_binary(source, loc)
            values.append(value)
        return tuple(values), loc


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.key_type: ClickHouseType = get_from_name(type_def.values[0])
        self.value_type: ClickHouseType = get_from_name(type_def.values[1])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        size, loc = read_leb128(source, loc)
        values = {}
        for x in range(size):
            key, loc = self.key_type.from_row_binary(source, loc)
            value, loc = self.value_type.from_row_binary(source, loc)
            values[key] = value
        return values, loc


class AggregateFunction(ClickHouseType):
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        raise NotSupportedError("Aggregate function serialization not supported")


class SimpleAggregateFunction(ClickHouseType):
    _slots = 'element_type',

    def __init__(self, type_def:TypeDef):
        super().__init__(type_def)
        self.element_type: ClickHouseType = get_from_name(type_def.values[1])
        self.name_suffix = type_def.arg_str

    def _from_row_binary(self, source, loc):
        return self.element_type.from_row_binary(source, loc)

