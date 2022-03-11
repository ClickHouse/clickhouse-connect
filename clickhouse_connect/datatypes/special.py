from typing import Union, Any, Collection, Dict
from binascii import hexlify

from clickhouse_connect.datatypes.registry import ClickHouseType, get_from_name, TypeDef
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.rowbinary import read_leb128, to_leb128


def _fixed_string_binary(value: bytearray):
    return value


def _fixed_string_decode(cls, value: bytearray):
    try:
        return value.decode(cls._encoding)
    except UnicodeDecodeError:
        return cls._encode_error(value)


def _hex_string(cls, value: bytearray):
    return hexlify(value).decode('utf8')


class FixedString(ClickHouseType):
    __slots__ = 'size',
    _encoding = 'utf8'
    _transform = staticmethod(_fixed_string_binary)
    _encode_error = staticmethod(_fixed_string_binary)

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.size = type_def.values[0]
        self.name_suffix = f'({self.size})'

    def _from_row_binary(self, source: bytearray, loc: int):
        return self._transform(source[loc:loc + self.size]), loc + self.size

    def _to_row_binary(self, value: Union[bytes, bytearray]) -> bytes:
        return value


def fixed_string_format(method: str, encoding: str, encoding_error: str):
    if method == 'binary':
        FixedString._transform = staticmethod(_fixed_string_binary)
    elif method == 'decode':
        FixedString._encoding = encoding
        FixedString._transform = classmethod(_fixed_string_decode)
        if encoding_error == 'hex':
            FixedString._encode_error = classmethod(_hex_string)
        else:
            FixedString._encode_error = classmethod(lambda cls: '<binary data>')
    elif method == 'hex':
        FixedString._transform = staticmethod(_hex_string)


class Nothing(ClickHouseType):
    @staticmethod
    def _from_row_binary(self, source: bytes, loc: int):
        return None, loc

    def _to_row_binary(self, value: Any, dest: bytearray):
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

    def _to_row_binary(self, values: Collection[Any], dest: bytearray):
        dest += to_leb128(len(values))
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

    def _from_row_binary(self, source: bytes, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, values: Collection, dest: bytearray):
        for value, conv in zip(values, self.to_rb_funcs):
            conv(value, dest)


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
