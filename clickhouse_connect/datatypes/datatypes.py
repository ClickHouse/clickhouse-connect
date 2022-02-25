import struct

from typing import Dict, Type, TYPE_CHECKING
from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone

from clickhouse_connect.driver.rowbinary import string_leb128, parse_leb128
from clickhouse_connect.datatypes.registry import TypeDef, register_bases


class ClickHouseType(metaclass=ABCMeta):
    base = None
    _instance_cache: Dict['TypeDef', 'ClickHouseType'] = {}

    __slots__ = 'size', 'name', 'from_row_binary'

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: 'TypeDef'):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.size = type_def.size
        name = type_def.name
        for wrapper in type_def.wrappers:
            name = f'{wrapper}({name})'
        self.name = name
        if 'Nullable' in type_def.wrappers:
            self.from_row_binary = self._nullable_from_row_binary
        else:
            self.from_row_binary = self._from_row_binary

    def label(self):
        return self.name

    @abstractmethod
    def _from_row_binary(self, source, loc):
        pass

    def _nullable_from_row_binary(self, source, loc):
        if source[loc] == 0:
            return self._from_row_binary(source, loc + 1)
        return None, loc + 1


class Int(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + self.size], 'little', signed=True), loc + self.size


class UInt(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc: loc + self.size], 'little'), loc + self.size


class Float(ClickHouseType):
    __slots__ = 'float_type',

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.float_type = 'd' if self.size == 8 else 'f'

    def _from_row_binary(self, source, loc):
        return struct.unpack(self.float_type, source[loc:loc + self.size])[0], loc + self.size


class DateTime(ClickHouseType):
    def _from_row_binary(self, source, loc):
        epoch = int.from_bytes(source[loc:loc + 4], 'little', signed=False)
        return datetime.fromtimestamp(epoch, timezone.utc), loc + 4


class Date(ClickHouseType):
    def _from_row_binary(self, source, loc):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little', signed=False)
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2


class Enum(Int):
    __slots__ = '_name_map', '_int_map'

    def __init__(self, type_def: 'TypeDef'):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = {key: value for key, value in zip(type_def.keys, type_def.values)}
        self._int_map = {value: key for key, value in zip(type_def.keys, type_def.values)}
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self.name = f'{self.name}({val_str})'

    def _from_row_binary(self, source, loc):
        value, loc = super().from_row_binary
        return self._int_map[value], loc


class String(ClickHouseType):
    _from_row_binary = staticmethod(string_leb128)


class Array(ClickHouseType):
    __slots__ = 'nested',

    def __init__(self, type_def: 'TypeDef'):
        super().__init__(type_def)
        self.nested = type_def.values[0]

    def _from_row_binary(self, source, loc):
        size, loc = parse_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.nested.from_row_binary(source, loc)
            values.append(value)
        return values, loc


register_bases(Int, UInt, Enum, Float, String, Date, DateTime, Array)
