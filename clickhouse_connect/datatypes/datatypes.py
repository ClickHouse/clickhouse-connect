import struct

from typing import Dict, Type
from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone

from clickhouse_connect.driver.rowbinary import string_leb128, parse_leb128
from clickhouse_connect.datatypes.registry import TypeDef, register_bases


class ClickHouseType(metaclass=ABCMeta):
    __slots__ = 'wrappers', 'from_row_binary', 'name_suffix'

    _instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.name_suffix = ''
        self.wrappers = type_def.wrappers
        if 'Nullable' in self.wrappers:
            self.from_row_binary = self._nullable_from_row_binary
        else:
            self.from_row_binary = self._from_row_binary

    @property
    def name(self):
        name = f'{self.__class__.__name__}{self.name_suffix}'
        for wrapper in self.wrappers:
            name = f'{wrapper}({name})'
        return name

    @abstractmethod
    def _from_row_binary(self, source, loc):
        pass

    def _nullable_from_row_binary(self, source, loc):
        if source[loc] == 0:
            return self._from_row_binary(source, loc + 1)
        return None, loc + 1


class Int(ClickHouseType):
    __slots__ = 'size',
    signed = True

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.size
        self.size = type_def.size // 8

    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + self.size], 'little', signed=self.signed), loc + self.size


class UInt(Int):
    signed = False


class Float(ClickHouseType):
    __slots__ = 'size', 'pack'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.name_suffix = type_def.size
        self.size = type_def.size // 8
        self.pack = 'd' if self.size == 8 else 'f'

    def _from_row_binary(self, source, loc):
        return struct.unpack(self.pack, source[loc:loc + self.size])[0], loc + self.size


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
        self.name_suffix = f'{type_def.size}({val_str})'

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
        self.name_suffix = f'({self.nested.name})'

    def _from_row_binary(self, source, loc):
        size, loc = parse_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.nested.from_row_binary(source, loc)
            values.append(value)
        return values, loc


register_bases(Int, UInt, Enum, Float, String, Date, DateTime, Array)
