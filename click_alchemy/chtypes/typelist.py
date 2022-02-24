import struct
from datetime import datetime, timezone

from sqlalchemy import Integer, String, Float, Date, DateTime
from sqlalchemy.sql.type_api import TypeEngine
from superset.utils.core import GenericDataType

from click_alchemy.driver.rowbinary import string_leb128, parse_leb128
from click_alchemy.chtypes.registry import ch_type, get_from_def, TypeDef


class ClickHouseType:

    typed_map = {}

    @classmethod
    def label(cls):
        return cls.__name__

    @classmethod
    def build(cls, type_def: TypeDef):
        return cls.typed_map.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.type_def = type_def
        if 'Nullable' in type_def.wrappers:
            self.from_row_binary = self._null_row_binary
        else:
            self.from_row_binary = self._from_row_binary

    def _from_row_binary(self, source, loc):
        raise NotImplementedError

    def _null_row_binary(self, source, loc):
        if source[loc] == 0:
            return self._from_row_binary(source, loc + 1)
        return None, loc + 1


@ch_type(Integer, GenericDataType.NUMERIC)
class UInt8(ClickHouseType):

    def _from_row_binary(self, source, loc):
        return source[loc], loc + 1


@ch_type(Integer, GenericDataType.NUMERIC)
class Int8(ClickHouseType):
    def _from_row_binary(self, source, loc):
        if self.nullable:
            if source[loc] == 0:
                return None, loc + 1
            loc = loc + 1
        x = source[loc]
        return x if x < 128 else x - 256, loc + 1


@ch_type(Integer, GenericDataType.NUMERIC)
class Int16(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 2], 'little', signed=True), loc + 2


@ch_type(Integer, GenericDataType.NUMERIC)
class UInt16(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 2], 'little', signed=False), loc + 2


@ch_type(Integer, GenericDataType.NUMERIC)
class Int32(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 4], 'little', signed=True), loc + 4


@ch_type(Integer, GenericDataType.NUMERIC)
class UInt32(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 4], 'little', signed=False), loc + 4


@ch_type(Integer, GenericDataType.NUMERIC)
class Int64(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 8], 'little', signed=True), loc + 8


@ch_type(Integer, GenericDataType.NUMERIC)
class UInt64(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return int.from_bytes(source[loc:loc + 8], 'little', signed=False), loc + 8


@ch_type(Float, GenericDataType.NUMERIC)
class Float32(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return struct.unpack('f', source[loc:loc + 4])[0], loc + 4


@ch_type(Float, GenericDataType.NUMERIC)
class Float64(ClickHouseType):
    def _from_row_binary(self, source, loc):
        return struct.unpack('d', source[loc:loc + 8])[0], loc + 8


@ch_type(DateTime, GenericDataType.TEMPORAL)
class DateTime(ClickHouseType):
    def _from_row_binary(self, source, loc):
        epoch = int.from_bytes(source[loc:loc + 4], 'little', signed=False)
        return datetime.fromtimestamp(epoch, timezone.utc), loc + 4


@ch_type(Date, GenericDataType.TEMPORAL)
class Date(ClickHouseType):
    def _from_row_binary(self, source, loc):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little', signed=False)
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2


class Enum(ClickHouseType):

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._map = {key: value for key, value in zip(type_def.keys, type_def.values)}
        self._reverse_map = {value: key for key, value in zip(type_def.keys, type_def.values)}
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self._label = f'{self.__class__.__name__}({val_str})'

    def label(self):
        return self._label


@ch_type(String, GenericDataType.STRING)
class Enum8(Enum):
    def _from_row_binary(self, source, loc):
        value = source[loc]
        if value > 127:
            value = value - 128
        return self._reverse_map[value], loc + 1


@ch_type(String, GenericDataType.STRING)
class Enum16(Enum):
    def _from_row_binary(self, source, loc):
        value = int.from_bytes(source[loc:loc + 2], 'little', signed=True)
        return self._reverse_map[value], loc + 2


@ch_type(String, GenericDataType.STRING)
class String(ClickHouseType):
    _from_row_binary = staticmethod(string_leb128)


@ch_type(TypeEngine, None)
class Array(ClickHouseType):

    def __init__(self, type_def):
        super().__init__(type_def)
        self.nested = get_from_def(type_def.nested[0])

    def _from_row_binary(self, source, loc):
        size, loc = parse_leb128(source, loc)
        values = []
        for x in range(size):
            value, loc = self.nested.from_row_binary(source, loc)
            values.append(value)
        return values, loc
