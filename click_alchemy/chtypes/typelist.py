import struct
from datetime import date, datetime, timezone

from sqlalchemy import INTEGER, String, FLOAT, DATE, DATETIME
from superset.utils.core import GenericDataType

from click_alchemy.driver.rowbinary import string_leb128
from click_alchemy.chtypes.registry import ch_type, TypeDef


class ClickHouseType:

    @classmethod
    def label(cls, *args, **kwargs):
        return cls.__name__

    @classmethod
    def build(cls, type_def: TypeDef):
        if not hasattr(cls, '_singleton'):
            cls._singleton = cls()
        return cls._singleton


@ch_type(INTEGER, GenericDataType.NUMERIC)
class UInt8(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return source[loc], loc + 1


@ch_type(INTEGER, GenericDataType.NUMERIC)
class Int8(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        x = source[loc]
        return x if x < 128 else x - 256, loc + 1


@ch_type(INTEGER, GenericDataType.NUMERIC)
class Int16(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 2], 'little', signed=True), loc + 2


@ch_type(INTEGER, GenericDataType.NUMERIC)
class UInt16(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 2], 'little', signed=False), loc + 2


@ch_type(INTEGER, GenericDataType.NUMERIC)
class Int32(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 4], 'little', signed=True), loc + 4


@ch_type(INTEGER, GenericDataType.NUMERIC)
class UInt32(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 4], 'little', signed=False), loc + 4


@ch_type(INTEGER, GenericDataType.NUMERIC)
class Int64(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 8], 'little', signed=True), loc + 8


@ch_type(INTEGER, GenericDataType.NUMERIC)
class UInt64(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 8], 'little', signed=False), loc + 8


@ch_type(FLOAT, GenericDataType.NUMERIC)
class Float32(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return struct.unpack('f', source[loc:loc + 4])[0], loc + 4


@ch_type(FLOAT, GenericDataType.NUMERIC)
class Float64(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return struct.unpack('d', source[loc:loc + 8])[0], loc + 8


@ch_type(DATETIME, GenericDataType.TEMPORAL)
class DateTime(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        epoch = int.from_bytes(source[loc:loc + 4], 'little', signed=False)
        return datetime.fromtimestamp(epoch, timezone.utc), loc + 4


@ch_type(DATE, GenericDataType.TEMPORAL)
class Date(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little', signed=False)
        return datetime.fromtimestamp(epoch_days * 86400, timezone.utc).date(), loc + 2


class Enum(ClickHouseType):
    @classmethod
    def build(cls, type_def: TypeDef):
        values = {key: value for key, value in zip(type_def.keys, type_def.values)}
        return cls(type_def.keys, type_def.values)

    def __init__(self, keys:tuple[str], values:tuple[int]):
        self._keys = keys
        self._escaped_keys = [key.replace("'", "\\'") for key in keys]
        self._values = values
        self._map = {key: value for key, value in zip(keys, values)}
        self._reverse_map = {value: key for key, value in zip(keys, values)}

    def label(self, *args, **kwargs):
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(self._escaped_keys, self._values))
        return f'{self.__class__.__name__}({val_str})'


@ch_type(String, GenericDataType.STRING)
class Enum8(Enum):
    def from_row_binary(self, source, loc):
        value = source[loc]
        if value > 127:
            value = value - 128
        return self._reverse_map[value], loc + 1


@ch_type(String, GenericDataType.STRING)
class Enum16(Enum):
    def from_row_binary(self, source, loc):
        value = int.from_bytes(source[loc:loc + 2], 'little', signed=True)
        return self._reverse_map[value], loc + 2


@ch_type(String, GenericDataType.STRING)
class String(ClickHouseType):
    from_row_binary = staticmethod(string_leb128)
