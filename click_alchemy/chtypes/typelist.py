import struct
from datetime import date, datetime

from sqlalchemy import INTEGER, String, FLOAT, DATE, DATETIME
from superset.utils.core import GenericDataType

from click_alchemy.driver.rowbinary import string_leb128
from click_alchemy.chtypes.registry import ch_type


class ClickHouseType:
    @classmethod
    def label(cls, *args, **kwargs):
        return cls.__name__


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
        return datetime.utcfromtimestamp(epoch), loc + 4


@ch_type(DATE, GenericDataType.TEMPORAL)
class Date(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        epoch_days = int.from_bytes(source[loc:loc + 2], 'little', signed=False)
        return date.utc(epoch_days * 86400), loc + 2


@ch_type(String, GenericDataType.STRING)
class String(ClickHouseType):
    from_row_binary = string_leb128
