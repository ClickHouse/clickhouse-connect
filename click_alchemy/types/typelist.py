from sqlalchemy import INTEGER, String
from superset.utils.core import GenericDataType

from click_alchemy.driver.rowbinary import leb128_string
from click_alchemy.types.registry import ch_type


@ch_type('UInt8', INTEGER, GenericDataType.NUMERIC)
class UInt8:
    @classmethod
    def from_row_binary(cls, source, loc):
        return source[loc], loc + 1


@ch_type('Int8', INTEGER, GenericDataType.NUMERIC)
class Int8:
    @classmethod
    def from_row_binary(cls, source, loc):
        x = source[loc]
        return x if x < 128 else x - 256, loc + 1


@ch_type('Int16', INTEGER, GenericDataType.NUMERIC)
class Int16:
    @classmethod
    def from_row_binary(cls, source, loc):
        return int.from_bytes(source[loc:loc + 2], 'little'), loc + 2


@ch_type('String', String, GenericDataType.STRING)
class String:
    from_row_binary = leb128_string
