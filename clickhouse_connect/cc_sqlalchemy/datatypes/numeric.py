from collections.abc import Sequence
from enum import Enum as PyEnum
from typing import Type

from sqlalchemy.types import Integer, Float, Numeric, Boolean as SqlaBoolean, UserDefinedType
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.driver.common import decimal_prec


class Int8(ChSqlaType, Integer):
    pass


class UInt8(ChSqlaType, Integer):
    pass


class Int16(ChSqlaType, Integer):
    pass


class UInt16(ChSqlaType, Integer):
    pass


class Int32(ChSqlaType, Integer):
    pass


class UInt32(ChSqlaType, Integer):
    pass


class Int64(ChSqlaType, Integer):
    pass


class UInt64(ChSqlaType, Integer):
    pass


class Int128(ChSqlaType, Integer):
    pass


class UInt128(ChSqlaType, Integer):
    pass


class Int256(ChSqlaType, Integer):
    pass


class UInt256(ChSqlaType, Integer):
    pass


class Float32(ChSqlaType, Float):
    pass


class Float64(ChSqlaType, Float):
    pass


class Bool(ChSqlaType, SqlaBoolean):
    def __init__(self):
        SqlaBoolean.__init__(self)
        ChSqlaType.__init__(self)


class Boolean(Bool):
    pass


class Decimal(ChSqlaType, Numeric):
    def __init__(self, precision: int = 0, scale: int = 0, type_def: TypeDef = None):
        if type_def:
            if type_def.size:
                precision = decimal_prec[type_def.size]
                scale = type_def.values[0]
            else:
                precision, scale = type_def.values
        elif not precision or not scale:
            raise ArgumentError("Precision and scale required for ClickHouse Decimal type")
        else:
            type_def = TypeDef(values=(precision, scale))
        ChSqlaType.__init__(self, type_def)
        Numeric.__init__(self, precision, scale)


class Enum(ChSqlaType, UserDefinedType):
    def __init__(self, enum: Type[PyEnum] = None, keys: Sequence[str] = None, values: Sequence[int] = None,
                 type_def: TypeDef = None):
        if not type_def:
            if enum:
                keys = [e.name for e in enum]
                values = [e.value for e in enum]
            type_def = TypeDef(keys=tuple(keys), values=tuple(values))
        super().__init__(type_def)


class Enum8(Enum):
    pass


class Enum16(Enum):
    pass
