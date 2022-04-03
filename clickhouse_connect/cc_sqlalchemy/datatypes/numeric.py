from sqlalchemy.types import Integer, Float, Numeric
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.numeric import Decimal as ChDecimal
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


class Decimal(ChSqlaType, Numeric):
    _ch_type_cls = ChDecimal

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
