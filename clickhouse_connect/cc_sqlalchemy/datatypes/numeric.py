from sqlalchemy.types import Integer, Float

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType


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