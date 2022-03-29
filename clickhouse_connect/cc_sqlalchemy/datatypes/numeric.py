from sqlalchemy.types import Integer

from cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.datatypes.registry import get_from_name


class Int8(Integer, ChSqlaType):
    ch_type = get_from_name('Int8')


class UInt16(Integer, ChSqlaType):
    ch_type = get_from_name('Int16')
