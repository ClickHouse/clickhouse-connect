from sqlalchemy.types import String as SqlaString

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.datatypes.base import TypeDef


class String(ChSqlaType, SqlaString):
    pass


class FixedString(ChSqlaType, SqlaString):
    def __init__(self, size: int = 0, type_def: TypeDef = None):
        if not type_def:
            type_def = TypeDef(values = (size,))
        ChSqlaType.__init__(self, type_def)
        SqlaString.__init__(self, size)
