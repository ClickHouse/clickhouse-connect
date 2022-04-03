from sqlalchemy.sql.compiler import TypeCompiler

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType


class ChTypeCompiler(TypeCompiler):

    def visit_ch_type(self, tp: ChSqlaType):
        return tp.ch_type.name
