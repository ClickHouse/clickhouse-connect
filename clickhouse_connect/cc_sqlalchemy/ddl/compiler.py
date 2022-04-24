from sqlalchemy.sql.compiler import DDLCompiler
from sqlalchemy.exc import CompileError

from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import TableEngine


class ChDDLCompiler(DDLCompiler):

    def visit_create_schema(self, create):
        schema = self.preparer.format_schema(create.element)
        return 'CREATE DATABASE ' + schema

    def visit_drop_schema(self, drop):
        schema = self.preparer.format_schema(drop.element)
        return 'DROP DATABASE ' + schema

    # Primary keys are part of the engine definition.  At some point we should enforce consistency but currently
    # setting 'primary_key=True' on a column has no practical effect
    def visit_primary_key_constraint(self, constraint):
        return ''

    def post_create_table(self, table):
        engine: TableEngine = getattr(table, 'engine', None)
        if not engine:
            raise CompileError('No engine defined for table')
        return engine.compile()

    # def get_column_specification(self, column, **kw):
    #    return column.compile()
