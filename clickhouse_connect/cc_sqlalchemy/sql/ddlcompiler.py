from sqlalchemy import Column
from sqlalchemy.sql.compiler import DDLCompiler

from clickhouse_connect.cc_sqlalchemy.sql import quote_id, format_table


class ChDDLCompiler(DDLCompiler):

    def visit_create_schema(self, create):
        return f'CREATE DATABASE {quote_id(create.element)}'

    def visit_drop_schema(self, drop):
        return f'DROP DATABASE {quote_id(drop.element)}'

    def visit_create_table(self, create):
        table = create.element
        text = f'CREATE TABLE {format_table(table)} ('
        text += ', '.join([self.get_column_specification(c.element) for c in create.columns])
        return text + ') ' + table.engine.compile()

    def get_column_specification(self, column: Column, **_):
        text = f'{quote_id(column.name)} {column.type.compile()}'
        return text
