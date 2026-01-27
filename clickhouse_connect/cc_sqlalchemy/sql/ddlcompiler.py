from sqlalchemy import Column
from sqlalchemy.sql.compiler import DDLCompiler

from clickhouse_connect.cc_sqlalchemy.sql import  format_table
from clickhouse_connect.driver.binding import quote_identifier


# pylint: disable=no-self-use
class ChDDLCompiler(DDLCompiler):

    def visit_create_schema(self, create, **_):
        return f'CREATE DATABASE {quote_identifier(create.element)}'

    def visit_drop_schema(self, drop, **_):
        return f'DROP DATABASE {quote_identifier(drop.element)}'

    def visit_create_table(self, create, **_):
        table = create.element

        if hasattr(table, "__visit_name__") and table.__visit_name__ == "dictionary":
            return self._visit_create_dictionary(create, table)

        text = f'CREATE TABLE {format_table(table)} ('
        text += ', '.join([self.get_column_specification(c.element) for c in create.columns])
        return text + ') ' + table.engine.compile()

    def _visit_create_dictionary(self, create, dictionary):
        text = f"CREATE DICTIONARY {format_table(dictionary)} ("
        text += ", ".join([self.get_column_specification(c.element) for c in create.columns])
        text += ")"

        if dictionary.primary_key_def:
            text += f" PRIMARY KEY {dictionary.primary_key_def}"

        if dictionary.source:
            text += f" SOURCE({dictionary.source})"

        if dictionary.layout:
            text += f" LAYOUT({dictionary.layout})"

        if dictionary.lifetime:
            text += f" LIFETIME({dictionary.lifetime})"

        return text

    def visit_drop_table(self, drop, **_):
        table = drop.element
        if hasattr(table, "__visit_name__") and table.__visit_name__ == "dictionary":
            return f"DROP DICTIONARY {format_table(table)}"
        return f"DROP TABLE {format_table(table)}"

    def visit_add_column(self, create, **_):
        return f"ALTER TABLE {format_table(create.element)} ADD COLUMN {self.get_column_specification(create.column)}"

    def visit_drop_column(self, drop, **_):
        return f"ALTER TABLE {format_table(drop.element)} DROP COLUMN {quote_identifier(drop.column.name)}"

    def get_column_specification(self, column: Column, **_):
        text = f'{quote_identifier(column.name)} {column.type.compile()}'
        return text
