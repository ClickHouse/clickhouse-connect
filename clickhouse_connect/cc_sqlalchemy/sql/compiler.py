from sqlalchemy.exc import CompileError
from sqlalchemy.sql.compiler import SQLCompiler

from clickhouse_connect.cc_sqlalchemy.sql import format_table


class ChStatementCompiler(SQLCompiler):

    # pylint: disable=attribute-defined-outside-init
    def visit_delete(self, delete_stmt, **kw):
        table = delete_stmt.table
        text = f"DELETE FROM {format_table(table)}"

        if delete_stmt.whereclause is not None:
            self._in_delete_where = True
            try:
                text += " WHERE " + self.process(delete_stmt.whereclause, **kw)
            finally:
                self._in_delete_where = False
        else:
            raise CompileError("ClickHouse DELETE statements require a WHERE clause. To delete all rows, use 'TRUNCATE TABLE' instead.")

        return text

    def visit_column(self, column, add_to_result_map=None, include_table=True, result_map_targets=(), **kw):
        if getattr(self, "_in_delete_where", False):
            return self.preparer.quote(column.name)

        return super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=include_table,
            result_map_targets=result_map_targets,
            **kw,
        )

    # Abstract methods required by SQLCompiler
    def delete_extra_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support DELETE with extra FROM clause")

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support UPDATE with FROM clause")

    # pylint: disable=unused-argument
    def visit_empty_set_expr(self, element_types, **kw):
        return "SELECT 1 WHERE 1=0"

    def visit_sequence(self, sequence, **kw):
        raise NotImplementedError("ClickHouse doesn't support sequences")
