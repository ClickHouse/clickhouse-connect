from sqlalchemy.exc import CompileError
from sqlalchemy.sql import elements, sqltypes
from sqlalchemy.sql.compiler import SQLCompiler

from clickhouse_connect.cc_sqlalchemy import ArrayJoin
from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.cc_sqlalchemy.sql import format_table


# pylint: disable=too-many-return-statements
def _resolve_ch_type_name(sqla_type):
    """Resolve a SQLAlchemy type instance to a ClickHouse type name string.

    Handles both native ChSqlaType instances which carry their ClickHouse name
    directly and generic SQLAlchemy types by mapping to reasonable ClickHouse defaults.
    """
    if isinstance(sqla_type, ChSqlaType):
        return sqla_type.name
    # Order matters so we need to check subtypes before parent types
    if isinstance(sqla_type, sqltypes.SmallInteger):
        return "Int16"
    if isinstance(sqla_type, sqltypes.BigInteger):
        return "Int64"
    if isinstance(sqla_type, sqltypes.Integer):
        return "Int32"
    if isinstance(sqla_type, sqltypes.Float):
        return "Float64"
    if isinstance(sqla_type, sqltypes.Numeric):
        p = sqla_type.precision or 18
        s = sqla_type.scale or 0
        return f"Decimal({p}, {s})"
    if isinstance(sqla_type, sqltypes.Boolean):
        return "Bool"
    if isinstance(sqla_type, sqltypes.DateTime):
        return "DateTime"
    if isinstance(sqla_type, sqltypes.Date):
        return "Date"
    if isinstance(sqla_type, sqltypes.String):
        return "String"
    return "String"


# pylint: disable=arguments-differ
class ChStatementCompiler(SQLCompiler):

    # pylint: disable=attribute-defined-outside-init,unused-argument
    def visit_delete(self, delete_stmt, visiting_cte=None, **kw):
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


    # pylint: disable=protected-access
    def visit_values(self, element, asfrom=False, from_linter=None, visiting_cte=None, **kw):
        """Compile a VALUES clause using ClickHouse's VALUES table function syntax.

        ClickHouse requires the column structure as the first argument:
            VALUES('col1 Type1, col2 Type2', (row1_val1, row1_val2), ...)

        This differs from standard SQL which places column names after the alias:
            (VALUES (row1), (row2)) AS name (col1, col2)
        """
        if getattr(element, "_independent_ctes", None):
            self._dispatch_independent_ctes(element, kw)

        structure = ", ".join(
            f"{col.name} {_resolve_ch_type_name(col.type)}"
            for col in element.columns
        )

        kw.setdefault("literal_binds", element.literal_binds)
        tuples = ", ".join(
            self.process(
                elements.Tuple(types=element._column_types, *elem).self_group(),
                **kw,
            )
            for chunk in element._data
            for elem in chunk
        )

        structure_literal = self.render_literal_value(structure, sqltypes.String())
        v = f"VALUES({structure_literal}, {tuples})"

        if element._unnamed:
            name = None
        elif isinstance(element.name, elements._truncated_label):
            name = self._truncated_identifier("values", element.name)
        else:
            name = element.name

        lateral = "LATERAL " if element._is_lateral else ""

        if asfrom:
            if from_linter:
                from_linter.froms[element._de_clone()] = (
                    name if name is not None else "(unnamed VALUES element)"
                )

            if visiting_cte is not None and visiting_cte.element is element:
                if element._is_lateral:
                    raise CompileError(
                        "Can't use a LATERAL VALUES expression inside of a CTE"
                    )
                v = f"SELECT * FROM {v}"
            elif name:
                kw["include_table"] = False
                v = f"{lateral}{v}{self.get_render_as_alias_suffix(self.preparer.quote(name))}"
            else:
                v = f"{lateral}{v}"

        return v

    def visit_array_join(self, array_join_clause, asfrom=False, from_linter=None, **kw):
        left = self.process(array_join_clause.left, asfrom=True, from_linter=from_linter, **kw)
        array_col = self.process(array_join_clause.array_column, **kw)
        join_type = "LEFT ARRAY JOIN" if array_join_clause.is_left else "ARRAY JOIN"
        text = f"{left} {join_type} {array_col}"
        if array_join_clause.alias:
            text += f" AS {self.preparer.quote(array_join_clause.alias)}"

        return text

    def visit_join(self, join, **kw):
        if isinstance(join, ArrayJoin):
            return self.visit_array_join(join, **kw)

        left = self.process(join.left, **kw)
        right = self.process(join.right, **kw)
        onclause = join.onclause

        is_cross = getattr(join, "_is_cross", False) or onclause is None
        if getattr(join, "full", False):
            join_type = "FULL OUTER JOIN"
        elif is_cross:
            join_type = "CROSS JOIN"
        elif join.isouter:
            join_type = "LEFT OUTER JOIN"
        else:
            join_type = "INNER JOIN"

        # ClickHouse modifiers: [GLOBAL] [ALL|ANY|ASOF] <join_type>
        distribution = getattr(join, "distribution", None)
        strictness = getattr(join, "strictness", None)
        parts = []
        if distribution:
            parts.append(distribution)
        if strictness:
            parts.append(strictness)
        parts.append(join_type)
        join_kw = " ".join(parts)

        text = f"{left} {join_kw} {right}"

        if not is_cross and onclause is not None:
            text += " ON " + self.process(onclause, **kw)

        return text

    def visit_column(self, column, add_to_result_map=None, include_table=True, result_map_targets=(), ambiguous_table_name_map=None, **kw):
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
    def delete_extra_from_clause(self, delete_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support DELETE with extra FROM clause")

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        raise NotImplementedError("ClickHouse doesn't support UPDATE with FROM clause")

    # pylint: disable=unused-argument
    def visit_empty_set_expr(self, element_types, **kw):
        return "SELECT 1 WHERE 1=0"

    def visit_sequence(self, sequence, **kw):
        raise NotImplementedError("ClickHouse doesn't support sequences")

    def group_by_clause(self, select, **kw):
        """Render GROUP BY using label aliases instead of full expressions."""
        kw["_ch_group_by"] = True
        return super().group_by_clause(select, **kw)

    # pylint: disable=protected-access
    def visit_label(
        self,
        label,
        within_columns_clause=False,
        render_label_as_label=None,
        **kw,
    ):
        ch_group_by = kw.pop("_ch_group_by", False)
        if ch_group_by and not within_columns_clause and render_label_as_label is None:
            if isinstance(label.name, elements._truncated_label):
                labelname = self._truncated_identifier("colident", label.name)
            else:
                labelname = label.name
            return self.preparer.format_label(label, labelname)
        return super().visit_label(
            label,
            within_columns_clause=within_columns_clause,
            render_label_as_label=render_label_as_label,
            **kw,
        )

    def get_from_hint_text(self, table, text):
        if text == "FINAL":
            return "FINAL"
        if text.startswith("SAMPLE"):
            return text
        return super().get_from_hint_text(table, text)
