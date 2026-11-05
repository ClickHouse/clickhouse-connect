"""Autogenerate comparison of explicit ClickHouse data skipping indexes."""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any, cast

from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.compare import comparators
from alembic.operations import ops
from alembic.util import CommandError, DispatchPriority
from sqlalchemy import Column, String, Table, bindparam, literal, text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy._schema import canonical_schema_kwargs
from clickhouse_connect.cc_sqlalchemy.alembic.compare import _unwrap_single
from clickhouse_connect.cc_sqlalchemy.alembic.operations import _AddClickHouseIndexOp, _DropClickHouseIndexOp
from clickhouse_connect.cc_sqlalchemy.ddl.index import ClickHouseIndex, index_definition, table_indexes
from clickhouse_connect.cc_sqlalchemy.inspector import _database_name, _get_clickhouse_index_catalog, with_internal_query_formats
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper
from clickhouse_connect.cc_sqlalchemy.sql.sqlparse import extract_parenthesized_block, split_top_level
from clickhouse_connect.driver.binding import format_str, quote_identifier
from clickhouse_connect.driver.exceptions import Error

logger = logging.getLogger("alembic.autogenerate.compare.clickhouse_indexes")

COMPARE_INDEXES_OPT = "clickhousedb_compare_indexes"


@dataclass
class _ComparisonState:
    database: str | None = None
    catalogs: dict[str, dict[str, list[ClickHouseIndex]]] = field(default_factory=dict)
    analyzer_enabled: bool | None = None


def _comparison_state(context: AutogenContext) -> _ComparisonState:
    state = getattr(context, "_clickhouse_index_comparison", None)
    if state is None:
        state = _ComparisonState()
        context._clickhouse_index_comparison = state  # type: ignore[attr-defined] # AutogenContext supports extension state.
    return cast(_ComparisonState, state)


def _reflected_indexes(context: AutogenContext, table_name: str, schema: str | None) -> list[ClickHouseIndex]:
    assert context.connection is not None
    state = _comparison_state(context)
    if schema:
        database = schema
    else:
        if state.database is None:
            state.database = _database_name(context.connection, None)
        database = state.database
    if database not in state.catalogs:
        state.catalogs[database] = _get_clickhouse_index_catalog(context.connection, database)
    return state.catalogs[database].get(table_name, [])


@dataclass(frozen=True)
class _BoundIndex(ClickHouseIndex):
    table: Table = field(kw_only=True, repr=False, compare=False)


def _bound_index(index: ClickHouseIndex, table: Table) -> _BoundIndex:
    return _BoundIndex(
        index.name,
        index.expression,
        index.type_,
        index.granularity,
        index.if_not_exists,
        index.first,
        index.after_index,
        table=table,
    )


class IndexComparisonError(CommandError):
    """Index comparison requires catalog or normalization coverage that is unavailable."""

    def __init__(
        self,
        table_name: str,
        schema: str | None,
        index_name: str | None,
        reflected: ClickHouseIndex | None,
        declared: ClickHouseIndex | None,
        reason: str,
    ) -> None:
        self.table_name = table_name
        self.schema = schema
        self.index_name = index_name
        self.reflected = reflected
        self.declared = declared
        self.reason = reason
        super().__init__(
            f"Cannot compare ClickHouse index {index_name!r} on {full_table(table_name, schema)}: {reason}. "
            f"Write an explicit migration or pass {COMPARE_INDEXES_OPT}=False to context.configure()."
        )


def _add_op(table_name: str, schema: str | None, index: ClickHouseIndex) -> _AddClickHouseIndexOp:
    return _AddClickHouseIndexOp(
        table_name,
        index.name,
        index.expression,
        index.type_,
        granularity=index.granularity,
        schema=schema,
    )


def _require_analyzer(context: AutogenContext) -> None:
    assert context.connection is not None
    state = _comparison_state(context)
    if state.analyzer_enabled is None:
        state.analyzer_enabled = bool(
            context.connection.execute(with_internal_query_formats(text("SELECT getSetting('enable_analyzer')"))).scalar()
        )
    if not state.analyzer_enabled:
        raise ValueError("Index normalization requires enable_analyzer=1 on the current connection")


def _key_expression(context: AutogenContext, expression: str) -> str:
    """Reproduce extractKeyExpressionList on server-formatted expression syntax."""
    expression = _unwrap_single(expression)
    if expression.startswith("tuple("):
        inner, end = extract_parenthesized_block(expression, len("tuple"))
        if end == len(expression) - 1:
            return f"tuple({inner})"
    elif expression.startswith("("):
        inner, end = extract_parenthesized_block(expression, 0)
        if end == len(expression) - 1 and len(split_top_level(inner)) > 1:
            assert context.connection is not None
            _require_analyzer(context)
            rows = context.connection.exec_driver_sql(
                f"EXPLAIN QUERY TREE run_passes=0 SELECT {expression} AS _index LIMIT 1",
                execution_options=with_internal_query_formats(text("")).get_execution_options(),
            ).fetchall()
            if [row[0] for row in rows[:3]] != ["QUERY id: 0", "  PROJECTION", "    LIST id: 1, nodes: 1"]:
                raise ValueError("Cannot identify the server's index expression root")
            root = rows[3][0] if len(rows) > 3 else ""
            if root == "      FUNCTION id: 2, alias: _index, function_name: tuple, function_type: ordinary":
                return f"tuple({inner})"
            if not root.startswith("      CONSTANT id: 2, alias: _index,"):
                raise ValueError("Cannot distinguish tuple expression syntax from a tuple literal")
    return f"tuple({expression})"


def _aliases(context: AutogenContext, table: Table) -> str:
    aliases = []
    for column in table.columns:
        alias = ClickHouseDDLHelper.get_option(column, "alias")
        if alias is not None:
            clause = literal(alias) if isinstance(alias, str) else alias
            rendered = clause.compile(dialect=context.dialect, compile_kwargs={"literal_binds": True})
            aliases.append(f"{rendered} AS {quote_identifier(column.name)}")
    return "WITH " + ", ".join(aliases) + " " if aliases else ""


def _analyzed_expression(context: AutogenContext, expression: str, table: Table, *, added_columns: Sequence[Column[Any]] = ()) -> list[str]:
    assert context.connection is not None
    _require_analyzer(context)
    source = full_table(table.name, table.schema)
    if added_columns:
        structure = ", ".join(
            f"{quote_identifier(column.name)} {ClickHouseDDLHelper.effective_column_type(column).compile(dialect=context.dialect)}"
            for column in added_columns
        )
        source += f" CROSS JOIN null({format_str(structure)}) AS _index_added_columns"
    statement = (
        "EXPLAIN SYNTAX run_query_tree_passes=1, query_tree_passes=1, oneline=1 "
        f"{_aliases(context, table)}SELECT {expression} AS _index FROM {source} LIMIT 1 "
        "SETTINGS prefer_column_name_to_alias=0"
    )
    rows = context.connection.exec_driver_sql(
        statement, execution_options=with_internal_query_formats(text("")).get_execution_options()
    ).fetchall()
    if not rows:
        raise ValueError("Server returned no analyzed index expression")
    return [row[0] for row in rows]


def _definitions_match(
    context: AutogenContext,
    declared: ClickHouseIndex,
    reflected: ClickHouseIndex,
    metadata_table: Table,
    conn_table: Table,
    added_columns: Sequence[Column[Any]] = (),
) -> bool:
    if (declared.expression, declared.type_, declared.granularity) == (
        reflected.expression,
        reflected.type_,
        reflected.granularity,
    ) and _aliases(context, metadata_table) == _aliases(context, conn_table):
        return True
    queries: dict[str, str] = {}
    for key, index in (("declared", declared), ("reflected", reflected)):
        # Validate the complete INDEX grammar even though comparison uses its shape and expression separately.
        definition = index_definition(replace(index, name="_index"))
        queries[key] = f"CREATE TABLE _table (_value UInt8, INDEX {definition}) ENGINE MergeTree ORDER BY tuple()"
        shape = index_definition(replace(index, name="_index", expression="_value"))
        queries[f"{key}_shape"] = f"CREATE TABLE _table (_value UInt8, INDEX {shape}) ENGINE MergeTree ORDER BY tuple()"
        queries[f"{key}_expression"] = "SELECT " + index.expression
    statement = text("SELECT " + ", ".join(f"formatQuerySingleLine(:{key})" for key in queries)).bindparams(
        *(bindparam(key, type_=String()) for key in queries)
    )
    if context.connection is None:
        raise ValueError("Index normalization requires an online connection")
    row = context.connection.execute(with_internal_query_formats(statement), queries).fetchone()
    if row is None or len(row) != len(queries) or not all(isinstance(value, str) and value for value in row):
        raise ValueError("Server returned no formatted index definitions")
    formatted = dict(zip(queries, cast(Sequence[str], row)))
    # IndexDescription::getIndexFromAST lowercases the type name; empty arguments do not change the type.
    shapes = [
        re.sub(
            r" TYPE ([A-Za-z_][A-Za-z_0-9]*)(?:\(\))?",
            lambda match: f" TYPE {match[1].lower()}",
            formatted[f"{key}_shape"],
            count=1,
        )
        for key in ("declared", "reflected")
    ]
    expressions = []
    for key in ("declared", "reflected"):
        expression = formatted[f"{key}_expression"]
        if not expression.startswith("SELECT "):
            raise ValueError("Server returned unrecognized index expression syntax")
        expressions.append(_key_expression(context, expression[len("SELECT ") :]))
    same_aliases = _aliases(context, metadata_table) == _aliases(context, conn_table)
    if not same_aliases and _analyzed_expression(context, expressions[0], metadata_table) != _analyzed_expression(
        context, expressions[0], conn_table
    ):
        raise ValueError("Index depends on changed ALIAS columns; migrate those columns explicitly before comparing the index")
    if shapes[0] != shapes[1]:
        return False
    if expressions[0] == expressions[1]:
        return True
    try:
        analyzed = _analyzed_expression(context, expressions[0], metadata_table)
    except DBAPIError as ex:
        if not isinstance(ex.orig, Error) or ex.orig.code != 47:
            raise
        missing = re.search(r"Unknown expression(?: or function)? identifier `([^`]+)` in scope", str(ex.orig))
        if missing is None:
            raise
        physical_additions = [
            column
            for column in added_columns
            if not any(existing.name == column.name for existing in conn_table.columns)
            and ClickHouseDDLHelper.get_option(column, "alias") is None
        ]
        if not any(column.name == missing[1] for column in physical_additions):
            raise
        _analyzed_expression(context, expressions[0], metadata_table, added_columns=physical_additions)
        return False
    return analyzed == _analyzed_expression(context, expressions[1], conn_table)


def _name_included(context: AutogenContext, name: str, schema: str | None, table_name: str) -> bool:
    return context.run_name_filters(name, "index", {"schema_name": schema, "table_name": table_name})


@comparators.dispatch_for("table", qualifier="clickhousedb")
def _compare_clickhouse_indexes(
    context: AutogenContext,
    modify_table_ops: ops.ModifyTableOps,
    schema: str | None,
    table_name: str,
    conn_table: Table | None,
    metadata_table: Table | None,
) -> None:
    if conn_table is None or not context.opts.get(COMPARE_INDEXES_OPT, True):
        return
    if ClickHouseDDLHelper.is_dictionary(conn_table) or ClickHouseDDLHelper.get_option(conn_table, "table_type"):
        return
    assert context.connection is not None
    try:
        reflected = _reflected_indexes(context, table_name, schema)
    except SQLAlchemyError as ex:
        raise IndexComparisonError(table_name, schema, None, None, None, f"cannot read explicit index catalog: {ex}") from ex
    old = {index.name: index for index in reflected if _name_included(context, index.name, schema, table_name)}
    if metadata_table is None:
        snapshot = tuple(
            index for name, index in old.items() if context.run_object_filters(_bound_index(index, conn_table), name, "index", True, None)
        )
        if snapshot:
            conn_table.kwargs["clickhousedb_indexes"] = snapshot
        return
    new = {index.name: index for index in table_indexes(metadata_table) if _name_included(context, index.name, schema, table_name)}
    for name in sorted(old.keys() | new.keys()):
        before, after = old.get(name), new.get(name)
        candidate = after if after is not None else before
        assert candidate is not None
        bound = _bound_index(candidate, metadata_table if after is not None else conn_table)
        compare_to = _bound_index(before, conn_table) if before is not None and after is not None else None
        if not context.run_object_filters(bound, name, "index", after is None, compare_to):
            continue
        if before is not None and after is not None:
            try:
                if _definitions_match(
                    context,
                    after,
                    before,
                    metadata_table,
                    conn_table,
                    [operation.column for operation in modify_table_ops.ops if isinstance(operation, ops.AddColumnOp)],
                ):
                    continue
            except (SQLAlchemyError, ValueError) as ex:
                raise IndexComparisonError(table_name, schema, name, before, after, f"cannot normalize index definitions: {ex}") from ex
            logger.info("Detected changed index %r on %r", name, table_name)
        elif after is None:
            logger.info("Detected removed index %r on %r", name, table_name)
        else:
            logger.info("Detected added index %r on %r", name, table_name)
        if before is not None:
            modify_table_ops.ops.append(_DropClickHouseIndexOp(table_name, name, schema=schema, existing_index=before))
        if after is not None:
            modify_table_ops.ops.append(_add_op(table_name, schema, after))


@comparators.dispatch_for("schema", qualifier="clickhousedb", priority=DispatchPriority.LAST)
def _filter_created_table_indexes(context: AutogenContext, upgrade_ops: ops.UpgradeOps, schemas: set[str | None]) -> None:
    """Apply index filters to inline CREATE definitions before policy hooks run."""
    for operation in upgrade_ops.ops:
        if not isinstance(operation, ops.CreateTableOp):
            continue
        operation.kw = canonical_schema_kwargs(operation.kw, ("indexes",))
        indexes = operation.kw.get("clickhousedb_indexes")
        if indexes is None:
            continue
        table = operation.to_table()
        operation.kw["clickhousedb_indexes"] = tuple(
            index
            for index in indexes
            if _name_included(context, index.name, operation.schema, operation.table_name)
            and context.run_object_filters(_bound_index(index, table), index.name, "index", False, None)
        )
