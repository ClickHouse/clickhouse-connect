"""Autogenerate comparison of explicit ClickHouse SELECT projections."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from typing import cast

from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.compare import comparators
from alembic.operations import ops
from alembic.util import CommandError, DispatchPriority
from sqlalchemy import String, Table, bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy._schema import canonical_schema_kwargs
from clickhouse_connect.cc_sqlalchemy.alembic.operations import _AddClickHouseProjectionOp, _DropClickHouseProjectionOp
from clickhouse_connect.cc_sqlalchemy.ddl.projection import ClickHouseProjection, projection_definition, table_projections
from clickhouse_connect.cc_sqlalchemy.inspector import (
    _database_name,
    _get_clickhouse_projection_catalog,
    _ProjectionCatalogEntry,
    with_internal_query_formats,
)
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper
from clickhouse_connect.driver.binding import _SQL_TOKEN_INVALID, _SQL_TOKEN_OPEN_PAREN, _SQL_TOKEN_TRIVIA, _SQL_TOKEN_WORD, _next_sql_token

logger = logging.getLogger("alembic.autogenerate.compare.clickhouse_projections")

COMPARE_PROJECTIONS_OPT = "clickhousedb_compare_projections"


@dataclass
class _ComparisonState:
    database: str | None = None
    function_names: tuple[dict[str, str], dict[str, str]] | None = None
    catalogs: dict[str, dict[str, list[_ProjectionCatalogEntry]]] = field(default_factory=dict)


def _comparison_state(context: AutogenContext) -> _ComparisonState:
    state = getattr(context, "_clickhouse_projection_comparison", None)
    if state is None:
        state = _ComparisonState()
        context._clickhouse_projection_comparison = state  # type: ignore[attr-defined] # AutogenContext supports extension state.
    return cast(_ComparisonState, state)


def _reflected_projections(context: AutogenContext, table_name: str, schema: str | None) -> list[_ProjectionCatalogEntry]:
    assert context.connection is not None
    state = _comparison_state(context)
    if schema:
        database = schema
    else:
        if state.database is None:
            state.database = _database_name(context.connection, None)
        database = state.database
    if database not in state.catalogs:
        state.catalogs[database] = _get_clickhouse_projection_catalog(context.connection, database)
    return state.catalogs[database].get(table_name, [])


@dataclass(frozen=True)
class _BoundProjection(ClickHouseProjection):
    table: Table = field(kw_only=True, repr=False, compare=False)


def _bound_projection(projection: ClickHouseProjection, table: Table) -> _BoundProjection:
    return _BoundProjection(
        projection.name,
        projection.select,
        projection.if_not_exists,
        projection.first,
        projection.after_projection,
        table=table,
    )


class ProjectionComparisonError(CommandError):
    """Projection comparison requires catalog or normalization coverage that is unavailable."""

    def __init__(
        self,
        table_name: str,
        schema: str | None,
        projection_name: str | None,
        reflected: ClickHouseProjection | None,
        declared: ClickHouseProjection | None,
        reason: str,
    ) -> None:
        self.table_name = table_name
        self.schema = schema
        self.projection_name = projection_name
        self.reflected = ClickHouseProjection(reflected.name, reflected.select) if reflected is not None else None
        self.declared = declared
        self.reason = reason
        super().__init__(
            f"Cannot compare ClickHouse projection {projection_name!r} on {full_table(table_name, schema)}: {reason}. "
            f"Write an explicit migration or pass {COMPARE_PROJECTIONS_OPT}=False to context.configure()."
        )


def _require_replayable(
    table_name: str, schema: str | None, projection: ClickHouseProjection, declared: ClickHouseProjection | None
) -> ClickHouseProjection:
    reason = getattr(projection, "unsupported_reason", None)
    if reason:
        raise ProjectionComparisonError(table_name, schema, projection.name, projection, declared, reason)
    return ClickHouseProjection(projection.name, projection.select)


def _add_op(table_name: str, schema: str | None, projection: ClickHouseProjection) -> _AddClickHouseProjectionOp:
    return _AddClickHouseProjectionOp(table_name, projection.name, projection.select, schema=schema)


def _function_names(context: AutogenContext) -> tuple[dict[str, str], dict[str, str]]:
    state = _comparison_state(context)
    if state.function_names is None:
        if context.connection is None:
            raise ValueError("Projection function normalization requires an online connection")
        scalar: dict[str, str] = {}
        aggregate: dict[str, str] = {}
        rows = context.connection.execute(
            with_internal_query_formats(
                text("SELECT name, toString(is_aggregate) AS is_aggregate, alias_to FROM system.functions WHERE case_insensitive = 1")
            )
        )
        for row in rows:
            names = aggregate if row.is_aggregate == "1" else scalar
            names[row.name.lower()] = row.alias_to or row.name
        if not scalar or not aggregate:
            raise ValueError("Server returned incomplete case-insensitive function metadata")
        state.function_names = scalar, aggregate
    return state.function_names


def _normalize_function_names(query: str, names: tuple[dict[str, str], dict[str, str]]) -> str:
    """Reproduce FunctionNameNormalizer's scalar-then-aggregate name lookup."""
    scalar, aggregate = names
    parts: list[str] = []
    copied = 0
    index = 0
    previous: tuple[int, int, int] | None = None
    qualified = False
    while index < len(query):
        kind, end = _next_sql_token(query, index, {})
        if kind == _SQL_TOKEN_INVALID:
            raise ValueError("Cannot tokenize the server's formatted projection definition")
        if kind == _SQL_TOKEN_TRIVIA:
            index = end
            continue
        if kind == _SQL_TOKEN_OPEN_PAREN and previous is not None and previous[0] == _SQL_TOKEN_WORD and not qualified:
            _, start, word_end = previous
            name = query[start:word_end]
            normalized = scalar.get(name.lower(), name)
            normalized = aggregate.get(normalized.lower(), normalized)
            if normalized != name:
                parts.extend((query[copied:start], normalized))
                copied = word_end
        qualified = previous is not None and query[previous[1] : previous[2]] == "."
        previous = kind, index, end
        index = end
    parts.append(query[copied:])
    return "".join(parts)


def _definitions_match(context: AutogenContext, declared: ClickHouseProjection, reflected: ClickHouseProjection) -> bool:
    if declared.select == reflected.select:
        return True
    queries = {}
    for key, projection in (("declared", declared), ("reflected", reflected)):
        definition = projection_definition(replace(projection, name="_projection"))
        queries[key] = f"CREATE TABLE _table (_value UInt8, PROJECTION {definition}) ENGINE MergeTree ORDER BY tuple()"
    statement = text("SELECT formatQuerySingleLine(:declared), formatQuerySingleLine(:reflected)").bindparams(
        bindparam("declared", type_=String()), bindparam("reflected", type_=String())
    )
    if context.connection is None:
        raise ValueError("Projection normalization requires an online connection")
    row = context.connection.execute(with_internal_query_formats(statement), queries).fetchone()
    if row is None or len(row) != 2 or not all(isinstance(value, str) and value for value in row):
        raise ValueError("Server returned no formatted projection definitions")
    if row[0] == row[1]:
        return True
    names = _function_names(context)
    return _normalize_function_names(row[0], names) == _normalize_function_names(row[1], names)


def _name_included(context: AutogenContext, name: str, schema: str | None, table_name: str) -> bool:
    # Alembic accepts extension filter types at runtime but annotates only built-ins.
    return context.run_name_filters(name, "projection", {"schema_name": schema, "table_name": table_name})  # type: ignore[arg-type]


@comparators.dispatch_for("table", qualifier="clickhousedb")
def _compare_clickhouse_projections(
    context: AutogenContext,
    modify_table_ops: ops.ModifyTableOps,
    schema: str | None,
    table_name: str,
    conn_table: Table | None,
    metadata_table: Table | None,
) -> None:
    if conn_table is None or not context.opts.get(COMPARE_PROJECTIONS_OPT, True):
        return
    if ClickHouseDDLHelper.is_dictionary(conn_table) or ClickHouseDDLHelper.get_option(conn_table, "table_type"):
        return
    assert context.connection is not None
    try:
        reflected = _reflected_projections(context, table_name, schema)
    except SQLAlchemyError as ex:
        raise ProjectionComparisonError(table_name, schema, None, None, None, f"cannot read explicit projection catalog: {ex}") from ex
    old: dict[str, ClickHouseProjection] = {
        projection.name: projection for projection in reflected if _name_included(context, projection.name, schema, table_name)
    }
    if metadata_table is None:
        snapshot = tuple(
            _require_replayable(table_name, schema, projection, None)
            for name, projection in old.items()
            if context.run_object_filters(_bound_projection(projection, conn_table), name, "projection", True, None)  # type: ignore[arg-type] # Extension filter type.
        )
        if snapshot:
            conn_table.kwargs["clickhousedb_projections"] = snapshot
        return
    new = {
        projection.name: projection
        for projection in table_projections(metadata_table)
        if _name_included(context, projection.name, schema, table_name)
    }
    drops: list[_DropClickHouseProjectionOp] = []
    additions: list[_AddClickHouseProjectionOp] = []
    for name in sorted(old.keys() | new.keys()):
        before, after = old.get(name), new.get(name)
        candidate = after if after is not None else before
        assert candidate is not None
        bound = _bound_projection(candidate, metadata_table if after is not None else conn_table)
        compare_to = _bound_projection(before, conn_table) if before is not None and after is not None else None
        if not context.run_object_filters(bound, name, "projection", after is None, compare_to):  # type: ignore[arg-type] # Extension filter type.
            continue
        if before is not None:
            before = _require_replayable(table_name, schema, before, after)
        if before is not None and after is not None:
            try:
                if _definitions_match(context, after, before):
                    continue
            except (SQLAlchemyError, ValueError) as ex:
                raise ProjectionComparisonError(
                    table_name, schema, name, before, after, f"cannot normalize projection definitions: {ex}"
                ) from ex
            logger.info("Detected changed projection %r on %r", name, table_name)
        elif after is None:
            logger.info("Detected removed projection %r on %r", name, table_name)
        else:
            logger.info("Detected added projection %r on %r", name, table_name)
        if before is not None:
            drops.append(_DropClickHouseProjectionOp(table_name, name, schema=schema, existing_projection=before))
        if after is not None:
            additions.append(_add_op(table_name, schema, after))
    modify_table_ops.ops[:0] = drops
    modify_table_ops.ops.extend(additions)


@comparators.dispatch_for("schema", qualifier="clickhousedb", priority=DispatchPriority.LAST)
def _filter_created_table_projections(context: AutogenContext, upgrade_ops: ops.UpgradeOps, schemas: set[str | None]) -> None:
    """Apply projection filters to inline CREATE definitions before policy hooks run."""
    for operation in upgrade_ops.ops:
        if not isinstance(operation, ops.CreateTableOp):
            continue
        operation.kw = canonical_schema_kwargs(operation.kw, ("projections",))
        projections = operation.kw.get("clickhousedb_projections")
        if projections is None:
            continue
        table = operation.to_table()
        operation.kw["clickhousedb_projections"] = tuple(
            projection
            for projection in projections
            if _name_included(context, projection.name, operation.schema, operation.table_name)
            and context.run_object_filters(_bound_projection(projection, table), projection.name, "projection", False, None)  # type: ignore[arg-type] # Extension filter type.
        )
