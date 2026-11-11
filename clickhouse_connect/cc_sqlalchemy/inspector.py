import ast
import re
from collections.abc import Collection
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy.schema as sa_schema
from sqlalchemy import String, bindparam, text
from sqlalchemy.engine import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import InvalidRequestError, NoResultFound, NoSuchTableError
from sqlalchemy.sql.elements import TextClause

from clickhouse_connect.cc_sqlalchemy._schema import COLUMN_OPTIONS, TABLE_OPTIONS, canonical_schema_kwargs
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.ddl.index import ClickHouseIndex
from clickhouse_connect.cc_sqlalchemy.ddl.projection import ClickHouseProjection
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import build_engine
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.sqlparse import (
    extract_parenthesized_block,
    find_top_level_clause,
    split_top_level,
)
from clickhouse_connect.driver.client import _INTERNAL_QUERY_FORMATS

if TYPE_CHECKING:
    from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

_INTERNAL_QUERY_OPTION = "ch_internal_query"
_INTERNAL_QUERY_SENTINEL = object()
VIEW_ENGINE_KINDS = {"View": "view", "MaterializedView": "materialized_view", "WindowView": "window_view", "LiveView": "live_view"}
_EXPRESSION_KEYS = ("server_default", "clickhouse_materialized", "clickhouse_alias", "clickhouse_ttl")


class _NoSuchTableError(NoSuchTableError, NoResultFound):
    """Preserve NoResultFound handlers for missing-table reflection."""


def with_internal_query_formats(clause: TextClause) -> TextClause:
    """Force String columns to decode as str for driver metadata introspection.

    User-configured global read formats such as set_default_formats("String", "bytes")
    must not affect DESCRIBE / system.tables / SHOW TABLES results used by reflection.
    Mirrors driver.client._INTERNAL_QUERY_FORMATS on the orchestration path and marks the
    statement internal so every native_codec mode decodes it with the Python codec.
    """
    return clause.execution_options(
        query_formats=dict(_INTERNAL_QUERY_FORMATS),
        **{_INTERNAL_QUERY_OPTION: _INTERNAL_QUERY_SENTINEL},
    )


def _database_name(connection, schema: str | None) -> str:
    if schema:
        return schema
    return connection.execute(with_internal_query_formats(text("SELECT currentDatabase()"))).scalar()


def get_table_metadata(connection, table_name, schema=None):
    database = _database_name(connection, schema)
    result_set = connection.execute(
        with_internal_query_formats(
            text("SELECT engine, engine_full, comment FROM system.tables WHERE database = :database AND name = :table_name").bindparams(
                bindparam("database", type_=String()), bindparam("table_name", type_=String())
            )
        ),
        {"database": database, "table_name": table_name},
    )
    row = next(result_set, None)
    if not row:
        raise NoResultFound(f"Table {database}.{table_name} does not exist")
    return row


def get_engine(connection, table_name, schema=None):
    row = get_table_metadata(connection, table_name, schema)
    return build_engine(row.engine_full)


def get_clickhouse_function_names(connection: Connection) -> list[str]:
    """Return server-global SQL UDF names, excluding built-in and executable functions."""
    statement = text("SELECT name FROM system.functions WHERE origin = 'SQLUserDefined' ORDER BY name")
    return [row.name for row in connection.execute(with_internal_query_formats(statement))]


def get_clickhouse_function_definition(connection: Connection, function_name: str) -> str:
    """Return the complete stored CREATE FUNCTION statement."""
    statement = text("SELECT create_query FROM system.functions WHERE origin = 'SQLUserDefined' AND name = :function_name").bindparams(
        bindparam("function_name", type_=String())
    )
    row = connection.execute(with_internal_query_formats(statement), {"function_name": function_name}).first()
    if row is None:
        raise NoResultFound(f"SQL function {function_name!r} does not exist")
    return str(row.create_query)


def get_clickhouse_indexes(connection: Connection, table_name: str, schema: str | None = None) -> list[ClickHouseIndex]:
    """Reflect explicit, replayable index definitions without guessing implicit names."""
    database = _database_name(connection, schema)
    try:
        get_table_metadata(connection, table_name, database)
    except NoResultFound as ex:
        raise _NoSuchTableError(str(ex)) from ex
    return _get_clickhouse_index_catalog(connection, database, table_name).get(table_name, [])


def _get_clickhouse_index_catalog(connection: Connection, database: str, table_name: str | None = None) -> dict[str, list[ClickHouseIndex]]:
    """Read index definitions for tables already known to exist."""
    statement = text(
        "SELECT table, name, expr, type_full, granularity, creation FROM system.data_skipping_indices "
        "WHERE database = :database" + (" AND table = :table_name" if table_name is not None else "") + " ORDER BY table, name"
    ).bindparams(bindparam("database", type_=String()))
    parameters = {"database": database}
    if table_name is not None:
        statement = statement.bindparams(bindparam("table_name", type_=String()))
        parameters["table_name"] = table_name
    indexes: dict[str, list[ClickHouseIndex]] = {}
    for row in connection.execute(with_internal_query_formats(statement), parameters):
        if row.creation == "Implicit":
            continue
        if row.creation != "Explicit":
            raise InvalidRequestError(f"Unrecognized ClickHouse index creation classification {row.creation!r}")
        indexes.setdefault(row.table, []).append(ClickHouseIndex(row.name, f"tuple({row.expr})", row.type_full, row.granularity))
    return indexes


@dataclass(frozen=True)
class _ProjectionCatalogEntry(ClickHouseProjection):
    unsupported_reason: str | None = field(default=None, kw_only=True, compare=False)


def get_clickhouse_projections(connection: Connection, table_name: str, schema: str | None = None) -> list[ClickHouseProjection]:
    """Reflect replayable SELECT projections, refusing definitions we cannot preserve."""
    database = _database_name(connection, schema)
    try:
        get_table_metadata(connection, table_name, database)
    except NoResultFound as ex:
        raise _NoSuchTableError(str(ex)) from ex
    projections = _get_clickhouse_projection_catalog(connection, database, table_name).get(table_name, [])
    for projection in projections:
        if projection.unsupported_reason:
            raise InvalidRequestError(f"Cannot reflect ClickHouse projection {projection.name!r}: {projection.unsupported_reason}")
    return [ClickHouseProjection(projection.name, projection.select) for projection in projections]


def _get_clickhouse_projection_catalog(
    connection: Connection, database: str, table_name: str | None = None
) -> dict[str, list[_ProjectionCatalogEntry]]:
    statement = text(
        "SELECT table, name, toString(type) AS type, query, settings FROM system.projections "
        "WHERE database = :database" + (" AND table = :table_name" if table_name is not None else "") + " ORDER BY table, name"
    ).bindparams(bindparam("database", type_=String()))
    parameters = {"database": database}
    if table_name is not None:
        statement = statement.bindparams(bindparam("table_name", type_=String()))
        parameters["table_name"] = table_name
    projections: dict[str, list[_ProjectionCatalogEntry]] = {}
    for row in connection.execute(with_internal_query_formats(statement), parameters):
        reason = None
        if row.settings:
            reason = "projection WITH SETTINGS is not supported; its settings cannot be preserved"
        elif not row.query.startswith(("SELECT ", "WITH ")):
            reason = "only SELECT projections can be reflected; index-based projection definitions are not supported"
        elif row.type not in ("Normal", "Aggregate"):
            reason = f"unsupported projection type {row.type!r}"
        projections.setdefault(row.table, []).append(_ProjectionCatalogEntry(row.name, row.query, unsupported_reason=reason))
    return projections


def get_view_definition(connection, view_name: str, schema: str | None = None) -> str | None:
    """Return the stored SELECT of a view or materialized view, or None for other objects."""
    database = _database_name(connection, schema)
    result_set = connection.execute(
        with_internal_query_formats(
            text("SELECT as_select FROM system.tables WHERE database = :database AND name = :view_name AND engine IN :engines").bindparams(
                bindparam("database", type_=String()),
                bindparam("view_name", type_=String()),
                bindparam("engines", expanding=True),
            )
        ),
        {"database": database, "view_name": view_name, "engines": sorted(VIEW_ENGINE_KINDS)},
    )
    row = next(result_set, None)
    return row.as_select if row else None


def get_dictionary_create_sql(connection, table_name: str, schema: str | None = None) -> str:
    create_sql = connection.execute(with_internal_query_formats(text(f"SHOW CREATE DICTIONARY {full_table(table_name, schema)}"))).scalar()
    return create_sql or ""


def _parse_dictionary_column(definition: str) -> dict[str, Any]:
    match = re.match(r"^`(?P<name>[^`]+)`\s+(?P<rest>.+)$", definition, flags=re.DOTALL)
    if not match:
        match = re.match(r"^(?P<name>\S+)\s+(?P<rest>.+)$", definition, flags=re.DOTALL)
    if not match:
        raise ValueError(f"Could not parse dictionary column definition: {definition}")

    name = match.group("name")
    remainder = match.group("rest").strip()
    type_index, _ = find_top_level_clause(
        remainder,
        (" DEFAULT ", " MATERIALIZED ", " ALIAS ", " TTL ", " COMMENT ", " CODEC("),
    )
    type_name = remainder[:type_index].strip() if type_index != -1 else remainder
    sqla_type = sqla_type_from_name(type_name.replace("\n", " "))
    column = {
        "name": name,
        "type": sqla_type,
        "nullable": sqla_type.nullable,
        "autoincrement": False,
    }

    comment_index, comment_clause = find_top_level_clause(remainder, (" COMMENT ",))
    if comment_clause:
        comment_sql = remainder[comment_index + len(comment_clause) :].strip()
        column["comment"] = ast.literal_eval(comment_sql)
        remainder = remainder[:comment_index].rstrip()

    default_index, default_clause = find_top_level_clause(remainder, (" DEFAULT ", " MATERIALIZED ", " ALIAS "))
    if default_clause:
        default_sql = remainder[default_index + len(default_clause) :].strip()
        if default_clause == " DEFAULT ":
            column["server_default"] = default_sql
        elif default_clause == " MATERIALIZED ":
            column["clickhouse_materialized"] = default_sql
        elif default_clause == " ALIAS ":
            column["clickhouse_alias"] = default_sql
    return column


def get_dictionary_columns(connection, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
    create_sql = get_dictionary_create_sql(connection, table_name, schema)
    if not create_sql:
        return []
    start = create_sql.find("(")
    if start == -1:
        return []
    column_block, _ = extract_parenthesized_block(create_sql, start)
    return [_parse_dictionary_column(column_sql) for column_sql in split_top_level(column_block)]


def get_dictionary_metadata(connection, table_name: str, schema: str | None = None) -> dict[str, Any]:
    create_sql = get_dictionary_create_sql(connection, table_name, schema)
    if not create_sql:
        return {}

    metadata: dict[str, Any] = {"clickhouse_table_type": "dictionary"}
    for line in (line.strip() for line in create_sql.splitlines()):
        if not line:
            continue
        if line.startswith("PRIMARY KEY "):
            metadata["clickhouse_dictionary_primary_key"] = line[len("PRIMARY KEY ") :]
        elif line.startswith("SOURCE(") and line.endswith(")"):
            metadata["clickhouse_dictionary_source"] = line[len("SOURCE(") : -1]
        elif line.startswith("LIFETIME(") and line.endswith(")"):
            metadata["clickhouse_dictionary_lifetime"] = line[len("LIFETIME(") : -1]
        elif line.startswith("LAYOUT(") and line.endswith(")"):
            metadata["clickhouse_dictionary_layout"] = line[len("LAYOUT(") : -1]
        elif line.startswith("COMMENT "):
            metadata["comment"] = ast.literal_eval(line[len("COMMENT ") :])
    return metadata


def get_columns(connection, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
    table_metadata = get_table_metadata(connection, table_name, schema)
    if table_metadata.engine == "Dictionary":
        return get_dictionary_columns(connection, table_name, schema)
    table_id = full_table(table_name, schema)
    result_set = connection.execute(with_internal_query_formats(text(f"DESCRIBE TABLE {table_id}")))
    if not result_set:
        raise NoResultFound(f"Table {table_id} does not exist")
    columns = []
    for row in result_set:
        sqla_type = sqla_type_from_name(row.type.replace("\n", ""))
        col = {
            "name": row.name,
            "type": sqla_type,
            "nullable": sqla_type.nullable,
            "autoincrement": False,
            "comment": row.comment or None,
            "clickhouse_codec": row.codec_expression or None,
            "clickhouse_ttl": row.ttl_expression or None,
        }
        if row.default_type == "DEFAULT" and row.default_expression:
            col["server_default"] = row.default_expression
        elif row.default_type == "MATERIALIZED" and row.default_expression:
            col["clickhouse_materialized"] = row.default_expression
        elif row.default_type == "ALIAS" and row.default_expression:
            col["clickhouse_alias"] = row.default_expression
        columns.append(col)
    return columns


class ChInspector(Inspector):
    def get_clickhouse_function_names(self) -> list[str]:
        """Return server-global SQL UDF names."""
        with self._operation_context() as connection:
            return cast("ClickHouseDialect", self.dialect).get_clickhouse_function_names(connection)

    def get_clickhouse_function_definition(self, function_name: str) -> str:
        """Return stored CREATE FUNCTION SQL; missing names raise NoResultFound."""
        with self._operation_context() as connection:
            return cast("ClickHouseDialect", self.dialect).get_clickhouse_function_definition(connection, function_name)

    def get_clickhouse_indexes(self, table_name: str, schema: str | None = None, **kwargs: Any) -> list[ClickHouseIndex]:
        """Return explicit skip indexes. Generic get_indexes remains unchanged."""
        with self._operation_context() as connection:
            return cast("ClickHouseDialect", self.dialect).get_clickhouse_indexes(connection, table_name, schema, **kwargs)

    def get_clickhouse_projections(self, table_name: str, schema: str | None = None, **kwargs: Any) -> list[ClickHouseProjection]:
        """Return replayable SELECT projection definitions."""
        with self._operation_context() as connection:
            return cast("ClickHouseDialect", self.dialect).get_clickhouse_projections(connection, table_name, schema, **kwargs)

    def reflect_table(
        self,
        table,
        include_columns: Collection[str] | None = None,
        exclude_columns: Collection[str] = (),
        resolve_fks: bool = True,
        *_args,
        **_kwargs,
    ):
        schema = table.schema
        with self._inspection_context() as inspector:
            connection = inspector.bind
            try:
                table_metadata = get_table_metadata(connection, table.name, schema)
            except NoResultFound as ex:
                raise _NoSuchTableError(str(ex)) from ex
            reflected_columns: list[dict[str, Any]]
            if table_metadata.engine == "Dictionary":
                reflected_columns = get_dictionary_columns(connection, table.name, schema)
            else:
                reflected_columns = cast(list[dict[str, Any]], inspector.get_columns(table.name, schema))

            for col in reflected_columns:
                name = col.pop("name")
                if (include_columns and name not in include_columns) or (exclude_columns and name in exclude_columns):
                    continue
                col_type = col.pop("type")
                # Reflection returns expressions as SQL strings; Column needs clauses.
                for key in _EXPRESSION_KEYS:
                    if isinstance(col.get(key), str):
                        col[key] = text(col[key])
                col_args = canonical_schema_kwargs({key: value for key, value in col.items() if value is not None}, COLUMN_OPTIONS)
                table.append_column(sa_schema.Column(name, col_type, **col_args))
            if table_metadata.engine == "Dictionary":
                dictionary_metadata = get_dictionary_metadata(connection, table.name, schema)
                table.comment = dictionary_metadata.pop("comment", None)
                for key, value in canonical_schema_kwargs(dictionary_metadata, TABLE_OPTIONS).items():
                    table.kwargs[key] = value
                return

            table.comment = table_metadata.comment or None
            view_kind = VIEW_ENGINE_KINDS.get(table_metadata.engine)
            if view_kind is not None:
                # Views carry no engine. Record the kind so autogenerate can tell them from tables.
                table.engine = None
                table.kwargs["clickhousedb_table_type"] = view_kind
                return
            table.engine = build_engine(table_metadata.engine_full)
            if table.engine is not None:
                table.kwargs["clickhousedb_engine"] = table.engine

    def get_columns(self, table_name, schema=None, **_kwargs):
        with self._operation_context() as connection:
            try:
                return get_columns(connection, table_name, schema)
            except NoResultFound as ex:
                raise _NoSuchTableError(str(ex)) from ex

    def get_materialized_view_names(self, schema: str | None = None, **kwargs: Any) -> list[str]:
        """Return materialized views, including on SQLAlchemy 1.4."""
        with self._operation_context() as connection:
            return self.dialect.get_materialized_view_names(connection, schema, info_cache=self.info_cache, **kwargs)
