"""Autogenerate comparison of ClickHouse table options."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from alembic.autogenerate.api import AutogenContext
from alembic.autogenerate.compare import comparators
from alembic.operations import ops
from alembic.util import CommandError
from sqlalchemy import String, Table, bindparam, text
from sqlalchemy.exc import SQLAlchemyError

from clickhouse_connect.cc_sqlalchemy.alembic.operations import _ModifyClickHouseTableSettingsOp
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import (
    TableEngine,
    _render_engine_expr,
    _render_setting_value,
    _render_summing_columns,
    _strip_string_quotes,
)
from clickhouse_connect.cc_sqlalchemy.inspector import with_internal_query_formats
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ClickHouseDDLHelper
from clickhouse_connect.cc_sqlalchemy.sql.sqlparse import split_top_level

__all__ = ["COMPARE_TABLE_OPTIONS_OPT", "TableOptionDrift", "UnsupportedTableChangeError"]

logger = logging.getLogger(__name__)

COMPARE_TABLE_OPTIONS_OPT = "clickhousedb_compare_table_options"
_KEY_CLAUSES = ("order_by", "partition_by", "primary_key", "sample_by", "ttl")
# Replication paths are deployment facts, not schema, and Cloud rewrites them.
_DEPLOYMENT_ARGS = frozenset({"zk_path", "replica"})
_ENGINE_FAMILY_PREFIXES = ("Shared", "Replicated")
# MergeTreeSettings::isReadonlySetting at server v26.6.1.1193-stable: fixed at CREATE, MODIFY SETTING is refused.
_READONLY_SETTINGS = frozenset(
    {
        "index_granularity",
        "index_granularity_bytes",
        "enable_mixed_granularity_parts",
        "add_minmax_index_for_numeric_columns",
        "add_minmax_index_for_string_columns",
        "add_minmax_index_for_temporal_columns",
        "add_minmax_index_for_block_number_column",
        "add_minmax_index_for_block_offset_column",
        "table_disk",
        "allow_tuple_element_aggregation",
        "share_nested_offsets",
    }
)

_Difference = tuple[str, str | None, str | None]


@dataclass(frozen=True)
class TableOptionDrift:
    """One table option whose declared value differs from the server."""

    table_name: str
    schema: str | None
    option: str
    reflected: str | None
    declared: str | None

    @property
    def qualified_name(self) -> str:
        return full_table(self.table_name, self.schema)


class UnsupportedTableChangeError(CommandError):
    """Autogenerate found table options that differ from the server and have no ALTER."""

    def __init__(self, drifts: Sequence[TableOptionDrift]) -> None:
        self.drifts = tuple(drifts)
        super().__init__(self._format(self.drifts))

    @staticmethod
    def _format(drifts: Sequence[TableOptionDrift]) -> str:
        lines = [
            f"Table {drifts[0].qualified_name} declares table options that differ from the server, and ClickHouse cannot ALTER them in place:"
        ]
        lines.extend(f"  {drift.option}: server {drift.reflected or 'none'}, model {drift.declared or 'none'}" for drift in drifts)
        lines.append(
            "Revert the model, or recreate the table with an explicit migration. "
            f"Pass {COMPARE_TABLE_OPTIONS_OPT}=False to context.configure() to skip this check."
        )
        return "\n".join(lines)


def _table_engine(table: Table) -> TableEngine | None:
    engine = getattr(table, "engine", None) or ClickHouseDDLHelper.get_option(table, "engine")
    return engine if isinstance(engine, TableEngine) else None


def _engine_family(name: str) -> str:
    for prefix in _ENGINE_FAMILY_PREFIXES:
        if name.startswith(prefix) and len(name) > len(prefix):
            name = name[len(prefix) :]
    return name


def _declared_clause(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (tuple, list)):
        return "(" + ",".join(_render_engine_expr(item) for item in value) + ")"
    return _render_engine_expr(value)


def normalize_expression(expression: str) -> str:
    """Reduce an engine clause to a whitespace-free, unquoted-identifier form for comparison."""
    chars: list[str] = []
    quote: str | None = None
    pos = 0
    while pos < len(expression):
        char = expression[pos]
        if quote:
            if char == "\\" and quote == "'" and pos + 1 < len(expression):
                chars.append(char)
                pos += 1
                chars.append(expression[pos])
            elif char == quote:
                quote = None
                if char != "`":
                    chars.append(char)
            else:
                chars.append(char)
        elif char in ("'", '"', "`"):
            quote = char
            if char != "`":
                chars.append(char)
        elif not char.isspace():
            chars.append(char)
        pos += 1
    normalized = "".join(chars)
    if normalized == "()":
        return "tuple()"
    return _unwrap_single(normalized)


def _unwrap_single(expression: str) -> str:
    # (a) and a are the same key, (a, b) is a tuple and stays wrapped.
    while expression.startswith("(") and expression.endswith(")"):
        depth = 0
        for index, char in enumerate(expression):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(expression) - 1:
                    return expression
        inner = expression[1:-1]
        if not inner or len(split_top_level(inner)) != 1:
            return expression
        expression = inner
    return expression


def _server_normalized(connection: Any, expressions: Sequence[str]) -> list[str] | None:
    """Ask the server to format each expression, or None when it cannot."""
    if connection is None:
        return None
    columns = ", ".join(f"formatQuerySingleLine(concat('SELECT ', :expr{index}))" for index in range(len(expressions)))
    statement = text(f"SELECT {columns}").bindparams(*(bindparam(f"expr{index}", type_=String()) for index in range(len(expressions))))
    try:
        row = connection.execute(
            with_internal_query_formats(statement), {f"expr{index}": value for index, value in enumerate(expressions)}
        ).fetchone()
    except SQLAlchemyError as ex:
        logger.warning("Server could not normalize engine expressions %r, comparing them textually: %s", list(expressions), ex)
        return None
    if row is None:
        return None
    return [normalize_expression(str(value)[len("SELECT ") :]) for value in row]


def _server_setting_defaults(connection: Any, names: Sequence[str]) -> dict[str, Any]:
    """Return server default values for MergeTree settings missing from engine_full."""
    if connection is None or not names:
        return {}
    statement = text("SELECT name, value FROM system.merge_tree_settings WHERE name IN :names").bindparams(
        bindparam("names", expanding=True)
    )
    try:
        rows = connection.execute(with_internal_query_formats(statement), {"names": list(names)}).fetchall()
    except SQLAlchemyError as ex:
        logger.warning("Could not read server defaults for settings %r: %s", list(names), ex)
        return {}
    return {row.name: _coerce_setting_value(row.value) for row in rows}


def _coerce_setting_value(value: str) -> Any:
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            continue
    return value


def _expressions_match(connection: Any, declared: str, reflected: str) -> bool:
    if normalize_expression(declared) == normalize_expression(reflected):
        return True
    formatted = _server_normalized(connection, (declared, reflected))
    return formatted is not None and formatted[0] == formatted[1]


def _positional_arg_names(engine: TableEngine) -> list[str]:
    return [arg_name for arg_name in type(engine).arg_names if arg_name not in _DEPLOYMENT_ARGS]


def _compare_engine_args(connection: Any, declared: TableEngine, reflected: TableEngine) -> list[_Difference]:
    # Pair by position so ReplicatedReplacingMergeTree(ver) lines up with reflected ReplacingMergeTree(version).
    declared_names = _positional_arg_names(declared)
    reflected_names = _positional_arg_names(reflected)
    drifts: list[_Difference] = []
    for index in range(max(len(declared_names), len(reflected_names))):
        declared_name = declared_names[index] if index < len(declared_names) else None
        reflected_name = reflected_names[index] if index < len(reflected_names) else None
        declared_value = declared._orig_kwargs.get(declared_name) if declared_name else None
        reflected_value = reflected._orig_kwargs.get(reflected_name) if reflected_name else None
        if declared_value is None and reflected_value is None:
            continue
        if declared_name == "columns":
            declared_text = _render_summing_columns(declared_value)
        else:
            declared_text = _declared_clause(_strip_string_quotes(declared_value))
        reflected_text = None if reflected_value is None else str(_strip_string_quotes(reflected_value))
        if declared_text is None or reflected_text is None or not _expressions_match(connection, declared_text, reflected_text):
            drifts.append((declared_name or reflected_name or "engine", reflected_text, declared_text))
    return drifts


def _key_clauses(engine: TableEngine, render: bool) -> dict[str, str | None]:
    values: dict[str, str | None] = {}
    for clause in _KEY_CLAUSES:
        if clause not in engine.eng_params:
            continue
        value = engine._orig_kwargs.get(clause)
        values[clause] = _declared_clause(value) if render else (None if value is None else str(value))
    # The server omits PRIMARY KEY when it equals ORDER BY, and ORDER BY defaults to PRIMARY KEY.
    if "primary_key" in values and "order_by" in values:
        values["primary_key"] = values["primary_key"] or values["order_by"]
        values["order_by"] = values["order_by"] or values["primary_key"]
    return values


def _compare_key_clauses(connection: Any, declared: TableEngine, reflected: TableEngine) -> list[_Difference]:
    declared_values = _key_clauses(declared, render=True)
    reflected_values = _key_clauses(reflected, render=False)
    drifts: list[_Difference] = []
    for clause, declared_text in declared_values.items():
        reflected_text = reflected_values.get(clause)
        if declared_text is None and reflected_text is None:
            continue
        if declared_text is None or reflected_text is None or not _expressions_match(connection, declared_text, reflected_text):
            drifts.append((clause, reflected_text, declared_text))
    return drifts


def _changed_settings(
    connection: Any, declared: TableEngine, reflected: TableEngine
) -> tuple[dict[str, Any], dict[str, Any], list[_Difference]]:
    # engine_full lists only settings set at CREATE, so a declared default is resolved against the server defaults.
    missing = [name for name in declared.settings if name not in reflected.settings]
    defaults = _server_setting_defaults(connection, missing)
    changed: dict[str, Any] = {}
    existing: dict[str, Any] = {}
    readonly: list[_Difference] = []
    for name, value in declared.settings.items():
        reflected_value = reflected.settings.get(name)
        effective = defaults.get(name) if reflected_value is None else reflected_value
        if effective is not None and _render_setting_value(effective) == _render_setting_value(value):
            continue
        if name in _READONLY_SETTINGS:
            readonly.append((f"settings.{name}", None if effective is None else str(effective), str(value)))
            continue
        changed[name] = value
        existing[name] = reflected_value
    return changed, existing, readonly


@comparators.dispatch_for("table", qualifier="clickhousedb")
def _compare_table_options(
    autogen_context: AutogenContext,
    modify_table_ops: ops.ModifyTableOps,
    schema: str | None,
    table_name: str,
    conn_table: Table | None,
    metadata_table: Table | None,
) -> None:
    """Refuse engine and key drift, emit settings changes as MODIFY SETTING ops."""
    if conn_table is None or metadata_table is None or not autogen_context.opts.get(COMPARE_TABLE_OPTIONS_OPT, True):
        return
    if ClickHouseDDLHelper.is_dictionary(metadata_table) or ClickHouseDDLHelper.is_dictionary(conn_table):
        return
    declared = _table_engine(metadata_table)
    if declared is None:
        return
    reflected = _table_engine(conn_table)
    connection = autogen_context.connection
    if reflected is None or _engine_family(reflected.name) != _engine_family(declared.name):
        raise UnsupportedTableChangeError(
            [TableOptionDrift(table_name, schema, "engine", reflected.name if reflected else None, declared.name)]
        )
    changed, existing, readonly = _changed_settings(connection, declared, reflected)
    differences = _compare_engine_args(connection, declared, reflected) + _compare_key_clauses(connection, declared, reflected) + readonly
    if differences:
        raise UnsupportedTableChangeError([TableOptionDrift(table_name, schema, *difference) for difference in differences])
    if changed:
        modify_table_ops.ops.append(_ModifyClickHouseTableSettingsOp(table_name, changed, schema=schema, existing_settings=existing))
