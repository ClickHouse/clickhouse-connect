from collections.abc import Callable, MutableMapping, Sequence
from functools import partial
from types import MethodType
from typing import Any, cast

import sqlalchemy.schema as sa_schema
from sqlalchemy import String, TypeDecorator, bindparam, text
from sqlalchemy import __version__ as sa_version
from sqlalchemy.engine import Connection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoResultFound, NoSuchTableError
from sqlalchemy.sql import operators, sqltypes, visitors
from sqlalchemy.sql.dml import Insert
from sqlalchemy.sql.elements import ClauseElement, ColumnClause, TextClause
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.selectable import CompoundSelect, SelectStatementGrouping
from sqlalchemy.sql.sqltypes import Boolean, Float, Interval, Numeric, PickleType, TupleType, TypeEngine, _Binary
from sqlalchemy.sql.sqltypes import Enum as SaEnum

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy import dialect_name, ischema_names
from clickhouse_connect.cc_sqlalchemy._schema import COLUMN_OPTIONS, TABLE_OPTIONS
from clickhouse_connect.cc_sqlalchemy.datatypes.base import (
    ChSqlaType,
    _datetime64_bind_signature,
    _datetime64_query_value,
    _DateTime64QuerySignature,
)
from clickhouse_connect.cc_sqlalchemy.inspector import (
    _INTERNAL_QUERY_OPTION,
    _INTERNAL_QUERY_SENTINEL,
    ChInspector,
    _database_name,
    _NoSuchTableError,
    get_columns,
    get_table_metadata,
    with_internal_query_formats,
)
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.compiler import ChStatementCompiler
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ChDDLCompiler
from clickhouse_connect.cc_sqlalchemy.sql.preparer import ChIdentifierPreparer
from clickhouse_connect.dbapi.cursor import _CURSOR_EXECUTE, Cursor, _NativeInsertPlan
from clickhouse_connect.driver.binding import quote_identifier

_MISSING = object()
_SQLALCHEMY_MAJOR_VERSION = int(sa_version.partition(".")[0])
_ENUM_RESULT_PROCESSOR = SaEnum.result_processor

# Generic result processors that read only dialect flags, never the server type code.
_Uuid = getattr(sqltypes, "Uuid", None)  # Not present on SQLAlchemy 1.4.
_SAFE_RESULT_PROCESSORS = frozenset(
    processor
    for processor in (
        TypeEngine.result_processor,
        String.result_processor,
        ChSqlaType.result_processor,
        Boolean.result_processor,
        Numeric.result_processor,
        Float.result_processor,
        _Binary.result_processor,
        _Uuid.result_processor if _Uuid is not None else None,
    )
    if processor is not None
)
# TypeDecorator processors that forward the type code to their inner implementation.
_FORWARDING_RESULT_PROCESSORS = frozenset((TypeDecorator.result_processor, PickleType.result_processor, Interval.result_processor))


def _supports_unknown_type_code(result_type: TypeEngine[Any]) -> bool:
    processor = result_type.result_processor
    # An instance attribute can bind another object's processor, so only trust methods bound to this type.
    if type(processor) is not MethodType or processor.__self__ is not result_type:
        return False
    if processor.__func__ in _SAFE_RESULT_PROCESSORS:
        return True
    # Enum calls super(), so subclass MROs can introduce a processor that needs the type code.
    if type(result_type) is SaEnum:
        return processor.__func__ is _ENUM_RESULT_PROCESSOR
    if isinstance(result_type, TypeDecorator) and processor.__func__ in _FORWARDING_RESULT_PROCESSORS:
        impl = result_type.impl_instance if _SQLALCHEMY_MAJOR_VERSION >= 2 else cast(TypeEngine[Any], result_type.impl)
        return _supports_unknown_type_code(impl)
    return False


class ClickHouseDialect(DefaultDialect):
    """
    See :py:class:`sqlalchemy.engine.interfaces`
    """

    name = dialect_name
    driver = "connect"
    colspecs: MutableMapping[type[TypeEngine[Any]], type[TypeEngine[Any]]] = {}

    default_schema_name = "default"
    supports_native_decimal = True
    supports_native_boolean = True
    supports_multivalues_insert = True
    supports_statement_cache = False
    supports_comments = True
    inline_comments = True
    returns_unicode_strings = True
    postfetch_lastrowid = False
    ddl_compiler = ChDDLCompiler
    statement_compiler = ChStatementCompiler
    preparer = ChIdentifierPreparer  # type: ignore[assignment]
    description_encoding = None
    max_identifier_length = 127
    ischema_names = ischema_names
    inspector = ChInspector
    construct_arguments = [
        (sa_schema.Table, dict.fromkeys(TABLE_OPTIONS)),
        (sa_schema.Column, dict.fromkeys(COLUMN_OPTIONS)),
    ]

    def __init__(self, server_side_params: bool = False, **kwargs):
        # Set before super().__init__() so ChIdentifierPreparer can read it when built.
        self.server_side_params = server_side_params
        super().__init__(**kwargs)

    @staticmethod
    def _ch_query_settings(context: Any) -> dict[str, Any] | None:
        # Deep-merge one level of execution_options["settings"], statement wins per key.
        if context is None:
            return None
        merged = context.execution_options.get("settings")
        stmt = getattr(context, "invoked_statement", None)
        stmt_settings = stmt.get_execution_options().get("settings") if stmt is not None else None
        if not stmt_settings:
            return merged
        if not merged:
            return dict(stmt_settings)
        return {**merged, **stmt_settings}

    @staticmethod
    def _ch_query_formats(context: Any) -> dict[str, str] | None:
        # Deep-merge one level of execution_options["query_formats"], statement wins per key.
        if context is None:
            return None
        merged = context.execution_options.get("query_formats")
        stmt = getattr(context, "invoked_statement", None)
        stmt_formats = stmt.get_execution_options().get("query_formats") if stmt is not None else None
        if not stmt_formats:
            return merged
        if not merged:
            return dict(stmt_formats)
        return {**stmt_formats, **{k: v for k, v in merged.items() if k not in stmt_formats}}

    @staticmethod
    def _ch_internal_query(context: Any) -> bool:
        # Set by inspector.with_internal_query_formats on dialect metadata statements.
        if context is None:
            return False
        if context.execution_options.get(_INTERNAL_QUERY_OPTION) is _INTERNAL_QUERY_SENTINEL:
            return True
        stmt = getattr(context, "invoked_statement", None)
        return bool(stmt is not None and stmt.get_execution_options().get(_INTERNAL_QUERY_OPTION) is _INTERNAL_QUERY_SENTINEL)

    def _ch_pyformat_encoded(self, context: Any) -> bool:
        compiled = getattr(context, "compiled", None)
        if compiled is None:
            return True
        return bool(getattr(compiled.preparer, "_double_percents", True))

    @staticmethod
    def _ch_result_columns(context: Any) -> tuple[str, ...] | None:
        compiled = getattr(context, "compiled", None)
        if compiled is None:
            return None
        result_columns = getattr(compiled, "_result_columns", None)
        if (
            not result_columns
            or not getattr(compiled, "_ordered_columns", False)
            or getattr(compiled, "_textual_ordered_columns", False)
            or getattr(compiled, "_ad_hoc_textual", False)
        ):
            return None
        names = tuple(column[0] for column in result_columns)
        if len(names) != len(set(names)):
            return None
        statement = getattr(getattr(compiled, "compile_state", None), "statement", None)
        while isinstance(statement, (CompoundSelect, SelectStatementGrouping)):
            statement = statement.selects[0] if isinstance(statement, CompoundSelect) else statement.element
        if (
            statement is None
            or getattr(statement, "_prefixes", False)
            or getattr(statement, "_suffixes", False)
            or any(name in ("*", compiled.dialect.name) for name, _ in getattr(statement, "_statement_hints", ()))
        ):
            return None
        # SQLAlchemy's positional mapping requires one server column per compiled expression.
        for column in result_columns:
            # Custom processors may require the server's type code, even for empty results.
            if not _supports_unknown_type_code(column[3].dialect_impl(compiled.dialect)):
                return None
            pending = list(column[2])
            seen: set[int] = set()
            while pending:
                obj = pending.pop()
                if not isinstance(obj, ClauseElement):
                    continue
                if id(obj) in seen:
                    continue
                seen.add(id(obj))
                for element in visitors.iterate(obj):
                    if isinstance(element, TextClause) or isinstance(element, ColumnClause) and element.is_literal:
                        return None
                    if getattr(getattr(element, "name", None), "quote", None) is False:
                        return None
                    if isinstance(getattr(element, "operator", None), operators.custom_op):
                        return None
                    if isinstance(element, FunctionElement):
                        name = getattr(element, "name", None)
                        if not isinstance(name, str) or name.casefold() in ("untuple", "columns"):
                            return None
                    if isinstance(element, ColumnClause):
                        pending.extend(proxy for proxy in element.proxy_set if proxy is not element)
        return names

    def _ch_result_column_provider(self, cursor: Cursor, statement: str, context: Any) -> Callable[[], Sequence[str] | None] | None:
        execute = cursor.execute
        if (
            isinstance(cursor, Cursor)
            and type(execute) is MethodType
            and execute.__func__ is _CURSOR_EXECUTE
            and statement == getattr(context, "statement", None)
        ):
            return partial(self._ch_result_columns, context)
        return None

    @staticmethod
    def _ch_native_insert_plan(context: Any, settings: dict[str, Any] | None) -> _NativeInsertPlan | None:
        compiled = getattr(context, "compiled", _MISSING)
        if compiled is _MISSING or getattr(compiled, "isinsert", _MISSING) is not True:
            return None
        compile_state = getattr(compiled, "compile_state", _MISSING)
        if compile_state is _MISSING:
            return None
        statement = getattr(compile_state, "statement", _MISSING)
        if not isinstance(statement, Insert):
            return None
        statement_attrs = {
            attr: getattr(statement, attr, _MISSING)
            for attr in (
                "_values",
                "_multi_values",
                "select",
                "_prefixes",
                "_hints",
                "_independent_ctes",
                "_post_values_clause",
                "_returning",
                "table",
            )
        }
        if any(value is _MISSING for value in statement_attrs.values()):
            return None
        if (
            statement_attrs["_values"] is not None
            or statement_attrs["_multi_values"]
            or statement_attrs["select"] is not None
            or statement_attrs["_prefixes"]
            or statement_attrs["_hints"]
            or statement_attrs["_independent_ctes"]
            or statement_attrs["_post_values_clause"] is not None
            or statement_attrs["_returning"]
        ):
            return None

        compiled_attrs = {
            attr: getattr(compiled, attr, _MISSING)
            for attr in (
                "returning",
                "literal_execute_params",
                "post_compile_params",
                "bind_names",
                "escaped_bind_names",
                "insert_prefetch",
                "preparer",
                "dialect",
            )
        }
        if any(value is _MISSING for value in compiled_attrs.values()) or any(
            compiled_attrs[attr] for attr in ("returning", "literal_execute_params", "post_compile_params")
        ):
            return None
        if _SQLALCHEMY_MAJOR_VERSION >= 2:
            returning_attrs = [getattr(compiled, attr, _MISSING) for attr in ("effective_returning", "implicit_returning")]
            if any(value is _MISSING for value in returning_attrs) or any(returning_attrs):
                return None
        elif any(getattr(compiled, attr, ()) for attr in ("effective_returning", "implicit_returning")):
            return None

        table = cast(Any, statement_attrs["table"])
        table_columns = tuple(table.columns)
        if any(column.default is not None and getattr(column.default, "is_clause_element", False) for column in table_columns):
            return None

        column_names: list[str] = []
        parameter_keys: list[str] = []
        resolved_columns: list[Any] = []
        escaped_bind_names = cast(Any, compiled_attrs["escaped_bind_names"]) or {}
        for bind, compiled_name in cast(Any, compiled_attrs["bind_names"]).items():
            if not getattr(bind, "_is_crud", False):
                return None
            bind_type = getattr(bind, "type", _MISSING)
            if bind_type is _MISSING:
                return None
            try:
                dialect_type = cast(Any, bind_type).dialect_impl(compiled_attrs["dialect"])
                has_bind_expression = getattr(dialect_type, "_has_bind_expression", _MISSING)
            except Exception:
                return None
            if has_bind_expression is not False:
                return None
            bind_column_keys = {
                str(key) for key in (compiled_name, getattr(bind, "key", None), getattr(bind, "_orig_key", None)) if key is not None
            }
            matches = [column for column in table_columns if str(column.key) in bind_column_keys]
            if len(matches) != 1 or any(matches[0] is column for column in resolved_columns):
                return None
            parameter_key = escaped_bind_names.get(compiled_name, compiled_name)
            if parameter_key in parameter_keys:
                return None
            resolved_columns.append(matches[0])
            column_names.append(str(matches[0].name))
            parameter_keys.append(str(parameter_key))

        if not column_names:
            return None
        if any(
            not any(prefetch_column is resolved_column for resolved_column in resolved_columns)
            for prefetch_column in cast(Any, compiled_attrs["insert_prefetch"])
        ):
            return None

        preparer = cast(Any, compiled_attrs["preparer"])
        double_percents = getattr(preparer, "_double_percents", _MISSING)
        if double_percents is _MISSING:
            return None
        table_name = preparer.format_table(table)
        execution_options = getattr(context, "execution_options", _MISSING)
        if execution_options is _MISSING:
            return None
        schema_translate_map = cast(Any, execution_options).get("schema_translate_map")
        if schema_translate_map:
            if not hasattr(preparer, "_render_schema_translates"):
                return None
            table_name = preparer._render_schema_translates(table_name, dict(schema_translate_map))
        if double_percents:
            table_name = table_name.replace("%%", "%")
        return _NativeInsertPlan(table_name, tuple(column_names), tuple(parameter_keys), settings)

    @staticmethod
    def _ch_datetime64_parameter_plan(context: Any) -> dict[str, _DateTime64QuerySignature]:
        compiled = getattr(context, "compiled", None)
        bind_names = getattr(compiled, "bind_names", None)
        if compiled is None or not bind_names or getattr(compiled.dialect, "server_side_params", False):
            return {}
        resolved = []
        has_datetime64 = False
        for bind, name in bind_names.items():
            types = bind.type.types if isinstance(bind.type, TupleType) else (bind.type,)
            signatures = tuple(_datetime64_bind_signature(type_, compiled.dialect) for type_ in types)
            if isinstance(bind.type, TupleType) and not bind.expanding:
                # Defensive handling for single-key tuple values at the dialect boundary.
                signatures = (("tuple", signatures) if any(signatures) else False,)
            has_datetime64 = has_datetime64 or any(signatures)
            resolved.append((name, signatures))
        if not has_datetime64:
            return {}
        escaped_names = compiled.escaped_bind_names or {}
        expanded = getattr(context, "_expanded_parameters", {})
        parameter_types: dict[str, _DateTime64QuerySignature] = {}
        conflicts: set[str] = set()
        for name, signatures in resolved:
            names = expanded.get(name)
            if names is None:
                names = (escaped_names.get(name, name),)
            for index, key in enumerate(names):
                signature = signatures[index % len(signatures)]
                if key in parameter_types and signature != parameter_types[key]:
                    conflicts.add(key)
                parameter_types[key] = signature
        # A shared key must remain valid for every occurrence, including DateTime.
        return {key: signature for key, signature in parameter_types.items() if signature and key not in conflicts}

    @staticmethod
    def _ch_datetime64_parameters(parameters: Any, plan: dict[str, _DateTime64QuerySignature]) -> Any:
        if not plan or not isinstance(parameters, dict):
            return parameters
        result = parameters
        for key, signature in plan.items():
            if key not in parameters:
                continue
            value = _datetime64_query_value(signature, parameters[key])
            if value is not parameters[key]:
                if result is parameters:
                    result = parameters.copy()
                result[key] = value
        return result

    def do_execute(self, cursor, statement, parameters, context=None):
        ch_cursor = cast(Cursor, cursor)
        if parameters:
            parameters = self._ch_datetime64_parameters(parameters, self._ch_datetime64_parameter_plan(context))
        internal = self._ch_internal_query(context)
        result_columns = self._ch_result_column_provider(ch_cursor, statement, context)
        if internal or result_columns is not None:
            Cursor._execute(
                ch_cursor,
                statement,
                parameters,
                settings=self._ch_query_settings(context),
                query_formats=self._ch_query_formats(context),
                pyformat_encoded=self._ch_pyformat_encoded(context),
                internal=internal,
                result_columns=result_columns,
            )
        else:
            ch_cursor.execute(
                statement,
                parameters,
                settings=self._ch_query_settings(context),
                query_formats=self._ch_query_formats(context),
                pyformat_encoded=self._ch_pyformat_encoded(context),
            )

    def do_executemany(self, cursor, statement, parameters, context=None):
        ch_cursor = cast(Cursor, cursor)
        settings = self._ch_query_settings(context)
        native_plan = None
        if statement == getattr(context, "statement", None):
            native_plan = self._ch_native_insert_plan(context, settings)
        if native_plan is not None:
            ch_cursor._executemany_native(native_plan, parameters)
            return
        datetime64_plan = self._ch_datetime64_parameter_plan(context)
        if datetime64_plan:
            parameters = [self._ch_datetime64_parameters(row, datetime64_plan) for row in parameters]
        ch_cursor.executemany(
            statement,
            parameters,
            settings=settings,
            query_formats=self._ch_query_formats(context),
            pyformat_encoded=self._ch_pyformat_encoded(context),
        )

    def do_execute_no_params(self, cursor, statement, context=None):
        ch_cursor = cast(Cursor, cursor)
        internal = self._ch_internal_query(context)
        result_columns = self._ch_result_column_provider(ch_cursor, statement, context)
        if internal or result_columns is not None:
            Cursor._execute(
                ch_cursor,
                statement,
                settings=self._ch_query_settings(context),
                query_formats=self._ch_query_formats(context),
                pyformat_encoded=self._ch_pyformat_encoded(context),
                internal=internal,
                result_columns=result_columns,
            )
        else:
            ch_cursor.execute(
                statement,
                settings=self._ch_query_settings(context),
                query_formats=self._ch_query_formats(context),
                pyformat_encoded=self._ch_pyformat_encoded(context),
            )

    # SQA 1 compatibility

    @classmethod
    def dbapi(cls):
        return dbapi

    # SQA 2 compatibility

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def _get_default_schema_name(self, connection):
        return connection.execute(with_internal_query_formats(text("SELECT currentDatabase()"))).scalar()

    def get_schema_names(self, connection, **_):
        return [row.name for row in connection.execute(with_internal_query_formats(text("SHOW DATABASES")))]

    @staticmethod
    def has_database(connection, db_name):
        # EXISTS DATABASE consults DatabaseCatalog directly, so it sees DataLakeCatalog
        # and other remote databases that system.databases omitted by default before server 26.5.
        result = connection.execute(with_internal_query_formats(text(f"EXISTS DATABASE {quote_identifier(db_name)}")))
        row = result.fetchone()
        return row[0] == 1

    def get_table_names(self, connection, schema=None, **kw):
        cmd = "SHOW TABLES"
        if schema:
            cmd += " FROM " + quote_identifier(schema)
        return [row.name for row in connection.execute(with_internal_query_formats(text(cmd)))]

    def get_columns(self, connection, table_name, schema=None, **kw):
        try:
            return get_columns(connection, table_name, schema)
        except NoResultFound as ex:
            raise _NoSuchTableError(str(ex)) from ex

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection: Connection, schema: str | None = None, **kw: Any) -> list[str]:
        query = text(
            "SELECT name FROM system.tables WHERE database = :database AND engine IN ('View', 'WindowView') ORDER BY name"
        ).bindparams(bindparam("database", type_=String()))
        rows = connection.execute(with_internal_query_formats(query), {"database": _database_name(connection, schema)})
        return [row.name for row in rows]

    def get_materialized_view_names(self, connection: Connection, schema: str | None = None, **kw: Any) -> list[str]:
        query = text("SELECT name FROM system.tables WHERE database = :database AND engine = 'MaterializedView' ORDER BY name").bindparams(
            bindparam("database", type_=String())
        )
        rows = connection.execute(with_internal_query_formats(query), {"database": _database_name(connection, schema)})
        return [row.name for row in rows]

    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        raise NoSuchTableError(f"{schema}.{view_name}" if schema else view_name)

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        try:
            table_metadata = get_table_metadata(connection, table_name, schema)
        except NoResultFound:
            raise NoSuchTableError(f"{schema}.{table_name}" if schema else table_name) from None
        return {"text": table_metadata.comment or None}

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None, **_kw):
        result = connection.execute(with_internal_query_formats(text(f"EXISTS TABLE {full_table(table_name, schema)}")))
        row = result.fetchone()
        return row[0] == 1

    def has_sequence(self, connection, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, connection, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, connection, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, connection):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return "AUTOCOMMIT"
