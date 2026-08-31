from typing import Any, cast

import sqlalchemy.schema as sa_schema
from sqlalchemy import __version__ as sa_version
from sqlalchemy import text
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import NoResultFound, NoSuchTableError
from sqlalchemy.sql.dml import Insert

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy import dialect_name, ischema_names
from clickhouse_connect.cc_sqlalchemy.inspector import (
    ChInspector,
    get_columns,
    get_table_metadata,
    with_internal_query_formats,
)
from clickhouse_connect.cc_sqlalchemy.sql import full_table
from clickhouse_connect.cc_sqlalchemy.sql.compiler import ChStatementCompiler
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ChDDLCompiler
from clickhouse_connect.cc_sqlalchemy.sql.preparer import ChIdentifierPreparer
from clickhouse_connect.dbapi.cursor import Cursor, _NativeInsertPlan
from clickhouse_connect.driver.binding import quote_identifier

_MISSING = object()
_SQLALCHEMY_MAJOR_VERSION = int(sa_version.partition(".")[0])


class ClickHouseDialect(DefaultDialect):
    """
    See :py:class:`sqlalchemy.engine.interfaces`
    """

    name = dialect_name
    driver = "connect"

    default_schema_name = "default"
    supports_native_decimal = True
    supports_native_boolean = True
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
        (
            sa_schema.Table,
            {
                "engine": None,
                "table_type": None,
                "dictionary_source": None,
                "dictionary_layout": None,
                "dictionary_lifetime": None,
                "dictionary_primary_key": None,
            },
        ),
        (
            sa_schema.Column,
            {
                "materialized": None,
                "alias": None,
                "codec": None,
                "ttl": None,
                "after": None,
                "settings": None,
            },
        ),
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

    def _ch_pyformat_encoded(self, context: Any) -> bool:
        compiled = getattr(context, "compiled", None)
        if compiled is None:
            return True
        return bool(getattr(compiled.preparer, "_double_percents", True))

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

    def do_execute(self, cursor, statement, parameters, context=None):
        cast(Cursor, cursor).execute(
            statement,
            parameters,
            settings=self._ch_query_settings(context),
            query_formats=self._ch_query_formats(context),
            pyformat_encoded=self._ch_pyformat_encoded(context),
        )

    def do_executemany(self, cursor, statement, parameters, context=None):
        ch_cursor = cast(Cursor, cursor)
        settings = self._ch_query_settings(context)
        native_plan = self._ch_native_insert_plan(context, settings)
        if native_plan is not None:
            ch_cursor._executemany_native(native_plan, parameters)
            return
        ch_cursor.executemany(
            statement,
            parameters,
            settings=settings,
            query_formats=self._ch_query_formats(context),
        )

    def do_execute_no_params(self, cursor, statement, context=None):
        cast(Cursor, cursor).execute(
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
        return get_columns(connection, table_name, schema)

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        return []

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
