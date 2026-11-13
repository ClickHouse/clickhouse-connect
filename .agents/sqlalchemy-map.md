# SQLAlchemy Navigation Map

Source lives in the active venv's `site-packages/sqlalchemy/`. The version actually being investigated is whatever `importlib.metadata.version("sqlalchemy")` returns at read time.

## How to use this map

This map is intentionally version-agnostic for SQLAlchemy 2.x. The high-level layout (`engine/`, `sql/`, `orm/`, `dialects/`, `schema.py`, `types.py`) has been stable across 2.0.x, and most of the entries below should apply across minor releases. The long tail drifts: new typing helpers, occasional file splits, renamed private modules.

Paths are relative to `site-packages/sqlalchemy/`. Before relying on a specific file or class pointer, sanity-check that it still exists at the installed version. If a cited path is missing, tell the user the map is out of date for that area and suggest updating the map rather than guessing a replacement.

Cite file paths and class or function names in your answers. Do not cite line numbers, they rot.

## Dialect base and interface

- **Dialect interface (abstract contract every dialect implements)**: `engine/interfaces.py`, class `Dialect`. Defines required attributes and method signatures (`do_execute`, `do_executemany`, `do_rollback`, `connect`, `initialize`, `get_table_names`, `get_columns`, `get_pk_constraint`, etc.).
- **Default base implementation**: `engine/default.py`, class `DefaultDialect`. Most third-party dialects subclass this. Also `StrCompileDialect` (compile-only, no DBAPI) and `DefaultExecutionContext` (per-statement execution state — bind processing, cursor description, rowcount handling).
- **Dialect feature flags worth knowing**: `supports_statement_cache`, `supports_native_boolean`, `supports_native_decimal`, `supports_comments`, `inline_comments`, `returns_unicode_strings`, `postfetch_lastrowid`, `max_identifier_length`, `construct_arguments`, `default_paramstyle`. All declared on `Dialect` and defaulted on `DefaultDialect`.
- **Registration / lookup**: `dialects/__init__.py` for the built-in registry; third-party dialects register via the `sqlalchemy.dialects` setuptools entry point group (see `dialects/__init__.py` and `util/langhelpers.py` `PluginLoader`).
- **Reference dialect implementations** (read these to see how real dialects override `DefaultDialect`): `dialects/postgresql/base.py`, `dialects/mysql/base.py`, `dialects/sqlite/base.py`. Each subpackage also has driver-specific shims (`asyncpg.py`, `psycopg.py`, `psycopg2.py`, `pymysql.py`, etc.) that customize DBAPI quirks.

## Compilation (SQL → string)

- **Compiler base classes**: `sql/compiler.py`. `Compiled` is the root. `SQLCompiler` handles SELECT / INSERT / UPDATE / DELETE statement compilation; `DDLCompiler` handles CREATE / ALTER / DROP. Dialects typically subclass these and override `visit_<construct_name>` methods.
- **Type compilation**: `sql/compiler.py`, `TypeCompiler` and `GenericTypeCompiler`. Maps a `TypeEngine` instance to its dialect-specific type string (e.g. `INTEGER`, `VARCHAR(n)`).
- **Identifier preparer (quoting)**: `sql/compiler.py`, `IdentifierPreparer`. Handles quoting rules, reserved words, schema-qualified names. Override `reserved_words`, `illegal_initial_characters`, or `_requires_quotes` on a subclass to tune.
- **DDL element library**: `sql/ddl.py` defines `CreateTable`, `DropTable`, `CreateColumn`, `CreateIndex`, `AddConstraint`, `SetTableComment`, etc. These are the visitable elements that `DDLCompiler` dispatches on.
- **Visitor protocol**: `sql/visitors.py`. The `Visitable` mixin and `visit_*` dispatch underlying both compilation and tree traversal.
- **Compiled cache key generation**: `sql/cache_key.py`. Relevant when `supports_statement_cache = True` — the dialect's compiled output must be stable for a given cache key.

## Type system

- **Type base class**: `sql/type_api.py`, `TypeEngine`. Every SQL type derives from this. Key methods: `bind_processor(dialect)`, `result_processor(dialect, coltype)`, `literal_processor(dialect)`, `compare_values`, `python_type`, `_type_affinity`.
- **Concrete generic types**: `sql/sqltypes.py`. `Integer`, `String`, `Numeric`, `Float`, `Boolean`, `DateTime`, `Date`, `Time`, `Interval`, `JSON`, `ARRAY`, `Enum`, `LargeBinary`, etc. These are what users import from `sqlalchemy`.
- **Type decorator (wrap an existing type with custom processing)**: `sql/type_api.py`, `TypeDecorator`. Override `impl`, `process_bind_param`, `process_result_value`. Common pattern for client-side encoding/decoding around a native type.
- **UserDefinedType**: `sql/type_api.py`, `UserDefinedType`. For types that have no SQLAlchemy generic equivalent — override `get_col_spec`, `bind_processor`, `result_processor`.
- **Dialect-specific type modules** (good patterns for non-standard types): `dialects/postgresql/types.py`, `dialects/postgresql/json.py`, `dialects/postgresql/array.py`, `dialects/postgresql/hstore.py`, `dialects/postgresql/ranges.py`, `dialects/postgresql/named_types.py`.
- **Type affinity / coercion**: `sql/sqltypes.py` (`_type_affinity` on each generic), `sql/default_comparator.py` (operator dispatch).
- **ischema_names mapping** (reflection: type string from DB → Python type class): each dialect's `base.py` defines `ischema_names`; see `dialects/postgresql/base.py` for the canonical example. The mapping is consulted by reflection (`get_columns`).

## Schema and DDL elements

- **User-facing schema objects**: `schema.py` re-exports from `sql/schema.py`. `Table`, `Column`, `MetaData`, `Index`, `ForeignKey`, `ForeignKeyConstraint`, `PrimaryKeyConstraint`, `UniqueConstraint`, `CheckConstraint`, `Sequence`, `Computed`, `Identity`.
- **DDL execution and events**: `sql/ddl.py`, `sql/events.py`. `DDLEvents` carries `before_create`, `after_create`, `before_drop`, `after_drop`, etc.
- **Dialect-specific construct arguments** (e.g. PG's `postgresql_using`, MySQL's `mysql_engine`): registered via `Dialect.construct_arguments`. The pattern is `(SchemaConstruct, {"option_name": default, ...})`. ClickHouse's table-level options (`engine`, `dictionary_*`) use this mechanism.

## Reflection (Inspector)

- **Inspector API**: `engine/reflection.py`, class `Inspector`. Public methods: `get_table_names`, `get_view_names`, `get_columns`, `get_pk_constraint`, `get_foreign_keys`, `get_indexes`, `get_unique_constraints`, `get_check_constraints`, `get_table_comment`, `get_table_options`. Each delegates to a dialect-implemented `get_*` method.
- **Dialect side**: every `Dialect.get_*` method that an `Inspector` calls must be implemented if the dialect supports reflection of that feature. See `dialects/postgresql/base.py` for the canonical full implementation.
- **`ReflectedColumn` / `ReflectedIndex` / `ReflectedForeignKeyConstraint`**: `engine/interfaces.py`. These `TypedDict`s define the exact dict shape each `get_*` must return.

## Engine, connection, execution

- **Engine factory**: `engine/create.py`, `create_engine`. Drives URL parsing, dialect loading, pool selection, connect-args plumbing.
- **URL parsing**: `engine/url.py`, class `URL`. Where `clickhouse+connect://...` URLs are split into `drivername`, `username`, `password`, `host`, `port`, `database`, `query`.
- **Engine and Connection runtime**: `engine/base.py`, `Engine` and `Connection`. Statement execution path, transaction handling, autocommit semantics, savepoints.
- **Cursor handling and result construction**: `engine/cursor.py` (`CursorResult`, `_NoResultMetaData`), `engine/result.py` (`Result`, `MappingResult`, `ScalarResult`), `engine/row.py` (`Row`).
- **Execution context**: `engine/default.py`, `DefaultExecutionContext`. Owns bind parameter processing (`get_lastrowid`, `pre_exec`, `post_exec`, `get_result_processor`, cursor description handling).
- **Bind parameter processors**: `engine/_py_processors.py` (pure Python) and `cyextension/processors.pyx` (C). Most client work happens through `TypeEngine.bind_processor` → these are the call targets.

## Async engine

- **Async API**: `ext/asyncio/`. `engine.py` (`AsyncEngine`, `AsyncConnection`), `result.py` (`AsyncResult`), `session.py` (`AsyncSession`). All thin wrappers that drive a sync dialect via `greenlet`.
- **Greenlet bridge**: `util/concurrency.py`, `await_only` / `await_fallback` / `greenlet_spawn`. Async dialects implement sync methods that internally `await_only(...)` async DBAPI calls.
- **Async DBAPI shape**: a dialect declares `is_async = True` and provides an async DBAPI; see `dialects/postgresql/asyncpg.py` for a complete example.

## ORM (less relevant to a dialect, but useful when reflection or session behavior is in scope)

- **Mapper and declarative**: `orm/mapper.py`, `orm/decl_api.py`, `orm/decl_base.py`. Map Python classes to `Table`s.
- **Session**: `orm/session.py`. Unit of work, flush ordering, identity map.
- **Bulk persistence and statement compilation for ORM**: `orm/bulk_persistence.py`, `orm/persistence.py`.
- **Query construction**: `orm/query.py` (legacy), `sql/dml.py` + `orm/context.py` (2.0 style).

## Events

- **Event infrastructure**: `event/api.py`, `event/base.py`, `event/registry.py`. `event.listen(target, name, fn)`.
- **DDL events**: `sql/events.py`, class `DDLEvents` — `before_execute`, `before_create`, `after_create`, `before_drop`, `after_drop`, `column_reflect`, `before_parent_attach`.
- **Engine and connection events**: `engine/events.py`, class `ConnectionEvents` — `before_execute`, `after_execute`, `before_cursor_execute`, `after_cursor_execute`, `handle_error`, `engine_connect`, `engine_disposed`.
- **Pool events**: `pool/events.py`.

## Exceptions

- **All public exceptions**: `exc.py`. `DBAPIError` (wraps DBAPI errors), `OperationalError`, `ProgrammingError`, `IntegrityError`, `NoSuchTableError`, `NoResultFound`, `MultipleResultsFound`, `StatementError`, `CompileError`, `InvalidRequestError`, `ArgumentError`. The `Dialect.dbapi_exception_translation_map` and `is_disconnect(...)` hook control which class gets raised.

## Testing utilities (third-party dialects)

- **Third-party dialect compliance suite**: `testing/`. `suite/` subpackage holds the official cross-dialect test suite that any dialect can run via `pytest`. `requirements.py` declares per-feature `Requirement` flags a dialect overrides to opt in or out of suite tests.
- **Provisioning hooks** (CREATE/DROP test schemas): `testing/provision.py`, plus per-dialect `provision.py` (see `dialects/postgresql/provision.py`).

## Where ClickHouse integration plugs in

For orientation only — these are the local repo paths, not SQLAlchemy itself. Useful when a caller question is "where does our code touch X":

- Dialect class: `clickhouse_connect/cc_sqlalchemy/dialect.py` (`ClickHouseDialect(DefaultDialect)`).
- Type mapping (`ischema_names`, `ChSqlaType` family): `clickhouse_connect/cc_sqlalchemy/datatypes/`.
- DDL / statement compilers, identifier preparer: `clickhouse_connect/cc_sqlalchemy/sql/`.
- Inspector: `clickhouse_connect/cc_sqlalchemy/inspector.py`.
- Table engines (CH-specific construct arguments): `clickhouse_connect/cc_sqlalchemy/ddl/`.
