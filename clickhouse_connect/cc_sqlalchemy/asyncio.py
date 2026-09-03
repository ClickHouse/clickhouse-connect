from __future__ import annotations

import asyncio
import re
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from datetime import tzinfo
from importlib.metadata import version
from importlib.util import find_spec
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse

_SQLALCHEMY_VERSION = version("sqlalchemy")
_SQLALCHEMY_VERSION_MATCH = re.match(r"^(\d+)\.(\d+)\.(\d+)", _SQLALCHEMY_VERSION)
if _SQLALCHEMY_VERSION_MATCH is None:
    raise ImportError(f"The ClickHouse async SQLAlchemy dialect cannot parse SQLAlchemy version {_SQLALCHEMY_VERSION!r}")

_SQLALCHEMY_VERSION_INFO = tuple(int(part) for part in _SQLALCHEMY_VERSION_MATCH.groups())
if _SQLALCHEMY_VERSION_INFO < (2, 0, 44) or _SQLALCHEMY_VERSION_INFO >= (3, 0, 0):
    raise ImportError(
        "The ClickHouse async SQLAlchemy dialect requires SQLAlchemy >=2.0.44,<3.0. "
        'Install with: pip install "clickhouse-connect[sqlalchemy-async]"'
    )

if find_spec("greenlet") is None:
    raise ImportError(
        'The ClickHouse async SQLAlchemy dialect requires greenlet. Install with: pip install "clickhouse-connect[sqlalchemy-async]"'
    )

from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection, AsyncAdapt_terminate  # noqa: E402
from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPICursor, DBAPIModule  # noqa: E402
from sqlalchemy.pool import AsyncAdaptedQueuePool, ConnectionPoolEntry, PoolProxiedConnection  # noqa: E402
from sqlalchemy.util import await_only  # noqa: E402
from sqlalchemy.util import queue as sqla_queue  # noqa: E402

from clickhouse_connect import dbapi  # noqa: E402
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect  # noqa: E402
from clickhouse_connect.datatypes.base import ClickHouseType  # noqa: E402
from clickhouse_connect.dbapi.cursor import Cursor  # noqa: E402
from clickhouse_connect.driver import create_async_client  # noqa: E402
from clickhouse_connect.driver.binding import _query_is_insert  # noqa: E402
from clickhouse_connect.driver.external import ExternalData  # noqa: E402
from clickhouse_connect.driver.query import QueryContext, QueryResult  # noqa: E402
from clickhouse_connect.driver.summary import QuerySummary  # noqa: E402

if TYPE_CHECKING:
    from clickhouse_connect.driver import Client
    from clickhouse_connect.driver.asyncclient import AsyncClient


class _AsyncClientFacade:
    __slots__ = ("driver_connection", "_execute_mutex")

    def __init__(self, driver_connection: AsyncClient, execute_mutex: asyncio.Lock):
        self.driver_connection = driver_connection
        self._execute_mutex = execute_mutex

    @property
    def server_tz(self) -> tzinfo:
        return self.driver_connection.server_tz

    async def _query(
        self,
        query: str | None,
        parameters: Sequence | dict[str, Any] | None,
        settings: dict[str, Any] | None,
        query_formats: dict[str, str] | None,
        context: QueryContext | None,
    ) -> QueryResult:
        async with self._execute_mutex:
            return await self.driver_connection.query(
                query=query,
                parameters=parameters,
                settings=settings,
                query_formats=query_formats,
                context=context,
            )

    def query(
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        context: QueryContext | None = None,
    ) -> QueryResult:
        return await_only(self._query(query, parameters, settings, query_formats, context))

    def create_query_context(
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
    ) -> QueryContext:
        return self.driver_connection.create_query_context(
            query=query,
            parameters=parameters,
            settings=settings,
            query_formats=query_formats,
        )

    async def _insert(
        self,
        table: str | None,
        data: Sequence[Sequence[Any]] | None,
        column_names: str | Sequence[str] | None,
        database: str | None,
        column_types: Sequence[ClickHouseType] | None,
        column_type_names: Sequence[str] | None,
        column_oriented: bool,
        settings: dict[str, Any] | None,
    ) -> QuerySummary:
        async with self._execute_mutex:
            return await self.driver_connection.insert(
                table=table,
                data=data,
                column_names=column_names,
                database=database,
                column_types=column_types,
                column_type_names=column_type_names,
                column_oriented=column_oriented,
                settings=settings,
            )

    def insert(
        self,
        table: str | None = None,
        data: Sequence[Sequence[Any]] | None = None,
        column_names: str | Sequence[str] | None = "*",
        database: str | None = None,
        column_types: Sequence[ClickHouseType] | None = None,
        column_type_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        settings: dict[str, Any] | None = None,
    ) -> QuerySummary:
        return await_only(
            self._insert(
                table,
                data,
                column_names,
                database,
                column_types,
                column_type_names,
                column_oriented,
                settings,
            )
        )

    async def _command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None,
        data: str | bytes | None,
        settings: dict[str, Any] | None,
        use_database: bool,
        external_data: ExternalData | None,
        transport_settings: dict[str, str] | None,
    ) -> str | int | Sequence[str] | QuerySummary:
        async with self._execute_mutex:
            return await self.driver_connection.command(
                cmd=cmd,
                parameters=parameters,
                data=data,
                settings=settings,
                use_database=use_database,
                external_data=external_data,
                transport_settings=transport_settings,
            )

    def command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict[str, Any] | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        return await_only(self._command(cmd, parameters, data, settings, use_database, external_data, transport_settings))

    def close(self) -> None:
        await_only(self.driver_connection.close())

    def _add_integration_tag(self, name: str) -> None:
        self.driver_connection._add_integration_tag(name)


class _AsyncCursor(Cursor):
    _awaitable_cursor_close = False

    def _try_bulk_insert(self, operation: str, data: Any, settings: dict[str, Any] | None = None) -> bool:
        return False

    def executemany(
        self,
        operation: str,
        parameters: Any,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
    ) -> None:
        summary_start = len(self._summary)
        super().executemany(operation, parameters, settings, query_formats)
        if not _query_is_insert(operation):
            return
        call_summaries = self._summary[summary_start:]
        if not call_summaries:
            return
        written_rows = 0
        for summary in call_summaries:
            value = summary.get("written_rows")
            if isinstance(value, bool) or not isinstance(value, (int, str)):
                self._rowcount = -1
                return
            try:
                count = int(value)
            except ValueError:
                self._rowcount = -1
                return
            if count < 0:
                self._rowcount = -1
                return
            written_rows += count
        self._rowcount = written_rows

    async def _async_soft_close(self) -> None:
        return


class _AsyncAdaptedConnection(AsyncAdapt_terminate, AsyncAdapt_dbapi_connection):
    __slots__ = ("_client_facade",)

    def __init__(self, async_dbapi: _AsyncDBAPI, driver_connection: AsyncClient):
        super().__init__(async_dbapi, cast(Any, driver_connection))
        self._client_facade = _AsyncClientFacade(driver_connection, self._execute_mutex)

    @property
    def timezone(self) -> tzinfo:
        return self._client_facade.server_tz

    def cursor(self, server_side: bool = False) -> Cursor:  # type: ignore[override]
        if server_side:
            raise dbapi.NotSupportedError("Server-side cursors are not supported by the ClickHouse async dialect")
        return _AsyncCursor(cast("Client", self._client_facade))

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self._client_facade.close()

    def is_closed(self) -> bool:
        try:
            session = cast("AsyncClient", self.driver_connection)._session
        except AttributeError:
            return False
        return session is None or session.closed

    async def _terminate_graceful_close(self) -> None:
        await self.driver_connection.close()

    def _terminate_force_close(self) -> None:
        self.driver_connection._force_close()

    def command(
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict[str, Any] | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        return self._client_facade.command(
            cmd,
            parameters,
            data,
            settings,
            use_database,
            external_data,
            transport_settings,
        )


class _AsyncDBAPI:
    apilevel = dbapi.apilevel
    threadsafety = dbapi.threadsafety
    paramstyle = dbapi.paramstyle

    Warning = dbapi.Warning
    Error = dbapi.Error
    InterfaceError = dbapi.InterfaceError
    DatabaseError = dbapi.DatabaseError
    DataError = dbapi.DataError
    OperationalError = dbapi.OperationalError
    IntegrityError = dbapi.IntegrityError
    InternalError = dbapi.InternalError
    ProgrammingError = dbapi.ProgrammingError
    NotSupportedError = dbapi.NotSupportedError

    Date = dbapi.Date
    Time = dbapi.Time
    Timestamp = dbapi.Timestamp
    Binary = dbapi.Binary

    DateFromTicks = staticmethod(dbapi.DateFromTicks)
    TimeFromTicks = staticmethod(dbapi.TimeFromTicks)
    TimestampFromTicks = staticmethod(dbapi.TimestampFromTicks)

    def connect(
        self,
        host: str | None = None,
        database: str | None = None,
        username: str = "",
        password: str = "",
        port: int | None = None,
        *,
        interface: str | None = None,
        secure: bool | str = False,
        dsn: str | None = None,
        access_token: str | None = None,
        token_provider: Callable[[], str | Awaitable[str]] | None = None,
        settings: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        connector_limit: int | str | None = None,
        connector_limit_per_host: int | str | None = None,
        keepalive_timeout: float | str | None = None,
        async_creator_fn: Callable[[], Awaitable[AsyncClient]] | None = None,
        **kwargs: Any,
    ) -> _AsyncAdaptedConnection:
        if async_creator_fn is not None:
            driver_connection = await_only(async_creator_fn())
        else:
            dsn_options = parse_qs(urlparse(dsn).query) if dsn else {}
            if "autogenerate_session_id" not in kwargs and "autogenerate_session_id" not in dsn_options:
                kwargs["autogenerate_session_id"] = True
            if connector_limit is None and "connector_limit" not in dsn_options:
                connector_limit = 1
            if connector_limit_per_host is None and "connector_limit_per_host" not in dsn_options:
                connector_limit_per_host = 1
            driver_connection = await_only(
                create_async_client(
                    host=host,
                    database=database,
                    username=username,
                    password=password,
                    access_token=access_token,
                    token_provider=token_provider,
                    interface=interface,
                    port=port,
                    secure=secure,
                    dsn=dsn,
                    settings=settings,
                    headers=headers,
                    generic_args=kwargs,
                    connector_limit=cast(int | None, connector_limit),
                    connector_limit_per_host=cast(int | None, connector_limit_per_host),
                    keepalive_timeout=cast(float | None, keepalive_timeout),
                )
            )
        facade = _AsyncAdaptedConnection(self, driver_connection)
        facade._client_facade._add_integration_tag("sqlalchemy")
        return facade


_ASYNC_DBAPI = _AsyncDBAPI()
_POOL_ACCOUNTING_EPOCH_KEY = "clickhouse_connect_async_pool_accounting_epoch"
_POOL_GENERATION_KEY = "clickhouse_connect_async_pool_generation"


class _ClickHouseAsyncAdaptedQueuePool(AsyncAdaptedQueuePool):
    def __init__(
        self,
        creator: Callable[..., Any],
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: float = 30.0,
        use_lifo: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(creator, pool_size, max_overflow, timeout, use_lifo, **kwargs)
        self._clickhouse_accounting_epoch = 0
        self._clickhouse_awaiting_recreate = False
        self._clickhouse_disposing = False
        self._clickhouse_generation = 0
        self._clickhouse_pending_records: deque[ConnectionPoolEntry] = deque()
        self._clickhouse_recreated = False

    def dispose(self) -> None:
        self._clickhouse_awaiting_recreate = False
        self._clickhouse_disposing = True
        self._clickhouse_generation += 1
        try:
            while True:
                try:
                    record = self._pool.get(False)
                except sqla_queue.Empty:
                    break
                try:
                    record.close()
                except BaseException:
                    if record.dbapi_connection is not None:
                        try:
                            record.close()
                        except BaseException:
                            pass
                    if record.dbapi_connection is None:
                        self._append_pending(record)
                    raise
                self._append_pending(record)
        except BaseException:
            self._clickhouse_disposing = False
            try:
                self._restore_pending_records()
            except BaseException:
                self.logger.warning("Failed to restore async pool records after interrupted disposal", exc_info=True)
            raise

        self._overflow = 0 - self.size()
        self._clickhouse_accounting_epoch += 1
        self._trim_pending_records()
        self._clickhouse_disposing = False
        self._promote_pending_record()
        self._clickhouse_awaiting_recreate = True
        self.logger.info("Pool disposed. %s", self.status())

    def _append_pending(self, record: ConnectionPoolEntry) -> None:
        if self._clickhouse_disposing or self._max_overflow < 0 or len(self._clickhouse_pending_records) < self.size() + self._max_overflow:
            self._clickhouse_pending_records.append(record)

    def _trim_pending_records(self) -> None:
        if self._max_overflow < 0:
            return
        capacity = self.size() + self._max_overflow
        while len(self._clickhouse_pending_records) > capacity:
            self._clickhouse_pending_records.pop()

    def _promote_pending_record(self) -> None:
        while self._clickhouse_pending_records and self._inc_overflow():
            record = self._clickhouse_pending_records.popleft()
            record_info = record.record_info
            assert record_info is not None
            try:
                self._pool.put_nowait(record)
            except sqla_queue.Full:
                self._dec_overflow()
                self._clickhouse_pending_records.appendleft(record)
                break
            record_info[_POOL_ACCOUNTING_EPOCH_KEY] = self._clickhouse_accounting_epoch

    def _restore_pending_records(self) -> None:
        pending_records = self._clickhouse_pending_records
        self._clickhouse_pending_records = deque()
        while pending_records:
            record = pending_records.popleft()
            record_info = record.record_info
            if record_info is not None and record_info.get(_POOL_ACCOUNTING_EPOCH_KEY) == self._clickhouse_accounting_epoch:
                self._return_current_record(record)
            else:
                self._append_pending(record)
        self._promote_pending_record()

    def _return_current_record(self, record: ConnectionPoolEntry) -> None:
        try:
            self._pool.put(record, False)
        except sqla_queue.Full:
            accounting_epoch = self._clickhouse_accounting_epoch
            try:
                record.close()
            finally:
                if accounting_epoch == self._clickhouse_accounting_epoch:
                    self._dec_overflow()
                elif record.dbapi_connection is None:
                    self._append_pending(record)
                    self._promote_pending_record()

    def _do_return_conn(self, record: ConnectionPoolEntry) -> None:
        # SQLAlchemy replaces a disposed pool without closing connections that
        # are still checked out. Close them when they return to the old pool.
        record_info = record.record_info
        stale = (
            self._clickhouse_disposing
            or self._clickhouse_recreated
            or record_info is None
            or record_info.get(_POOL_GENERATION_KEY) != self._clickhouse_generation
        )
        if stale:
            record.close()
            if self._clickhouse_disposing:
                self._append_pending(record)
            elif record_info is not None and record_info.get(_POOL_ACCOUNTING_EPOCH_KEY) == self._clickhouse_accounting_epoch:
                self._return_current_record(record)
            else:
                self._append_pending(record)
                self._promote_pending_record()
        else:
            self._return_current_record(record)

    def _create_connection(self) -> ConnectionPoolEntry:
        accounting_epoch = self._clickhouse_accounting_epoch
        try:
            record = super()._create_connection()
        except BaseException:
            if accounting_epoch != self._clickhouse_accounting_epoch:
                # Neutralize QueuePool._do_get's stale permit decrement after the reset.
                if self._max_overflow == -1:
                    self._overflow += 1
                else:
                    with self._overflow_lock:
                        self._overflow += 1
            raise
        record_info = record.record_info
        assert record_info is not None
        record_info[_POOL_ACCOUNTING_EPOCH_KEY] = accounting_epoch
        return record

    def _do_get(self) -> ConnectionPoolEntry:
        self._clickhouse_awaiting_recreate = False
        generation = self._clickhouse_generation
        while True:
            record = super()._do_get()
            record_info = record.record_info
            assert record_info is not None
            if record_info[_POOL_ACCOUNTING_EPOCH_KEY] != self._clickhouse_accounting_epoch:
                if not self._inc_overflow():
                    record.close()
                    self._append_pending(record)
                    self._promote_pending_record()
                    continue
                record_info[_POOL_ACCOUNTING_EPOCH_KEY] = self._clickhouse_accounting_epoch
            record_info[_POOL_GENERATION_KEY] = generation
            self._promote_pending_record()
            return record

    def recreate(self) -> _ClickHouseAsyncAdaptedQueuePool:
        replacement = super().recreate()
        if self._clickhouse_awaiting_recreate:
            self._clickhouse_recreated = True
            self._clickhouse_awaiting_recreate = False
        return cast("_ClickHouseAsyncAdaptedQueuePool", replacement)


class ClickHouseAsyncDialect(ClickHouseDialect):
    """Native asyncio dialect for ClickHouse Connect."""

    driver = "async"
    is_async = True
    has_terminate = True
    poolclass = _ClickHouseAsyncAdaptedQueuePool
    supports_server_side_cursors: bool = False
    supports_statement_cache: bool = False

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the async DB-API adapter module."""
        return cast(DBAPIModule, _ASYNC_DBAPI)

    def get_driver_connection(self, connection: DBAPIConnection) -> AsyncClient:
        """Return the native async driver connection."""
        return cast("AsyncClient", cast(_AsyncAdaptedConnection, connection).driver_connection)

    def is_disconnect(
        self,
        e: DBAPIModule.Error,
        connection: PoolProxiedConnection | DBAPIConnection | None,
        cursor: DBAPICursor | None,
    ) -> bool:
        del e, cursor
        if connection is None:
            return False
        is_closed = getattr(connection, "is_closed", None)
        if is_closed is None:
            return False
        return bool(is_closed())

    def do_close(self, dbapi_connection: DBAPIConnection) -> None:
        cast(_AsyncAdaptedConnection, dbapi_connection).close()

    def do_terminate(self, dbapi_connection: DBAPIConnection) -> None:
        cast(_AsyncAdaptedConnection, dbapi_connection).terminate()
