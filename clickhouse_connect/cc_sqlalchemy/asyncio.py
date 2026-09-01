from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Sequence
from datetime import tzinfo
from importlib.metadata import version
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

from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_connection  # noqa: E402
from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPIModule  # noqa: E402
from sqlalchemy.pool import AsyncAdaptedQueuePool  # noqa: E402
from sqlalchemy.util import await_only  # noqa: E402

from clickhouse_connect import dbapi  # noqa: E402
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect  # noqa: E402
from clickhouse_connect.datatypes.base import ClickHouseType  # noqa: E402
from clickhouse_connect.dbapi.cursor import Cursor  # noqa: E402
from clickhouse_connect.driver import create_async_client  # noqa: E402
from clickhouse_connect.driver.binding import _query_is_insert  # noqa: E402
from clickhouse_connect.driver.external import ExternalData  # noqa: E402
from clickhouse_connect.driver.query import QueryResult  # noqa: E402
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
    ) -> QueryResult:
        async with self._execute_mutex:
            return await self.driver_connection.query(
                query=query,
                parameters=parameters,
                settings=settings,
                query_formats=query_formats,
            )

    def query(
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
    ) -> QueryResult:
        return await_only(self._query(query, parameters, settings, query_formats))

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


class _AsyncAdaptedConnection(AsyncAdapt_dbapi_connection):
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
            kwargs.setdefault("autogenerate_session_id", False)
            dsn_options = parse_qs(urlparse(dsn).query) if dsn else {}
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


class ClickHouseAsyncDialect(ClickHouseDialect):
    """Native asyncio dialect for ClickHouse Connect."""

    driver = "async"
    is_async = True
    poolclass = AsyncAdaptedQueuePool
    supports_server_side_cursors: bool = False
    supports_statement_cache: bool = False

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """Return the async DB-API adapter module."""
        return cast(DBAPIModule, _ASYNC_DBAPI)

    def get_driver_connection(self, connection: DBAPIConnection) -> AsyncClient:
        """Return the native async driver connection."""
        return cast("AsyncClient", cast(_AsyncAdaptedConnection, connection).driver_connection)
