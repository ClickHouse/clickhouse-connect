"""
Async wrapper around ChdbClient.

chdb has no native async API, so this client delegates each call to the wrapped
sync ChdbClient via `asyncio.get_running_loop().run_in_executor(...)`. Because
ChdbClient serializes concurrent calls on a per-client `threading.Lock`,
gather()-style concurrency on a single AsyncChdbClient does not actually run in
parallel — for true parallelism, create multiple clients.
"""

from __future__ import annotations

import asyncio
import io
import logging
from collections.abc import Generator, Iterable, Sequence
from datetime import tzinfo
from typing import TYPE_CHECKING, Any, BinaryIO

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.chdbclient import ChdbClient
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.common import StreamContext
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext, QueryResult, TzMode
from clickhouse_connect.driver.summary import QuerySummary

if TYPE_CHECKING:
    import numpy
    import pandas
    import polars
    import pyarrow

logger = logging.getLogger(__name__)


class AsyncChdbClient(Client):
    """
    Async-facing client for the in-process chdb backend. Each public coroutine
    schedules the corresponding sync ChdbClient call on the default thread
    executor. Sync-only methods (settings, min_version) are passed through
    directly.
    """

    valid_transport_settings: set[str] = ChdbClient.valid_transport_settings
    optional_transport_settings: set[str] = ChdbClient.optional_transport_settings

    def __init__(self, sync: ChdbClient):
        self._sync = sync
        # Mirror attributes commonly read off the client object so user code that
        # touches them (server_version, server_tz, database, etc.) keeps working.
        self.server_tz = sync.server_tz
        self.server_version = sync.server_version
        self.server_settings = sync.server_settings
        self.database = sync.database
        self.uri = sync.uri
        self.query_limit = sync.query_limit
        self.query_retries = sync.query_retries
        self.tz_mode = sync.tz_mode
        self._tz_source = sync._tz_source
        self._apply_server_tz = sync._apply_server_tz
        self._dst_safe = sync._dst_safe
        self.show_clickhouse_errors = sync.show_clickhouse_errors
        self.protocol_version = sync.protocol_version
        self.write_compression = sync.write_compression
        self.compression = sync.compression
        self._read_format = sync._read_format
        self._write_format = sync._write_format
        self._transform = sync._transform

    @property
    def chdb_connection(self):
        return self._sync.chdb_connection

    async def _run(self, func, *args, **kwargs):
        loop = asyncio.get_running_loop()
        if kwargs:
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
        return await loop.run_in_executor(None, func, *args)

    # ---- sync passthroughs (no I/O) ----

    def set_client_setting(self, key: str, value: Any) -> None:
        self._sync.set_client_setting(key, value)

    def get_client_setting(self, key: str) -> str | None:
        return self._sync.get_client_setting(key)

    def set_access_token(self, access_token: str) -> None:
        self._sync.set_access_token(access_token)

    def min_version(self, version_str: str) -> bool:
        return self._sync.min_version(version_str)

    # ---- async overrides ----

    async def _query_with_context(self, context: QueryContext) -> QueryResult:  # type: ignore[override]
        return await self._run(self._sync._query_with_context, context)

    async def query(  # type: ignore[override]
        self,
        query: str | None = None,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        query_formats: dict[str, str] | None = None,
        column_formats: dict[str, str | dict[str, str]] | None = None,
        encoding: str | None = None,
        use_none: bool | None = None,
        column_oriented: bool | None = None,
        use_numpy: bool | None = None,
        max_str_len: int | None = None,
        context: QueryContext | None = None,
        query_tz: str | tzinfo | None = None,
        column_tzs: dict[str, str | tzinfo] | None = None,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
        tz_mode: TzMode | None = None,
    ) -> QueryResult:
        return await self._run(
            lambda: self._sync.query(
                query=query,
                parameters=parameters,
                settings=settings,
                query_formats=query_formats,
                column_formats=column_formats,
                encoding=encoding,
                use_none=use_none,
                column_oriented=column_oriented,
                use_numpy=use_numpy,
                max_str_len=max_str_len,
                context=context,
                query_tz=query_tz,
                column_tzs=column_tzs,
                external_data=external_data,
                transport_settings=transport_settings,
                tz_mode=tz_mode,
            )
        )

    async def query_column_block_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_column_block_stream(*args, **kwargs))

    async def query_row_block_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_row_block_stream(*args, **kwargs))

    async def query_rows_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_rows_stream(*args, **kwargs))

    async def query_np(self, *args, **kwargs) -> numpy.ndarray:
        return await self._run(lambda: self._sync.query_np(*args, **kwargs))

    async def query_np_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_np_stream(*args, **kwargs))

    async def query_df(self, *args, **kwargs) -> pandas.DataFrame:
        return await self._run(lambda: self._sync.query_df(*args, **kwargs))

    async def query_df_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_df_stream(*args, **kwargs))

    async def query_arrow(self, *args, **kwargs) -> pyarrow.Table:
        return await self._run(lambda: self._sync.query_arrow(*args, **kwargs))

    async def query_arrow_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_arrow_stream(*args, **kwargs))

    async def query_df_arrow(self, *args, **kwargs) -> pandas.DataFrame | polars.DataFrame:
        return await self._run(lambda: self._sync.query_df_arrow(*args, **kwargs))

    async def query_df_arrow_stream(self, *args, **kwargs) -> StreamContext:  # type: ignore[override]
        return await self._run(lambda: self._sync.query_df_arrow_stream(*args, **kwargs))

    async def command(  # type: ignore[override]
        self,
        cmd: str,
        parameters: Sequence | dict[str, Any] | None = None,
        data: str | bytes | None = None,
        settings: dict[str, Any] | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> str | int | Sequence[str] | QuerySummary:
        return await self._run(
            lambda: self._sync.command(
                cmd,
                parameters=parameters,
                data=data,
                settings=settings,
                use_database=use_database,
                external_data=external_data,
                transport_settings=transport_settings,
            )
        )

    async def ping(self) -> bool:  # type: ignore[override]
        return await self._run(self._sync.ping)

    async def raw_query(  # type: ignore[override]
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> bytes:
        return await self._run(
            lambda: self._sync.raw_query(
                query,
                parameters=parameters,
                settings=settings,
                fmt=fmt,
                use_database=use_database,
                external_data=external_data,
                transport_settings=transport_settings,
            )
        )

    async def raw_stream(  # type: ignore[override]
        self,
        query: str,
        parameters: Sequence | dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        use_database: bool = True,
        external_data: ExternalData | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> io.IOBase:
        return await self._run(
            lambda: self._sync.raw_stream(
                query,
                parameters=parameters,
                settings=settings,
                fmt=fmt,
                use_database=use_database,
                external_data=external_data,
                transport_settings=transport_settings,
            )
        )

    async def insert(  # type: ignore[override]
        self,
        table: str | None = None,
        data=None,
        column_names: str | Iterable[str] = "*",
        database: str | None = None,
        column_types: Sequence[ClickHouseType] | None = None,
        column_type_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        settings: dict[str, Any] | None = None,
        context: InsertContext | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        return await self._run(
            lambda: self._sync.insert(
                table=table,
                data=data,
                column_names=column_names,
                database=database,
                column_types=column_types,
                column_type_names=column_type_names,
                column_oriented=column_oriented,
                settings=settings,
                context=context,
                transport_settings=transport_settings,
            )
        )

    async def insert_df(self, *args, **kwargs) -> QuerySummary:  # type: ignore[override]
        return await self._run(lambda: self._sync.insert_df(*args, **kwargs))

    async def insert_arrow(self, *args, **kwargs) -> QuerySummary:  # type: ignore[override]
        return await self._run(lambda: self._sync.insert_arrow(*args, **kwargs))

    async def insert_df_arrow(self, *args, **kwargs) -> QuerySummary:  # type: ignore[override]
        return await self._run(lambda: self._sync.insert_df_arrow(*args, **kwargs))

    async def data_insert(self, context: InsertContext) -> QuerySummary:  # type: ignore[override]
        return await self._run(self._sync.data_insert, context)

    async def raw_insert(  # type: ignore[override]
        self,
        table: str | None = None,
        column_names: Sequence[str] | None = None,
        insert_block: str | bytes | Generator[bytes, None, None] | BinaryIO | None = None,
        settings: dict[str, Any] | None = None,
        fmt: str | None = None,
        compression: str | None = None,
        transport_settings: dict[str, str] | None = None,
    ) -> QuerySummary:
        return await self._run(
            lambda: self._sync.raw_insert(
                table=table,
                column_names=column_names,
                insert_block=insert_block,
                settings=settings,
                fmt=fmt,
                compression=compression,
                transport_settings=transport_settings,
            )
        )

    async def close(self) -> None:  # type: ignore[override]
        await self._run(self._sync.close)

    async def close_connections(self) -> None:  # type: ignore[override]
        await self._run(self._sync.close_connections)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    # Some helper methods on Client (like create_insert_context, create_query_context)
    # do synchronous local work and call self.query/self.command for schema lookup. We
    # can't await inside a sync method, so users should normally rely on insert/query
    # which we already async-wrap.

    def create_insert_context(self, *args, **kwargs):
        return self._sync.create_insert_context(*args, **kwargs)

    def create_query_context(self, *args, **kwargs):
        return self._sync.create_query_context(*args, **kwargs)
