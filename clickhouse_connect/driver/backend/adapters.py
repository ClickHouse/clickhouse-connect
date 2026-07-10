from __future__ import annotations

from typing import TYPE_CHECKING

from clickhouse_connect.driver.backend.operations import CommandOp, Operation, QueryOp, RawQueryOp

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.client import Client

# Orchestration queries are internal, so their decode must not be affected by
# user-configured global read formats such as set_default_formats("String", "bytes").
_INTERNAL_QUERY_FORMATS = {"String": "string"}


class SyncClientExecutor:
    def __init__(self, client: Client):
        self._client = client

    def execute(self, operation: Operation) -> object:
        settings = dict(operation.settings) or None
        if isinstance(operation, CommandOp):
            return self._client.command(operation.text, settings=settings, use_database=operation.use_database)
        if isinstance(operation, QueryOp):
            return self._client.query(operation.text, settings=settings, query_formats=dict(_INTERNAL_QUERY_FORMATS))
        if isinstance(operation, RawQueryOp):
            return self._client.raw_query(operation.text, settings=settings, fmt=operation.fmt)
        raise TypeError(f"Unsupported operation type: {type(operation).__name__}")


class AsyncClientExecutor:
    def __init__(self, client: AsyncClient):
        self._client = client

    async def execute(self, operation: Operation) -> object:
        settings = dict(operation.settings) or None
        if isinstance(operation, CommandOp):
            return await self._client.command(operation.text, settings=settings, use_database=operation.use_database)
        if isinstance(operation, QueryOp):
            return await self._client.query(operation.text, settings=settings, query_formats=dict(_INTERNAL_QUERY_FORMATS))
        if isinstance(operation, RawQueryOp):
            return await self._client.raw_query(operation.text, settings=settings, fmt=operation.fmt)
        raise TypeError(f"Unsupported operation type: {type(operation).__name__}")
