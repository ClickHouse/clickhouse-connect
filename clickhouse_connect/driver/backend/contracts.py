from __future__ import annotations

from typing import Protocol

from clickhouse_connect.driver.backend.models import Capabilities
from clickhouse_connect.driver.backend.operations import Operation


class SyncExecutor(Protocol):
    def execute(self, operation: Operation) -> object: ...


class AsyncExecutor(Protocol):
    async def execute(self, operation: Operation) -> object: ...


class SyncBackend(SyncExecutor, Protocol):
    capabilities: Capabilities

    def open(self) -> None: ...

    def ping(self) -> bool: ...

    def close(self) -> None: ...

    def close_connections(self) -> None: ...


class AsyncBackend(AsyncExecutor, Protocol):
    capabilities: Capabilities

    async def open(self) -> None: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...

    async def close_connections(self) -> None: ...
