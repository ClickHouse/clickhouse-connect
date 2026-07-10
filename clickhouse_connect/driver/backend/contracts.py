from __future__ import annotations

from typing import Protocol

from clickhouse_connect.driver.backend.models import Capabilities
from clickhouse_connect.driver.backend.operations import Operation


class SyncBackend(Protocol):
    capabilities: Capabilities

    def open(self) -> None: ...

    def execute(self, operation: Operation) -> object: ...

    def ping(self) -> bool: ...

    def close(self) -> None: ...

    def close_connections(self) -> None: ...


class AsyncBackend(Protocol):
    capabilities: Capabilities

    async def open(self) -> None: ...

    async def execute(self, operation: Operation) -> object: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...

    async def close_connections(self) -> None: ...
