from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import tzinfo
from types import MappingProxyType
from typing import Any

from clickhouse_connect.driver.models import SettingDef
from clickhouse_connect.driver.query import TzSource


def _freeze_mapping(values: Mapping[str, Any]) -> Mapping[str, Any]:
    # MappingProxyType fields make the frozen dataclasses that hold them
    # unhashable. They are value objects compared by equality only.
    return MappingProxyType(dict(values))


@dataclass(frozen=True)
class Capabilities:
    native_async: bool = False
    sessions: bool = False


@dataclass(frozen=True)
class ClientConfig:
    database: str | None = None
    query_limit: int = 0
    query_retries: int = 2
    settings: Mapping[str, Any] = field(default_factory=dict)
    timezone_policy: TzSource = "auto"

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class ServerInfo:
    version: str
    timezone: tzinfo
    settings: Mapping[str, SettingDef]
    capabilities: Capabilities = field(default_factory=Capabilities)

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class QueryRuntime:
    """Backend-neutral per-call execution inputs resolved by the facade."""

    database: str | None = None
    protocol_version: int = 0
    settings: Mapping[str, str] = field(default_factory=dict)
    retries: int = 0


@dataclass
class QueryExecution:
    """Result of a backend query execution.

    Either a byte source (an object exposing a chunk generator via .gen,
    consumed by the response buffer and closable) or, for a columns-only
    metadata probe, the column metadata as name/type mappings.
    """

    source: Any | None = None
    columns: list[dict[str, Any]] | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    response_tz_name: str | None = None
