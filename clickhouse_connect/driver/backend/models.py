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
