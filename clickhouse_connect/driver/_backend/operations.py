from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from clickhouse_connect.driver._backend.models import _freeze_mapping

if TYPE_CHECKING:
    from clickhouse_connect.driver.query import QueryContext


@dataclass(frozen=True)
class CommandOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)
    use_database: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


@dataclass(frozen=True)
class QueryOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)
    column_formats: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))
        object.__setattr__(self, "column_formats", _freeze_mapping(self.column_formats))

    def make_context(self, create_context: Callable[..., QueryContext], query_formats: dict[str, str]) -> QueryContext:
        context = create_context(
            query=self.text,
            settings=dict(self.settings) or None,
            query_formats=dict(query_formats),
            column_formats=dict(self.column_formats) or None,
        )
        context.internal = True
        return context


@dataclass(frozen=True)
class RawQueryOp:
    text: str
    settings: Mapping[str, Any] = field(default_factory=dict)
    fmt: str = "Native"

    def __post_init__(self) -> None:
        object.__setattr__(self, "settings", _freeze_mapping(self.settings))


Operation = CommandOp | QueryOp | RawQueryOp
