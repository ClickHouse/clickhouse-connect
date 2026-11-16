"""Declarative ClickHouse data skipping indexes."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.schema import SchemaItem

from clickhouse_connect.cc_sqlalchemy._schema import canonical_schema_kwargs
from clickhouse_connect.driver.binding import quote_identifier


@dataclass(frozen=True)
class ClickHouseIndex(SchemaItem):
    """An immutable skip index definition. Expressions and types are raw SQL."""

    name: str
    expression: str
    type_: str
    granularity: int | None = None
    if_not_exists: bool = False
    first: bool = False
    after_index: str | None = None

    def __post_init__(self) -> None:
        if self.granularity is not None and (isinstance(self.granularity, bool) or not isinstance(self.granularity, int)):
            raise TypeError("ClickHouseIndex granularity must be an integer or None")
        if self.first and self.after_index is not None:
            raise ValueError("ClickHouseIndex cannot specify both first and after placement")

    def _set_parent(self, parent: SchemaEventTarget, **kwargs: Any) -> None:
        if not isinstance(parent, Table):
            raise ArgumentError("ClickHouseIndex must be attached to a Table")
        indexes = table_indexes(parent)
        for position, index in enumerate(indexes):
            if index.name != self.name:
                continue
            if index == self and kwargs.get("allow_replacements", True):
                return
            if kwargs.get("allow_replacements"):
                parent.kwargs["clickhousedb_indexes"] = (*indexes[:position], self, *indexes[position + 1 :])
                return
            raise ArgumentError(f"Duplicate ClickHouse index name {self.name!r}")
        parent.kwargs["clickhousedb_indexes"] = (*indexes, self)


def table_indexes(table: Table) -> tuple[ClickHouseIndex, ...]:
    """Return the table's explicit skip index declarations."""
    indexes = tuple(canonical_schema_kwargs(table.kwargs, ("indexes",)).get("clickhousedb_indexes") or ())
    if any(not isinstance(index, ClickHouseIndex) for index in indexes):
        raise ArgumentError("clickhousedb_indexes must contain ClickHouseIndex definitions")
    names = [index.name for index in indexes]
    if len(set(names)) != len(names):
        raise ArgumentError("ClickHouse index names must be unique within a table")
    return indexes


def index_definition(index: ClickHouseIndex) -> str:
    """Render the definition shared by CREATE TABLE and ALTER TABLE."""
    definition = f"{quote_identifier(index.name)} {index.expression} TYPE {index.type_}"
    if index.granularity is not None:
        definition += f" GRANULARITY {index.granularity}"
    return definition
