"""Declarative ClickHouse projections."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Table
from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.schema import SchemaItem

from clickhouse_connect.cc_sqlalchemy._schema import canonical_schema_kwargs
from clickhouse_connect.driver.binding import quote_identifier


@dataclass(frozen=True)
class ClickHouseProjection(SchemaItem):
    """An immutable projection definition. select is the raw SQL body inside the parens."""

    name: str
    select: str
    if_not_exists: bool = False
    first: bool = False
    after_projection: str | None = None

    def __post_init__(self) -> None:
        if self.first and self.after_projection is not None:
            raise ValueError("ClickHouseProjection cannot specify both first and after placement")

    def _set_parent(self, parent: SchemaEventTarget, **kwargs: Any) -> None:
        if not isinstance(parent, Table):
            raise ArgumentError("ClickHouseProjection must be attached to a Table")
        projections = table_projections(parent)
        for position, projection in enumerate(projections):
            if projection.name != self.name:
                continue
            if projection == self and kwargs.get("allow_replacements", True):
                return
            if kwargs.get("allow_replacements"):
                parent.kwargs["clickhousedb_projections"] = (*projections[:position], self, *projections[position + 1 :])
                return
            raise ArgumentError(f"Duplicate ClickHouse projection name {self.name!r}")
        parent.kwargs["clickhousedb_projections"] = (*projections, self)


def table_projections(table: Table) -> tuple[ClickHouseProjection, ...]:
    """Return the table's explicit projection declarations."""
    projections = tuple(canonical_schema_kwargs(table.kwargs, ("projections",)).get("clickhousedb_projections") or ())
    if any(not isinstance(projection, ClickHouseProjection) for projection in projections):
        raise ArgumentError("clickhousedb_projections must contain ClickHouseProjection definitions")
    names = [projection.name for projection in projections]
    if len(set(names)) != len(names):
        raise ArgumentError("ClickHouse projection names must be unique within a table")
    return projections


def projection_definition(projection: ClickHouseProjection) -> str:
    """Render the definition shared by CREATE TABLE and ALTER TABLE."""
    return f"{quote_identifier(projection.name)} ({projection.select})"
