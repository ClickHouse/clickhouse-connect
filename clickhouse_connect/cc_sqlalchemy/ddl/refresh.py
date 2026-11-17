"""ClickHouse materialized view refresh strategies."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import render_settings
from clickhouse_connect.driver.binding import quote_identifier


@dataclass(frozen=True)
class ClickHouseRefresh:
    """A refresh strategy. Intervals are raw SQL and dependencies use the session database."""

    every: str | None = None
    after: str | None = None
    offset: str | None = None
    randomize_for: str | None = None
    depends_on: tuple[str | tuple[str, str], ...] = ()
    settings: Mapping[str, Any] | None = field(default=None, hash=False)
    append: bool = False

    def __post_init__(self) -> None:
        if (self.every is not None) == (self.after is not None):
            raise ValueError("ClickHouseRefresh requires exactly one of every or after")
        if self.offset is not None and self.every is None:
            raise ValueError("ClickHouseRefresh offset requires every")
        for name in ("every", "after", "offset", "randomize_for"):
            value = getattr(self, name)
            if value is not None and not (isinstance(value, str) and value.strip()):
                raise ValueError(f"ClickHouseRefresh {name} must be a non-empty string")
        if isinstance(self.depends_on, str):
            raise TypeError("ClickHouseRefresh depends_on must be a sequence of names, not a string")
        depends_on = tuple(entry if isinstance(entry, str) else tuple(entry) for entry in self.depends_on)
        for entry in depends_on:
            if not isinstance(entry, str) and (len(entry) != 2 or not all(isinstance(part, str) for part in entry)):
                raise ValueError("ClickHouseRefresh depends_on entries must be names or (schema, name) pairs")
        object.__setattr__(self, "depends_on", depends_on)
        object.__setattr__(self, "settings", dict(self.settings) if self.settings else None)

    def __repr__(self) -> str:
        fields = (
            ("every", self.every, None),
            ("after", self.after, None),
            ("offset", self.offset, None),
            ("randomize_for", self.randomize_for, None),
            ("depends_on", self.depends_on, ()),
            ("settings", self.settings, None),
            ("append", self.append, False),
        )
        args = ", ".join(f"{name}={value!r}" for name, value, default in fields if value != default)
        return f"ClickHouseRefresh({args})"

    def render(self) -> str:
        """Render the complete REFRESH clause."""
        sql = f"REFRESH EVERY {self.every}" if self.every is not None else f"REFRESH AFTER {self.after}"
        if self.offset is not None:
            sql += f" OFFSET {self.offset}"
        if self.randomize_for is not None:
            sql += f" RANDOMIZE FOR {self.randomize_for}"
        if self.depends_on:
            names = (name.split(".", 1) if isinstance(name, str) else name for name in self.depends_on)
            sql += " DEPENDS ON " + ", ".join(".".join(quote_identifier(part) for part in name) for name in names)
        if self.settings is not None:
            sql += f" SETTINGS {render_settings(self.settings)}"
        if self.append:
            sql += " APPEND"
        return sql
