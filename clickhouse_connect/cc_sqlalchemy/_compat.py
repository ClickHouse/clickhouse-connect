"""Compatibility with other providers of SQLAlchemy's ``clickhouse`` name."""

import warnings
from importlib.metadata import entry_points

from sqlalchemy.dialects import registry


def register_clickhouse_alias() -> None:
    """Register the bare alias only when no other provider claims it."""
    if "clickhouse" in registry.impls:
        return
    if any(not ep.value.startswith("clickhouse_connect.") for ep in entry_points(group="sqlalchemy.dialects", name="clickhouse")):
        warnings.warn(
            "The 'clickhouse://' URL remains registered to another SQLAlchemy dialect. "
            "ClickHouse Connect is available through 'clickhousedb://' or 'clickhousedb+connect://'.",
            UserWarning,
            stacklevel=3,
        )
        return
    registry.register("clickhouse", "clickhouse_connect.cc_sqlalchemy.dialect", "ClickHouseDialect")
