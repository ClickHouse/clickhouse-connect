"""Schema option names shared by registration, reflection, and migration output."""

from collections.abc import Iterable, Mapping
from typing import Any

TABLE_OPTIONS = (
    "engine",
    "table_type",
    "dictionary_source",
    "dictionary_layout",
    "dictionary_lifetime",
    "dictionary_primary_key",
)
COLUMN_OPTIONS = ("materialized", "alias", "codec", "ttl", "after", "settings")


def canonical_schema_kwargs(kwargs: Mapping[str, Any], names: Iterable[str]) -> dict[str, Any]:
    """Rename explicit legacy schema options to our namespace."""
    result = dict(kwargs)
    for name in names:
        legacy = f"clickhouse_{name}"
        canonical = f"clickhousedb_{name}"
        if legacy in result:
            legacy_value = result.pop(legacy)
            if result.get(canonical) is None:
                result[canonical] = legacy_value
    return result
