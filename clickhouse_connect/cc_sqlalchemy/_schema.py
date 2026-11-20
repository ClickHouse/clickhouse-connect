"""Schema option names shared by registration, reflection, and migration output."""

import re
from collections.abc import Iterable, Mapping
from typing import Any

TABLE_OPTIONS = (
    "engine",
    "indexes",
    "projections",
    "table_type",
    "dictionary_source",
    "dictionary_layout",
    "dictionary_lifetime",
    "dictionary_primary_key",
)
COLUMN_OPTIONS = ("materialized", "alias", "codec", "ttl", "after", "settings")
_SETTING_SEGMENT = r"""(?:[A-Za-z_$][A-Za-z0-9_$]*|`(?:[^`\\]|\\.|``)+`|"(?:[^"\\]|\\.|"")+")"""
_SETTING_NAME = re.compile(rf"{_SETTING_SEGMENT}(?:\.{_SETTING_SEGMENT})*")


def validate_setting_name(name: str) -> None:
    """Require a setting identifier."""
    if not isinstance(name, str) or _SETTING_NAME.fullmatch(name) is None:
        raise ValueError(f"Invalid ClickHouse setting name {name!r}")


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
