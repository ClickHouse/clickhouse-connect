"""
Runtime discovery of out-of-tree execution backends.

Backends register through the ``clickhouse_connect.backends`` entry-point group: the name
maps to a :class:`~clickhouse_connect.driver.backend.Backend` factory object that
``get_client(backend=...)`` loads. clickhouse-connect never imports a backend package by
name; discovery is lazy and entry points are resolved on demand, so a user who has not
installed any backend pays nothing.
"""

from __future__ import annotations

import importlib.metadata as _md
from typing import TYPE_CHECKING

from clickhouse_connect.driver.exceptions import BackendNotInstalled

if TYPE_CHECKING:
    from clickhouse_connect.driver.backend import Backend

BACKENDS_GROUP = "clickhouse_connect.backends"

# Install hints for backends maintained outside this repo. Adding an entry here is purely
# a UX nicety for the BackendNotInstalled error; it creates no code dependency.
_INSTALL_HINTS = {
    "chdb": "pip install clickhouse-connect[chdb]",
}


def _entry_points(group: str):
    """Return entry points in a group, compatible across Python 3.9 - 3.12+."""
    try:
        # Python 3.10+: selectable API
        return list(_md.entry_points(group=group))
    except TypeError:
        # Python 3.9: entry_points() returns a dict keyed by group
        return list(_md.entry_points().get(group, []))


def available_backend_names() -> list[str]:
    """Names of every registered backend, with the built-in ``http`` always present."""
    names = {ep.name for ep in _entry_points(BACKENDS_GROUP)}
    names.add("http")
    return sorted(names)


def resolve_backend(name: str) -> "Backend":
    """Load and return the registered backend factory for ``name``.

    Raises :class:`BackendNotInstalled` (with an install hint when known) if no backend by
    that name is registered.
    """
    for ep in _entry_points(BACKENDS_GROUP):
        if ep.name == name:
            return ep.load()
    raise BackendNotInstalled(name, available=available_backend_names(), hint=_INSTALL_HINTS.get(name))
