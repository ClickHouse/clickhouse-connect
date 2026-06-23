"""
Pluggable execution backends for clickhouse-connect.

clickhouse-connect's built-in HTTP transport (``HttpClient`` / ``AsyncClient``) is the
default and only in-tree backend. Additional execution backends -- for example chDB's
in-process embedded ClickHouse engine -- live in *their own* packages and register
themselves through the ``clickhouse_connect.backends`` entry-point group. This module
defines the narrow contract those packages implement; clickhouse-connect contains no
backend-specific code and never imports a backend package directly.

The coupling surface between clickhouse-connect and an out-of-tree backend is therefore
exactly two things:

* the ``Backend`` (and optionally ``AsyncBackend``) protocols below, and
* the entry-point group name ``clickhouse_connect.backends``.

Everything else -- the engine version, its output formats, its private APIs -- is
invisible to clickhouse-connect.

Protocol evolution discipline (so a published backend never breaks):

1. Methods are only ever added, never removed and never re-signed.
2. A new factory method must be optional (resolved with ``getattr``) so an older backend
   that predates it keeps working untouched.
3. Capabilities are advertised with ``supports_*`` flags whose meaning is frozen once
   shipped; a new capability gets a new flag.
"""

from __future__ import annotations

from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from clickhouse_connect.driver.asyncclient import AsyncClient
    from clickhouse_connect.driver.client import Client


@runtime_checkable
class Backend(Protocol):
    """Sync factory contract that an out-of-tree backend package implements.

    The object registered at the ``clickhouse_connect.backends`` entry point is loaded by
    :mod:`clickhouse_connect.driver.registry` and asked to build a client. It returns a
    fully-formed :class:`~clickhouse_connect.driver.client.Client` -- so a backend reuses
    every public method, the type system, settings normalization, the Native parser and the
    error model that already live on ``Client``.
    """

    #: Stable identifier, must match the entry-point name (e.g. ``"chdb"``).
    backend_name: str

    def create_client(self, **kwargs: Any) -> Client:
        """Build and return a synchronous client for this backend."""
        ...


@runtime_checkable
class AsyncBackend(Protocol):
    """Optional async-factory protocol; backends that support async additionally implement this.

    The async factory must return an :class:`~clickhouse_connect.driver.asyncclient.AsyncClient`
    (or an awaitable producing one) so user code can use ``await client.query(...)`` and the
    other coroutine methods on the public async API. A protocol-typed ``Client`` would be
    misleading here -- ``Client.query`` is synchronous, while an async backend must surface
    coroutine versions of the same methods.
    """

    backend_name: str

    def create_async_client(self, **kwargs: Any) -> AsyncClient | Awaitable[AsyncClient]:
        """Build and return an async-capable client (or an awaitable producing one)."""
        ...


# ── Capability flags ────────────────────────────────────────────────────────────────
#
# These are read off the *client* a backend produces (the client carries the capability,
# since that is what the public API consults at call time). A flag absent on a client is
# treated as its documented default below.

#: ``True``  -> ``Client.query_df`` should prefer the Arrow -> pandas path, because the
#:             backend's ``query_arrow`` is a genuine in-process zero-copy buffer.
#: ``False`` -> ``query_df`` keeps using the Native parser path (better type fidelity for
#:             LowCardinality / Enum / nested / Decimal; for HTTP an Arrow round-trip buys
#:             nothing since wire bytes are unavoidable).
#:
#: Default when the attribute is missing: ``False`` (the HTTP behavior, unchanged).
SUPPORTS_ZERO_COPY_ARROW = "supports_zero_copy_arrow"


def client_supports(client: Client, capability: str) -> bool:
    """Read a capability flag off a client, defaulting to ``False`` when absent."""
    return bool(getattr(client, capability, False))
