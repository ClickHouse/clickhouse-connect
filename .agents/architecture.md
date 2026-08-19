# AI Architecture And Repo Context

This document provides repo-specific context for AI agents working in `clickhouse-connect`.

`AGENTS.md` is the operational source of truth. This file is required reading before substantial code changes, but it does not override `AGENTS.md`.

## Repository Overview

`clickhouse-connect` is the official Python driver for ClickHouse. The standard clients use the HTTP interface. The package also provides an experimental synchronous backend for the embedded chDB engine. Downstream integrations include Pandas, NumPy, PyArrow, Polars, SQLAlchemy, Alembic, Superset, and DB-API.

Top-level areas that matter most:

```text
clickhouse_connect/
  driver/            Public client behavior, contexts, binding, streaming, and client facades
    _backend/        Typed backend contracts, HTTP transports, chDB execution, and shared orchestration
  driverc/           Cython hot-path extensions
  datatypes/         ClickHouse type implementations and serialization logic
  cc_sqlalchemy/     SQLAlchemy dialect, ClickHouse extensions, reflection, and Alembic support
  dbapi/             PEP 249 wrapper layer
  tools/             User-facing helpers
tests/
  unit_tests/        Fast tests, including backend and optional chDB coverage
  integration_tests/ Client-level and wire-level behavior against ClickHouse
  type_check/        Downstream consumer typing checks
examples/            Usage examples and ad hoc performance scripts
```

## Core Invariants

### Clients and backends share one contract

The client implementation separates public client behavior from execution backends:

- `clickhouse_connect/driver/client.py` owns shared client state, context preparation, settings validation, and result helpers.
- `clickhouse_connect/driver/_backendclient.py` implements the synchronous query, command, insert, raw access, and lifecycle methods against the typed `SyncBackend` contract.
- `clickhouse_connect/driver/httpclient.py` constructs the synchronous HTTP client and preserves its compatibility surface.
- `clickhouse_connect/driver/asyncclient.py` implements the asynchronous client behavior against `HttpAsyncBackend`.
- `clickhouse_connect/driver/_chdbclient.py` uses the synchronous backend client with `ChdbBackend`.
- `clickhouse_connect/driver/_backend/contracts.py` defines the sync and async backend protocols.
- `clickhouse_connect/driver/_backend/orchestration.py` shares initialization and insert-context workflows between sync and async clients.

Put changes at the lowest layer that owns the behavior:

- Shared client semantics usually belong in `Client`, shared contexts, binding, formats, or orchestration.
- Synchronous semantic changes belong in `SyncBackendClient`, with corresponding async consideration in `AsyncClient`.
- HTTP request, retry, response, authentication, proxy, timeout, and compression changes usually need matching work in `HttpSyncBackend` and `HttpAsyncBackend`.
- Changes to synchronous backend behavior must consider both the HTTP and chDB backends.
- Initialization and insert-context changes should preserve the sync and async orchestration sequence.

The chDB backend does not support the async client or external data. Do not force HTTP-only behavior into the shared synchronous facade, and do not claim chDB parity for features its documented capabilities exclude.

### Public API stability matters

Treat the following as public surface:

- `clickhouse_connect.get_client`, `get_async_client`, and related top-level entry points
- public names in `clickhouse_connect.driver.*`, including `create_client` and `create_async_client`
- `Client` and `AsyncClient` behavior and method signatures
- factory behavior for the public `interface="chdb"` and `chdb://` entry points
- datatype read and write behavior
- DB-API behavior
- SQLAlchemy and Alembic behavior
- result structures and dtype choices across Python, Pandas, NumPy, Arrow, and Polars

The project is in the `1.x` release line. Backward-incompatible changes to the public API or observable behavior are not allowed as routine changes. Preserve existing signatures, accepted inputs, defaults, return values, exception behavior, serialization, and result semantics. If a change appears to require a compatibility break, stop and get an explicit maintainer decision. An approved exception needs an appropriate deprecation or migration plan, documentation, changelog treatment, and versioning decision.

The public surface is statically typed. The package ships `py.typed`, CI runs `mypy`, and CI checks the installed package from a downstream consumer environment. Public annotations are part of the compatibility contract. A public signature change must keep its annotations correct, pass `mypy`, pass the consumer smoke tests, and must not weaken the public type-completeness ratchet. See `Type Checking` in `AGENTS.md`.

Be cautious with:

- return types and result structures
- accepted argument types and defaults
- null handling
- ordering
- timezone behavior
- error types and user-visible error messages

Small internal refactors can still create breaking behavior in this repo.

### Optional dependencies and bare installs are deliberate

The base package must not eagerly require every optional integration dependency.

Important examples:

- async support is optional and must not require `aiohttp` when importing the base package
- the chDB backend is optional and must not require `chdb` unless that backend is selected
- Pandas, PyArrow, Polars, and NumPy integrations must stay behind the existing optional dependency patterns
- SQLAlchemy and Alembic imports must not become requirements for base driver use

CI covers a bare install and lazy optional dependency behavior. Preserve the helpful import errors and installation guidance at optional feature boundaries.

### Cython, pure Python, and free-threaded paths must remain correct

`clickhouse_connect/driverc/` contains compiled fast paths. The pure Python implementations remain supported and are exercised separately.

When changing serialization, conversion, buffering, or native-format behavior:

- keep the compiled and pure Python paths behaviorally aligned
- do not assume the Cython path is available
- verify that both `CLICKHOUSE_CONNECT_USE_C=1` and `CLICKHOUSE_CONNECT_USE_C=0` still make sense
- preserve the Cython free-threading declarations
- treat new shared mutable state, caches, and check-then-act code as concurrency-sensitive

CI includes an experimental, non-blocking Python `3.14t` job with the GIL disabled. Free-threading support is not yet a reason to distort ordinary code, but changes must not silently re-enable the GIL or introduce unsafe shared state.

### Type and output stability are first-class concerns

This repo is sensitive to behavior changes in:

- numeric precision and width
- decimal and float behavior
- timezone handling for `DateTime`, `DateTime64`, `Date`, and `Date32`
- string vs bytes behavior
- low cardinality and enum handling
- Pandas, NumPy, Arrow, and Polars dtype selection
- null representation
- parameter binding and identifier quoting

If a change can affect any of those, treat it as significant even if the diff looks small.

### SQLAlchemy, Alembic, and DB-API compatibility are maintained intentionally

The SQLAlchemy dialect supports Core queries, ClickHouse-specific query clauses, DDL, schema reflection, basic ORM reads and inserts, and Alembic migrations. It is also the supported ClickHouse connection for Superset. SQLAlchemy `1.4.40` through `2.x` is supported, although a small number of features require SQLAlchemy 2.x. SQLAlchemy 1.4 compatibility remains required because Superset still depends on it.

Alembic support includes autogeneration and ClickHouse-specific migration operations. Review generated DDL, reflection, renderers, and SQLAlchemy 1.4 and 2.x behavior together when a change crosses those areas.

DB-API is intentionally thin. Preserve PEP 249 behavior and avoid leaking driver-specific internals through the DB-API layer unless the existing design explicitly exposes them.

## Compatibility Matrix

The current CI workflow is the source of truth for the exact matrix. Important axes include:

- Python `3.10` through `3.14`
- experimental free-threaded Python `3.14t`
- supported ClickHouse LTS and stable server versions
- compiled and pure Python execution paths
- bare install behavior
- installed-package public typing checks
- Pandas `3.x` compatibility
- SQLAlchemy `1.4` compatibility
- cloud integration coverage when secrets are present

When evaluating change risk, inspect `.github/workflows/on_push.yml` and think across the relevant matrix instead of relying on one local environment.

## Performance-Sensitive Areas

Be especially careful in:

- `clickhouse_connect/driver/transform.py`
- `clickhouse_connect/driver/dataconv.py`
- `clickhouse_connect/driver/npconv.py`
- `clickhouse_connect/driver/buffer.py`
- `clickhouse_connect/driver/streaming.py`
- `clickhouse_connect/driver/bytesource.py`
- HTTP and chDB request and response paths under `clickhouse_connect/driver/_backend/`
- datatype column read and write paths
- all files under `clickhouse_connect/driverc/`

Prefer predictable, low-allocation changes. Avoid per-row overhead, unnecessary conversions, and exception-driven control flow in hot paths unless there is a strong reason.

Do not accept a performance change that quietly alters formatting, dtype behavior, timezone handling, null semantics, or result structures unless that behavior change is explicitly intended.

There is no `tests/performance/` directory. Local performance scripts live under `examples/`, and PR comparison runs through `.github/workflows/stresshouse-benchmark-compare.yml`. Use the appropriate existing harness and record the environment with any claimed result.

## Testing Layout

Tests live in:

- `tests/unit_tests/` for logic that does not need an external ClickHouse server
- `tests/integration_tests/` for client-level and wire-level behavior
- `tests/type_check/` for installed-consumer and public typing checks

For behavior shared by the HTTP clients, use integration tests that run in both sync and async modes with the fixtures in `tests/integration_tests/conftest.py`:

- `client_mode`
- `call`
- `consume_stream`
- `client_factory`
- `param_client`

Use the existing patterns instead of inventing a one-off fixture arrangement. A sync-only integration test for shared HTTP client behavior needs a concrete reason.

Backend changes also need focused unit coverage:

- `tests/unit_tests/test_backend_orchestration.py` for shared operation sequences
- `tests/unit_tests/test_driver/test_backend_http.py` for common HTTP backend behavior
- `tests/unit_tests/test_driver/test_httpclient.py` for synchronous HTTP compatibility behavior
- `tests/unit_tests/test_driver/test_chdb.py` for the optional chDB backend

The chDB tests skip when the optional package is absent. Install the `chdb` extra when a change must exercise that backend.

## Ad Hoc Validation Expectations

For changes that touch the wire, query execution, insert behavior, streaming, compression, session handling, error handling, or timezone behavior, do not rely only on static reasoning.

At minimum:

- run targeted pytest coverage
- validate the changed path against a real local ClickHouse instance when practical
- exercise both sync and async behavior when the HTTP behavior is shared
- exercise each affected backend when behavior crosses a backend boundary
- run `ruff` and `mypy` as required by `AGENTS.md`

## How To Use This Doc

Use this file to understand what is structurally important in the repo before changing code.

Use `.agents/review.md` when the task is specifically code review, review feedback, or patch analysis.
