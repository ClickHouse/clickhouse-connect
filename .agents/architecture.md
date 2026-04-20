# AI Architecture And Repo Context

This document provides repo-specific context for AI agents working in `clickhouse-connect`.

`AGENTS.md` is the operational source of truth. This file is required reading before substantial code changes, but it does not override `AGENTS.md`.

## Repository Overview

`clickhouse-connect` is the official Python driver for ClickHouse over the HTTP interface. It supports the core client plus downstream integrations including Pandas, NumPy, PyArrow, Polars, SQLAlchemy Core, Superset, and DB-API.

Top-level areas that matter most:

```text
clickhouse_connect/
  driver/            Core client behavior, HTTP plumbing, query and insert contexts, streaming
  driverc/           Cython hot-path extensions
  datatypes/         ClickHouse type implementations and serialization logic
  cc_sqlalchemy/     SQLAlchemy dialect
  dbapi/             PEP 249 wrapper layer
  tools/             User-facing helpers
tests/
  unit_tests/        Fast tests, no server required
  integration_tests/ Client-level and wire-level behavior
  performance/       Benchmarks and regression measurements
```

## Core Invariants

### Two clients, one contract

The sync client in `clickhouse_connect/driver/httpclient.py` and the async client in `clickhouse_connect/driver/asyncclient.py` are parallel implementations of the same client contract.

Any change to shared client behavior usually needs matching consideration in both paths, including:

- request construction
- headers and query parameters
- settings handling
- query and insert behavior
- retries, timeouts, compression, and session handling
- error mapping
- resource cleanup and lifecycle behavior

If a change truly only applies to one path, that should be explicit, not assumed.

### Public API stability matters

Treat the following as public surface:

- `clickhouse_connect.get_client`, `get_async_client`, `create_client`, and related top-level entry points
- public names in `clickhouse_connect.driver.*`
- `Client` behavior and method signatures
- datatype read and write behavior
- DB-API behavior
- SQLAlchemy dialect behavior
- result shapes and dtype choices across Python, Pandas, NumPy, Arrow, and Polars

Be cautious with:

- return types
- defaults
- null handling
- ordering
- timezone behavior
- error types and user-visible error messages

Small internal refactors can still create breaking behavior in this repo.

### Optional dependencies and bare installs are deliberate

This repo must support a bare install that does not eagerly require every optional integration dependency.

Important examples:

- async support is optional and should not require `aiohttp` at import time for the base package
- Pandas, PyArrow, Polars, and NumPy integrations should stay behind the existing optional dependency patterns

The repo has CI coverage for bare install behavior and lazy optional dependency handling. Do not casually add import-time requirements that break that.

### Cython and pure Python paths must both remain correct

`clickhouse_connect/driverc/` contains compiled fast paths. The pure Python implementations remain important and are exercised separately.

When changing hot-path serialization, conversion, buffering, or native-format behavior:

- keep the compiled and pure Python paths behaviorally aligned
- do not assume the Cython path is always available
- verify that both `CLICKHOUSE_CONNECT_USE_C=1` and `CLICKHOUSE_CONNECT_USE_C=0` scenarios still make sense

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

### SQLAlchemy and DB-API compatibility are maintained intentionally

The SQLAlchemy dialect is aimed at SQLAlchemy Core and Superset use cases. The repo still carries SQLAlchemy `1.4` compatibility because Superset depends on it.

DB-API is intentionally thin. Preserve PEP 249 behavior and avoid leaking driver-specific internals through the DB-API layer unless the design already opts into that.

## Compatibility Matrix

The repo’s expectations are broader than a single local test run. CI currently covers important compatibility axes including:

- Python `3.10` through `3.14`
- multiple ClickHouse server versions
- compiled and non-compiled execution paths
- bare install behavior
- Pandas `3.x` compatibility
- SQLAlchemy `1.4` compatibility
- cloud integration coverage when secrets are present

When evaluating change risk, think in terms of that matrix, not just your local environment.

## Performance-Sensitive Areas

Be especially careful in:

- `clickhouse_connect/driver/transform.py`
- `clickhouse_connect/driver/dataconv.py`
- `clickhouse_connect/driver/npconv.py`
- `clickhouse_connect/driver/buffer.py`
- `clickhouse_connect/driver/streaming.py`
- `clickhouse_connect/driver/bytesource.py`
- datatype column read and write paths
- all files under `clickhouse_connect/driverc/`

Prefer predictable, low-allocation changes. Avoid per-row overhead, unnecessary conversions, and exception-driven control flow in hot paths unless there is a strong reason.

Do not accept a performance change that quietly alters formatting, dtype behavior, timezone handling, null semantics, or result shapes unless that behavior change is explicitly intended.

## Testing Layout

Tests live in:

- `tests/unit_tests/` for logic that does not need a server
- `tests/integration_tests/` for client-level and wire-level behavior
- `tests/performance/` for benchmarks and regression measurements

For behavior that exists in both clients, prefer integration tests that run in both sync and async modes using the shared fixtures in `tests/integration_tests/conftest.py`, especially:

- `client_mode`
- `call`
- `consume_stream`
- `client_factory`
- `param_client`

Use the existing patterns before inventing a new one-off fixture shape.

## Ad Hoc Validation Expectations

For changes that touch the wire, query execution, insert behavior, streaming, compression, session handling, error handling, or timezone behavior, do not rely only on static reasoning.

At minimum:

- run targeted pytest coverage
- validate the changed path against a real local ClickHouse instance when practical
- think through both sync and async behavior if the code path is shared

## How To Use This Doc

Use this file to understand what is structurally important in the repo before changing code.

Use `.agents/review.md` when the task is specifically code review, review feedback, or patch analysis.
