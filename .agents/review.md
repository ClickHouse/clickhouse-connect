# AI Review Guide

This document is for AI-assisted code review, patch review, and PR analysis in this repository.

Read `AGENTS.md` first. Read `.agents/architecture.md` when the review touches substantive code paths.

## Review Priorities

Prioritize findings in this order:

1. Correctness bugs
2. Regressions in observable behavior
3. Unapproved breaking changes in the `1.x` release line
4. Public API and typing contract violations
5. Sync, async, and backend parity gaps
6. Packaging, bare-install, and optional dependency regressions
7. Supported Python, ClickHouse, Pandas, SQLAlchemy, and integration compatibility
8. Performance regressions in hot paths
9. Missing or weak tests and documentation
10. Style and nits

## The 1.x Compatibility Rule

The project is in the `1.x` release line. Treat backward-incompatible changes to public APIs or observable behavior as not allowed unless a maintainer explicitly approves an exception.

Check for changes to:

- public signatures and accepted argument types
- defaults and return values
- exceptions and user-visible error messages
- serialized values, query binding, and wire behavior
- result structures, dtype choices, nulls, timezones, precision, and ordering
- SQLAlchemy, Alembic, DB-API, and optional integration behavior

Do not accept a break as incidental cleanup. An approved exception needs a clear compatibility decision, migration or deprecation plan, documentation, changelog treatment, and versioning decision.

## Repo-Specific Review Checklist

When reviewing a change, explicitly check whether it affects:

- ownership boundaries described in `.agents/architecture.md`
- sync and async HTTP behavior
- synchronous HTTP and chDB backend behavior
- public method signatures, defaults, return types, or annotations
- the installed-package typing contract and public type-completeness ratchet
- null handling, dtype behavior, timezone behavior, precision, or result structures
- Cython and pure Python parity
- free-threaded Python safety and Cython free-threading declarations
- bare installs or lazy optional dependency imports
- SQLAlchemy, Alembic, DB-API, Pandas, NumPy, PyArrow, Polars, or chDB compatibility
- Python and ClickHouse versions covered by the current CI matrix
- changelog and public documentation requirements

## Client And Backend Parity

Review client changes at the layer that owns the behavior:

- Shared client semantics should not be duplicated into one facade when they belong in `Client`, a context, binding, formats, or shared orchestration.
- A change to synchronous client semantics in `SyncBackendClient` needs corresponding async consideration in `AsyncClient`.
- HTTP transport changes usually need matching checks in `HttpSyncBackend` and `HttpAsyncBackend`.
- Synchronous backend changes must consider both `HttpSyncBackend` and `ChdbBackend`, subject to documented chDB limitations.
- Initialization and insert-context changes must preserve the sync and async sequences in `clickhouse_connect/driver/_backend/orchestration.py`.

For shared HTTP client behavior, confirm that integration tests exercise both sync and async paths with the fixtures in `tests/integration_tests/conftest.py`. A sync-only test needs a concrete transport-specific reason.

For backend work, inspect the focused coverage in:

- `tests/unit_tests/test_backend_orchestration.py`
- `tests/unit_tests/test_driver/test_backend_http.py`
- `tests/unit_tests/test_driver/test_httpclient.py`
- `tests/unit_tests/test_driver/test_chdb.py`

## Typing And Supported Python Versions

The package ships `py.typed`. Public annotations are observable API, not internal documentation.

- Confirm `mypy` passes from the repository root.
- Do not accept new modules in the `ignore_errors` baseline.
- Flag incorrect, missing, or weakened public annotations.
- Flag unjustified broad `Any` types or blanket `# type: ignore` comments.
- Check that installed-consumer typing tests and the public type-completeness ratchet still make sense.
- Preserve Python 3.10 syntax and standard library compatibility.
- Treat new shared mutable state, caches, and check-then-act code as free-threading risks.

Run the real checker before reporting a suspected type error. Do not raise type findings from memory alone.

## Tests And Evidence

- Review the complete diff and the relevant surrounding implementation and tests.
- A bug fix needs a regression test that fails without the fix and passes with it. Confirm both directions before calling the coverage sufficient.
- Check happy paths, failures, and relevant edge cases.
- For ClickHouse type formatting or conversion, apply the composed type matrix required by `AGENTS.md`.
- Do not accept changes to an existing correct assertion solely to make a new implementation pass.
- Run targeted pytest coverage when practical.
- Run `ruff` and `mypy` for substantive Python changes.
- Require measured before and after evidence for performance claims that cannot be established from the code.

When a finding depends on ClickHouse server behavior, follow `Server Behavior Is Authoritative` in `AGENTS.md`. Do not infer server behavior from this client alone. If the required server-source investigation cannot be completed, report the validation gap instead of guessing.

When a finding depends on a third-party API, inspect the installed package or its primary documentation before raising it. Do not invent a signature or rely on stale memory.

## What Good Review Feedback Looks Like

- Lead with findings, not a summary.
- Order findings by severity.
- Use `file:line` references.
- State the impact and who or what could break.
- Give a concrete fix or direction for each finding.
- Distinguish confirmed issues from inferred risks.
- Avoid praise, filler, and a tour of the diff.

If no material issues are found, say that explicitly and mention any residual testing, compatibility, or source-validation gaps.

## Preferred Review Output

Use this order:

1. Findings, ordered by severity
2. Open questions or assumptions
3. Brief change summary, only if useful

Each finding should answer:

- what is wrong
- why it matters
- who or what it could break
- what evidence in the diff or repository supports it
- how to fix it

Keep findings brief, factual, and actionable.

## Review Closing Checklist

Before saying a change looks good, confirm:

- no unapproved `1.x` compatibility break was introduced
- sync, async, and affected backend parity was considered
- public and user-visible behavior is intentional
- public annotations and the typing ratchet remain correct
- optional dependency and bare-install behavior still works
- the change holds up across the current CI matrix in `.github/workflows/on_push.yml`
- regression tests are targeted, meaningful, and proven against the unfixed behavior
- a `CHANGELOG.md` entry exists for every user-facing change
- `README.md` and `docs/*.mdx` remain synchronized with public behavior
- the required docs sync review was run before PR submission when applicable
- all important validation that was not run is called out explicitly
