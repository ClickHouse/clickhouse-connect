# AI Review Guide

This document is for AI-assisted code review, patch review, and PR analysis in this repository.

Read `AGENTS.md` first. If the review touches substantive code paths, read `.agents/architecture.md` before reviewing.

## Review Priorities

Prioritize findings in this order:

1. Correctness bugs
2. Regressions in observable behavior
3. Public API and compatibility risk
4. Sync and async parity gaps
5. Packaging and optional dependency regressions
6. Performance regressions in hot paths
7. Missing or weak tests
8. Style and nits

## Repo-Specific Review Checklist

When reviewing a change, explicitly check whether it affects:

- sync and async parity
- public method signatures, defaults, or return types
- null handling, dtype behavior, timezone behavior, or result shapes
- Cython and pure Python parity
- bare install or optional dependency import behavior
- SQLAlchemy or DB-API compatibility
- compatibility expectations for Python, ClickHouse, Pandas, or SQLAlchemy versions covered in CI

For client-level behavior changes, confirm whether the tests exercise both sync and async paths using the shared integration fixtures.

## What Good Review Feedback Looks Like

- Lead with findings, not summary.
- Order findings by severity.
- Use `file:line` references.
- Be explicit about impact.
- Call out what could break for real users.
- Distinguish confirmed issues from inferred risk.

If no material issues are found, say that explicitly and mention any residual testing or compatibility gaps.

## Preferred Review Output

Use a structure like this:

1. Findings, ordered by severity
2. Open questions or assumptions
3. Brief change summary, only if useful

Each finding should answer:

- what is wrong
- why it matters
- who or what it could break
- what evidence in the diff or repo context supports the concern

These points should be brief but factual and accurate.

## Review Closing Checklist

Before saying a change looks good, make sure you understand:

- whether sync and async parity was considered
- whether public or user-visible behavior changed
- whether optional dependency and packaging behavior still make sense
- whether the change holds up across the compatibility axes in `.agents/architecture.md` (Python versions, Cython on and off, bare install, SQLAlchemy 1.4, ClickHouse server versions)
- whether tests are targeted and meaningful
- whether any important validation still has not been run
