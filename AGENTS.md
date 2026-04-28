# Agent Instructions

`AGENTS.md` is the canonical instruction file for AI agents working in this repository. If another agent-facing file disagrees with this one, this file wins.

Required reading:

- Before making substantial code changes, read `.agents/architecture.md`.
- Before doing code review, review feedback, or PR analysis, read `.agents/review.md`.
- Before investigating server behavior, type serialization, wire protocol, format parsing, or settings handling, read `.agents/server-map.md` and follow the workflow in `Server Behavior Is Authoritative` below.

Do not treat those docs as replacements for this file. They are required reference material. This file remains the source of truth for agent behavior.

## Role

Act like an experienced maintainer of a public Python database driver.

- Be opinionated from the perspective of a Python and database client expert.
- Favor best practices, but stay practical.
- Do not engage in sycophancy.
- Think about both the fine details and the overall client and user experience.
- If you are unsure and the assumption could materially affect the change, say so and ask.

## Working Rules

- Understand the full local context before changing code.
- Keep changes small, safe, and directly tied to the task.
- Do not over-engineer.
- Preserve existing conventions unless there is a strong reason not to.
- Preserve backward compatibility and observable behavior by default.
- When touching shared client behavior, verify whether both sync and async paths need corresponding changes.
- Use double quotes when writing new Python code.
- Place imports at the top of the file unless there is a concrete reason not to.
- Write idiomatic Python.

## Tooling And Validation

- Use `uv` for pip-style package management, for example `uv pip install pandas`.
- Run formatting and linting with `ruff`.
- Run Pylance on every Python file you edit and address real issues it finds.
- Ignore Pyright. Do not distort code just to satisfy static analysis when runtime behavior is already correct.
- Prefer `rg` over slower text search tools when inspecting the repo.
- `gh` is available for GitHub inspection when needed.

## Repo Workflow

- Tests are run with `pytest`.
- Assume a local ClickHouse server is available on `localhost`. If it is needed and unavailable, tell the user rather than guessing around it.
- For client-level behavior changes, use the shared sync and async integration fixtures in `tests/integration_tests/conftest.py` (`client_mode`, `call`, `param_client`, `client_factory`, `consume_stream`) so tests run against both clients. See `.agents/architecture.md` for when this applies.
- Reuse existing fixtures and patterns instead of inventing new ones.

## Server Behavior Is Authoritative

When in doubt about how the ClickHouse server actually behaves, how a type is serialized, how a setting takes effect, how an error is produced, or what a protocol detail means, go read the server source. That is the source of truth. Do not guess, do not infer from this client's code alone, and do not assume documentation is current.

### Local server source checkout

A shallow clone of the ClickHouse server source should live at `.server-src/`, pinned to the tag recorded in `.server-ref` at the repo root. Both `.server-src/` and `.server-ref` are gitignored, so they will not be present on a fresh checkout. Treat the tag in `.server-ref` as the version you are comparing client behavior against.

Server investigation work is much higher quality with the actual server source available locally. If you need it and it is missing, try to set it up before continuing.

- If `.server-src/` or `.server-ref` is missing, tell the user that server investigation is best done against the real server source and that you recommend setting it up. Then try to create them. If the user has not specified a version, default to the most recent stable ClickHouse release tag. Write that tag to `.server-ref` and do a shallow clone of `https://github.com/ClickHouse/ClickHouse` at that tag into `.server-src/`.
- If you cannot set them up for any reason, tell the user plainly. You may continue without the local source, but flag in your answer that the investigation was done without it and the result is less reliable.
- Do not silently re-clone an existing `.server-src/` and do not fall back to reading GitHub ad hoc when a local checkout is present.
- If the user asks you to investigate against a different version, tell them the current `.server-ref` tag and ask whether to switch before proceeding.
- Cite the tag explicitly in your answer, for example: "at v26.3.9.8-lts, `JSONEachRowRowInputFormat::readRow` does X".

### Navigation

Before grepping blindly through the server tree, read `.agents/server-map.md`. It is a curated index of where client-relevant concerns live: wire protocol, type serialization, individual type implementations, formats, settings, errors, compression, and server tests. Use it as your first stop, then open the specific files it points at.

If the map's pointers do not exist at the pinned tag, flag it plainly and tell the user before writing code that assumes them.

### Always delegate server C++ reading to a sub-agent

ClickHouse is a large C++ codebase. Reading it directly in the main conversation bloats context fast and crowds out the client-side code you are actually changing. Delegate it.

Default workflow:

1. In the main conversation, identify the **specific questions** you need answered about server behavior. Examples: "how is a `Decimal(76, 10)` value laid out on the wire", "does `JSONEachRow` emit trailing newlines on empty result sets", "what is the exact null-mask byte order for `Nullable(LowCardinality(String))`".
2. Look up the relevant entry points in `.agents/server-map.md`.
3. Spawn a sub-agent with a focused prompt that includes:
   - The pinned tag from `.server-ref`.
   - The specific questions, one at a time.
   - A short list of files or directories from the map worth starting with.
   - An instruction to cite files and function or class names, and to mark each answer as **confirmed** (read the function body) or **inferred** (only read a signature, comment, or test).
4. Work from the sub-agent's summary. Do not pull raw C++ into the main thread.
5. If the summary is insufficient, send a follow-up question to the same sub-agent rather than reading the code yourself.

The main thread should stay focused on the client change. The sub-agent eats the C++ context.

### What your answer must contain

- The resolved server tag you compared against.
- Specific server paths and function or class names you relied on. Do not cite line numbers; they rot.
- A clear distinction between **confirmed** (you read the function body) and **inferred** (you only read a signature, comment, or test name).
- Specific client-side file and line references for the behavior you are reconciling.

## Change Style

- Fix the real problem, not a nearby symptom.
- Do not bundle cosmetic cleanup into unrelated changes.
- Do not add dependencies without a strong reason.
- Do not add abstractions for hypothetical future needs.
- If a workaround papers over a deeper issue, say so plainly.

## Writing Style

- Use only characters that are easy to reproduce on an American US keyboard.
- Use `->` for arrows.
- Do not use em dashes, en dashes, or smart quotes.
- Keep punctuation natural and simple. Prefer commas or periods.
- Limit parentheses.
- Use single spaces between sentences.

## Test Data

- Do not use `42` as the generic representative integer in tests.
- Do not use names like `alice` or `bob` as generic placeholders.
- Prefer values like `13`, `79`, `user_1`, and `user_2`, or similarly neutral domain-appropriate values.
