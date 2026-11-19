# Instructions for agents: upstream SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-connect.
See [`README.md`](README.md) for how it works and how to run it. The rules of
the root [`AGENTS.md`](../../AGENTS.md) apply here too (ruff, writing style).

## The goal

The goal is to find bugs in clickhouse-connect. To pass more tests is not the
goal. A failed test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A
   test fails for one of three causes:
   - **The client** (clickhouse-connect: the HTTP transport, the query and
     command paths, the `Native` decoding in Python and Cython, and the type
     mapping). This is what the harness is for.
   - **The shim** (the program in this directory). It does not do what
     `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this
     server (see [Environment causes](#environment-causes)).
2. **The default for a client bug is a fix in the client**, in a pull request
   of its own, with a regression test (see the root `AGENTS.md`). Do not fix a
   client bug in a pull request that changes the harness.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy
   `clickhouse-client` more exactly. Do not change the shim to hide a client
   bug, for example by a different read format, a workaround for a wrong value,
   or a change of the statement text. The shim must use the client as a user
   does: `get_client`, `query`, `command`, and the default read formats
   (except the `typed` format for `Variant` and `Geometry`, see the README).
4. **Remove a test from the allowlist only when it cannot pass for an
   environment cause.** Write the reason in the commit message.
5. **A test that fails because of a client bug or a shim gap stays
   visible.** Do not delete it. Put it in [`allowlist.txt`](allowlist.txt)
   as a known failure, `# <test>  # [<id>]`, in the group of its cause. Each
   group has a header that describes the cause. The header of a client bug
   also gives a small reproduction and an `Issue:` line. Make a new group
   when no group fits. See the README for
   the format and for how to run one group (`KNOWN_FAILURES=<id>`).
6. **Do not list a test that cannot pass for an environment cause.**

## How to find the cause

Do these steps in this order.

1. **Run the test through the harness:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) CLICKHOUSE_CLIENT_CLI_LOG=/tmp/cli.log \
     tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   ```

2. **Run the test through the real `clickhouse-client`** in the server
   container, on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here,
   the cause is the client or the shim. Note that the real client uses the
   native protocol: a test can fail over HTTP for a cause of the HTTP
   interface itself (see below).
3. **Find the statement that fails.** The log of `CLICKHOUSE_CLIENT_CLI_LOG`
   shows each statement, the error, the Python traceback and the log records
   of clickhouse-connect. Read the diff in the output of the runner, and the
   `.stdout` and `.stderr` files next to the test.
4. **Compare the shim with the server on that one statement.** Run it through
   the shim (`tests/clickhouse-test-runner/bin/clickhouse client -q "<SQL>"`)
   and through `curl` with `FORMAT TSV`. Then make the statement smaller
   until it is as small as possible.
5. **Decide between the client and the shim with a small Python program**
   that uses only the public clickhouse-connect API (`get_client`, `query`,
   `command`, `result_rows`, `column_types`). If the program also gives a
   wrong value or an error, the bug is in the client. If the program gives the
   correct value, the bug is in the shim. Try also `CLICKHOUSE_CONNECT_USE_C=0`:
   a difference between the Cython and the pure Python paths is a client bug.

### Environment causes

Find the cause on the server of CI: the stock image of the pinned version with
`CLICKHOUSE_SKIP_USER_SETUP=1` and `--hostname` (step 2 of "Run the tests
locally" in the README). A server with other users or another
configuration gives other failures. For example, a default user without
named collection control gives `ACCESS_DENIED` in tests that pass in CI.

A test cannot pass through any client on the CI server when it needs one of
these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL
  sources.
- Server configuration of the upstream CI: named collections (such as
  `s3_conn`), dictionaries, storage policies, disks, users and grants,
  macros (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`,
  `UNKNOWN_POLICY`, `UNKNOWN_DISK`). The default user of the stock image
  also cannot see the secrets of named collections.
- Features that the server build or platform does not have
  (`SUPPORT_IS_DISABLED`).
- Stateful datasets (`--no-stateful`) or long tests (`--no-long`).
- Behavior of the HTTP interface itself. For example, the server sends no
  `WITH TOTALS` row and no `extremes` in the `Native` format over HTTP, and an
  HTTP request fails when the current database of the session was dropped.
- Features of `clickhouse-client` itself that are not part of a driver:
  `INTO OUTFILE`, `FROM INFILE`, client-side parsing of `INSERT` data, the
  progress and profile-event output, and client-only settings.
- The tz database. ClickHouse and Python can give different historical
  offsets for the same zone.
- Tests that conflict with other tests of the set: a fixed table UUID, or
  server state that a test leaves behind (failpoints that a failed test did
  not disable, row policies). Such a test passes alone on a fresh server and
  fails in a full run. A failure message with "Injected failure" comes from a
  failpoint of another test.
- Memory limits of the server, when the test also fails through the real
  `clickhouse-client`.
- Load from parallel tests. Some `EXPLAIN PIPELINE` and `EXPLAIN ANALYZE`
  outputs and stream counts change when many tests run at the same time.
  Such a test fails in a full run at a high `-j`, and passes alone through
  the program and through the real `clickhouse-client`.
- The host name of the server. `tests/clickhouse-test` replaces the host
  name of its own machine with `localhost` in the output, and some
  `.reference` files have `localhost` for `hostName()`. So the server must
  have the host name of the machine that runs the tests (`--hostname` in
  the README and in CI).
- A test that fails now and then also through the real `clickhouse-client`,
  when it runs alone (a race in the server or in the test). Run it ten times
  or more alone, through the program and through the real client, before
  you decide this.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value.
- A decode error, a Python exception from inside clickhouse-connect, or a
  `ProgrammingError` for a statement that the server accepts.
- A query text that the client changed (the client strips the `;` and adds
  `FORMAT Native`), or a setting that the client sends with each request, when
  a test sees it.
- A difference between `CLICKHOUSE_CONNECT_USE_C=1` and `0`.

## Conditions for the allowlist

- Add a test only when it passes.
- Run new tests three times, also with a higher `-j` than CI (CI uses
  `-j 4`), on a **fresh** server (`docker rm -fv clickhouse-upstream-tests`,
  then the `docker run` of the README). Tests change the state of the server
  (row policies, settings profiles, users). A test that passes only after
  other tests ran must not go in.
- Do not run two sweeps at the same time on one server. The memory pressure
  causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
- CI does not run failed tests again (`RETRY_FAILED`), because no known
  client bug makes random tests fail. Do not use `RETRY_FAILED` when you grow
  or check the allowlist. If a client bug makes random tests fail, document
  it in its group before CI uses the retry.
- When a test in the allowlist starts to fail sometimes, find the cause
  first (rule 1). Remove it only when the cause is in the environment or in
  the test itself, and write the reason.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). A new ClickHouse release changes
nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own,
and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` or `v<version>-lts` tag upstream, normally the
   version of the `latest` image.
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A
   release changes the tests and the server output, so many tests can fail
   at the same time with no change in the client. To see whether the client
   is the cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures (`KNOWN_FAILURES=all`). Enable the tests that pass
   now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. You can also try the new tests of the release (see "Grow the allowlist" in
   the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and commit
   `clickhouse_test_runner/errorcodes.py` with it.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output
formats, settings, read formats), update [`README.md`](README.md), this file
and the workflow in the same change. Run `ruff format` and `ruff check` with
`ruff==0.15.8`, the version of `on_push.yml`. That workflow lints `tests`,
so it lints this directory too. It also runs `pytest tests`, which collects
the unit tests of this directory, so they must pass on each Python version
of that workflow.
