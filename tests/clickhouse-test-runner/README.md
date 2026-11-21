# clickhouse-test-runner

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through clickhouse-connect.

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with
the `.reference` file of the test. This harness is a small Python program that
imitates `clickhouse-client`. The runner can drive it without changes: it has
the same flags, the same stdin and `--query` behavior, and the
`extract-from-config` shortcut.

The program sends every statement through the public clickhouse-connect API,
over HTTP. It uses `client.query()` for the statements that return rows, and
`client.command()` for the other statements. clickhouse-connect reads each
result as `Native` and decodes it into Python values, with the Cython
extensions as a normal install does. The program writes these values as
`TabSeparated` text itself, from the column types of the result in
`QueryResult.column_types`. Thus a test passes only when clickhouse-connect
decodes every value correctly.

It is a port of the
[Go](https://github.com/ClickHouse/clickhouse-go/tree/main/tests/clickhouse-test-runner),
[JavaScript](https://github.com/ClickHouse/clickhouse-js/tree/main/tests/clickhouse-test-runner)
and [Java](https://github.com/ClickHouse/clickhouse-java/tree/main/tests/clickhouse-client)
harnesses.

## Run the tests locally

You need Python 3.10 or later, a C compiler for the Cython extensions, git,
curl and Docker. Run the commands from the repository root.

1. Install clickhouse-connect from this checkout and build its Cython
   extensions in place, as CI does:

   ```bash
   pip install "cython==3.2.5" "setuptools>=77.0.3" jinja2 pytest pytest-xdist
   CLICKHOUSE_CONNECT_SKIP_CYTHON=1 pip install -e ".[tzdata]"
   CLICKHOUSE_CONNECT_REQUIRE_C=1 python setup.py build_ext --inplace
   ```

   Without `CLICKHOUSE_CONNECT_REQUIRE_C=1`, a failed compile continues
   with pure Python, and the run script then stops. The `tzdata` extra
   gives Python `zoneinfo` all time zone names. Some tests use old names
   (`US/Samoa`, `Cuba`, `Asia/Calcutta`) that a system zoneinfo can lack
   (on Ubuntu, they are in the `tzdata-legacy` package). Without them,
   clickhouse-connect fails a column with such a time zone, and it reads a
   value in such a `session_timezone` in the server time zone.

2. Start the pinned ClickHouse server. The version is in
   [`clickhouse-version`](clickhouse-version):

   ```bash
   docker run -d --name clickhouse-upstream-tests --hostname "$(hostname)" \
     -p 8123:8123 -p 9000:9000 \
     -e CLICKHOUSE_SKIP_USER_SETUP=1 --ulimit nofile=262144:262144 \
     "clickhouse/clickhouse-server:$(tests/clickhouse-test-runner/scripts/pinned-version.sh)"
   ```

   This is the server of CI. `CLICKHOUSE_SKIP_USER_SETUP=1` keeps the
   default user of the image, which accepts connections from other hosts and
   has the access management and named collection control that some tests
   need. `--hostname` gives the server the host name of your machine:
   `tests/clickhouse-test` replaces its own host name with `localhost` in
   the output, and some `.reference` files have `localhost` for
   `hostName()`. Use this server, not the server of the repository's
   `docker-compose.yml`, which has another version and its own
   `config.xml` and `users.xml`.

3. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-test-runner/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example
   `v26.10.4.207-stable`) into `tests/clickhouse-test-runner/.upstream/ClickHouse`,
   sparse and shallow. The `.reference` files of that tag were made by the
   same server build. The script also generates
   [`clickhouse_test_runner/errorcodes.py`](clickhouse_test_runner/errorcodes.py)
   from the same checkout. To try another version, give it as the argument
   (`fetch-upstream.sh 26.11.1.10`), and start the server of that version.
   An LTS release has only a `-lts` tag (`v26.8.9.10-lts`): when the
   `-stable` tag does not exist, the script uses the `-lts` tag.
   Another argument is a ref, for example `master`. Then the tests are not
   of a release, and the run needs `ALLOW_VERSION_MISMATCH=1`.

   The run script stops when the server and the tests are of different
   versions, because then the `.reference` files do not match. Set
   `ALLOW_VERSION_MISMATCH=1` to run anyway.

4. Run the allowlisted tests:

   ```bash
   tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   ```

The script puts [`bin`](bin) first in `PATH` for the runner only, and starts
`tests/clickhouse-test`. `bin/clickhouse` (and its `clickhouse-client`
symlink) starts the Python program, with this checkout first in `PYTHONPATH`.
The script gives the runner
`--no-stateful --no-random-settings --no-random-merge-tree-settings`; other
arguments go to the runner after these. For example, to run one test that is
not in the allowlist:

```bash
UPSTREAM_TEST_LIST=<(echo 01277_toUnixTimestamp64) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

The runner prints the diff of each failed test. It keeps the output of a
failed test next to the test, in
`tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless/`:
`<test>.stdout` and `<test>.stderr`, with the process ID before the
extension for a test that ran in parallel.

The unit tests of the harness need no server. The live tests compare the
output of the program with the server's own `TabSeparated` output for many
expressions, and need the server of step 2:

```bash
CLICKHOUSE_TEST_RUNNER_LIVE=1 pytest tests/clickhouse-test-runner
```

Without `CLICKHOUSE_TEST_RUNNER_LIVE=1`, the live tests skip. `pytest tests`
from the repository root (the tests of `on_push.yml`) also collects the unit
tests of the harness. They take about one second and need only
clickhouse-connect, so a change in the client that breaks the program shows
in the pull request of that change. The package build does not include this
directory.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `UPSTREAM_CLICKHOUSE_DIR` | `tests/clickhouse-test-runner/.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_TEST_LIST` | `tests/clickhouse-test-runner/allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `KNOWN_FAILURES` | (none) | Run the known failures of these groups instead (comma-separated IDs, or `all`). |
| `CLICKHOUSE_TEST_RUNNER_PYTHON` | `python3` | The Python interpreter of the program and of the runner. |
| `CLICKHOUSE_CONNECT_USE_C` | `1` | `0` tests the pure Python decoding of clickhouse-connect instead of the Cython extensions. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | A file. Each invocation of the program adds its arguments (the `--password` value hidden), statements, errors and the log records of clickhouse-connect to it. |
| `CLICKHOUSE_HOST` | `localhost` | The server host. The runner gives it to the program as `--host`. |
| `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `9000`, `8123` | The ports of the server. The runner gives `--port=$CLICKHOUSE_PORT_TCP` (the native port); the program uses `CLICKHOUSE_PORT_HTTP` for it. |
| `CLICKHOUSE_PORT_TCP_SECURE`, `CLICKHOUSE_PORT_HTTPS` | `9440`, `8443` | The same mapping for `--secure`. |
| `ALLOW_VERSION_MISMATCH` | `0` | `1` runs the tests also when the server and the upstream tests are of different versions. |
| `CLICKHOUSE_TEST_RUNNER_LIVE` | (none) | `1` runs the live tests of [`test_live.py`](test_live.py) against the server at `CLICKHOUSE_HOST` and `CLICKHOUSE_PORT_HTTP`. |
| `UPSTREAM_REPO_URL` | the ClickHouse repository on GitHub | The repository that `fetch-upstream.sh` fetches the tests from. |
| `CLICKHOUSE_CONTAINER` | `clickhouse-upstream-tests` | `run-with-real-client.sh` only: the server container whose `clickhouse-client` runs the tests. |

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test
name per line. The name is the file name without `.sql` or `.sql.j2`. `#` starts a comment. The script
gives the runner anchored regular expressions, so each name selects exactly
one test. (The runner itself matches each name as a substring, so
`00396_uuid` would also select `00396_uuid_v7`.)

The file has two kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs. |
| `# 01277_toUnixTimestamp64  # [dt64-sub-microsecond]` | A **known failure**: the test does not run, because of a bug in clickhouse-connect or a gap in the shim. The ID in brackets names the group. The header of the group, lower in the file, describes the cause. |

The known failures are at the end of the file, in groups. Each group starts
with a header:

```text
#-----------------------------------------------------------------------------
# [<id>] <client|shim|?>: <one-line description>
#   <the cause>
#   Reproduction:
#     <a small program that uses only the public clickhouse-connect API>
#   Issue: <the issue, or "not filed yet">
#   Tests: <the number of tests in the group>
# <test>  # [<id>]
```

`client` is a bug in clickhouse-connect, and only these groups have the
`Reproduction:` and `Issue:` lines. `shim` is a gap in this program. `?` is
the `[untriaged]` group: tests that pass through the real `clickhouse-client`
and fail here for a cause that is not known yet. An ID has only lowercase
letters, digits and `-`.

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=dt64-sub-microsecond tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

In the client fix pull request, move its live pin to a passing test or remove
it, and enable the tests of its group. Keep harness logic changes in a
separate pull request. When the tests pass, remove the `# ` at the start and
the `# [<id>]` at the end of their lines. When all tests of a group pass,
remove the group header. Then update the counts: `Running:` and
`Known failures:` at the top of the file, and `Tests:` of each group that
changed.

The live tests have a case for each `client` group: `CLIENT_BUGS`,
`CLIENT_BUG_SCRIPTS` and the separate known-client-bug tests in
[`test_live.py`](test_live.py). A case that starts to pass fails its test, so
that the group is checked again. When the client is fixed, move the case of
`CLIENT_BUGS` to `EXPRESSIONS`, and change the other cases into tests of the
correct result.

### Grow the allowlist

1. Run the candidate tests through the harness. To try all `.sql` tests:

   ```bash
   ls tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   UPSTREAM_TEST_LIST=/tmp/candidates.txt \
     tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8 --no-long --timeout 120 \
       --max-failures-chain 1000000 | tee /tmp/run.log
   ```

   Do not run two sweeps at the same time on one server: the memory pressure
   on the server causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
   `--max-failures-chain` turns off a limit of the runner: by default it stops
   after 20 failures in a row, which a list of failing tests reaches at once.

2. Keep only the tests that report `[ OK ]`:

   ```bash
   sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run.log | sort -u
   ```

3. Run the new tests again, two or more times, with a higher `-j` than CI
   (CI uses `-j 4`). Find the cause of each test that does not pass every
   time before you leave it out.

4. Make sure that the tests also pass on a fresh server, because CI starts
   from one: `docker rm -fv clickhouse-upstream-tests`, then the
   `docker run` of step 2. For example, a test that reads `system.query_log`
   can depend on the queries of other tests.

5. Add the new names to `allowlist.txt` in sorted order, above the known
   failures, and update the `Running:` count at the top.

**Rule:** add only tests that pass reliably. Before you remove a test or
decide not to add it, find out whether it fails because of a bug in
clickhouse-connect. The harness exists to find those bugs. See
[`AGENTS.md`](AGENTS.md) for the steps.

To compare with the real `clickhouse-client`, run the same tests through it,
in the server container:

```bash
UPSTREAM_TEST_LIST=<(echo 01277_toUnixTimestamp64) \
  tests/clickhouse-test-runner/scripts/run-with-real-client.sh
```

A test that passes there and fails through clickhouse-connect shows a bug in
clickhouse-connect or in this program. `run-with-real-client.sh` reads the
running tests of the list only. It does not read `KNOWN_FAILURES`
or `SHARD_*`, and it does not compare the versions.

## How the program imitates clickhouse-client

The program sends nothing that `clickhouse-client` does not send, because
some tests read their own queries from `system.query_log`. What
clickhouse-connect itself adds or changes (see "Known failures") stays
visible.

- **Command line.** `tests/clickhouse-test` starts the program as
  `clickhouse client ...` and `clickhouse-client ...`, with the same flags as
  for the real client: `--host`, `--port`, `--user`, `--password`,
  `--database`, `--secure`, `--query` (`-q`), `--param_<name>=<value>` and
  `--<setting>=<value>`. With `--query`, the program does not read stdin.
  Without it, the program reads the queries from stdin. `--no-secure` cancels
  `--secure`. `--multiquery` (`-n`, `--multi-query`), `--multiline` (`-m`),
  `--config-file` (`-C`), `--client_logs_file`, `--allow_repeated_settings`,
  `--fake-drop` and unknown short options have no effect. Each other long
  option is a setting (see "Settings" below). Its value can also be the next
  argument. `--help` prints the options. `clickhouse extract-from-config
  --key listen_host` prints `127.0.0.1`, and other keys print nothing.
- **Port.** `--port` is the native TCP port (`CLICKHOUSE_PORT_TCP`), because
  the runner gives that port to `clickhouse-client`. clickhouse-connect
  speaks HTTP only, so the program connects to `CLICKHOUSE_PORT_HTTP`
  instead. Any other `--port` value is used as given.
- **Statements.** The program splits the input into statements as
  `clickhouse-client` does: at a top-level `;`, with the same-line trailing
  comment kept with its statement. It gives each statement to the client with
  its `;`. The inline data of `INSERT ... FORMAT <name>` (other than `Values`)
  ends at an empty line; the program gives the statement and its data to
  `client.command()` as one text, as a user does. The `-- Tags:` header is not
  part of a statement. Every statement runs, with or without `--multiquery`.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and
  `-- { error X }` make the error the expected result. `X` is a list of codes
  or names, and an unknown name stops the program. An error hint in a
  leading comment is ignored, as upstream. Through clickhouse-connect the
  server finds all errors, so a server error can satisfy a `clientError` hint.
  `-- { echo }`, `-- { echoOn }` and `-- { echoOff }` count in any comment.
  They print the statements, but not a statement with a syntax error:
  `clickhouse-client` parses a statement before it echoes it. A
  `SYNTAX_ERROR` that the server finds when it analyzes the parsed query
  (the message has `In scope`) does not stop the echo. The first error that
  a hint does not expect stops the program, as in `clickhouse-client`.
  `-- { retry ... }` is not supported.
- **Sessions.** For each invocation, the program makes one client with
  `clickhouse_connect.get_client(..., session_id=<a new random ID>)`. Thus
  `SET`, `USE` and temporary tables last for the whole invocation.
  clickhouse-connect sends the database of the client with each
  request, and that database replaces the current database of the session.
  After a `USE`, the program sets `client.database` to the new database.
- **Statement types.** Statements that start with `SELECT`, `WITH`, `FROM`,
  `SHOW`, `DESC`, `DESCRIBE`, `EXISTS`, `EXPLAIN` or `CHECK` run with
  `client.query()`. All other statements run with `client.command()`. After
  `SET dialect` to another language, such as KQL or PRQL, all statements
  except `SET` run with `client.query()`. After `SET implicit_select = 1`, all
  statements run with `client.query()`, except the statements that start with
  `INSERT`, `CREATE`, `DROP`, `ALTER`, `SET`, `USE`, `TRUNCATE`, `RENAME`,
  `OPTIMIZE`, `SYSTEM`, `GRANT`, `REVOKE`, `ATTACH`, `DETACH`, `KILL`,
  `EXCHANGE`, `UNDROP`, `BACKUP` or `RESTORE`.
- **Values.** The program reads `QueryResult.result_rows` and writes each
  value from the type name of its column (`QueryResult.column_types[i].name`).
  It uses the default read formats of clickhouse-connect, except for
  `Variant` and `Geometry`. For these, it uses the documented `typed` read
  format: each value comes with the name of its member type. The program
  needs that name to write the value as the server does. A `Dynamic` value comes without its
  type, so the program writes it from its Python type.
- **Rendering.** [`render.py`](clickhouse_test_runner/render.py) writes each
  value as the server writes it in `TabSeparated`. At the top level,
  strings are escaped (`\\`, `\'`, `\t`, `\n`, `\r`, `\0`, `\b`, `\f`) and
  not quoted, and NULL is `\N`. In an `Array`, `Tuple` or `Map`, strings are
  quoted and NULL is `NULL`. Floats get the shortest text that reads back
  as the same value. The notation is positional when the decimal exponent
  is in [-6, 21), else scientific (`1e21`, `1.5e-7`). The special values are
  `nan`, `inf`, `-inf` and `-0`. Decimals have no trailing zeros. `DateTime64(P)` and
  `Time64(P)` have exactly P fractional digits. `Time` hours can be more
  than 24, and the server writes a value above `999:59:59` as `999:59:59`.
  IPv6 follows the server's `formatIPv6` (`::ffff:1.2.3.4`). A `Map` keeps
  the order of the server, because clickhouse-connect gives a `dict` in that
  order. The live tests compare this text with the server's own
  `FORMAT TSV` output for many expressions.
- **Totals and extremes.** Over HTTP, the server sends no `WITH TOTALS` row
  and no `extremes` in the `Native` output, so the program writes none. Tests
  that need them are an environment cause.
- **Output formats.** clickhouse-connect always reads results as `Native`
  (it adds `FORMAT Native` to the query), and the program writes them itself
  in the `TabSeparated` family (also `TSVRaw` and `...WithNames[AndTypes]`).
  It removes the `FORMAT` clause of these formats from the query. A
  statement with `FORMAT Null` goes to the server as it is, through
  `Client.command`, and the server sends no rows (as with
  `clickhouse-client`); `EXPLAIN ... FORMAT Null` gives the plan, because
  the server ignores `FORMAT Null` for `EXPLAIN` by default. A statement
  with any other output format fails. When its test hint expects an error,
  the statement first goes to the server as it is, through the documented
  `client.raw_query()`, because the error can depend on the format.
- **Settings.** Of the settings on the command line, only `log_comment`,
  `max_insert_threads`, `max_threads` and `send_logs_level` go to the server,
  with each call (the `settings` argument), as in `clickhouse-client`. A `SET`
  of the same setting in the test replaces the command-line value.
  `--param_<name>=<value>` gives a query parameter to the statements that have
  a `{<name>:<type>}` placeholder.
- **Errors.** An unexpected error stops the program. It writes the error with
  its code and name to stderr. The exit code is the error code modulo 256,
  or 1 when that is 0 or when the error has no code. A command line that the
  program cannot read and an empty input also give the exit code 1.
  The log records of clickhouse-connect and Python warnings go to
  `CLICKHOUSE_CLIENT_CLI_LOG`, never to stderr: the runner fails a `.sql` test
  that writes to stderr.
- **Error names.** [`clickhouse_test_runner/errorcodes.py`](clickhouse_test_runner/errorcodes.py)
  maps the error names of test hints to codes.
  [`scripts/fetch-upstream.sh`](scripts/fetch-upstream.sh) generates it from
  `src/Common/ErrorCodes.cpp` of the upstream checkout, so it always matches
  the tests. Commit the file when it changes.

## Known failures

The end of [`allowlist.txt`](allowlist.txt) lists the tests that fail
because of a bug in clickhouse-connect or a gap in this program, in groups.
Each group header describes the cause and gives a small reproduction. A test
that cannot pass for an environment cause (a cluster, ZooKeeper, server
configuration, features of `clickhouse-client` itself, the HTTP interface) is
not listed; see [`AGENTS.md`](AGENTS.md#environment-causes). The program does
not try `.sh` tests.

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml)
runs the allowlist nightly, manually, and on pushes to `main` and pull
requests that change this directory or the workflow.

To run it on another pull request, for example a change in the client, add
the `tests:run-core-sql` label
([`upstream-sql-tests-label.yml`](../../.github/workflows/upstream-sql-tests-label.yml)).
The run tests the pull request as it is when the label is added, and removes
the label when it ends: to test a later push, add the label again. The token
of a pull request from a fork cannot remove the label, so remove it by hand
before you add it again.

- The matrix is 10 round-robin shards of the allowlist, about 1,020 tests
  each, at `-j 4`. After its upstream tests, the first shard also runs the
  unit and live tests of the harness.
- A new push to a pull request cancels the older run of that pull request.
  Nightly, manual and push runs do not cancel each other.
- Each job starts the stock server image of the pinned version with
  `CLICKHOUSE_SKIP_USER_SETUP=1` and the host name of the runner, as in step
  2 of "Run the tests locally".
- Each failed test fails its job. The script turns off the runner's retries
  (`--dont_retry_failures`). A failed
  job uploads an artifact. It has the output of the runner with the diffs,
  the `.stdout` and `.stderr` files of the failed tests, and the log of the
  program (`CLICKHOUSE_CLIENT_CLI_LOG`).
- The server version and the upstream tests are pinned together in
  [`clickhouse-version`](clickhouse-version). A new ClickHouse release
  changes nothing in CI, so a pull request never fails because of a release
  that it does not touch. A bump is a pull request of its own. Try the new
  version with the `clickhouse_version` input below, and find the cause of
  each new failure. Then change `clickhouse-version`, and commit the
  `errorcodes.py` that `fetch-upstream.sh` generates. The steps are in
  [`AGENTS.md`](AGENTS.md#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version
  without a commit, for example before a bump.
- When an allowlisted test is not in the checkout (upstream renamed or
  removed it), the run shows a warning with the names.
- If a shard takes much more than 10 minutes, increase the number of
  shards: change the `shard` list and `SHARD_TOTAL` together.
