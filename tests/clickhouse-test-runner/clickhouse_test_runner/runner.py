"""Run the statements of one invocation through clickhouse-connect, on one session."""

from __future__ import annotations

import os
import re
import uuid
from typing import Any, BinaryIO

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import Error as ClickHouseConnectError

from .args import Options
from .errorcodes import ERROR_NAMES
from .formats import OutputFormat, returns_rows, split_output_format, unquote_identifier
from .lexer import leading_keyword, skip_quoted, skip_whitespace_and_comments, word_end
from .log import InvocationLog
from .render import ColumnWriter, escape_string, render_untyped
from .split import Statement

SYNTAX_ERROR = 62
# The server adds this to an error that it finds while it analyzes a parsed query.
_ANALYSIS_ERROR = ": In scope "

# The read formats that the shim selects. Both are documented options of Client.query: the "typed"
# format gives each Variant (and Geometry) value with the name of its member type, which the renderer
# needs to write the value as the server does. All other types use the default read format.
QUERY_FORMATS = {"Variant": "typed", "Geometry": "typed"}

_CODE_IN_MESSAGE = re.compile(r"Code: (\d+)\. ")
_URL_TRAILER = re.compile(r"\s*\(for url [^)]*\)\s*$")


class HintError(Exception):
    """A result that does not match the test hint."""

    def __init__(self, msg: str, code: int = 0):
        super().__init__(msg)
        self.code = code


class ShimError(Exception):
    """A statement that the shim cannot run as clickhouse-client does."""


def error_code(ex: BaseException) -> int | None:
    """Return the ClickHouse error code of an exception of clickhouse-connect, or None."""
    if isinstance(ex, ClickHouseConnectError):
        if ex.code is not None:
            return int(ex.code)
        m = _CODE_IN_MESSAGE.search(str(ex))
        if m:
            return int(m.group(1))
    return None


def server_message(ex: BaseException) -> str:
    """Write a server error the way clickhouse-client does (Code: N. DB::Exception: ... (NAME)), so that a
    reader can find the error name."""
    text = str(ex)
    code = error_code(ex)
    start = text.find("Code: ")
    message = _URL_TRAILER.sub("", text[start:] if start >= 0 else text)
    name = ERROR_NAMES.get(code) if code is not None else None
    if start < 0 and code is not None:
        message = f"Code: {code}. {message}"
    if name and f"({name})" not in message:
        message += f" ({name})"
    return message


def http_port(port: int, secure: bool) -> int:
    """Map the port that tests/clickhouse-test gives (the native TCP port, from CLICKHOUSE_PORT_TCP) to the
    HTTP port that the runner exports. Any other port is used as given."""
    native_env, http_env, native_default, http_default = "CLICKHOUSE_PORT_TCP", "CLICKHOUSE_PORT_HTTP", 9000, 8123
    if secure:
        native_env, http_env, native_default, http_default = "CLICKHOUSE_PORT_TCP_SECURE", "CLICKHOUSE_PORT_HTTPS", 9440, 8443
    native = _env_int(native_env, native_default)
    if port and port != native:
        return port
    return _env_int(http_env, http_default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, ""))
    except ValueError:
        return default


class Runner:
    """Runs the statements of one shim invocation on one session.

    Session state (SET, USE, temporary tables) must survive from one statement to the next, as in
    clickhouse-client. Every request of the client has the same session_id, and the server keeps the
    session after an error. The shim sends nothing that clickhouse-client does not send, because some
    tests read their own queries from system.query_log.
    """

    def __init__(self, opts: Options, out: BinaryIO, log: InvocationLog):
        self.opts = opts
        self.out = out
        self.log = log
        # The command-line settings that go with each query, as clickhouse-client sends them. A SET of
        # the same setting in the test removes it from here, because the session then holds the value.
        self.settings = dict(opts.settings)
        # The value of the last SET dialect.
        self.dialect = ""
        # Set by SET implicit_select = 1, with which an expression alone (1 + 2) is a query.
        self.implicit = False
        # SET ignore_format_null_for_explain (on by default).
        self.explain_ignores_null = True
        self.session_id = "clickhouse-connect-test-runner-" + uuid.uuid4().hex
        self.client: Client | None = None
        names = "|".join(re.escape(p) for p in opts.params)
        self._param_ref = re.compile(r"\{\s*(?:" + names + r")\s*:") if names else None

    def open(self) -> None:
        port = http_port(self.opts.port, self.opts.secure)
        self.log.add("session_id", self.session_id)
        self.log.add("address", f"{self.opts.host}:{port}")
        self.client = clickhouse_connect.get_client(
            host=self.opts.host,
            port=port,
            username=self.opts.user,
            password=self.opts.password,
            database=self.opts.database,
            secure=self.opts.secure,
            session_id=self.session_id,
        )

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None

    def run(self, statements: list[Statement]) -> None:
        """Run the statements in order. Stop at the first statement whose result does not match its test
        hint, as clickhouse-client does."""
        self.open()
        for st in statements:
            self.run_one(st)

    def run_one(self, st: Statement) -> None:
        if st.hint.parse_error:
            self._echo(st)
            raise HintError(st.hint.parse_error)
        self.log.add("query", st.sql)
        result = ""
        err: BaseException | None = None
        try:
            result = self.execute(st.sql, st.hint.expects_error())
        except Exception as ex:  # noqa: BLE001 - every error is a result of the statement
            err = ex
        code = error_code(err) if err is not None else None
        # clickhouse-client parses a statement before it echoes it, so it does not echo a statement with a
        # syntax error. A SYNTAX_ERROR from the analysis of the query ("In scope ...", for example a function
        # that parses its argument) comes after the statement was parsed: that statement is echoed.
        if code != SYNTAX_ERROR or (err is not None and _ANALYSIS_ERROR in str(err)):
            self._echo(st)
        self.out.write(result.encode("utf-8", "surrogateescape"))
        self.out.flush()

        hint = st.hint
        if hint.expects_error():
            if err is None:
                raise HintError(f"expected {hint.label()}, but the query succeeded (query: {st.full})")
            if code is not None:
                if not hint.matches(code):
                    raise HintError(f"expected {hint.label()}, but got {server_message(err)} (query: {st.full})", code)
                self.log.add("expected_error_matched", hint.label())
                return
        if err is not None:
            raise err
        self._after_success(st.sql)

    def _echo(self, st: Statement) -> None:
        if st.echo:
            self.out.write(st.full.encode("utf-8", "surrogateescape") + b"\n")

    def _after_success(self, sql: str) -> None:
        """Update the session state after a USE or SET statement."""
        keyword = leading_keyword(sql)
        if keyword == "USE":
            db = parse_use_database(sql, self.opts.params)
            if db is None:
                raise ShimError(f"clickhouse-test-runner: cannot read the database name of {sql!r}")
            # clickhouse-connect sends the database of the client with each request, and that database
            # replaces the current database of the session. A user of the client sets the database
            # attribute of the client.
            assert self.client is not None
            self.client.database = db
        elif keyword == "SET":
            for name, value in parse_set_assignments(sql).items():
                self.settings.pop(name, None)
                if name == "dialect":
                    self.dialect = value
                elif name == "implicit_select":
                    self.implicit = _setting_true(value)
                elif name == "ignore_format_null_for_explain":
                    self.explain_ignores_null = _setting_true(value)

    def _other_dialect(self, sql: str) -> bool:
        """Tell if the session uses another query language than SQL (SET dialect = 'kusto', 'prql', ...).
        The shim cannot see if a statement in such a language returns rows, so it runs it as a query."""
        return bool(self.dialect) and self.dialect.lower() != "clickhouse" and leading_keyword(sql) != "SET"

    def _implicit_select(self, sql: str) -> bool:
        if not self.implicit:
            return False
        return leading_keyword(sql) not in _NOT_IMPLICIT

    def _call_args(self, sql: str) -> dict[str, Any]:
        args: dict[str, Any] = {}
        if self.settings:
            args["settings"] = dict(self.settings)
        # A user gives the query parameters to the statements that use them. Without a {name:Type}
        # placeholder, clickhouse-connect binds the parameters on the client side, with %(name)s.
        if self._param_ref is not None and self._param_ref.search(sql):
            args["parameters"] = dict(self.opts.params)
        return args

    def execute(self, sql: str, expects_error: bool = False) -> str:
        assert self.client is not None
        other = self._other_dialect(sql)
        if not returns_rows(sql) and not other and not self._implicit_select(sql):
            self.client.command(sql, **self._call_args(sql))
            return ""
        query, fmt = sql, OutputFormat()
        if not other:
            # The FORMAT clause is SQL; other dialects can use the word differently (the format pipe of
            # LogsQL).
            query, fmt = split_output_format(sql)
        if not fmt.supported:
            if expects_error:
                # The test expects an error, so no rows are written. The statement goes to the server as it
                # is, through the documented raw API, because the error can depend on the format.
                self.client.raw_query(sql, **self._call_args(sql))
            raise ShimError(
                f"clickhouse-test-runner: output format {fmt.name} is not supported: clickhouse-connect reads results as "
                "Native, and the shim writes them only as TabSeparated"
            )
        if fmt.discard and leading_keyword(sql) == "EXPLAIN" and self.explain_ignores_null:
            # With ignore_format_null_for_explain (on by default), the server ignores FORMAT Null for
            # EXPLAIN and sends the plan.
            fmt = OutputFormat()
        elif fmt.discard:
            # With FORMAT Null, the server sends no rows. The statement goes to the server as it is, with its
            # FORMAT clause, as clickhouse-client sends it (tests read their queries from system.query_log).
            # Client.command runs a statement without reading a result set; Client.query adds FORMAT Native
            # and reads every row into memory.
            self.client.command(sql, **self._call_args(sql))
            return ""
        result = self.client.query(query, query_formats=QUERY_FORMATS, **self._call_args(query))
        return write_rows(result, fmt)


def _setting_true(value: str) -> bool:
    return value not in ("0", "") and value.lower() != "false"


_NOT_IMPLICIT = frozenset(
    "INSERT CREATE DROP ALTER SET USE TRUNCATE RENAME OPTIMIZE SYSTEM GRANT REVOKE ATTACH DETACH KILL EXCHANGE UNDROP "
    "BACKUP RESTORE".split()
)


def write_rows(result: Any, fmt: OutputFormat) -> str:
    """Write a QueryResult as TabSeparated text."""
    names = list(result.column_names)
    types = list(result.column_types)
    lines: list[str] = []
    if types:
        writers = [ColumnWriter(t.name) for t in types]
        raw = fmt.raw
        # A Raw format writes the header without escapes, as its rows (TabSeparatedRowOutputFormat::writeLine).
        if fmt.names:
            lines.append("\t".join(n if raw else escape_string(n, False) for n in names) + "\n")
        if fmt.types:
            lines.append("\t".join(t.name if raw else escape_string(t.name, False) for t in types) + "\n")
        for row in result.result_rows:
            line = "\t".join([w.write(v, raw) for w, v in zip(writers, row)])
            lines.append(line + "\n")
    else:
        # Client.query sent the statement through Client.command (for example CHECK TABLE), and the result
        # has no column types.
        for row in result.result_rows:
            lines.append(render_untyped(row) + "\n")
    return "".join(lines)


def parse_use_database(sql: str, params: dict[str, str]) -> str | None:
    """Return the database name of a USE statement. A {name:Identifier} query parameter is replaced by its
    value."""
    i = skip_whitespace_and_comments(sql, 0)
    i = word_end(sql, i)
    rest = sql[i:].strip()
    if rest.endswith(";"):
        rest = rest[:-1].strip()
    word, _, after = rest.partition(" ")
    if word.upper() == "DATABASE" and after:
        rest = after.strip()
    if not rest:
        return None
    if rest[0] in '`"':
        if skip_quoted(rest, 0) != len(rest):
            return None
        return unquote_identifier(rest)
    if rest[0] == "{":
        name, sep, _ = rest.strip("{}").partition(":")
        if not sep:
            return None
        return params.get(name.strip())
    if word_end(rest, 0) != len(rest):
        return None
    return rest


def parse_set_assignments(sql: str) -> dict[str, str]:
    """Return the settings that a `SET a = 1, b = 'x'` statement assigns, with their values (string values
    without quotes)."""
    i = word_end(sql, skip_whitespace_and_comments(sql, 0))
    body = sql[i:].strip()
    if body.endswith(";"):
        body = body[:-1]
    out: dict[str, str] = {}

    def add(part: str) -> None:
        name, sep, value = part.partition("=")
        if not sep:
            return
        value = value.strip()
        if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
            value = value[1:-1]
        out[name.strip()] = value

    depth = start = 0
    j = 0
    while j < len(body):
        c = body[j]
        if c in "'\"`":
            j = skip_quoted(body, j)
            continue
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        elif c == "," and depth == 0:
            add(body[start:j])
            start = j + 1
        j += 1
    add(body[start:])
    return out
