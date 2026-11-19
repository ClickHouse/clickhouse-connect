"""Unit tests of the command line, the session helpers and the FORMAT clause. No server needed."""

import io

import pytest
from clickhouse_test_runner.__main__ import main
from clickhouse_test_runner.args import ArgsError, Options, parse_args, redact_password
from clickhouse_test_runner.formats import split_output_format
from clickhouse_test_runner.log import InvocationLog
from clickhouse_test_runner.runner import Runner, http_port, parse_set_assignments, parse_use_database
from clickhouse_test_runner.split import split_statements

from clickhouse_connect.driver.exceptions import DatabaseError


def test_sql_test_command_line():
    # The command line that tests/clickhouse-test uses for a .sql test.
    opts = parse_args(
        [
            "client",
            "--host=127.0.0.1",
            "--port=9000",
            "--no-secure",
            "--database=test_abc",
            "--send_logs_level=warning",
            "--multiquery",
            "--log_comment",
            "00001_select_1.sql-test_abc",
            "--max_block_size=1",
            "--allow_repeated_settings",
            "--client_logs_file=/tmp/x-fatal",
            "--param_CLICKHOUSE_DATABASE=test_abc",
        ]
    )
    assert (opts.host, opts.port, opts.database, opts.secure, opts.query) == ("127.0.0.1", 9000, "test_abc", False, None)
    assert list(opts.settings.items()) == [("send_logs_level", "warning"), ("log_comment", "00001_select_1.sql-test_abc")]
    assert opts.params == {"CLICKHOUSE_DATABASE": "test_abc"}


def test_short_options():
    opts = parse_args(["-h", "db", "-u", "me", "-d", "x", "-q", "SELECT 1", "-n", "-s"])
    assert (opts.host, opts.user, opts.database, opts.query, opts.secure) == ("db", "me", "x", "SELECT 1", True)


def test_setting_values():
    opts = parse_args(["--max_threads", "-1", "--max-insert-threads", "2", "--query=SELECT 1"])
    assert opts.settings == {"max_threads": "-1", "max_insert_threads": "2"}


def test_redact_password():
    argv = ["--user", "u", "--password", "secret", "--password=secret2", "--query", "SELECT 1", "--password"]
    assert redact_password(argv) == ["--user", "u", "--password", "***", "--password=***", "--query", "SELECT 1", "--password"]
    # The parser reads the first --password as the user name, and the value after the second one as the
    # password.
    argv = ["-u", "--password", "--password", "secret"]
    assert parse_args(argv).password == "secret"
    assert redact_password(argv) == ["-u", "--password", "***", "***"]


@pytest.mark.parametrize("argv", [["--port=abc"], ["--query"]])
def test_bad_arguments(argv):
    with pytest.raises(ArgsError):
        parse_args(argv)


def test_http_port(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_PORT_TCP", raising=False)
    monkeypatch.delenv("CLICKHOUSE_PORT_HTTP", raising=False)
    assert [http_port(0, False), http_port(9000, False), http_port(18123, False), http_port(9440, True)] == [8123, 8123, 18123, 8443]
    monkeypatch.setenv("CLICKHOUSE_PORT_TCP", "19000")
    monkeypatch.setenv("CLICKHOUSE_PORT_HTTP", "18124")
    assert [http_port(19000, False), http_port(9000, False)] == [18124, 9000]


@pytest.mark.parametrize(
    "argv, expected",
    [
        (["extract-from-config", "--try", "--config", "x.xml", "--key", "listen_host"], b"127.0.0.1\n"),
        (["extract-from-config", "--key=tcp_port"], b""),
    ],
)
def test_extract_from_config(argv, expected):
    out = io.BytesIO()
    assert main(argv, io.BytesIO(), out, io.StringIO()) == 0
    assert out.getvalue() == expected


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("USE db1;", "db1"),
        ("use `my db`", "my db"),
        ("-- c\nUSE DATABASE db2", "db2"),
        ("USE {CLICKHOUSE_DATABASE:Identifier};", "test_x"),
        ('USE "db3"', "db3"),
        ("USE db1, db2", None),
    ],
)
def test_parse_use_database(sql, expected):
    assert parse_use_database(sql, {"CLICKHOUSE_DATABASE": "test_x"}) == expected


def test_parse_set_assignments():
    assert parse_set_assignments("SET dialect = 'kusto', max_threads=1, x = (1, 2);") == {
        "dialect": "kusto",
        "max_threads": "1",
        "x": "(1, 2)",
    }


@pytest.mark.parametrize(
    "sql, rest, name, supported",
    [
        ("SELECT 1", "SELECT 1", "TabSeparated", True),
        ("SELECT 1 FORMAT TSV", "SELECT 1 ", "TSV", True),
        ("SELECT 1 FORMAT TSVWithNames SETTINGS max_threads = 1", "SELECT 1  SETTINGS max_threads = 1", "TSVWithNames", True),
        ("SELECT 1 FORMAT JSONEachRow", "SELECT 1 ", "JSONEachRow", False),
        ("SELECT 'FORMAT JSON'", "SELECT 'FORMAT JSON'", "TabSeparated", True),
        ("SELECT * FROM (SELECT 1 FORMAT JSON)", "SELECT * FROM (SELECT 1 FORMAT JSON)", "TabSeparated", True),
        ("SELECT t.format FROM t", "SELECT t.format FROM t", "TabSeparated", True),
        ("SELECT 1 FORMAT null", "SELECT 1 ", "null", True),
        ("SELECT 1 FORMAT TSVRaw", "SELECT 1 ", "TSVRaw", True),
        ("SELECT 1 FORMAT `Null`;", "SELECT 1 ;", "Null", True),
        # FORMAT as an identifier, not the clause.
        ("SELECT format FROM t", "SELECT format FROM t", "TabSeparated", True),
        ("SELECT x AS format FROM t WHERE format = 1", "SELECT x AS format FROM t WHERE format = 1", "TabSeparated", True),
        ("SELECT 1 AS format FROM numbers(1)", "SELECT 1 AS format FROM numbers(1)", "TabSeparated", True),
        ("SELECT format FROM (SELECT 1 AS format)", "SELECT format FROM (SELECT 1 AS format)", "TabSeparated", True),
        ("SELECT 1 AS format", "SELECT 1 AS format", "TabSeparated", True),
        ("SELECT 1 AS format ORDER BY format DESC", "SELECT 1 AS format ORDER BY format DESC", "TabSeparated", True),
        ("SELECT 1 AS format ORDER BY format ASC", "SELECT 1 AS format ORDER BY format ASC", "TabSeparated", True),
        ("SELECT 1 AS format ORDER BY format DESC FORMAT TSV", "SELECT 1 AS format ORDER BY format DESC ", "TSV", True),
        ("SELECT 1 FORMAT TSV -- c\n;", "SELECT 1  -- c\n;", "TSV", True),
    ],
)
def test_split_output_format(sql, rest, name, supported):
    query, fmt = split_output_format(sql)
    assert (query, fmt.name, fmt.supported) == (rest, name, supported)


class _RecordingClient:
    """Records the calls of the runner. Client.query returns an empty result set."""

    def __init__(self):
        self.calls = []

    def query(self, sql, **kwargs):
        self.calls.append(("query", sql))
        return type("Result", (), {"column_names": (), "column_types": (), "result_rows": []})()

    def command(self, sql, **kwargs):
        self.calls.append(("command", sql))
        return ""


@pytest.mark.parametrize(
    "statements, expected",
    [
        # FORMAT Null: the statement goes to the server as it is, and no rows are read.
        (["SELECT 1 FORMAT Null;"], [("command", "SELECT 1 FORMAT Null;")]),
        (["SELECT 1 format `Null` SETTINGS max_threads = 1;"], [("command", "SELECT 1 format `Null` SETTINGS max_threads = 1;")]),
        # Another format of the TabSeparated family: the shim reads the rows and writes them.
        (["SELECT 1 FORMAT TSV;"], [("query", "SELECT 1 ;")]),
        # The server ignores FORMAT Null for EXPLAIN (ignore_format_null_for_explain = 1 by default) and sends the plan.
        (["EXPLAIN SELECT 1 FORMAT Null;"], [("query", "EXPLAIN SELECT 1 ;")]),
        (
            ["SET ignore_format_null_for_explain = 0;", "EXPLAIN SELECT 1 FORMAT Null;"],
            [("command", "SET ignore_format_null_for_explain = 0;"), ("command", "EXPLAIN SELECT 1 FORMAT Null;")],
        ),
    ],
)
def test_format_null_reads_no_rows(statements, expected):
    runner = Runner(Options(), io.BytesIO(), InvocationLog())
    client = _RecordingClient()
    runner.client = client
    for sql in statements:
        assert runner.execute(sql) == ""
        runner._after_success(sql)
    assert client.calls == expected


class _FailingClient(_RecordingClient):
    """Client.query raises the server error that it is given."""

    def __init__(self, message):
        super().__init__()
        self.message = message

    def query(self, sql, **kwargs):
        raise DatabaseError(self.message)


@pytest.mark.parametrize(
    "message, echoed",
    [
        # The statement did not parse: clickhouse-client does not echo it.
        ("Code: 62. DB::Exception: Syntax error: failed at position 8 (end of query). (SYNTAX_ERROR)", False),
        # The statement parsed, and a function failed to parse its argument: clickhouse-client echoes it.
        ("Code: 62. DB::Exception: Syntax error: failed at position 8: In scope SELECT formatQuery('SELECT'). (SYNTAX_ERROR)", True),
    ],
)
def test_echo_of_a_statement_with_a_syntax_error(message, echoed):
    out = io.BytesIO()
    runner = Runner(Options(), out, InvocationLog())
    runner.client = _FailingClient(message)
    st = split_statements("-- { echo }\nSELECT f(); -- { serverError SYNTAX_ERROR }")[0]
    runner.run_one(st)
    assert out.getvalue() == ((st.full.encode() + b"\n") if echoed else b"")
