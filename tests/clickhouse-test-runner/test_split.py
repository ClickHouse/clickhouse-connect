"""Unit tests of the statement splitter and the test hints. No server needed."""

import pytest
from clickhouse_test_runner.hint import parse_test_hint
from clickhouse_test_runner.lexer import leading_keyword
from clickhouse_test_runner.split import split_statements


@pytest.mark.parametrize(
    "text, expected",
    [
        ("SELECT 1", ["SELECT 1"]),
        ("SELECT 1;\nSELECT 2;\n", ["SELECT 1;", "SELECT 2;"]),
        ("SELECT 'a;b'; SELECT 2", ["SELECT 'a;b';", "SELECT 2"]),
        ("SELECT 'a\\';b'; SELECT 2", ["SELECT 'a\\';b';", "SELECT 2"]),
        ("SELECT 'a'';b'; SELECT 2", ["SELECT 'a'';b';", "SELECT 2"]),
        ("SELECT 1 AS `a;b`; SELECT 2", ["SELECT 1 AS `a;b`;", "SELECT 2"]),
        ("SELECT 1 -- a;b\n; SELECT 2", ["SELECT 1 -- a;b\n;", "SELECT 2"]),
        ("SELECT /* a;b */ 1; SELECT 2", ["SELECT /* a;b */ 1;", "SELECT 2"]),
        ("SELECT /* a /* b; */ c; */ 1; SELECT 2", ["SELECT /* a /* b; */ c; */ 1;", "SELECT 2"]),
        ("SELECT $$a;'b$$; SELECT $x$;$x$", ["SELECT $$a;'b$$;", "SELECT $x$;$x$"]),
        ("SELECT 1 # a;b\n; SELECT 2", ["SELECT 1 # a;b\n;", "SELECT 2"]),
        ("SELECT 1;\n-- the end\n", ["SELECT 1;"]),
        ("  \n-- only a comment\n", []),
        # The tags header is not part of a statement.
        ("-- Tags: no-fasttest\n-- Tag no-fasttest: reason\n\nSELECT 1;", ["SELECT 1;"]),
        # A leading comment stays with its statement.
        ("SELECT 1;\n-- next\nSELECT 2;", ["SELECT 1;", "-- next\nSELECT 2;"]),
        # The inline data of INSERT ... FORMAT ends at an empty line.
        ("INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;\n\nSELECT 1;", ["INSERT INTO t FORMAT CSV\n1,'a;b'\n2,c;", "SELECT 1;"]),
        ("INSERT INTO t FORMAT Values (1);\nSELECT 1;", ["INSERT INTO t FORMAT Values (1);", "SELECT 1;"]),
        ("SELECT 1 FORMAT TSV;\nSELECT 2;", ["SELECT 1 FORMAT TSV;", "SELECT 2;"]),
        # FORMAT is an identifier here.
        ("INSERT INTO t SELECT format FROM s;\nSELECT 1;", ["INSERT INTO t SELECT format FROM s;", "SELECT 1;"]),
        (
            "INSERT INTO t SELECT 1 AS format FROM numbers(1);\nSELECT 1;",
            ["INSERT INTO t SELECT 1 AS format FROM numbers(1);", "SELECT 1;"],
        ),
        ("INSERT INTO t SELECT format x FROM s;\nSELECT 1;", ["INSERT INTO t SELECT format x FROM s;", "SELECT 1;"]),
        ("INSERT INTO t SELECT format DIV 2 FROM s;\nSELECT 1;", ["INSERT INTO t SELECT format DIV 2 FROM s;", "SELECT 1;"]),
        (
            "INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);\nSELECT 1;",
            ["INSERT INTO t SELECT x FROM s WHERE format GLOBAL IN (1);", "SELECT 1;"],
        ),
        # INSERT ... SELECT ... FROM input(...) FORMAT has inline data.
        (
            "INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2\n\nSELECT 1;",
            ["INSERT INTO t SELECT x FROM input('x UInt8') FORMAT TSV\n1;\n2", "SELECT 1;"],
        ),
        (
            "INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1\n\nSELECT 1;",
            ["INSERT INTO t (a) SETTINGS async_insert = 0 FORMAT CSV\n1", "SELECT 1;"],
        ),
        ('insert into t format JSONEachRow {"a": 1}\n\nSELECT 1;', ['insert into t format JSONEachRow {"a": 1}', "SELECT 1;"]),
        ("SELECT 1;;\nSELECT 2", ["SELECT 1;", "SELECT 2"]),
        # The trailing semicolon stays; system.query_log shows it.
        ("SELECT 1; -- c\nSELECT 2\n", ["SELECT 1;", "SELECT 2"]),
        # `//` starts a line comment, as in the ClickHouse lexer.
        ("SELECT 1 // a;b\n; SELECT 2", ["SELECT 1 // a;b\n;", "SELECT 2"]),
        ("SELECT 1; // c\nSELECT 2;", ["SELECT 1;", "SELECT 2;"]),
        # The Unicode whitespace that the ClickHouse lexer skips.
        ("SELECT 1;\n\u0085\u00a0\u2000\u200a\u2028SELECT\u2029 2;", ["SELECT 1;", "SELECT\u2029 2;"]),
    ],
)
def test_split_statements(text, expected):
    assert [s.sql for s in split_statements(text)] == expected


def test_trailing_hint_belongs_to_the_statement():
    stmts = split_statements("SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }\nSELECT 2;")
    assert len(stmts) == 2
    assert stmts[0].full == "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }"
    assert stmts[0].hint.server_errors == [395]
    assert not stmts[1].hint.expects_error()


def test_hint_on_the_next_line_is_leading():
    # clickhouse-client takes only the same-line comment as the trailing comment. A hint on the next line
    # is a leading comment of the next statement, and an error hint in a leading comment is ignored.
    stmts = split_statements("SELECT 1;\n-- { serverError 60 }\nSELECT 2;")
    assert [s.hint.expects_error() for s in stmts] == [False, False]


def test_hint_inside_the_statement():
    stmts = split_statements("SELECT x -- { serverError UNKNOWN_IDENTIFIER }\n;")
    assert len(stmts) == 1
    assert stmts[0].hint.server_errors == [47]


def test_echo():
    stmts = split_statements("SELECT 1;\n-- { echoOn }\nSELECT 2; -- two\nSELECT 3;\n-- { echoOff }\nSELECT 4;\nSELECT 5; -- { echo }\n")
    assert [s.echo for s in stmts] == [False, True, True, False, True]
    assert stmts[1].full == "-- { echoOn }\nSELECT 2; -- two"


def test_echo_of_the_last_statement_without_semicolon():
    # clickhouse-client echoes the rest of the text, with the newlines.
    stmts = split_statements("SELECT 1;\nSELECT 2\n\n")
    assert [(s.sql, s.full) for s in stmts] == [("SELECT 1;", "SELECT 1;"), ("SELECT 2", "SELECT 2\n\n")]


@pytest.mark.parametrize(
    "sql, keyword",
    [
        ("  -- c\n/* d */ (select 1)", "SELECT"),
        ("insert into t values (1)", "INSERT"),
        ("-- only a comment", ""),
        ("// c\nSELECT 1", "SELECT"),
        ("\u0085\u00a0\u180e\u2000\u200b\u200d\u2028\u2029\u202f\u205f\u2060\u3000\ufeffSELECT\ufeff1", "SELECT"),
    ],
)
def test_leading_keyword(sql, keyword):
    assert leading_keyword(sql) == keyword


@pytest.mark.parametrize(
    "text, server, client",
    [
        ("SELECT 1; -- { serverError UNKNOWN_TABLE }", [60], []),
        ("SELECT 1; -- { serverError 60 }", [60], []),
        ("SELECT 1; -- { serverError 153, ILLEGAL_DIVISION }", [153, 153], []),
        ("SELECT; -- { clientError SYNTAX_ERROR }", [], [62]),
        ("SELECT 1; -- { error 62 }", [62], [62]),
        ("SELECT 1 /* { serverError 60 } */;", [60], []),
        ("SELECT 1; -- { serverError 60 } because there is no table", [60], []),
        ("-- { serverError 60 }\nSELECT 1;", [], []),
        ("SELECT '-- { serverError 60 }';", [], []),
        ("SELECT 1; -- a comment", [], []),
    ],
)
def test_parse_test_hint(text, server, client):
    h = parse_test_hint(text)
    assert h.parse_error is None
    assert (h.server_errors, h.client_errors) == (server, client)


@pytest.mark.parametrize("text", ["SELECT 1; -- { serverError NOT_A_REAL_ERROR_NAME }", "SELECT 1; -- { retry 3 until serverError 60 }"])
def test_parse_test_hint_errors(text):
    assert parse_test_hint(text).parse_error


def test_hint_matches():
    h = parse_test_hint("SELECT; -- { clientError SYNTAX_ERROR }")
    # Through clickhouse-connect the server finds syntax errors.
    assert h.matches(62)
    assert not h.matches(60)
    assert h.label() == "clientError SYNTAX_ERROR"


@pytest.mark.parametrize("text, echo", [("-- { echoOn }\nSELECT 1;", True), ("SELECT 1; -- { echoOff }", False)])
def test_parse_test_hint_echo(text, echo):
    assert parse_test_hint(text).echo is echo
