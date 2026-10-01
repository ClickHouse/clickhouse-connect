from unittest.mock import Mock

import pytest

from clickhouse_connect.driver import binding
from clickhouse_connect.driver.binding import (
    MAX_URL_BIND_PARAM_LENGTH,
    _binding_keeps_query_structure,
    _contains_insert_bareword,
    _query_is_insert,
    _query_is_read_only,
    _strip_trailing_semicolons,
    bind_query,
    finalize_query,
    quote_identifier,
    use_form_encoding,
)

SERVER_UNICODE_WHITESPACE = [
    "\u0085",
    "\u00a0",
    "\u180e",
    *(chr(codepoint) for codepoint in range(0x2000, 0x200E)),
    "\u2028",
    "\u2029",
    "\u202f",
    "\u205f",
    "\u2060",
    "\u3000",
    "\ufeff",
]


@pytest.mark.parametrize(
    "identifier, expected",
    [
        ("foo", "`foo`"),
        ("foo`bar", "`foo\\`bar`"),
        ('foo"bar', '`foo"bar`'),
        ("", "``"),
    ],
)
def test_quote_identifier_raw(identifier, expected):
    assert quote_identifier(identifier) == expected


@pytest.mark.parametrize(
    "identifier",
    [
        "`foo`",
        '"foo"',
        "`foo\\`bar`",
        "`foo``bar`",
        '"foo""bar"',
    ],
)
def test_quote_identifier_valid_prequoted_passthrough(identifier):
    assert quote_identifier(identifier) == identifier


@pytest.mark.parametrize(
    "identifier, expected",
    [
        ("`weird`name`", "`\\`weird\\`name\\``"),
        ('"weird"name"', '`"weird"name"`'),
        ("`foo\\`", "`\\`foo\\\\\\``"),
        ("`", "`\\``"),
    ],
)
def test_quote_identifier_invalid_prequoted_escaped_as_raw(identifier, expected):
    assert quote_identifier(identifier) == expected


def test_use_form_encoding_empty():
    assert use_form_encoding("SELECT 1", {}) is False
    assert use_form_encoding("SELECT 1", {}, force_form=True) is True


def test_use_form_encoding_force():
    assert use_form_encoding("SELECT {id:UInt32}", {"param_id": "1"}, force_form=True) is True


def test_use_form_encoding_small_params_stay_in_url():
    assert use_form_encoding("SELECT 1", {"param_id": "123", "param_name": "abc"}) is False


def test_use_form_encoding_large_params_promote():
    big = {"param_big": "x" * (MAX_URL_BIND_PARAM_LENGTH + 1)}
    assert use_form_encoding("SELECT {big:String}", big) is True


def test_use_form_encoding_total_across_params():
    # Many individually small params whose combined encoded length exceeds the budget
    params = {f"param_{i}": "v" * 200 for i in range(40)}
    assert use_form_encoding("SELECT 1", params) is True


def test_use_form_encoding_percent_expansion_promotes():
    # Raw length is under the budget but percent-encoding expands each space to %20
    params = {"param_s": " " * (MAX_URL_BIND_PARAM_LENGTH // 2)}
    assert use_form_encoding("SELECT 1", params) is True


def test_use_form_encoding_binary_query_not_promoted():
    # Binary binds make the query bytes; auto-promotion must not kick in unless forced
    big = {"param_big": "x" * (MAX_URL_BIND_PARAM_LENGTH + 1)}
    assert use_form_encoding(b"SELECT \xff", big) is False
    assert use_form_encoding(b"SELECT \xff", big, force_form=True) is True


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT 13;", "SELECT 13"),
        ("SELECT 13;;", "SELECT 13"),
        ("SELECT 13;\n", "SELECT 13\n"),
        ("SELECT 13; -- trailing", "SELECT 13 -- trailing"),
        ("SELECT 13; // trailing", "SELECT 13 // trailing"),
        ("SELECT 13; # trailing", "SELECT 13 # trailing"),
        ("SELECT 13; #!trailing", "SELECT 13 #!trailing"),
        ("SELECT 13; /* trailing */", "SELECT 13 /* trailing */"),
        ("SELECT 13; /* outer /* inner */ outer */", "SELECT 13 /* outer /* inner */ outer */"),
        ("SELECT 13 /* keep */;", "SELECT 13 /* keep */"),
        ("SELECT 13 -- keep\n;", "SELECT 13 -- keep\n"),
        ("SELECT 'quote '' and semicolon;';", "SELECT 'quote '' and semicolon;'"),
        ("SELECT 'backslash \\' and semicolon;';", "SELECT 'backslash \\' and semicolon;'"),
        ('SELECT 13 AS "col""quoted";', 'SELECT 13 AS "col""quoted"'),
        ("SELECT 13 AS `col``quoted`;", "SELECT 13 AS `col``quoted`"),
        ("SELECT \u2018curly;\u2019;", "SELECT \u2018curly;\u2019"),
        ("SELECT $$it's;$$;", "SELECT $$it's;$$"),
        ("SELECT $doc$a;b$doc$; -- trailing", "SELECT $doc$a;b$doc$ -- trailing"),
        ("SELECT foo$x$bar;", "SELECT foo$x$bar"),
    ],
)
def test_strip_trailing_semicolons(query, expected):
    assert _strip_trailing_semicolons(query) == expected


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 13",
        "SELECT 13 -- comment;",
        "SELECT 13 // comment;",
        "SELECT 13 # comment;",
        "SELECT 13 /* comment; */",
        "SELECT '13;'",
        'SELECT 13 AS "col;"',
        "SELECT 13 AS `col;`",
        "SELECT \u2018curly;\u2019",
        "SELECT $$it's;$$",
        "SELECT $doc$a;b$doc$",
        "SELECT 13; SELECT 79",
        "SELECT 13; #not_a_comment",
        "SELECT 13; /* unterminated",
        "SELECT 'unterminated;",
        "SELECT \u2018unterminated;",
        "SELECT 13; $tag$",
    ],
)
def test_strip_trailing_semicolons_leaves_non_terminators(query):
    assert _strip_trailing_semicolons(query) == query


@pytest.mark.parametrize("whitespace", SERVER_UNICODE_WHITESPACE)
def test_strip_trailing_semicolons_accepts_server_unicode_whitespace(whitespace):
    assert _strip_trailing_semicolons(f"SELECT 13;{whitespace}") == f"SELECT 13{whitespace}"
    assert _strip_trailing_semicolons(f"SELECT 13;{whitespace};") == f"SELECT 13{whitespace}"


def test_strip_trailing_semicolons_preserves_internal_statements():
    assert _strip_trailing_semicolons("SELECT 13; SELECT 79;") == "SELECT 13; SELECT 79"
    assert _strip_trailing_semicolons("SELECT 13; ;") == "SELECT 13 "
    assert _strip_trailing_semicolons("SELECT 13; /* separator */ ;") == "SELECT 13 /* separator */ "


def test_binding_entry_points_strip_only_literal_trailing_semicolons():
    assert finalize_query("SELECT %(value)s;", {"value": 13}) == "SELECT 13"
    assert finalize_query("SELECT %(value)s; -- trailing", {"value": 13}) == "SELECT 13; -- trailing"
    assert bind_query("SELECT 13;;", None) == ("SELECT 13", {})
    assert bind_query("SELECT {value:UInt8}; /* trailing */", {"value": 13}) == (
        "SELECT {value:UInt8}; /* trailing */",
        {"param_value": "13"},
    )


def test_bind_query_preserves_inline_insert_data():
    inline = "INSERT INTO tbl (s) FORMAT TabSeparated\nvalue_1;\n"
    assert bind_query(inline, None) == (inline, {})


def test_bind_query_binary_only_does_not_format_percent_literal():
    query, parameters = bind_query("SELECT $value$, '100%';", {"$value$": b"13"})
    assert query == b"SELECT $value$13$value$, '100%'"
    assert parameters == {}


def test_binding_structure_gate_matches_actual_bind_mode():
    class Statement:
        def __str__(self):
            return "SELECT 13; -- trailing"

    assert _binding_keeps_query_structure("SELECT {value:UInt8}", {"value": 13}) is True
    assert _binding_keeps_query_structure("SELECT $value$", {"$value$": b"13"}) is True
    assert _binding_keeps_query_structure("SELECT $value$", {"$value$": b"13", "unused": 79}) is True
    assert _binding_keeps_query_structure("%($statement$)s", {"$statement$": Statement()}) is False
    assert (
        _binding_keeps_query_structure(
            "%(statement)s, {$value$:String}",
            {"statement": Statement(), "$value$": b"13"},
        )
        is False
    )


@pytest.mark.parametrize(
    "query, expected",
    [
        ("insert_value", False),
        ("inserted_at", False),
        ("foo$insert", False),
        ("insert$foo", False),
        ("INSERT", True),
    ],
)
def test_contains_insert_bareword(query, expected):
    assert _contains_insert_bareword(query) is expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT ' INSERT INTO '", False),
        ("INSERT INTO tbl VALUES (13)", True),
        ("/* leading */ INSERT INTO tbl VALUES (13)", True),
        ("WITH ' INSERT INTO ' AS value SELECT value", False),
        ("WITH $doc$ INSERT INTO $doc$ AS value SELECT value", False),
        ("WITH value AS (SELECT 13) INSERT INTO tbl SELECT value", True),
        ("WITH 13 AS value /* INSERT INTO */ SELECT value", False),
        (
            "WITH value AS (SELECT 13) INSERT/* outer /* inner */ outer */\ufeffINTO tbl SELECT value",
            True,
        ),
        ("WITH $doc$/* INSERT */$doc$ AS value SELECT value", False),
        ("WITH {SELECT:Int32} AS value INSERT INTO tbl SELECT value", True),
        ("WITH {INSERT:Int32} AS value SELECT value", False),
        ("WITH 13SELECT AS value INSERT INTO tbl SELECT value", True),
        ("WITH 13INSERT AS value SELECT value", False),
        ("INSERT /* c */ INTO tbl VALUES (13)", True),
        ("INSERT tbl VALUES (13)", False),
        ("WITH insert AS (SELECT 13) SELECT * FROM insert", False),
        ("WITH 13 AS insert SELECT insert", False),
        ("WITH (SELECT 13) AS insert SELECT insert", False),
        ("WITH [13, insert] AS arr SELECT 13", False),
        ("WITH {p:Int32} AS insert SELECT insert", False),
        ("WITH 13 AS select INSERT INTO tbl SELECT 13", True),
        ("with x as (select 13) insert into tbl select 13", True),
    ],
)
def test_query_is_insert_ignores_non_sql_tokens(query, expected):
    assert _query_is_insert(query) is expected


@pytest.mark.parametrize("prefix", ["", "-- $hint$\n/* outer /* $nested$ */ comment */ "])
@pytest.mark.parametrize(
    "query",
    [
        "SELECT 13",
        "sElEcT 13; -- done\n /* comment */ ;",
        "/* outer /* nested */ comment */ SELECT 13",
        "-- note\n// another\n#! comment\nSELECT 13",
        "(SELECT 13 UNION ALL SELECT 79)",
        "WITH 13 AS value SELECT value",
        "WITH (13 + 79) AS value, [13, 79] AS values SELECT value, values",
        "WITH source AS (SELECT 13 AS value) SELECT * FROM source",
        "WITH RECURSIVE source AS (SELECT 13 AS value) SELECT * FROM source",
        "WITH 13 AS insert SELECT insert",
        "WITH select + 13 AS value SELECT value FROM source",
        "WITH 13 AS `select` SELECT `select`",
        "WITH 13 AS 13value SELECT 13value",
        "WITH 13 AS $value SELECT $value",
        "WITH 13 AS \u201cvalue\u201d SELECT \u201cvalue\u201d",
        "WITH 13 AS {alias:Identifier} SELECT {alias:Identifier}",
        "SELECT 'PARALLEL WITH INSERT INTO t SELECT 79; INTO OUTFILE'",
        "SELECT 'escaped\\' quote; PARALLEL WITH INSERT'",
        "SELECT $$PARALLEL WITH INSERT INTO t SELECT 79;$$",
        "SELECT $doc$INTO OUTFILE; PARALLEL WITH INSERT$doc$",
        'SELECT `PARALLEL WITH INSERT`, "INTO OUTFILE" FROM source',
        "SELECT {PARALLEL:UInt32}, {WITH:UInt32}",
        "SELECT 13 /* PARALLEL WITH INSERT */;",
        "SHOW TABLES",
        "SHOW CREATE TABLE source",
        "DESCRIBE TABLE source",
        "DESC SELECT 13",
        "EXISTS TABLE source",
        "EXPLAIN SELECT 13",
        "EXPLAIN PLAN SELECT 13",
        "EXPLAIN PIPELINE SELECT 13",
        "EXPLAIN AST SELECT 13",
        "EXPLAIN SYNTAX SELECT 13",
        "EXPLAIN QUERY TREE SELECT 13",
        "EXPLAIN ESTIMATE SELECT 13",
        "EXPLAIN indexes = 1 SELECT 13",
        "EXPLAIN PLAN indexes = 1, actions = 1 SELECT 13",
        "EXPLAIN PIPELINE graph = 1 SELECT 13",
        "EXPLAIN QUERY TREE run_passes = 0 WITH 13 AS value SELECT value",
        "EXPLAIN (SELECT 13)",
        "EXPLAIN PLAN (SELECT 13)",
        "EXPLAIN indexes = 1 (SELECT 13)",
        b"SELECT '\xff'",
    ],
)
def test_read_only_query_retry_eligibility(query, prefix):
    assert _query_is_read_only(prefix.encode() + query if isinstance(query, bytes) else prefix + query)


@pytest.mark.parametrize(
    "query",
    [
        "",
        "-- comment",
        "INSERT INTO target SELECT 13",
        "INSERT INTO target VALUES (13)",
        "WITH source AS (SELECT 13 AS value) INSERT INTO target SELECT * FROM source",
        "WITH 13 AS select INSERT INTO target SELECT 13",
        "WITH select + 13 AS value INSERT INTO target SELECT value FROM source",
        "WITH {SELECT:UInt32} AS value INSERT INTO target SELECT value",
        "CREATE TABLE target AS SELECT 13",
        "DROP TABLE target",
        "ALTER TABLE target UPDATE value = 79 WHERE value = 13",
        "DELETE FROM target WHERE value = 13",
        "TRUNCATE TABLE target",
        "EXCHANGE TABLES target AND source",
        "RENAME TABLE source TO target",
        "SYSTEM FLUSH LOGS",
        "SET max_threads = 13",
        "USE default",
        "CHECK TABLE target",
        "GRANT SELECT ON source TO user_1",
        "OPTIMIZE TABLE target",
        "BACKUP TABLE source TO Disk('backups', 'data')",
        "UNKNOWN STATEMENT SELECT 13",
        "SELECT 13; INSERT INTO target SELECT 79",
        "SELECT 13; SELECT 79",
        "SELECT 13 PARALLEL WITH INSERT INTO target SELECT 79",
        "SELECT 13 PARALLEL /* comment */ WITH CREATE TABLE target (value UInt32) ENGINE = Memory",
        "SHOW TABLES PARALLEL WITH TRUNCATE TABLE target",
        "WITH 13 AS value SELECT value PARALLEL WITH INSERT INTO target SELECT 79",
        "SELECT 13 INTO OUTFILE 'result.csv'",
        "SHOW TABLES INTO /* comment */ OUTFILE 'result.csv'",
        "EXPLAIN PIPELINE INSERT INTO target SELECT 13",
        "EXPLAIN PLAN indexes = 1 INSERT INTO target SELECT 13",
        "SELECT 'unterminated PARALLEL WITH INSERT INTO target SELECT 79",
        "SELECT 13 /* unterminated PARALLEL WITH INSERT INTO target SELECT 79",
        "SELECT (13; INSERT INTO target SELECT 79",
        "SELECT 13) PARALLEL WITH INSERT INTO target SELECT 79",
    ],
)
def test_write_or_unknown_query_is_not_retryable(query):
    assert not _query_is_read_only(query)


@pytest.mark.parametrize("as_bytes", [False, True])
@pytest.mark.parametrize(
    "query",
    [
        "INSERT INTO target FORMAT JSONEachRow\n" + '{"value":"$payload$13$payload$"}\n' * 13,
        "/* $hint$ */ INSERT INTO target VALUES " + ", ".join(["('$payload$13$payload$')"] * 13),
        "-- $hint$\nUNKNOWN STATEMENT " + "$payload$13$payload$ " * 13,
    ],
    ids=["json_data", "values_data", "unknown"],
)
def test_retry_classifier_skips_heredoc_scan_for_non_read_statements(monkeypatch, query, as_bytes):
    scanner = Mock(wraps=binding._heredoc_start_re)
    monkeypatch.setattr(binding, "_heredoc_start_re", scanner)

    assert not _query_is_read_only(query.encode() if as_bytes else query)

    scanner.finditer.assert_not_called()
