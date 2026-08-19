import pytest

from clickhouse_connect.driver.binding import (
    MAX_URL_BIND_PARAM_LENGTH,
    _binding_keeps_query_structure,
    _contains_insert_bareword,
    _query_is_insert,
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
