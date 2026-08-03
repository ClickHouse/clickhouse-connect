import time

import pytest

from clickhouse_connect.driver.binding import (
    MAX_URL_BIND_PARAM_LENGTH,
    bind_query,
    finalize_query,
    quote_identifier,
    strip_trailing_semicolon,
    use_form_encoding,
)


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
        # A bare trailing semicolon is stripped, as it always was
        ("SELECT 13;", "SELECT 13"),
        ("SELECT 13;;", "SELECT 13"),
        # Whitespace after the semicolon no longer leaves it in place
        ("SELECT 13;\n", "SELECT 13"),
        ("SELECT 13; ", "SELECT 13"),
        ("SELECT 13;\t", "SELECT 13"),
        ("SELECT 13;\r\n", "SELECT 13"),
        ("SELECT 13;\x0b\x0c", "SELECT 13"),
        # Every comment form the server accepts after the semicolon
        ("SELECT 13; -- done", "SELECT 13"),
        ("SELECT 13; // done", "SELECT 13"),
        ("SELECT 13; # done", "SELECT 13"),
        ("SELECT 13; #!done", "SELECT 13"),
        ("SELECT 13; /* done */", "SELECT 13"),
        ("SELECT 13; /* spans\nlines */", "SELECT 13"),
        # Block comments nest, so the terminator run ends at the outer "*/"
        ("SELECT 13; /* outer /* inner */ outer */", "SELECT 13"),
        ("SELECT 13 ;\n /* a */ -- b\n\t// c\n # d\n", "SELECT 13 "),
        # Text before the terminator is preserved, including comments
        ("SELECT 13 /* keep */;", "SELECT 13 /* keep */"),
        ("SELECT 13 /* outer /* inner */ keep */;", "SELECT 13 /* outer /* inner */ keep */"),
        ("SELECT 13 -- keep\n;", "SELECT 13 -- keep\n"),
        # "#trailing" is not a comment, so the ";" after it is still the terminator
        ("SELECT 13 #trailing;", "SELECT 13 #trailing"),
        # Quoted tokens end where the server ends them, so the following ";" is a terminator
        ("SELECT 'quote '' and semicolon;';", "SELECT 'quote '' and semicolon;'"),
        ("SELECT 'backslash \\' and semicolon;';", "SELECT 'backslash \\' and semicolon;'"),
        ("SELECT 13 AS `col``quoted`;", "SELECT 13 AS `col``quoted`"),
        ('SELECT 13 AS "col""quoted";', 'SELECT 13 AS "col""quoted"'),
        # Heredocs hold unescaped quotes and semicolons
        ("SELECT $$it's;$$;\n", "SELECT $$it's;$$"),
        ("SELECT $doc$a;b$doc$; -- done", "SELECT $doc$a;b$doc$"),
    ],
)
def test_strip_trailing_semicolon(query, expected):
    assert strip_trailing_semicolon(query) == expected


@pytest.mark.parametrize(
    "query",
    [
        # Nothing to strip, so the query reaches the server byte for byte
        "SELECT 13",
        "SELECT 13\n",
        "SELECT 13 -- trailing",
        "SELECT 13 // trailing",
        "SELECT 13 # trailing",
        "SELECT 13 /* trailing */",
        "SELECT 13 /* outer /* inner */ outer */",
        # A semicolon inside a comment, a string literal or a quoted identifier is not a terminator
        "SELECT 13 -- note;",
        "SELECT 13 /* note; */",
        "SELECT '13;'",
        "SELECT 'quote '' and semicolon;'",
        "SELECT 'backslash \\' and semicolon;'",
        'SELECT 13 AS "col;"',
        "SELECT 13 AS `col;`",
        # A second statement follows, so the run is not a trailing terminator
        "SELECT 13; SELECT 79",
        # "#" is only a comment when followed by a space or "!", so these tails are not terminator runs
        "SELECT 13; #trailing",
        "SELECT 13; #",
        # A semicolon inside a heredoc is not a terminator, quotes and all
        "SELECT $$it's;$$",
        "SELECT $doc$a;b$doc$",
        # Unterminated comments, quotes and heredocs are left for the server to reject
        "SELECT 13; /* unterminated",
        "SELECT 13; /* outer /* inner */",
        "SELECT 13; /*/",
        "SELECT '13;",
        "SELECT 'trailing backslash;\\",
        "SELECT 13 AS `col;",
        "SELECT $$unterminated;",
        "SELECT $doc$a;b$other$",
    ],
)
def test_strip_trailing_semicolon_unchanged(query):
    assert strip_trailing_semicolon(query) == query


@pytest.mark.parametrize(
    "suffix",
    [
        ";" + " " * 100000,
        ";" + " -" * 50000,
        ";" + "/*" * 50000,
        ";" + "-" * 100000,
        ";" + "/* " + "a" * 100000,
        ";" + "'" * 100000,
        ";" + "$" * 100000,
    ],
)
def test_strip_trailing_semicolon_is_linear(suffix):
    # The scan is a single pass, so even a pathological suffix cannot blow up the run time
    query = "SELECT 13" + suffix
    start = time.perf_counter()
    strip_trailing_semicolon(query)
    assert time.perf_counter() - start < 5


@pytest.mark.parametrize(
    "query",
    [
        "SELECT %(val)s;",
        "SELECT %(val)s;\n",
        "SELECT %(val)s; -- done",
        "SELECT %(val)s; /* done */",
    ],
)
def test_finalize_query_strips_terminator(query):
    assert finalize_query(query, {"val": 13}) == "SELECT 13"


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT {val:UInt8};", "SELECT {val:UInt8}"),
        ("SELECT {val:UInt8};\n", "SELECT {val:UInt8}"),
        ("SELECT {val:UInt8}; -- done", "SELECT {val:UInt8}"),
        ("SELECT {val:UInt8}; /* done */", "SELECT {val:UInt8}"),
    ],
)
def test_bind_query_strips_terminator(query, expected):
    bound_query, bound_params = bind_query(query, {"val": 13})
    assert bound_query == expected
    assert bound_params == {"param_val": "13"}
