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
        # A bare trailing semicolon is stripped, matching the historical behavior.
        ("SELECT 13", "SELECT 13"),
        ("SELECT 13;", "SELECT 13"),
        ("SELECT 13;;", "SELECT 13"),
        # Whitespace or comments after the semicolon no longer leave it in place (issue #903).
        ("SELECT 13;\n", "SELECT 13"),
        ("SELECT 13; ", "SELECT 13"),
        ("SELECT 13;\t", "SELECT 13"),
        ("SELECT 13;\r\n", "SELECT 13"),
        ("SELECT 13; -- done", "SELECT 13"),
        ("SELECT 13; /* done */", "SELECT 13"),
        ("SELECT 13 ;\n  /* a */ -- b\n", "SELECT 13 "),
        ("SELECT 13; /* spans\nlines */", "SELECT 13"),
        # A comment before the terminator is preserved.
        ("SELECT 13 /* keep */;", "SELECT 13 /* keep */"),
        # No trailing terminator, so the query is returned unchanged.
        ("SELECT 13\n", "SELECT 13\n"),
        ("SELECT 13 -- trailing", "SELECT 13 -- trailing"),
        ("SELECT 13 /* trailing */", "SELECT 13 /* trailing */"),
        # A semicolon inside a string literal or a comment is not a terminator.
        ("SELECT '13;'", "SELECT '13;'"),
        ("SELECT '13;';", "SELECT '13;'"),
        ("SELECT 13 -- note;", "SELECT 13 -- note;"),
    ],
)
def test_strip_trailing_semicolon(query, expected):
    assert strip_trailing_semicolon(query) == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SELECT 13;", "SELECT 13"),
        ("SELECT 13;\n", "SELECT 13"),
        ("SELECT 13; -- done", "SELECT 13"),
        ("SELECT 13; /* done */", "SELECT 13"),
        ("SELECT 13 -- keep", "SELECT 13 -- keep"),
    ],
)
def test_binding_entry_points_strip_trailing_terminator(query, expected):
    # Both binding entry points feed context.final_query, to which a FORMAT or LIMIT clause is
    # later appended, so both must drop a trailing terminator.
    assert finalize_query(query, None) == expected
    assert bind_query(query, None) == (expected, {})
