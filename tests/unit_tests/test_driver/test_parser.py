import pytest

from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.binding import _format_identifier, format_str, quote_identifier
from clickhouse_connect.driver.common import unescape_identifier
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.parser import parse_callable, parse_columns, parse_enum
from clickhouse_connect.driver.query import remove_sql_comments


def test_parse_callable():
    assert parse_callable("CALLABLE(1, 5)") == ("CALLABLE", (1, 5), "")
    assert parse_callable("Enum4('v1' = 5) other stuff") == ("Enum4", ("'v1'= 5",), "other stuff")
    assert parse_callable("BareThing") == ("BareThing", (), "")
    assert parse_callable("Tuple(Tuple (String), Int32)") == ("Tuple", ("Tuple(String)", "Int32"), "")
    assert parse_callable("ReplicatedMergeTree('/clickhouse/tables/test', '{replica'}) PARTITION BY key") == (
        "ReplicatedMergeTree",
        ("'/clickhouse/tables/test'", "'{replica'}"),
        "PARTITION BY key",
    )
    assert parse_callable("JSON(`a)b` UInt32)") == ("JSON", ("`a)b` UInt32",), "")
    assert parse_callable("JSON(`a``(b` UInt32)") == ("JSON", ("`a``(b` UInt32",), "")


@pytest.mark.parametrize("separator", ["\t", "\n", "\r\n"])
def test_parse_columns_accepts_sql_whitespace_after_skip(separator):
    assert parse_columns(f"(SKIP{separator}path)", preserve_names=True) == (("SKIP",), ("path",))
    assert parse_columns(f"(SKIP{separator}REGEXP 'x')", preserve_names=True) == (("SKIP",), ("REGEXP 'x'",))


def test_parse_enum():
    assert parse_enum("Enum8('one' = 1)") == (("one",), (1,))
    assert parse_enum("Enum16('**\\'5' = 5, '578' = 7)") == (("**'5", "578"), (5, 7))


@pytest.mark.parametrize(
    "expr, expected",
    [
        ("Enum8('a''b' = 1)", (("a'b",), (1,))),
        ("Enum8('''' = 1)", (("'",), (1,))),
        ("Enum16('user''1' = 1, 'user_2' = 2)", (("user'1", "user_2"), (1, 2))),
        ("Enum8('a\\'b' = 1)", (("a'b",), (1,))),
        ("Enum16('**\\'5' = 5, '578' = 7)", (("**'5", "578"), (5, 7))),
    ],
)
def test_parse_enum_quote_escapes(expr, expected):
    assert parse_enum(expr) == expected


@pytest.mark.parametrize(
    "type_name, expected_name",
    [
        ("Enum('low' = -128, 'high' = 127)", "Enum8('low' = -128, 'high' = 127)"),
        ("Enum('low' = -129, 'high' = 128)", "Enum16('low' = -129, 'high' = 128)"),
        ("Enum('low' = -32768, 'high' = 32767)", "Enum16('low' = -32768, 'high' = 32767)"),
    ],
)
def test_generic_enum_selects_server_storage_family(type_name, expected_name):
    assert get_from_name(type_name).name == expected_name


ESCAPED_ENUM = "Enum8('user\\'1' = 1, 'user_2' = 2)"
REPEATED_NON_FIRST_ENUM = "Enum16('user_1' = 1, 'user\\'2\\'3' = 2)"
# Server-emitted enum names escape a backslash as \\ (v26.6.1.1193, DataTypeEnum
# generateName -> writeQuotedString).
BACKSLASH_ENUM = "Enum8('a\\\\b' = 1, 'user_2' = 2)"
TRAILING_BACKSLASH_ENUM = "Enum8('user\\\\' = 1, 'user_2' = 2)"


def test_parse_columns_escaped_quote_in_scalar_enum():
    assert parse_columns(f"({ESCAPED_ENUM}, String)") == (
        (),
        (ESCAPED_ENUM, "String"),
    )


def test_parse_columns_can_preserve_identifier_quoting():
    assert parse_columns("(`SKIP` UInt32, SKIP `ignored`, `a\\`b` String)", preserve_names=True) == (
        ("`SKIP`", "SKIP", "`a\\`b`"),
        ("UInt32", "`ignored`", "String"),
    )
    assert parse_columns("(SKIP REGEXP 'a' , SKIP REGEXPfoo )", preserve_names=True) == (
        ("SKIP", "SKIP"),
        ("REGEXP 'a'", "REGEXPfoo"),
    )
    assert parse_columns("(SKIP REGEXP 'a' 'b')", preserve_names=True) == (("SKIP",), ("REGEXP 'a' 'b'",))
    assert parse_columns("(SKIP  `a`, SKIP   REGEXP 'x')", preserve_names=True) == (
        ("SKIP", "SKIP"),
        ("`a`", "REGEXP 'x'"),
    )
    assert parse_columns("(SKIP REGEXP.foo)", preserve_names=True) == (("SKIP",), ("REGEXP.foo",))


@pytest.mark.parametrize(
    "type_name, expected_name",
    [
        (ESCAPED_ENUM, ESCAPED_ENUM),
        (f"Array({ESCAPED_ENUM})", f"Array({ESCAPED_ENUM})"),
        (f"Tuple({ESCAPED_ENUM}, UInt32)", f"Tuple({ESCAPED_ENUM}, UInt32)"),
        (f"Array(Tuple({ESCAPED_ENUM}, UInt32))", f"Array(Tuple({ESCAPED_ENUM}, UInt32))"),
        (f"Nullable({ESCAPED_ENUM})", f"Nullable({ESCAPED_ENUM})"),
        (f"Variant({ESCAPED_ENUM}, String)", f"Variant({ESCAPED_ENUM}, String)"),
        (f"Nested(status {ESCAPED_ENUM}, value UInt32)", f"Nested(status {ESCAPED_ENUM}, value UInt32)"),
        (f"JSON(status {ESCAPED_ENUM})", f"JSON(`status` {ESCAPED_ENUM})"),
        (f"Tuple({REPEATED_NON_FIRST_ENUM}, UInt32)", f"Tuple({REPEATED_NON_FIRST_ENUM}, UInt32)"),
        (BACKSLASH_ENUM, BACKSLASH_ENUM),
        (f"Tuple({TRAILING_BACKSLASH_ENUM}, UInt32)", f"Tuple({TRAILING_BACKSLASH_ENUM}, UInt32)"),
    ],
)
def test_get_from_name_preserves_escaped_quote_in_enum(type_name, expected_name):
    assert get_from_name(type_name).name == expected_name


def test_unescape_identifier():
    # Plain and single backtick-quoted identifiers are unchanged.
    assert unescape_identifier("directory") == "directory"
    assert unescape_identifier("`directory`") == "directory"
    # An unquoted dotted path is preserved as-is.
    assert unescape_identifier("a.b.c") == "a.b.c"
    # A single identifier that literally contains a dot keeps the dot.
    assert unescape_identifier("`weird.name`") == "weird.name"
    # Compound backtick-quoted identifiers (the wire form of a Nested sub-column)
    # must lose every backtick, not just the outermost pair.
    assert unescape_identifier("`directory`.`id`") == "directory.id"
    # A literal backtick inside a quoted part is escaped either by doubling it
    # or with a backslash. The server accepts both forms and quote_identifier
    # emits the backslash form, so both must reverse to the single backtick the
    # column name actually contains (verified against the server: `a``b` and
    # `a\`b` both name the column a`b).
    assert unescape_identifier("`a``b`") == "a`b"
    assert unescape_identifier("`a\\`b`") == "a`b"
    # A backslash escapes the following character, so a doubled backslash is one
    # literal backslash.
    assert unescape_identifier("`a\\\\b`") == "a\\b"
    assert unescape_identifier('"a""b"') == 'a"b'
    assert unescape_identifier('"a\\"b"') == 'a"b'


def test_unescape_dotted_backtick_identifier():
    # Reproduction from clickhouse-go#1587 (Python sibling): the column list of a
    # Nested INSERT round-trips through parse_callable + unescape_identifier.
    column_list = "(`directory`.`id`,`directory`.`type`,`directory`.`path`)"
    _, cols, _ = parse_callable(column_list)
    assert [unescape_identifier(c) for c in cols] == ["directory.id", "directory.type", "directory.path"]


def test_unescape_identifier_inverts_quote_identifier():
    # unescape_identifier reverses the quoting that quote_identifier applies, so
    # quote_identifier -> unescape_identifier is the identity even for names that
    # contain the characters quote_identifier escapes (backticks and backslashes).
    for name in ["simple", "directory.id", "weird.name", "a`b", "a`", "`a", "a\\b", "a\\", "a``b"]:
        assert unescape_identifier(quote_identifier(name)) == name


def test_map_type():
    ch_type = get_from_name("Map(String, Decimal(5, 5))")
    assert ch_type.name == "Map(String, Decimal(5, 5))"


def test_variant_type():
    ch_type = get_from_name("Variant(UInt64, String, Array(UInt64))")
    assert ch_type.name == "Variant(Array(UInt64), String, UInt64)"
    assert ch_type.type_def.values == ("Array(UInt64)", "String", "UInt64")
    assert ch_type == get_from_name("Variant(Array(UInt64), String, UInt64)")


@pytest.mark.parametrize(
    "type_name, expected_name, tagged_type",
    [
        ("Variant(UInt32, UInt32)", "Variant(UInt32)", "UInt32"),
        ("Variant(Decimal32(2), Decimal(9, 2))", "Variant(Decimal(9, 2))", "Decimal32(2)"),
    ],
)
def test_variant_deduplicates_canonical_member_names(type_name, expected_name, tagged_type):
    ch_type = get_from_name(type_name)

    assert ch_type.name == expected_name
    assert len(ch_type.element_types) == 1
    assert ch_type._resolve_disc(typed_variant(13, tagged_type)) == (0, 13)


def test_empty_tuple_uses_server_canonical_name():
    tuple_type = get_from_name("Tuple()")

    assert get_from_name("Tuple").name == "Tuple()"
    assert tuple_type.name == "Tuple()"
    assert tuple_type.read_column_data(None, 3, None, []) == ((), (), ())


@pytest.mark.parametrize(
    "type_name, expected_name",
    [
        ("Array(Variant(UInt32, String))", "Array(Variant(String, UInt32))"),
        ("Map(String, Variant(UInt32, String))", "Map(String, Variant(String, UInt32))"),
        ("Tuple(Variant(UInt32, String), UInt8)", "Tuple(Variant(String, UInt32), UInt8)"),
        ("Tuple(v Variant(UInt32, String), n Nullable(UInt8))", "Tuple(`v` Variant(String, UInt32), `n` Nullable(UInt8))"),
        (
            "SimpleAggregateFunction(any, Variant(UInt32, String))",
            "SimpleAggregateFunction(any, Variant(String, UInt32))",
        ),
        ("AggregateFunction(any, Variant(UInt32, String))", "AggregateFunction(any, Variant(String, UInt32))"),
        (
            "Map(String, Nested(`path one` Variant(UInt32, String)))",
            "Map(String, Nested(`path one` Variant(String, UInt32)))",
        ),
    ],
)
def test_variant_order_is_canonical_through_containers(type_name, expected_name):
    ch_type = get_from_name(type_name)

    assert ch_type.name == expected_name
    assert get_from_name(expected_name) == ch_type


@pytest.mark.parametrize(
    "type_name, expected_name, expected_values",
    [
        ("Array(Decimal32(2))", "Array(Decimal(9, 2))", ("Decimal32(2)",)),
        ("Map(String, Decimal32(2))", "Map(String, Decimal32(2))", ("String", "Decimal32(2)")),
        ("Tuple(Decimal32(2), UInt8)", "Tuple(Decimal32(2), UInt8)", ("Decimal32(2)", "UInt8")),
        ("Nested(value Decimal32(2))", "Nested(value Decimal(9, 2))", ("Decimal32(2)",)),
        (
            "SimpleAggregateFunction(any, Decimal32(2))",
            "SimpleAggregateFunction(any, Decimal32(2))",
            ("any", "Decimal32(2)"),
        ),
        (
            "AggregateFunction(any, Decimal32(2))",
            "AggregateFunction(any, Decimal32(2))",
            ("any", "Decimal32(2)"),
        ),
        (
            "Map(String, Nested(`path one` Decimal32(2)))",
            "Map(String, Nested(`path one` Decimal32(2)))",
            ("String", "Nested(`path one` Decimal32(2))"),
        ),
        (
            "Tuple(Nested(`path one` Decimal32(2)), UInt8)",
            "Tuple(Nested(`path one` Decimal32(2)), UInt8)",
            ("Nested(`path one` Decimal32(2))", "UInt8"),
        ),
        (
            "AggregateFunction(any, Nested(`path one` Decimal32(2)))",
            "AggregateFunction(any, Nested(`path one` Decimal32(2)))",
            ("any", "Nested(`path one` Decimal32(2))"),
        ),
    ],
)
def test_non_variant_nested_alias_spelling_is_preserved(type_name, expected_name, expected_values):
    ch_type = get_from_name(type_name)

    assert ch_type.name == expected_name
    assert ch_type.type_def.values == expected_values


def test_simple_aggregate_function_defers_non_numpy_time64_dtype():
    ch_type = get_from_name("SimpleAggregateFunction(any, Time64(2))")

    assert ch_type.name == "SimpleAggregateFunction(any, Time64(2))"
    with pytest.raises(ProgrammingError, match="numpy or Pandas"):
        _ = ch_type.np_type


def test_json_type():
    names = [
        "JSON",
        "JSON(max_dynamic_paths=100, a.b UInt32, SKIP `a.e`)",
        "JSON(max_dynamic_types = 55, SKIP REGEXP 'a[efg]')",
        "JSON(max_dynamic_types = 33, `a.b` UInt64, b.c String)",
    ]
    parsed = [
        "JSON",
        "JSON(max_dynamic_paths = 100, `a.b` UInt32, SKIP `a.e`)",
        "JSON(max_dynamic_types = 55, SKIP REGEXP 'a[efg]')",
        "JSON(max_dynamic_types = 33, `a.b` UInt64, `b.c` String)",
    ]
    for name, x in zip(names, parsed):
        ch_type = get_from_name(name)
        assert x == ch_type.name


def test_json_type_discards_repeated_skip_separator_whitespace():
    ch_type = get_from_name("JSON(SKIP  `a`, SKIP   REGEXP 'x')")

    assert ch_type.skip_paths == ["a"]
    assert ch_type.skip_regexps == ["x"]
    assert ch_type.name == "JSON(SKIP `a`, SKIP REGEXP 'x')"


@pytest.mark.parametrize(
    "type_name, expected_name",
    [
        ("JSON(`SKIP` UInt32)", "JSON(`SKIP` UInt32)"),
        ("JSON(SKIP `SKIP`)", "JSON(SKIP `SKIP`)"),
        ("JSON(`a``b` UInt32)", "JSON(`a\\`b` UInt32)"),
        ("JSON(`a\\`b` UInt32)", "JSON(`a\\`b` UInt32)"),
        (
            "JSON(max_dynamic_paths = 1024, max_dynamic_types = 32, z UInt64, a String, "
            "SKIP z, SKIP a, SKIP z, SKIP REGEXP 'z.*', SKIP REGEXP 'a\\\\d', SKIP REGEXP 'z.*')",
            "JSON(`a` String, `z` UInt64, SKIP `a`, SKIP `z`, SKIP REGEXP 'a\\\\d', SKIP REGEXP 'z.*', SKIP REGEXP 'z.*')",
        ),
    ],
)
def test_json_type_canonicalizes_quoted_paths_and_directives(type_name, expected_name):
    assert get_from_name(type_name).name == expected_name


def test_json_type_preserves_literal_boundary_quotes_in_paths():
    double_typed = '"typed"'
    double_skipped = '"skipped"'
    type_name = (
        f"JSON({_format_identifier('`typed`')} UInt32, {_format_identifier(double_typed)} String, "
        f"SKIP {_format_identifier('`skipped`')}, SKIP {_format_identifier(double_skipped)})"
    )

    ch_type = get_from_name(type_name)
    assert ch_type.typed_paths == ['"typed"', "`typed`"]
    assert ch_type.skip_paths == ['"skipped"', "`skipped`"]
    reflected = get_from_name(ch_type.name)
    assert reflected.typed_paths == ch_type.typed_paths
    assert reflected.skip_paths == ch_type.skip_paths


def test_json_type_parses_complex_skip_regexps():
    regexps = ["space value", "comma,value", "paren(value)", "quote'value", r"back\\slash"]
    type_name = f"JSON({', '.join(f'SKIP REGEXP {format_str(regexp)}' for regexp in regexps)})"

    ch_type = get_from_name(type_name)
    assert ch_type.skip_regexps == sorted(regexps)
    assert ch_type.name == f"JSON({', '.join(f'SKIP REGEXP {format_str(regexp)}' for regexp in sorted(regexps))})"


@pytest.mark.parametrize(
    "regexp",
    ["a' = b", "a')", "x)y", "nested, value"],
)
def test_json_type_preserves_regexp_literal_boundaries(regexp):
    type_name = f"JSON(SKIP REGEXP {format_str(regexp)})"
    ch_type = get_from_name(type_name)

    assert ch_type.skip_regexps == [regexp]
    assert get_from_name(ch_type.name).skip_regexps == [regexp]


@pytest.mark.parametrize(
    "type_name, expected_regexp",
    [
        ("JSON(`a` JSON(SKIP REGEXP 'x)y'))", "x)y"),
        ("JSON(`a` json(SKIP REGEXP 'x)y'))", "x)y"),
        ("JSON(`a` Array(JSON(SKIP REGEXP 'x)y')))", "x)y"),
        ("JSON(`a` Map(String, JSON(SKIP REGEXP 'x)y')))", "x)y"),
    ],
)
def test_json_type_parses_nested_regexp_literals(type_name, expected_regexp):
    ch_type = get_from_name(type_name)
    nested = ch_type.typed_types[0]
    while nested.base_type in ("Array", "Map"):
        nested = nested.element_type if nested.base_type == "Array" else nested.value_type

    assert nested.skip_regexps == [expected_regexp]
    assert get_from_name(ch_type.name).name == ch_type.name


@pytest.mark.parametrize("path", ["a` b", "a` b,c", "a` b)c"])
def test_json_type_round_trips_backtick_before_delimiters(path):
    type_name = f"JSON({_format_identifier(path)} UInt32)"
    ch_type = get_from_name(type_name)

    assert ch_type.typed_paths == [path]
    assert get_from_name(ch_type.name).typed_paths == [path]


def test_json_skip_paths_starting_with_regexp_are_not_directives():
    ch_type = get_from_name("JSON(SKIP REGEXPfoo, SKIP REGEXP_foo)")
    assert ch_type.skip_paths == ["REGEXP_foo", "REGEXPfoo"]
    assert ch_type.skip_regexps == []


@pytest.mark.parametrize(
    "type_name, expected_path",
    [
        ("JSON(SKIP `REGEXP`.`foo`)", "REGEXP.foo"),
        ("JSON(SKIP REGEXP_foo.foo)", "REGEXP_foo.foo"),
    ],
)
def test_json_skip_paths_distinguish_regexp_keyword_component(type_name, expected_path):
    ch_type = get_from_name(type_name)

    assert ch_type.skip_paths == [expected_path]
    assert get_from_name(ch_type.name).skip_paths == [expected_path]


def test_remove_comments():
    sql = """SELECT -- 6dcd92a04feb50f14bbcf07c661680ba
* FROM benchmark_results /*With an inline comment */ WHERE result = 'True'
/*  A single line */
LIMIT
/*  A multiline comment
   
*/
2
-- 6dcd92a04feb50f14bbcf07c661680ba
"""
    # a block comment leaves a single space behind, a line comment leaves the newline that ended it
    assert remove_sql_comments(sql) == "SELECT \n* FROM benchmark_results   WHERE result = 'True'\n \nLIMIT\n \n2\n\n"


def test_remove_comments_no_space_after_dashes():
    # leading `--sql` comment at start of input
    assert remove_sql_comments("--sql\nSELECT 1") == "\nSELECT 1"
    # mid-query comment with no space after the dashes
    assert remove_sql_comments("SELECT 1--1") == "SELECT 1"
    # comment running to end of input with no trailing newline
    assert remove_sql_comments("SELECT 1 --done") == "SELECT 1 "
    # `--` inside quoted strings is preserved
    assert remove_sql_comments("SELECT 'a--b'") == "SELECT 'a--b'"
    assert remove_sql_comments('SELECT "a--b"') == 'SELECT "a--b"'


@pytest.mark.parametrize(
    "sql, expected",
    [
        # a block comment separates the tokens around it for the server, so it cannot be dropped
        ("SELECT/*c*/number FROM numbers(9)", "SELECT number FROM numbers(9)"),
        ("SELECT number FROM numbers(9)/*c*/LIMIT 1", "SELECT number FROM numbers(9) LIMIT 1"),
        ("SELECT number FROM numbers(9) LIMIT/*c*/0", "SELECT number FROM numbers(9) LIMIT 0"),
        ("INSERT/*c*/INTO/*c*/t VALUES", "INSERT INTO t VALUES"),
        ("SELECT/*\nmultiline\n*/1", "SELECT 1"),
        ("SELECT/*a*//*b*/1", "SELECT  1"),
        # a comment at either end of the query separates it from nothing, but is still a separator
        ("/*c*/SELECT 1", " SELECT 1"),
        ("SELECT 1/*c*/", "SELECT 1 "),
        # a line comment ends at its newline, which is kept, so it needs no replacement
        ("SELECT\n--c\n1", "SELECT\n\n1"),
        ("SELECT 1--c", "SELECT 1"),
        # a block comment inside a quoted string or identifier is not a comment
        ("SELECT '/*c*/'", "SELECT '/*c*/'"),
        ('SELECT "/*c*/"', 'SELECT "/*c*/"'),
    ],
)
def test_remove_comments_separates_tokens(sql: str, expected: str):
    assert remove_sql_comments(sql) == expected
