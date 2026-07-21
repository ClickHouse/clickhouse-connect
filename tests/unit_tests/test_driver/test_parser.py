from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.binding import quote_identifier
from clickhouse_connect.driver.common import unescape_identifier
from clickhouse_connect.driver.parser import parse_callable, parse_enum
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


def test_parse_enum():
    assert parse_enum("Enum8('one' = 1)") == (("one",), (1,))
    assert parse_enum("Enum16('**\\'5' = 5, '578' = 7)") == (("**'5", "578"), (5, 7))


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
    assert ch_type.name == "Variant(UInt64, String, Array(UInt64))"


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
    assert remove_sql_comments(sql) == "SELECT \n* FROM benchmark_results  WHERE result = 'True'\n\nLIMIT\n\n2\n\n"


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
