"""Live tests: the shim against a ClickHouse server.

They run only with CLICKHOUSE_TEST_RUNNER_LIVE=1, against the server at CLICKHOUSE_HOST (default localhost)
and CLICKHOUSE_PORT_HTTP (default 8123), such as the pinned server of README.md.
"""

import io
import os
import urllib.parse
import urllib.request
import uuid

import pytest
from clickhouse_test_runner.__main__ import main

LIVE_ENV = "CLICKHOUSE_TEST_RUNNER_LIVE"
HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
PORT = os.environ.get("CLICKHOUSE_PORT_HTTP", "8123")

pytestmark = pytest.mark.skipif(os.environ.get(LIVE_ENV) != "1", reason=f"set {LIVE_ENV}=1 to run the tests against a ClickHouse server")


def server_tsv(query: str) -> bytes:
    """Return the server's own TabSeparated text for a query."""
    url = "http" + "://" + f"{HOST}:{PORT}/?" + urllib.parse.urlencode({"default_format": "TabSeparated"})
    req = urllib.request.Request(url, data=query.encode(), method="POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req) as res:
        return res.read()


def run_shim(text: str | bytes, database: str = "default") -> tuple[bytes, str, int]:
    out, err = io.BytesIO(), io.StringIO()
    data = text if isinstance(text, bytes) else text.encode()
    code = main(["--host", HOST, "--port", PORT, "--database", database], io.BytesIO(data), out, err)
    return out.getvalue(), err.getvalue(), code


# A log_comment of this run, to find its own query_log rows.
RUN_ID = uuid.uuid4().hex


# Each expression is a SELECT list. For each one, the text that the shim writes from the values that
# clickhouse-connect decoded must be the same as the server's text.
EXPRESSIONS = [
    "toInt8(-1), toUInt8(255), toInt64(-9223372036854775808), toUInt64(18446744073709551615)",
    "toInt128(-170141183460469231731687303715884105728), toUInt256(115792089237316195423570985008687907853269984665640564039457584007913129639935)",
    "arrayJoin([1e15, 1e21, 1.5e-7, 0.1 + 0.2, nan, inf, -inf, -0., 5e-324, 1e-6, 123456789012345680000])",
    "toFloat32(0.1), toFloat32(3.4028235e38), toFloat32(-1.5e-7), toFloat32(16777217), toBFloat16(1.5), toBFloat16(0.1)",
    "toDecimal32(1.50, 2), toDecimal64(-0.001, 5), toDecimal128(100, 3), toDecimal256('1.5', 10), toDecimal32(0, 4)",
    "toDecimal128('-12345678901234567890123456789.123456789', 9), toDecimal256('1234567890123456789012345678901234567890.123456789012', 30)",
    "'a\\tb\\\\c\\nd''e\\0\\b\\f\\v\\x01\\r', toFixedString('ab', 4), '', 'Привет'",
    "true, false, NULL, CAST(NULL AS Nullable(String)), [NULL, 1], tuple(NULL)",
    "toDate('2020-01-02'), toDate32('1900-01-01'), toDate32('2299-12-31'), toDate('2149-06-06'), toDateTime('2020-01-01 10:00:00', 'UTC')",
    "toDateTime('2020-01-01 10:00:00', 'Asia/Tokyo'), toDateTime64('2020-01-01 00:00:00.120', 3), toDateTime64('2020-01-01 00:00:00', 9, 'Asia/Tokyo')",
    "toDateTime64('1900-01-01 00:00:00', 3, 'UTC'), toDateTime64('2299-12-31 23:59:59.999999', 6, 'UTC'), toDateTime64('1969-12-31 23:59:59.5', 1, 'UTC')",
    "toDateTime64('1677-09-21 00:12:44', 0, 'UTC'), toDateTime64('2262-04-11 23:47:17', 6, 'UTC'), toDateTime64('1960-05-05 10:00:00.123456', 6, 'Europe/Amsterdam')",
    "toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), toIPv4('1.2.3.4'), toIPv6('::ffff:1.2.3.4'), toIPv6('::1.2.3.4'), toIPv6('2001:db8::1'), toIPv6('::')",
    "[1, 2], ['a''b', NULL], [[1], []], [toDate('2020-01-01')], [toDateTime64('2020-01-01 00:00:00.5', 2)], [toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')]",
    "(1, 'x', NULL), CAST((1, 'x') AS Tuple(a Int32, b String)), [(1, 'a')], tuple(1), CAST(tuple(1) AS Tuple(`a b` UInt8))",
    "map('k', 1, 'a', 2), map(1, [2]), map('x', map('y', 1, 'z', 2)), [map('a', 1, 'b', 2)], (map('a', 1, 'b', 2), 1), map()",
    "CAST('a' AS Enum8('a' = 1, 'b' = 2)), [CAST('b' AS Enum16('a' = 1, 'b' = 2))], CAST('x''y' AS Enum8('x''y' = 1))",
    "toLowCardinality('x'), toLowCardinality(toNullable('y')), [toLowCardinality('z')], toLowCardinality(toNullable(NULL::Nullable(String)))",
    "INTERVAL 3 DAY, toIntervalSecond(-5), [INTERVAL 1 MONTH]",
    "toTime('12:34:56'), toTime('-1:02:03'), toTime64('12:00:00.5', 3), toTime64('100:00:00.123456', 6), toTime(3600000)",
    "(1.5, 2.5)::Point, [(1, 2), (3, 4)]::Ring, [[(1, 2), (3, 4)]]::Polygon, [[[(1, 2)]]]::MultiPolygon, [(1, 2)]::LineString",
    "number, toString(number), if(number % 2, NULL, number) FROM numbers(3)",
    "1::Variant(String, UInt64), 'a'::Variant(String, UInt64), NULL::Variant(String, UInt64), [1::Variant(String, Float64)]",
    "1::Dynamic, 'a'::Dynamic, NULL::Dynamic, toDate('2020-01-01')::Dynamic, [1, 2]::Array(UInt8)::Dynamic",
    'CAST(\'{"a":"x\\\\u0001/y\\\\t\\\\\\\\z\\\\"q \\\\u2028", "b":1.5, "f":[1,"a",null], "g":{"h":{"i":2}}, "l":"2020-01-01", "m":1e300}\' AS JSON)',
    'CAST(\'{"a":[1,2.5], "b":[[1],[2]], "c":"2020-01-01 00:00:00", "d":"2020-01-01 00:00:00.123", "f":[{"x":1}]}\' AS JSON), [CAST(\'{"a":"x"}\' AS JSON)]',
    "CAST('{\"a\":1.5}' AS JSON(a Decimal(10, 2), b Map(String, UInt8), c Tuple(x UInt8, y String), d UUID, e IPv4, f Int128, SKIP z))",
    # Go bug classes: a null-map byte other than 1, LowCardinality(Nullable) in a named tuple, tuples in
    # tuples, Nullable(Tuple), Int128 in a tuple, element names in backticks with spaces, Tuple().
    "if(number % 5, NULL, toInt8OrZero(toString(number))) FROM numbers(6)",
    "CAST(tuple(NULL) AS Tuple(x LowCardinality(Nullable(String)))), CAST(tuple(NULL, 'a') AS Tuple(x LowCardinality(Nullable(String)), y String))",
    "CAST(((1, 'a'), 2) AS Tuple(Tuple(UInt8, String), UInt8)), CAST((1, 'a') AS Nullable(Tuple(UInt8, String))), tuple(toInt128(-1), toUInt256(1))",
    "CAST((1, 2) AS Tuple(`a b` UInt8, `c(d)` UInt8)), tuple(), [tuple()]",
    "map((1, 2), 3), map(tuple(1), [2])",
    # DateTime64 outside 1677-09-21 .. 2262-04-11 (the int64 nanosecond range).
    "toDateTime64('1500-01-01 00:00:00.123', 3, 'UTC'), toDateTime64('2500-06-01 12:00:00.123456', 6, 'UTC'), toDateTime64('1200-03-04 05:06:07.5', 1, 'Asia/Tokyo')",
    # SimpleAggregateFunction is read as its value type, also for types that are not plain values.
    "CAST('{\"a\":1}'::JSON AS SimpleAggregateFunction(anyLast, JSON)), CAST(1::Dynamic AS SimpleAggregateFunction(anyLast, Dynamic))",
    "CAST(toIntervalSecond(5) AS SimpleAggregateFunction(anyLast, IntervalSecond)), CAST([1::Variant(String, UInt64)] AS SimpleAggregateFunction(groupArrayArray, Array(Variant(String, UInt64))))",
    # A column or alias named format is not the FORMAT clause.
    "1 AS format ORDER BY format DESC",
    "format FROM (SELECT 1 AS format)",
    # JSON paths in sorted order (the client gives the typed paths first), also in an Array(JSON) path.
    '\'{"d":1,"c":2,"a":3}\'::JSON(a UInt32, d UInt32), \'{"arr":[{"y":1,"x":2}],"b":{"z":1,"y":2}}\'::JSON(`b.z` UInt8)',
    # A key with a dot (escaped in the path): the server sorts the path with %2E.
    '\'{"a.b" : 42, "a" : {"b" : "x"}}\'::JSON SETTINGS json_type_escape_dots_in_keys = 1',
]

# Expressions that clickhouse-connect reads wrong. Each one names its group in allowlist.txt. The test
# fails when the client is fixed, so that the group is checked again.
CLIENT_BUGS = {
    "toDateTime64('1960-01-01 00:00:00.123456789', 9, 'UTC'), toDateTime64('2020-01-01 00:00:00.000000001', 9, 'UTC')": "dt64-sub-microsecond",
    "toTime64('00:00:01.1234567', 7)": "time64-sub-microsecond",
    "map('a', 1, 'a', 2)": "map-duplicate-keys",
    "map([1], 2)": "map-unhashable-key",
    "map(map(1, 2), 3)": "map-unhashable-key",
    "map(CAST(tuple(1) AS Tuple(z Int32)), 'a')": "map-unhashable-key",
    # Python date and datetime hold only the years 1 to 9999.
    "toDate32('0000-01-01')": "date-outside-python-range",
    "toDateTime64(-1e30, 6, 'UTC')": "date-outside-python-range",
    "reinterpret(toInt64(9223372036854775806), 'DateTime64(7)')": "date-outside-python-range",
    "CAST(1e20 AS DateTime64(6, 'Europe/Amsterdam'))": "date-outside-python-range",
    # A column name that is not valid UTF-8 (the server keeps the bytes of the literal in the name).
    "hex('\\xAA')": "column-name-invalid-utf8",
    # A String value that is not valid UTF-8 comes back as its hex text.
    "'\\xFF' AS x": "string-invalid-utf8",
    "sumState(1)": "type-aggregate-function",
    "[1, -2]::QBit(Int8, 2)": "qbit-int8",
    "'{\"a\": 1}'::JSON(SHARED a UInt64)": "type-json-shared-path",
    # A JSON path that is a value and also the prefix of other paths.
    '\'{"a": 1, "a.b": 2}\'::JSON': "json-leaf-and-object-path",
    '\'{"c0.c1": [], "c0": "x"}\'::JSON::Dynamic': "json-leaf-and-object-path",
    # Settings that change the layout of the Native output, which the client reads.
    "1 SETTINGS output_format_native_encode_types_in_binary_format = 1": "native-format-settings",
    "'{\"a\": 1}'::JSON SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization = 1": "native-format-settings",
    # The client sends cast_string_to_dynamic_use_inference = 1 with each query.
    "'[1, 2]'::Dynamic": "client-default-settings",
    # A Dynamic value comes without its type: JSON, a named Tuple and a Map are all dicts.
    "'{\"a\": 1}'::JSON::Dynamic": "dynamic-value-without-type",
    "toDateTime64('2020-01-01 00:00:00', 3, 'UTC')::Dynamic": "dynamic-value-without-type",
    # A Variant type in the shared variant of a Dynamic comes back as its binary encoding.
    "[1::Variant(String, UInt64)]::Dynamic(max_types=0)": "dynamic-shared-variant-raw-bytes",
    # QBit(T, dimension, stride): the stride is dropped and the values are wrong.
    "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]::QBit(Float32, 16, 8)": "qbit-stride",
    # A Decimal value with more digits than its precision (Decimal(38, 38) arithmetic) is rounded.
    "materialize(toDecimal128('0.42', 38)) + 21": "decimal-wider-than-precision",
    # Client.query adds FORMAT Native to the query text, and the server reads FORMAT as a column.
    "1,": "query-format-native",
}

# Scripts that clickhouse-connect runs wrong, with the output of clickhouse-client. Each one names its group
# in allowlist.txt. The test fails when the client is fixed, so that the group is checked again.
CLIENT_BUG_SCRIPTS = {
    # Client.query adds FORMAT Native, which is not valid after an expression alone or in another dialect.
    "SET implicit_select = 1; 1 + 2;": (b"3\n", "query-format-native"),
    "SET allow_experimental_kusto_dialect = 1, dialect = 'kusto'; print 1;": (b"1\n", "query-format-native"),
    # EXPLAIN AST explains the query with the added FORMAT Native, and the answer is text, not Native.
    "EXPLAIN AST SELECT 1;": (
        b"SelectWithUnionQuery (children 1)\n ExpressionList (children 1)\n  SelectQuery (children 1)\n"
        b"   ExpressionList (children 1)\n    Literal UInt64_1\n",
        "query-format-native",
    ),
    # Client.query takes the query text as str and encodes it as strict UTF-8.
    b"SELECT hex('\xff');": (b"FF\n", "query-text-invalid-utf8"),
    # The client sends its default settings with each request, so they replace a SET of the same name.
    "SET date_time_input_format = 'basic'; SELECT getSetting('date_time_input_format');": (b"basic\n", "client-default-settings"),
    # Client.query sends a statement that starts with a word of its command list (CHECK) through
    # Client.command: the rows come back as one row of text.
    "DROP TABLE IF EXISTS live_check_table;\n"
    "CREATE TABLE live_check_table (x UInt8) ENGINE = MergeTree ORDER BY x PARTITION BY x;\n"
    "INSERT INTO live_check_table VALUES (1), (2);\n"
    "CHECK TABLE live_check_table SETTINGS check_query_single_value_result = 0, max_threads = 1;\n"
    "DROP TABLE live_check_table;\n": (b"1_1_1_0\t1\t\n2_2_2_0\t1\t\n", "query-routed-to-command"),
    # Client.query sends wait_end_of_query=1, and the server reads the query in another way.
    "DROP TABLE IF EXISTS live_wait_end_of_query;\n"
    "CREATE TABLE live_wait_end_of_query (a UInt64, b UInt64, c UInt8) ENGINE = MergeTree ORDER BY tuple()"
    " SETTINGS min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;\n"
    "INSERT INTO live_wait_end_of_query SELECT number, number, number FROM numbers(10000);\n"
    "SYSTEM CLEAR MARK CACHE;\n"
    f"SELECT sum(b) FROM live_wait_end_of_query PREWHERE a < 5000 SETTINGS log_comment = 'live-{RUN_ID}';\n"
    "SYSTEM FLUSH LOGS query_log;\n"
    "SELECT ProfileEvents['FileOpen'] FROM system.query_log"
    f" WHERE log_comment = 'live-{RUN_ID}' AND type = 'QueryFinish' AND event_date >= yesterday();\n"
    "DROP TABLE live_wait_end_of_query;\n": (b"12497500\n4\n", "wait-end-of-query"),
}


@pytest.mark.parametrize("expr", EXPRESSIONS)
def test_render_matches_server(expr):
    query = f"SELECT {expr}"
    out, err, code = run_shim(query + ";")
    assert (code, err) == (0, "")
    assert out == server_tsv(query)


@pytest.mark.parametrize("expr, group", CLIENT_BUGS.items())
def test_known_client_bugs(expr, group):
    query = f"SELECT {expr}"
    out, err, code = run_shim(query + ";")
    assert out != server_tsv(query) or code != 0, f"[{group}] passes now: check the group in allowlist.txt"


@pytest.mark.parametrize("script, expected", CLIENT_BUG_SCRIPTS.items())
def test_known_client_bug_scripts(script, expected):
    output, group = expected
    out, err, code = run_shim(script)
    assert out != output or code != 0, f"[{group}] passes now: check the group in allowlist.txt"


def test_known_client_bug_startup_queries():
    # get_client runs its own queries in the database of the client. clickhouse-client runs none, so the
    # query_log of the database has only the user's query.
    database = f"live_startup_{RUN_ID}"
    server_tsv(f"CREATE DATABASE {database}")
    try:
        _, err, code = run_shim("SELECT 1;", database)
        assert (code, err) == (0, "")
        server_tsv("SYSTEM FLUSH LOGS query_log")
        count = server_tsv(
            f"SELECT count() FROM system.query_log WHERE current_database = '{database}' AND type = 'QueryFinish' AND event_date >= yesterday()"
        )
    finally:
        server_tsv(f"DROP DATABASE {database}")
    assert count != b"1\n", "[client-startup-queries] passes now: check the group in allowlist.txt"


def test_session_state():
    # SET, temporary tables and inline INSERT data last for the whole invocation, also after an error that
    # a test hint expects.
    script = "\n".join(
        [
            "SET max_threads = 3;",
            "SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }",
            "SELECT getSetting('max_threads');",
            "CREATE TEMPORARY TABLE t (x UInt8);",
            "INSERT INTO t VALUES (1), (2);",
            "INSERT INTO t FORMAT TSV",
            "3",
            "",
            "SELECT sum(x) FROM t;",
        ]
    )
    out, err, code = run_shim(script)
    assert (code, err) == (0, "")
    assert out == b"3\n6\n"


def test_session_timezone():
    # The values of a DateTime without a time zone are in the session_timezone of the session.
    out, err, code = run_shim(
        "SET session_timezone = 'Asia/Tokyo';\n"
        "SELECT toDateTime('2020-01-01 00:00:00'), toDateTime64('2020-01-01 00:00:00.5', 1), timezone(), now() > '2000-01-01';"
    )
    assert (code, err) == (0, "")
    assert out == b"2020-01-01 00:00:00\t2020-01-01 00:00:00.5\tAsia/Tokyo\t1\n"


def test_errors():
    # An unexpected error stops the script, writes the error name to stderr, and sets the exit code to the
    # error code.
    out, err, code = run_shim("SELECT 1; SELECT * FROM no_such_table_xyz; SELECT 2;")
    assert (out, code) == (b"1\n", 60)
    assert "UNKNOWN_TABLE" in err
    # A query that succeeds although the hint expects an error.
    _, err, code = run_shim("SELECT 1; -- { serverError UNKNOWN_TABLE }")
    assert code != 0 and "expected serverError UNKNOWN_TABLE" in err
    # A different error than the hint expects.
    _, err, code = run_shim("SELECT * FROM no_such_table_xyz; -- { serverError SYNTAX_ERROR }")
    assert code == 60, err


def test_output_formats():
    out, err, code = run_shim(
        "SELECT 1 AS x, 'a\\tb' AS y FORMAT TSVWithNamesAndTypes; SELECT 'a\\tb' FORMAT TSVRaw; SELECT 1 FORMAT Null;"
    )
    assert (code, err) == (0, "")
    assert out == b"x\ty\nUInt8\tString\n1\ta\\tb\na\tb\n"


def test_format_null_reads_no_rows():
    # With FORMAT Null, the statement goes to the server as it is, and the server sends no rows. The client
    # reads no values, so a type that it cannot read (AggregateFunction) does not fail.
    out, err, code = run_shim("SELECT sumState(1) FORMAT Null; SELECT 1;")
    assert (out, err, code) == (b"1\n", "", 0)


def test_lexer_whitespace_comments_and_echo():
    # The Unicode whitespace that the ClickHouse lexer skips, `//` comments, and the echo of a statement
    # that parsed and then failed with SYNTAX_ERROR. The expected text is the output of clickhouse-client.
    script = (
        "SELECT\ufeff1;\n\u0085\u00a0\u2028SELECT 2;\nSELECT 3; // c;d\n// e\nSELECT 4;\n"
        "-- { echo }\nSELECT formatQuery('SELECT'); -- { serverError SYNTAX_ERROR }\nSELECT 5;\n"
    )
    out, err, code = run_shim(script)
    assert (code, err) == (0, "")
    assert out == (b"1\n2\n3\n4\n-- { echo }\nSELECT formatQuery('SELECT'); -- { serverError SYNTAX_ERROR }\nSELECT 5;\n5\n")
