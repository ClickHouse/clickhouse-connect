import json
import os
import random
import string
import subprocess
import sys

import pytest

from clickhouse_connect.driver import query
from clickhouse_connect.driver.binding import _sql_opaque_regions

PATTERNS = (query.select_re, query.limit_re, query.command_re)
WORD_CHARS = string.ascii_letters + string.digits + "_"


class SqlText(str):
    pass


def classify(sql):
    uncommented = query.remove_sql_comments(sql)
    matches = tuple(query._matches_outside_quotes(pattern, text) for text in (sql, uncommented) for pattern in PATTERNS)
    return uncommented, type(uncommented), matches


@pytest.fixture
def c_sqlscan():
    return pytest.importorskip("clickhouse_connect.driverc.sqlscan")


@pytest.fixture
def assert_parity(monkeypatch, c_sqlscan):
    monkeypatch.setattr(query, "_c_sqlscan", None)

    def check(sql):
        assert c_sqlscan._opaque_regions(sql) == list(_sql_opaque_regions(sql)), repr(sql)
        query._c_sqlscan = c_sqlscan
        try:
            compiled = classify(sql)
        finally:
            query._c_sqlscan = None
        assert compiled == classify(sql), repr(sql)

    return check


EDGE_CASES = [
    "",
    "SELECT 13",
    "SELECT 13 /* outer /* inner */ still outer */ LIMIT 79",
    "SELECT 13 /* open /* nested */ LIMIT 79",
    "SELECT 13 /* open",
    "SELECT/*c*/13/**/LIMIT/**/79",
    "/*/ SELECT 13 */ LIMIT 79",
    "/*/*/ LIMIT 79",
    "/* a */*/ LIMIT 79",
    "*/ SELECT 13 /*",
    "SELECT 13 /* *** // -- */ LIMIT 79",
    "SELECT 13 -- LIMIT 79",
    "SELECT 13 -- c\nLIMIT 79",
    "SELECT 13 // c\nLIMIT 79",
    "SELECT 13 # c\nLIMIT 79",
    "SELECT 13 #! c\nLIMIT 79",
    "SELECT 13 #c LIMIT 79",
    "SELECT 13 #\tc\nLIMIT 79",
    "  -- c\nSELECT 13",
    "SELECT 'it\\'s -- not' LIMIT 79",
    "SELECT 'tail\\\\' LIMIT 79",
    "SELECT 'open\\' LIMIT 79",
    "SELECT 'it''s /* not */' LIMIT 79",
    'SELECT "col""x" FROM t LIMIT 79',
    "SELECT `a--b` FROM t LIMIT 79",
    "SELECT `LIMIT 79`",
    "SELECT `open LIMIT 79",
    "SELECT $$ LIMIT 79 -- c $$",
    "SELECT $tag$ /* $$ */ LIMIT 79 $tag$ LIMIT 13",
    "SELECT $tag$ open LIMIT 79",
    "SELECT $a$$b$ LIMIT 79 $b$ $a$",
    "SELECT a$b$ LIMIT 79 $b$",
    "SELECT $1 LIMIT 79",
    "SELECT 13$a$ LIMIT 79 $a$",
    "SELECT 13a$b$ LIMIT 79 $b$",
    "SELECT $13$ LIMIT 79 $13$",
    "SELECT {name:String} -- c\nLIMIT 79",
    "SELECT {a:Enum8('x' = 1)} LIMIT 79",
    "SELECT {13:Int32} LIMIT 79",
    "SELECT {idé:String} LIMIT 79",
    "SELECT {$a:String} LIMIT 79",
    "SELECT {é:a'b} 'c' LIMIT 79",
    "SELECT { 'LIMIT 79' }",
    "SELECT ‘LIMIT 79’",
    "SELECT “x -- y” LIMIT 13",
    "SELECT ‘open LIMIT 79",
    "SELECT 'é' /* ü */ LIMIT 79",
    "SELECT 'ж' /* 中 */ LIMIT 79 -- 中",
    "SELECT '😀' /* 😀 */ LIMIT 79 -- 😀\n",
    "SELECT 13 /* 😀 /* é */ 😀 */ LIMIT 79",
    "SELECT \ud800 /* \udfff */ LIMIT 79",
    "INSERT INTO t VALUES ('LIMIT 79')",
    "DROP TABLE t -- c",
    "WITH $$ SELECT 13 LIMIT 79 -- kept $$ SELECT 13 -- cut",
    " -- a\n /* b */ # c\n //d\n" * 20 + "SELECT 13 LIMIT 79",
    "SELECT $h$ LIMIT 79 $h$ " + " ".join(f"$t{i}$" for i in range(48)) + " LIMIT 13 -- c",
    " ".join(f"$t{i}$" for i in range(48)) + " SELECT $h$ LIMIT 79 $h$ -- c",
    "SELECT $t3$ LIMIT 79 " + "".join(f"$t{i}$" for i in range(48)) + " $t3$ LIMIT 13",
    "SELECT $u1$, $u2$, $h$ LIMIT 79 $h$ " + " ".join(f"$t{i}$" for i in range(48)) + " LIMIT 13 -- c",
    "SELECT $u1$, $u2$, " + " ".join(f"$t{i}$" for i in range(48)) + " $h$ LIMIT 79 $h$ -- c",
]


@pytest.mark.parametrize("sql", EDGE_CASES)
def test_sqlscan_edge_parity(assert_parity, sql):
    assert_parity(sql)


def test_sqlscan_probe_budget_knob_gives_up(assert_parity, c_sqlscan):
    sql = "SELECT $u1$, $u2$, $h$ LIMIT 79 $h$ -- c"
    assert_parity(sql)
    assert c_sqlscan._opaque_regions(sql, 0, 0) is None


class RaisingScanner:
    @staticmethod
    def remove_sql_comments(sql):
        raise AssertionError("compiled scanner called")

    matches_outside_quotes = remove_sql_comments


class GivingUpScanner:
    @staticmethod
    def remove_sql_comments(sql):
        return None

    @staticmethod
    def matches_outside_quotes(pattern, sql, position):
        return None


def test_sqlscan_str_subclass_uses_python_path(monkeypatch):
    monkeypatch.setattr(query, "_c_sqlscan", RaisingScanner)
    assert query.remove_sql_comments(SqlText("SELECT 13 /* c */ LIMIT 79")) == "SELECT 13   LIMIT 79"
    assert query._matches_outside_quotes(query.limit_re, SqlText("SELECT 'x' LIMIT 79")) is True


@pytest.mark.parametrize("sql", ["SELECT 13 -- c\nLIMIT 79", "SELECT $h$ LIMIT 79 $h$ /* c */", "SELECT 'LIMIT 79' -- c"])
def test_sqlscan_gave_up_uses_python_path(monkeypatch, sql):
    monkeypatch.setattr(query, "_c_sqlscan", None)
    expected = classify(sql)
    monkeypatch.setattr(query, "_c_sqlscan", GivingUpScanner)
    assert classify(sql) == expected


FRAGMENTS = [
    "SELECT 13",
    " LIMIT 79",
    " limit 0",
    "DROP ",
    "'",
    '"',
    "`",
    "\\",
    "''",
    "'a\\'b'",
    "`a--b`",
    "$$",
    "$a$",
    "$13$",
    "$",
    "a$b",
    "13",
    "--",
    "//",
    "/*",
    "*/",
    "*",
    "/",
    "/**/",
    "/*/",
    "# ",
    "#!",
    "#",
    "\n",
    " ",
    "{a:String}",
    "{é:String}",
    "13a",
    "{",
    "}",
    "‘",
    "’",
    "“",
    "”",
    "é",
    "中",
    "😀",
    "\ud800",
    ";",
    "(",
]


def test_sqlscan_fuzz_parity(assert_parity):
    rng = random.Random(925)
    for _ in range(3000):
        assert_parity("".join(rng.choices(FRAGMENTS, k=rng.randint(1, 16))))


def test_sqlscan_heredoc_fuzz_parity(assert_parity):
    rng = random.Random(1186)
    for _ in range(150):
        tags = [f"${''.join(rng.choices(WORD_CHARS, k=rng.randint(0, 3)))}{i}$" for i in range(rng.randint(8, 200))]
        pieces = tags + rng.choices(tags, k=rng.randint(1, len(tags))) + rng.choices(FRAGMENTS, k=rng.randint(0, 40))
        rng.shuffle(pieces)
        assert_parity("".join(pieces))


FALLBACK_SCRIPT = """
import json
import sys

if sys.argv[1] == "blocked":
    sys.modules["clickhouse_connect.driverc.sqlscan"] = None
elif sys.argv[1] == "pypy":
    sys.implementation.name = "pypy"
from clickhouse_connect.driver import ctypes, query

results = []
for sql in json.loads(sys.argv[2]):
    context = query.QueryContext(sql)
    results.append([query.remove_sql_comments(sql), context.is_select, context.has_limit, context.is_command])
scanners = [module and module.__name__ for module in (ctypes.sql_scan, query._c_sqlscan)]
print(json.dumps({"scanners": scanners, "results": results}))
"""

FALLBACK_CASES = [
    ("SELECT 13 /* cut */", "SELECT 13  ", True, False, False),
    ("WITH $$ SELECT 13 LIMIT 79 -- kept $$ SELECT 13 -- cut", "WITH $$ SELECT 13 LIMIT 79 -- kept $$ SELECT 13 ", True, False, False),
    ("SELECT 13 -- cut\nLIMIT 79", "SELECT 13 \nLIMIT 79", True, True, False),
    ("SELECT 'LIMIT 79' /* a /* b */ c */", "SELECT 'LIMIT 79'  ", True, False, False),
    ("/* c */ DROP TABLE t", "  DROP TABLE t", False, False, True),
]


@pytest.mark.parametrize("mode", ["blocked", "pypy"])
def test_sqlscan_fallback(mode):
    env = {**os.environ, "CLICKHOUSE_CONNECT_USE_C": "1"}
    sqls = [case[0] for case in FALLBACK_CASES]
    result = subprocess.run(
        [sys.executable, "-c", FALLBACK_SCRIPT, mode, json.dumps(sqls)], capture_output=True, check=True, env=env, text=True
    )
    state = json.loads(result.stdout)
    assert state["scanners"] == [None, None]
    assert state["results"] == [list(case[1:]) for case in FALLBACK_CASES]
