"""The statements that return rows, and the FORMAT clause of a query."""

from __future__ import annotations

from dataclasses import dataclass

from .lexer import (
    heredoc_end,
    is_word_char,
    leading_keyword,
    skip_block_comment,
    skip_line_comment,
    skip_quoted,
    skip_whitespace_and_comments,
    starts_line_comment,
    word_end,
)
from .split import is_clause_keyword

# The leading keywords of the statements that return a result set. The shim runs these with
# Client.query and the other statements with Client.command.
RESULT_KEYWORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "SHOW",
        "DESC",
        "DESCRIBE",
        "EXISTS",
        "EXPLAIN",
        "CHECK",
        # FROM-first syntax: FROM t SELECT x, and pipe syntax: FROM t |> ...
        "FROM",
    }
)


def returns_rows(sql: str) -> bool:
    return leading_keyword(sql) in RESULT_KEYWORDS


@dataclass
class OutputFormat:
    """How the shim writes a result set."""

    name: str = "TabSeparated"
    names: bool = False  # a header line with the column names
    types: bool = False  # a header line with the column types
    raw: bool = False  # top-level strings are not escaped
    discard: bool = False  # the Null format
    supported: bool = True


_FORMATS = {
    "tabseparated": OutputFormat(),
    "tsv": OutputFormat(),
    "tabseparatedwithnames": OutputFormat(names=True),
    "tsvwithnames": OutputFormat(names=True),
    "tabseparatedwithnamesandtypes": OutputFormat(names=True, types=True),
    "tsvwithnamesandtypes": OutputFormat(names=True, types=True),
    "tabseparatedraw": OutputFormat(raw=True),
    "tsvraw": OutputFormat(raw=True),
    "raw": OutputFormat(raw=True),
    "tabseparatedrawwithnames": OutputFormat(raw=True, names=True),
    "tsvrawwithnames": OutputFormat(raw=True, names=True),
    "rawwithnames": OutputFormat(raw=True, names=True),
    "tabseparatedrawwithnamesandtypes": OutputFormat(raw=True, names=True, types=True),
    "tsvrawwithnamesandtypes": OutputFormat(raw=True, names=True, types=True),
    "rawwithnamesandtypes": OutputFormat(raw=True, names=True, types=True),
    "null": OutputFormat(discard=True),
}


def split_output_format(sql: str) -> tuple[str, OutputFormat]:
    """Remove a top-level `FORMAT <name>` clause from a result-set statement, and return the statement
    and the format.

    clickhouse-connect always reads the result as Native (it adds `FORMAT Native` to the query), and the
    shim writes it as TabSeparated, so only the TabSeparated family and Null can be written. A
    statement with another format fails.
    """
    start, end, name = find_format_clause(sql)
    if start < 0:
        return sql, OutputFormat()
    known = _FORMATS.get(name.lower())
    if known is None:
        return sql[:start] + sql[end:], OutputFormat(name=name, supported=False)
    fmt = OutputFormat(**{**known.__dict__, "name": name})
    return sql[:start] + sql[end:], fmt


def find_format_clause(sql: str) -> tuple[int, int, str]:
    """Return the position of a top-level `FORMAT <name>` clause, or start=-1."""
    depth = 0
    i = 0
    n = len(sql)
    while i < n:
        c = sql[i]
        if c in "'\"`":
            i = skip_quoted(sql, i)
        elif starts_line_comment(sql, i):
            i = skip_line_comment(sql, i)
        elif sql.startswith("/*", i):
            i = skip_block_comment(sql, i)
        elif c == "$":
            e = heredoc_end(sql, i)
            i = e if e > 0 else i + 1
        elif c in "([":
            depth += 1
            i += 1
        elif c in ")]":
            depth -= 1
            i += 1
        elif is_word_char(c):
            j = word_end(sql, i)
            if depth == 0 and j - i == 6 and sql[i:j].upper() == "FORMAT" and (i == 0 or sql[i - 1] != "."):
                # `ORDER BY format DESC` ends the query too: a clause keyword after FORMAT shows an
                # identifier named `format`.
                found = _format_name(sql, j)
                if found is not None:
                    m, name = found
                    if not is_clause_keyword(name) and _ends_query(sql, m):
                        return i, m, name
            i = j
        else:
            i += 1
    return -1, -1, ""


def unquote_identifier(s: str) -> str | None:
    q = s[0]
    if len(s) < 2 or s[-1] != q:
        return None
    out = []
    i = 1
    while i < len(s) - 1:
        c = s[i]
        if c == "\\" and i + 1 < len(s) - 1:
            i += 1
            out.append(s[i])
        elif c == q and i + 1 < len(s) - 1 and s[i + 1] == q:
            i += 1
            out.append(q)
        else:
            out.append(c)
        i += 1
    return "".join(out)


def _format_name(sql: str, j: int) -> tuple[int, str] | None:
    """Read the format name after the FORMAT keyword that ends at j. Return the end of the name and the
    name."""
    k = skip_whitespace_and_comments(sql, j)
    if k == j or k >= len(sql):
        return None
    if sql[k] in '`"':
        # A quoted format name: FORMAT `Null`.
        m = skip_quoted(sql, k)
        name = unquote_identifier(sql[k:m])
        return None if name is None else (m, name)
    m = word_end(sql, k)
    if m == k:
        return None
    return m, sql[k:m]


def _ends_query(sql: str, i: int) -> bool:
    """Tell if the text from i on can follow the FORMAT clause of a query: nothing, a semicolon, or a
    SETTINGS clause. Otherwise FORMAT is an identifier, as in `SELECT format FROM t`."""
    k = skip_whitespace_and_comments(sql, i)
    if k >= len(sql) or sql[k] == ";":
        return True
    return sql[k : word_end(sql, k)].upper() == "SETTINGS"
