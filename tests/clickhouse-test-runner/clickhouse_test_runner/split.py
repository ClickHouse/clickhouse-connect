"""Split a multi-query script into statements, the way clickhouse-client does it.

See ClientBase::executeMultiQuery and TestHint.cpp in ClickHouse/ClickHouse.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .hint import TestHint, parse_test_hint
from .lexer import (
    has_significant_token,
    heredoc_end,
    is_word_char,
    leading_keyword,
    skip_block_comment,
    skip_line_comment,
    skip_quoted,
    skip_whitespace,
    skip_whitespace_and_comments,
    starts_line_comment,
    word_end,
)


@dataclass
class Statement:
    # The text to send to the server: from the first non-whitespace character (leading comments
    # included) to the end of the query, with the semicolon.
    sql: str
    # sql plus the semicolon and the same-line trailing comment. The client reads the test hints from
    # this text and prints it for `echo`.
    full: str
    hint: TestHint = field(default_factory=TestHint)
    # Whether the statement is printed before it runs (the echo hints).
    echo: bool = False


def test_tags_length(text: str) -> int:
    """Mirror getTestTagsLength: the length of the leading `-- Tags: ...` line and the `--` comment lines
    right after it."""
    pos = 0
    first = True
    n = len(text)
    while True:
        line_start = pos
        while pos < n and text[pos] in " \t":
            pos += 1
        if not text.startswith("--", pos):
            return line_start
        pos += 2
        if first:
            while pos < n and text[pos] in " \t":
                pos += 1
            if not text.startswith("Tags:", pos):
                return 0
            pos += len("Tags:")
            first = False
        eol = -1
        for k in range(pos, n):
            if text[k] in "\r\n":
                eol = k
                break
        if eol < 0:
            return n
        eol_len = 1
        if eol + 1 < n and text[eol + 1] in "\r\n" and text[eol + 1] != text[eol]:
            eol_len = 2
        pos = eol + eol_len


# SQL words that can follow a column or an alias named `format`, as in `INSERT INTO t SELECT format FROM
# s`. No input or output format has such a name, so after FORMAT they show an identifier, not the FORMAT
# clause. A word after FORMAT that is not in this list is a format name: `INSERT INTO t SELECT * FROM
# input('x UInt8') FORMAT TSV` has inline data.
CLAUSE_KEYWORDS = frozenset(
    "FROM WHERE PREWHERE AS AND OR NOT IN IS LIKE ILIKE BETWEEN GROUP ORDER LIMIT OFFSET HAVING QUALIFY WINDOW UNION "
    "EXCEPT INTERSECT JOIN INNER LEFT RIGHT FULL CROSS ARRAY ON USING SETTINGS INTO ASC DESC NULLS COLLATE WITH THEN "
    "ELSE END WHEN OVER GLOBAL DIV MOD REGEXP FINAL SAMPLE FILL TIES BY".split()
)


def is_clause_keyword(word: str) -> bool:
    return word.upper() in CLAUSE_KEYWORDS


def top_level_words(sql: str) -> list[str]:
    """Return the words of sql outside quotes, heredocs, comments and parentheses."""
    words = []
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
            end = heredoc_end(sql, i)
            i = end if end > 0 else i + 1
        elif c in "([":
            depth += 1
            i += 1
        elif c in ")]":
            depth -= 1
            i += 1
        elif is_word_char(c):
            j = word_end(sql, i)
            if depth == 0:
                words.append(sql[i:j])
            i = j
        else:
            i += 1
    return words


def format_after_select_from(head: str) -> bool:
    """Tell if a FORMAT that follows head can be the FORMAT clause of an INSERT: head has no top-level
    SELECT, or a top-level FROM follows its last top-level SELECT."""
    after_select = saw_from = False
    for w in top_level_words(head):
        u = w.upper()
        if u == "SELECT":
            after_select, saw_from = True, False
        elif u == "FROM":
            saw_from = True
    return not after_select or saw_from


def insert_data_start(text: str, start: int, format_end: int) -> int:
    """Return the offset of the inline data when the FORMAT keyword that ends at format_end starts the
    inline data of an INSERT statement that begins at start, else -1. The Values format is excluded:
    its data ends at `;`, like a query."""
    if leading_keyword(text[start:format_end]) != "INSERT":
        return -1
    i = format_end
    n = len(text)
    if i >= n or text[i] not in " \t\n\r":
        return -1
    while i < n and text[i] in " \t\n\r":
        i += 1
    j = word_end(text, i)
    if j == i or text[i:j].upper() == "VALUES" or is_clause_keyword(text[i:j]):
        return -1
    # After a SELECT, the FORMAT clause comes at the end of the SELECT, so after its top-level FROM:
    # `INSERT INTO t SELECT * FROM input('x UInt8') FORMAT TSV`. Before that FROM, `format` is in the
    # select list (`SELECT format x FROM s`, `SELECT format DIV 2 FROM s`).
    if not format_after_select_from(text[start : format_end - len("FORMAT")]):
        return -1
    return j


def scan_statement_end(text: str, start: int) -> int:
    """Return the offset of the `;` that ends the statement at start, or len(text)."""
    parens = 0
    i = start
    n = len(text)
    while i < n:
        c = text[i]
        if c in "'\"`":
            i = skip_quoted(text, i)
        elif (c == "-" and text.startswith("--", i)) or (c == "/" and text.startswith("//", i)):
            i = skip_line_comment(text, i)
        elif c == "#" and (text.startswith("#!", i) or text.startswith("# ", i)):
            i = skip_line_comment(text, i)
        elif c == "/" and text.startswith("/*", i):
            i = skip_block_comment(text, i)
        elif c == "$":
            end = heredoc_end(text, i)
            i = end if end > 0 else i + 1
        elif c in "([":
            parens += 1
            i += 1
        elif c in ")]":
            parens -= 1
            i += 1
        elif c == ";" and parens <= 0:
            return i
        elif is_word_char(c):
            j = word_end(text, i)
            if parens <= 0 and j - i == 6 and text[i:j].upper() == "FORMAT":
                data_start = insert_data_start(text, start, j)
                if data_start >= 0:
                    # The inline data ends at an empty line.
                    k = text.find("\n\n", data_start)
                    return k if k >= 0 else n
            i = j
        else:
            i += 1
    return n


def split_statements(text: str) -> list[Statement]:
    """Split a script into statements.

    It follows clickhouse-client:
      - The `-- Tags:` header (the first line and the `--` lines after it) is not part of any statement.
      - A statement ends at a top-level `;`. A comment on the same line after the `;` belongs to that
        statement; this is where test hints are.
      - The inline data of `INSERT ... FORMAT <name>` (other than Values) ends at an empty line, not
        at `;`.
      - Comment-only text after the last statement is not a statement.
    """
    out: list[Statement] = []
    pos = test_tags_length(text)
    echo = False
    n = len(text)
    while True:
        pos = skip_whitespace(text, pos)
        if pos >= n or not has_significant_token(text, pos):
            return out
        begin = pos
        end = scan_statement_end(text, pos)
        sql_end = end
        # Consume the semicolons after the statement.
        pos = end
        while pos < n and text[pos] == ";":
            pos += 1
            nxt = skip_whitespace_and_comments(text, pos)
            if nxt < n and text[nxt] == ";":
                pos = nxt
        # Add the same-line trailing comment when the next statement starts after the end of this line.
        newline = text.find("\n", pos)
        if newline < 0:
            newline = n
        if newline <= skip_whitespace_and_comments(text, pos):
            pos = newline
        # clickhouse-client sends the statement with its semicolon. Tests that read system.query_log
        # see it.
        if sql_end < n and text[sql_end] == ";":
            sql = text[begin : sql_end + 1]
        else:
            sql = text[begin:sql_end].rstrip(" \t\r\n")
        full = text[begin:pos].rstrip(" \t\r\n")
        if sql_end == n:
            # The last statement has no semicolon: clickhouse-client echoes all the rest of the text,
            # with its trailing newlines.
            full = text[begin:]
        hint = parse_test_hint(full)
        if hint.echo is not None:
            echo = hint.echo
        out.append(Statement(sql=sql, full=full, hint=hint, echo=echo))
