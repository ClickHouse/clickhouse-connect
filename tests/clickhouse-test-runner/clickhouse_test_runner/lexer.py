"""Lexical helpers shared by the statement splitter, the test hints and the FORMAT clause search.

They follow the lexer of the ClickHouse server: quoted strings and identifiers with backslash escapes and
doubled quotes, `--`, `//`, `#!` and `# ` line comments, nested block comments, `$tag$` heredocs, and the
Unicode whitespace that the lexer skips.
"""

from __future__ import annotations

import re

_WORD = re.compile(r"[A-Za-z0-9_]+")
# ASCII whitespace, and the Unicode whitespace that the ClickHouse lexer also skips (text copied from word
# processors): U+0085, U+00A0, U+180E, U+2000..U+200D, U+2028, U+2029, U+202F, U+205F, U+2060, U+3000, U+FEFF.
SPACE = " \t\n\r\f\v\u0085\u00a0\u180e" + "".join(map(chr, range(0x2000, 0x200E))) + "\u2028\u2029\u202f\u205f\u2060\u3000\ufeff"


def is_word_char(c: str) -> bool:
    return c == "_" or ("a" <= c <= "z") or ("A" <= c <= "Z") or ("0" <= c <= "9")


def word_end(text: str, i: int) -> int:
    m = _WORD.match(text, i)
    return m.end() if m else i


def skip_quoted(text: str, i: int) -> int:
    """Return the offset after the quoted string, identifier or name that starts at i."""
    q = text[i]
    j = i + 1
    n = len(text)
    while j < n:
        c = text[j]
        if c == "\\":
            j += 2
            continue
        if c == q:
            # A doubled quote is an escaped quote.
            if j + 1 < n and text[j + 1] == q:
                j += 2
                continue
            return j + 1
        j += 1
    return n


def skip_line_comment(text: str, i: int) -> int:
    nl = text.find("\n", i)
    return len(text) if nl < 0 else nl


def skip_block_comment(text: str, i: int) -> int:
    # Block comments nest in ClickHouse.
    depth = 0
    j = i
    n = len(text)
    while j < n - 1:
        if text[j] == "/" and text[j + 1] == "*":
            depth += 1
            j += 2
        elif text[j] == "*" and text[j + 1] == "/":
            depth -= 1
            j += 2
            if depth == 0:
                return j
        else:
            j += 1
    return n


def heredoc_end(text: str, i: int) -> int:
    """Return the end offset of the `$tag$ ... $tag$` heredoc that starts at i, or 0."""
    tag_end = text.find("$", i + 1)
    if tag_end < 0:
        return 0
    for k in range(i + 1, tag_end):
        if not is_word_char(text[k]):
            return 0
    delim = text[i : tag_end + 1]
    end = text.find(delim, tag_end + 1)
    if end < 0:
        return 0
    return end + len(delim)


def starts_line_comment(text: str, i: int) -> bool:
    return text.startswith(("--", "//", "#!", "# "), i)


def skip_whitespace(text: str, i: int) -> int:
    n = len(text)
    while i < n and text[i] in SPACE:
        i += 1
    return i


def skip_whitespace_and_comments(text: str, i: int) -> int:
    n = len(text)
    while i < n:
        c = text[i]
        if c in SPACE:
            i += 1
        elif starts_line_comment(text, i):
            i = skip_line_comment(text, i)
        elif text.startswith("/*", i):
            i = skip_block_comment(text, i)
        else:
            return i
    return i


def has_significant_token(text: str, i: int = 0) -> bool:
    return skip_whitespace_and_comments(text, i) < len(text)


def leading_keyword(sql: str) -> str:
    """Return the first word of a statement in upper case, after comments, whitespace and opening
    parentheses."""
    i = skip_whitespace_and_comments(sql, 0)
    while i < len(sql) and (sql[i] == "(" or sql[i] in SPACE):
        i = skip_whitespace_and_comments(sql, i + 1)
    return sql[i : word_end(sql, i)].upper()
