"""Test hints, as clickhouse-client parses them (see src/Client/TestHint.cpp).

    SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
    SELECT 1 / 0;      -- { serverError 153, 6 }
    -- { echoOn }

An error hint counts only when it is not in a leading comment. An echo hint counts in any comment.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .errorcodes import ERROR_CODES, ERROR_NAMES
from .lexer import SPACE, heredoc_end, skip_block_comment, skip_line_comment, skip_quoted, starts_line_comment

# The characters that can start a comment, a quoted text or a heredoc.
_INTERESTING = re.compile(r"[-#/'\"`$]")


@dataclass
class TestHint:
    __test__ = False  # not a pytest test class

    server_errors: list[int] = field(default_factory=list)
    client_errors: list[int] = field(default_factory=list)
    # Set by echo, echoOn (True) or echoOff (False).
    echo: bool | None = None
    # Set when a hint is malformed or names an unknown error.
    parse_error: str | None = None

    def expects_error(self) -> bool:
        return bool(self.server_errors or self.client_errors)

    def matches(self, code: int) -> bool:
        """Tell if the error code satisfies the hint. Through clickhouse-connect the server finds every
        error, also the syntax errors that clickhouse-client finds locally, so a server error can satisfy
        a clientError hint too."""
        return code in self.server_errors or code in self.client_errors

    def label(self) -> str:
        def codes(values: list[int]) -> str:
            return ", ".join(ERROR_NAMES.get(c, str(c)) for c in values)

        if self.server_errors and self.client_errors:
            return "error " + codes(self.server_errors)
        if self.server_errors:
            return "serverError " + codes(self.server_errors)
        return "clientError " + codes(self.client_errors)

    def _parse(self, body: str, leading: bool) -> None:
        tokens = _tokenize(body)
        for i, tok in enumerate(tokens):
            if tok in ("echo", "echoOn"):
                self.echo = True
            elif tok == "echoOff":
                self.echo = False
            elif tok == "retry":
                # `retry N until|while serverError X` is used by a few tests that wait for a background
                # process. It is not supported.
                self.parse_error = 'the "retry" test hint is not supported'
                return
            elif tok in ("serverError", "clientError", "error"):
                if leading:
                    continue
                values = []
                for t in tokens[i + 1 :]:
                    if t == ",":
                        continue
                    try:
                        values.append(int(t))
                        continue
                    except ValueError:
                        pass
                    code = ERROR_CODES.get(t)
                    if code is None:
                        self.parse_error = f"unknown error name {t!r} in test hint"
                        return
                    values.append(code)
                if tok == "serverError":
                    self.server_errors = values
                elif tok == "clientError":
                    self.client_errors = values
                else:
                    self.server_errors, self.client_errors = values, list(values)
                return


def _tokenize(body: str) -> list[str]:
    out = []
    i = 0
    n = len(body)
    while i < n:
        c = body[i]
        if c in SPACE:
            i += 1
        elif c == ",":
            out.append(",")
            i += 1
        else:
            j = i
            while j < n and body[j] not in SPACE and body[j] != ",":
                j += 1
            out.append(body[i:j])
            i = j
    return out


def parse_test_hint(full: str) -> TestHint:
    h = TestHint()
    leading = True
    i = 0
    n = len(full)
    while i < n:
        c = full[i]
        if c in SPACE:
            i += 1
            continue
        if starts_line_comment(full, i):
            end = skip_line_comment(full, i)
        elif full.startswith("/*", i):
            end = skip_block_comment(full, i)
        elif c in "'\"`":
            leading = False
            i = skip_quoted(full, i)
            continue
        elif c == "$":
            leading = False
            e = heredoc_end(full, i)
            i = e if e > 0 else i + 1
            continue
        else:
            leading = False
            m = _INTERESTING.search(full, i + 1)
            i = m.start() if m else n
            continue
        comment = full[i:end]
        open_brace = comment.find("{")
        if open_brace >= 0:
            closing = comment.find("}", open_brace)
            if closing >= 0:
                h._parse(comment[open_brace + 1 : closing], leading)
        i = end
    return h
