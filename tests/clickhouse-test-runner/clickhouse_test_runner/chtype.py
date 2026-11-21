"""A parser for ClickHouse type names, such as `Array(Nullable(DateTime64(3, 'UTC')))`.

The renderer reads the precision of a DateTime64, the element names of a Tuple and other details from
the type name of each result column (QueryResult.column_types[i].name).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .lexer import is_word_char


class TypeParseError(ValueError):
    pass


@dataclass
class TypeArg:
    """One parameter of a type: a nested type (type is set) or a literal, such as the 3 and 'UTC' above
    or the `'a' = 1` of an Enum8. name is set for the named elements of a Tuple or Nested."""

    name: str = ""
    type: CHType | None = None
    literal: str = ""


@dataclass
class CHType:
    base: str
    args: list[TypeArg] = field(default_factory=list)

    def __str__(self) -> str:
        if not self.args:
            return self.base
        parts = []
        for a in self.args:
            if a.type is not None and a.name:
                parts.append(f"{a.name} {a.type}")
            elif a.type is not None:
                parts.append(str(a.type))
            else:
                parts.append(a.literal)
        return f"{self.base}({', '.join(parts)})"

    def type_arg(self, i: int) -> CHType | None:
        """Return the nested type of argument i, or None."""
        return self.args[i].type if i < len(self.args) else None

    def literal_arg(self, i: int) -> str:
        """Return the literal of argument i without its single quotes, or "" when it is absent."""
        return self.args[i].literal.strip("'") if i < len(self.args) else ""


def _is_name_char(c: str) -> bool:
    return is_word_char(c) or c == "."


def parse_type(s: str) -> CHType:
    """Parse the type name of a result column."""
    t, rest = _parse_type_at(s.strip())
    if rest.strip():
        raise TypeParseError(f"parse type {s!r}: unexpected trailing text {rest!r}")
    return t


def _parse_type_at(s: str) -> tuple[CHType, str]:
    i = 0
    while i < len(s) and _is_name_char(s[i]):
        i += 1
    if i == 0:
        raise TypeParseError(f"expected a type name at {s!r}")
    t = CHType(base=s[:i])
    rest = s[i:].lstrip(" ")
    if not rest.startswith("("):
        return t, rest
    parts, rest = _split_type_args(rest)
    t.args = [_parse_type_arg(t.base, p) for p in parts]
    return t, rest


def _try_parse(raw: str) -> CHType | None:
    try:
        t, rest = _parse_type_at(raw)
    except TypeParseError:
        return None
    return t if not rest.strip() else None


def _parse_type_arg(base: str, raw: str) -> TypeArg:
    """Turn one raw parameter into a nested type, a named nested type, or a literal."""
    raw = raw.strip()
    if not raw or raw[0] in "'-" or raw[0].isdigit():
        return TypeArg(literal=raw)
    if base in ("Enum8", "Enum16", "Enum"):
        return TypeArg(literal=raw)
    if base in ("AggregateFunction", "SimpleAggregateFunction"):
        # The first argument is a function name, such as sum or quantiles(0.5, 0.9). It is not a type.
        t = _try_parse(raw)
        if t is not None and t.base in KNOWN_TYPE_NAMES:
            return TypeArg(type=t)
        return TypeArg(literal=raw)
    if base == "JSON" and (raw.upper().startswith("SKIP ") or "=" in raw):
        # SKIP <path>, SKIP REGEXP '<re>' and max_dynamic_paths = N are not typed paths.
        return TypeArg(literal=raw)
    if base in ("Tuple", "Nested", "JSON", "Object", "Variant"):
        # Named element: `name Type` (the name can be quoted with backticks).
        named = _split_named_element(raw)
        if named is not None:
            t = _try_parse(named[1])
            if t is not None:
                return TypeArg(name=named[0], type=t)
    t = _try_parse(raw)
    if t is not None:
        return TypeArg(type=t)
    return TypeArg(literal=raw)


def _split_named_element(raw: str) -> tuple[str, str] | None:
    """Split `name Type` into its two parts, or return None for an element that is only a type, such as
    Int32 or Array(String)."""
    if raw.startswith("`"):
        out = []
        i = 1
        while i < len(raw):
            c = raw[i]
            if c == "\\" and i + 1 < len(raw):
                out.append(raw[i + 1])
                i += 2
                continue
            if c == "`":
                if i + 1 < len(raw) and raw[i + 1] == "`":
                    out.append("`")
                    i += 2
                    continue
                return "".join(out), raw[i + 1 :].strip()
            out.append(c)
            i += 1
        return None
    i = 0
    while i < len(raw) and _is_name_char(raw[i]):
        i += 1
    if i == 0 or i >= len(raw) or raw[i] != " ":
        return None
    return raw[:i], raw[i:].strip()


def _split_type_args(s: str) -> tuple[list[str], str]:
    """Split `(a, b(c, d), 'e,f')rest` into its top-level parameters and the text after the closing
    parenthesis."""
    parts: list[str] = []
    depth = 0
    start = 1
    quote = ""
    i = 0
    while i < len(s):
        c = s[i]
        if quote:
            if c == "\\":
                i += 1
            elif c == quote:
                quote = ""
            i += 1
            continue
        if c in "'`\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                p = s[start:i].strip()
                if p or parts:
                    parts.append(p)
                return parts, s[i + 1 :]
        elif c == "," and depth == 1:
            parts.append(s[start:i].strip())
            start = i + 1
        i += 1
    raise TypeParseError(f"unbalanced parentheses in {s!r}")


# Used only to tell SimpleAggregateFunction(sum, UInt64) (function name, then type) apart; a false
# negative only makes the renderer report an unsupported type.
KNOWN_TYPE_NAMES = frozenset(
    "Int8 Int16 Int32 Int64 Int128 Int256 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256 Float32 Float64 BFloat16 Bool "
    "String FixedString Date Date32 DateTime DateTime64 Time Time64 UUID IPv4 IPv6 Decimal Decimal32 Decimal64 "
    "Decimal128 Decimal256 Enum Enum8 Enum16 Nullable LowCardinality Array Tuple Nested Map Nothing Point Ring "
    "LineString MultiPoint MultiLineString Polygon MultiPolygon Geometry Variant Dynamic JSON Object QBit "
    "IntervalNanosecond IntervalMicrosecond IntervalMillisecond IntervalSecond IntervalMinute IntervalHour "
    "IntervalDay IntervalWeek IntervalMonth IntervalQuarter IntervalYear".split()
)
