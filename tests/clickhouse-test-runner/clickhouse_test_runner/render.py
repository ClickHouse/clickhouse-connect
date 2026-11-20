"""Write the Python values that clickhouse-connect decoded as the text that the server writes for the
TabSeparated format.

The upstream .reference files hold the server's text, so a test passes only when the client decoded
every value correctly. The rendering is type-directed: the Python value alone is not sufficient (for
example, the precision of a DateTime64 is in the type only), so each column renderer is built from the
type name of the result column.

ClickHouse uses two text forms:
  - top level (serializeTextEscaped): strings are escaped, not quoted, and NULL is \\N;
  - nested in Array, Tuple and Map (serializeTextQuoted): strings are quoted, and NULL is NULL.
"""

from __future__ import annotations

import datetime as dt
import decimal
import ipaddress
import math
import struct
import uuid
from collections.abc import Callable
from typing import Any

from .chtype import CHType, parse_type

Renderer = Callable[[Any], str]


class RenderError(Exception):
    """A value that the shim cannot write. The statement fails, so the test cannot pass by accident."""


class UnsupportedTypeError(RenderError):
    def __init__(self, type_name: str, detail: str = ""):
        super().__init__(f"clickhouse-test-runner: no TabSeparated renderer for type {type_name}{detail}")


def _mismatch(t: CHType, v: Any) -> RenderError:
    return RenderError(f"clickhouse-test-runner: unexpected Python value {v!r} ({type(v).__name__}) for type {t}")


# The escapes of ClickHouse's writeAnyEscapedString with the single quote as the quote character.
_ESCAPES = str.maketrans(
    {
        "\\": "\\\\",
        "'": "\\'",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\0": "\\0",
    }
)


def to_text(v: str | bytes) -> str:
    """Return v as text. Bytes that are not UTF-8 stay as surrogate escapes, and the output writer turns
    them back into the same bytes."""
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", "surrogateescape")
    return v


def escape_string(s: str, nested: bool) -> str:
    s = s.translate(_ESCAPES)
    return f"'{s}'" if nested else s


def _quote_if(s: str, nested: bool) -> str:
    return f"'{s}'" if nested else s


def format_float(f: float, bits: int = 64) -> str:
    """Write f as ClickHouse does: the shortest text that reads back as the same value, in positional
    notation when the decimal exponent is in [-6, 21), else in scientific notation without a + sign or
    exponent padding (1e21, 1.5e-7). This is the layout of JavaScript's Number.prototype.toString."""
    if math.isnan(f):
        return "nan"
    if math.isinf(f):
        return "inf" if f > 0 else "-inf"
    if f == 0:
        return "-0" if math.copysign(1.0, f) < 0 else "0"
    neg = f < 0
    digits, n = _shortest_digits(-f if neg else f, bits)
    # The value is 0.digits x 10^n.
    k = len(digits)
    if k <= n <= 21:
        out = digits + "0" * (n - k)
    elif 0 < n <= 21:
        out = digits[:n] + "." + digits[n:]
    elif -6 < n <= 0:
        out = "0." + "0" * (-n) + digits
    else:
        out = digits[0] + ("." + digits[1:] if k > 1 else "") + "e" + str(n - 1)
    return "-" + out if neg else out


def _f32(x: float) -> float:
    try:
        return struct.unpack("<f", struct.pack("<f", x))[0]
    except OverflowError:
        return math.inf


def _digits_of(text: str) -> tuple[str, int]:
    """Return the significant digits of a positive decimal number and the position n of its decimal point,
    so that the value is 0.digits x 10^n."""
    t = decimal.Decimal(text).as_tuple()
    digits = "".join(map(str, t.digits)).rstrip("0")
    exponent = int(t.exponent) + (len(t.digits) - len(digits))
    stripped = digits.lstrip("0")
    return stripped, len(stripped) + exponent


def _shortest_digits(f: float, bits: int) -> tuple[str, int]:
    if bits == 64:
        return _digits_of(repr(f))
    # Float32: the shortest decimal that reads back as the same 32-bit value, the nearest one when
    # several have the same length.
    for precision in range(1, 10):
        text = f"{f:.{precision - 1}e}"
        candidates = [text]
        mantissa, _, exp = text.partition("e")
        # The nearest decimal of this length can be just outside the rounding interval while its
        # neighbor is inside (next to a power of two).
        scaled = int(mantissa.replace(".", ""))
        for neighbor in (scaled - 1, scaled + 1):
            if neighbor > 0:
                candidates.append(f"{neighbor}e{int(exp) - (precision - 1)}")
        matches = [c for c in candidates if _f32(float(c)) == f]
        if matches:
            best = min(matches, key=lambda c: abs(decimal.Decimal(c) - decimal.Decimal(f)))
            return _digits_of(best)
    return _digits_of(repr(f))


def format_decimal(d: decimal.Decimal) -> str:
    """Write a Decimal without trailing zeros (1.5, not 1.50) and without an exponent."""
    s = f"{d:f}"
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    if s in ("-0", ""):
        s = "0"
    return s


def format_date(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"


def format_datetime(v: dt.datetime, precision: int = 0) -> str:
    """Write the wall-clock time of v. The client sets the time zone of v: the time zone of the column, or
    the time zone of the session or the server for a DateTime without one. A naive value is UTC."""
    s = f"{v.year:04d}-{v.month:02d}-{v.day:02d} {v.hour:02d}:{v.minute:02d}:{v.second:02d}"
    if precision > 0:
        nanos = v.microsecond * 1000 + getattr(v, "nanosecond", 0)
        s += "." + f"{nanos:09d}"[:precision]
    return s


MAX_TIME_SECONDS = 999 * 3600 + 59 * 60 + 59


def format_time(td: dt.timedelta, precision: int = 0) -> str:
    """Write a Time / Time64 value: [-]HH:MM:SS[.fff]. The hours can be more than 24. The server writes a
    value above 999:59:59 as 999:59:59, with a zero fraction."""
    micros = (td.days * 86400 + td.seconds) * 1_000_000 + td.microseconds
    sign = "-" if micros < 0 else ""
    micros = abs(micros)
    total, frac_micros = divmod(micros, 1_000_000)
    if total > MAX_TIME_SECONDS:
        total, frac_micros = MAX_TIME_SECONDS, 0
    s = f"{sign}{total // 3600:02d}:{total // 60 % 60:02d}:{total % 60:02d}"
    if precision > 0:
        s += "." + f"{frac_micros * 1000:09d}"[:precision]
    return s


def format_ipv6(packed: bytes) -> str:
    """Follow ClickHouse's formatIPv6: the longest run of two or more zero words becomes ::, and an
    IPv4-mapped (::ffff:a.b.c.d) or IPv4-compatible (::a.b.c.d) address ends in dotted form."""
    words = [packed[2 * i] << 8 | packed[2 * i + 1] for i in range(8)]
    best_base, best_len = -1, 0
    i = 0
    while i < 8:
        if words[i] != 0:
            i += 1
            continue
        j = i
        while j < 8 and words[j] == 0:
            j += 1
        if j - i > best_len:
            best_base, best_len = i, j - i
        i = j
    if best_len < 2:
        best_base = -1
    ipv4_compatible = best_base == 0 and best_len == 6
    ipv4_mapped = best_base == 0 and best_len == 5 and words[5] == 0xFFFF
    out = []
    for i in range(8):
        if best_base != -1 and best_base <= i < best_base + best_len:
            if i == best_base:
                out.append(":")
            continue
        if i != 0:
            out.append(":")
        if i == 6 and (ipv4_compatible or ipv4_mapped):
            out.append(".".join(str(b) for b in packed[12:16]))
            return "".join(out)
        out.append(f"{words[i]:x}")
    if best_base != -1 and best_base + best_len == 8:
        out.append(":")
    return "".join(out)


_INT_TYPES = frozenset(
    "Int8 Int16 Int32 Int64 Int128 Int256 UInt8 UInt16 UInt32 UInt64 UInt128 UInt256 IntervalNanosecond IntervalMicrosecond "
    "IntervalMillisecond IntervalSecond IntervalMinute IntervalHour IntervalDay IntervalWeek IntervalMonth IntervalQuarter "
    "IntervalYear".split()
)
_STRING_TYPES = frozenset({"String", "FixedString", "Enum8", "Enum16", "Enum"})
_DECIMAL_TYPES = frozenset({"Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256"})


def _precision(t: CHType, default: int | None = None) -> int:
    text = t.literal_arg(0)
    if not text and default is not None:
        return default
    try:
        p = int(text)
    except ValueError:
        raise UnsupportedTypeError(str(t)) from None
    if not 0 <= p <= 9:
        raise UnsupportedTypeError(str(t))
    return p


def compile_renderer(t: CHType, nested: bool = False) -> Renderer:
    """Return a function that writes one value of type t."""
    base = t.base
    null_text = "NULL" if nested else "\\N"

    if base in ("Nullable", "LowCardinality"):
        inner_type = t.type_arg(0)
        if inner_type is None:
            raise UnsupportedTypeError(str(t))
        inner = compile_renderer(inner_type, nested)
        if base == "LowCardinality":
            return inner

        def nullable(v: Any) -> str:
            return null_text if v is None else inner(v)

        return nullable
    if base == "SimpleAggregateFunction":
        inner_type = t.type_arg(len(t.args) - 1)
        if inner_type is None:
            raise UnsupportedTypeError(str(t))
        return compile_renderer(inner_type, nested)
    if base == "Nothing":
        # A Nothing column has no values; a value here is a NULL.
        return lambda v: null_text
    if base in ("Variant", "Geometry"):
        return _variant_renderer(t, nested)
    if base == "Dynamic":
        return lambda v: render_dynamic(v, nested)

    fn = _value_renderer(t, nested)

    def strict(v: Any) -> str:
        if v is None:
            raise _mismatch(t, v)
        return fn(v)

    return strict


def _value_renderer(t: CHType, nested: bool) -> Renderer:
    base = t.base
    if base in _INT_TYPES:

        def integer(v: Any) -> str:
            if type(v) is not int:
                raise _mismatch(t, v)
            return str(v)

        return integer
    if base in ("Float32", "BFloat16", "Float64"):
        bits = 64 if base == "Float64" else 32

        def floating(v: Any) -> str:
            if type(v) is not float:
                raise _mismatch(t, v)
            return format_float(v, bits)

        return floating
    if base == "Bool":

        def boolean(v: Any) -> str:
            if type(v) is not bool:
                raise _mismatch(t, v)
            return "true" if v else "false"

        return boolean
    if base in _STRING_TYPES:

        def string(v: Any) -> str:
            if not isinstance(v, (str, bytes)):
                raise _mismatch(t, v)
            return escape_string(to_text(v), nested)

        return string
    if base in _DECIMAL_TYPES:

        def dec(v: Any) -> str:
            if not isinstance(v, decimal.Decimal):
                raise _mismatch(t, v)
            return format_decimal(v)

        return dec
    if base in ("Date", "Date32"):

        def date(v: Any) -> str:
            if type(v) is not dt.date:
                raise _mismatch(t, v)
            return _quote_if(format_date(v), nested)

        return date
    if base in ("DateTime", "DateTime64"):
        precision = _precision(t) if base == "DateTime64" else 0

        def datetime(v: Any) -> str:
            if not isinstance(v, dt.datetime):
                raise _mismatch(t, v)
            return _quote_if(format_datetime(v, precision), nested)

        return datetime
    if base in ("Time", "Time64"):
        precision = _precision(t) if base == "Time64" else 0

        def time(v: Any) -> str:
            if not isinstance(v, dt.timedelta):
                raise _mismatch(t, v)
            return _quote_if(format_time(v, precision), nested)

        return time
    if base == "UUID":

        def uuid_(v: Any) -> str:
            if not isinstance(v, uuid.UUID):
                raise _mismatch(t, v)
            return _quote_if(str(v), nested)

        return uuid_
    if base == "IPv4":

        def ipv4(v: Any) -> str:
            if not isinstance(v, ipaddress.IPv4Address):
                raise _mismatch(t, v)
            return _quote_if(str(v), nested)

        return ipv4
    if base == "IPv6":

        def ipv6(v: Any) -> str:
            if not isinstance(v, ipaddress.IPv6Address):
                raise _mismatch(t, v)
            return _quote_if(format_ipv6(v.packed), nested)

        return ipv6
    if base == "Array":
        element = t.type_arg(0)
        if element is None:
            raise UnsupportedTypeError(str(t))
        return _sequence_renderer(t, compile_renderer(element, True))
    if base == "QBit":
        element = t.type_arg(0)
        if element is None:
            raise UnsupportedTypeError(str(t))
        return _sequence_renderer(t, compile_renderer(element, True))
    if base in ("Tuple", "Nested"):
        return _tuple_renderer(t)
    if base == "Map":
        return _map_renderer(t)
    if base == "JSON":
        return _json_renderer(t, nested)
    if base == "Point":
        return _point
    if base in ("Ring", "LineString", "MultiPoint"):
        return _sequence_renderer(t, _point)
    if base in ("Polygon", "MultiLineString"):
        return _sequence_renderer(t, _value_renderer(CHType("Ring"), True))
    if base == "MultiPolygon":
        return _sequence_renderer(t, _value_renderer(CHType("Polygon"), True))
    raise UnsupportedTypeError(str(t))


def _point(v: Any) -> str:
    if not isinstance(v, (tuple, list)) or len(v) != 2:
        raise RenderError(f"clickhouse-test-runner: unexpected Python value {v!r} for a Point")
    return f"({format_float(v[0])},{format_float(v[1])})"


def _sequence_renderer(t: CHType, element: Renderer) -> Renderer:
    def sequence(v: Any) -> str:
        if not isinstance(v, (list, tuple)):
            raise _mismatch(t, v)
        return "[" + ",".join(element(x) for x in v) + "]"

    return sequence


def _tuple_renderer(t: CHType) -> Renderer:
    if not t.args and t.base == "Tuple":
        # Tuple(): the server writes ().
        def empty(v: Any) -> str:
            if v not in ((), [], {}):
                raise _mismatch(t, v)
            return "()"

        return empty
    elements = []
    for a in t.args:
        if a.type is None:
            raise UnsupportedTypeError(str(t))
        elements.append((a.name, compile_renderer(a.type, True)))
    names = [name for name, _ in elements]
    renderers = [fn for _, fn in elements]

    def tuple_(v: Any) -> str:
        if isinstance(v, dict):
            # A named tuple: the client gives a dict, so the order comes from the type.
            if not all(names) or len(v) != len(names):
                raise _mismatch(t, v)
            try:
                return "(" + ",".join(fn(v[name]) for name, fn in zip(names, renderers)) + ")"
            except KeyError:
                raise _mismatch(t, v) from None
        if not isinstance(v, (tuple, list)) or len(v) != len(renderers):
            raise _mismatch(t, v)
        return "(" + ",".join(fn(x) for fn, x in zip(renderers, v)) + ")"

    if t.base == "Nested":
        # Nested(a T, b U) is Array(Tuple(a T, b U)); the client gives a list of dicts.
        return _sequence_renderer(t, tuple_)
    return tuple_


def _map_renderer(t: CHType) -> Renderer:
    key_type, value_type = t.type_arg(0), t.type_arg(1)
    if key_type is None or value_type is None:
        raise UnsupportedTypeError(str(t))
    key, value = compile_renderer(key_type, True), compile_renderer(value_type, True)

    def map_(v: Any) -> str:
        # The client gives a dict in the order of the server.
        if not isinstance(v, dict):
            raise _mismatch(t, v)
        return "{" + ",".join(f"{key(k)}:{value(x)}" for k, x in v.items()) + "}"

    return map_


# The escapes of ClickHouse's writeJSONString (with output_format_json_escape_forward_slashes on).
_JSON_ESCAPES = str.maketrans(
    {
        **{chr(c): f"\\u{c:04X}" for c in range(0x20)},
        '"': '\\"',
        "\\": "\\\\",
        "/": "\\/",
        "\b": "\\b",
        "\f": "\\f",
        "\n": "\\n",
        "\r": "\\r",
        "\t": "\\t",
        "\u2028": "\\u2028",
        "\u2029": "\\u2029",
    }
)
_JSON_TEXT_TYPES = frozenset({"Date", "Date32", "DateTime", "DateTime64", "Time", "Time64", "UUID", "IPv4", "IPv6"})


def json_string(s: str) -> str:
    return '"' + s.translate(_JSON_ESCAPES) + '"'


def _unwrap(t: CHType) -> CHType:
    while t.base in ("Nullable", "LowCardinality") and t.type_arg(0) is not None:
        t = t.type_arg(0)  # type: ignore[assignment]
    return t


def json_value(v: Any, t: CHType | None) -> str:
    """Write one value of a JSON column as the server writes it in JSON text. t is the type of a typed
    path; a dynamic path has no type, so its type comes from the Python value."""
    if v is None:
        return "null"
    if t is not None:
        t = _unwrap(t)
        base = t.base
        if base in ("Float32", "BFloat16"):
            return format_float(v, 32)
        if base in _STRING_TYPES:
            return json_string(to_text(v))
        if base in _JSON_TEXT_TYPES:
            return json_string(compile_renderer(t)(v))
        if base == "Array" and t.type_arg(0) is not None:
            return "[" + ",".join(json_value(x, t.type_arg(0)) for x in v) + "]"
        if base == "Map" and t.type_arg(1) is not None:
            return "{" + ",".join(f"{json_string(str(k))}:{json_value(x, t.type_arg(1))}" for k, x in v.items()) + "}"
        if base == "Tuple" and isinstance(v, dict):
            types = {a.name: a.type for a in t.args}
            return "{" + ",".join(f"{json_string(k)}:{json_value(x, types.get(k))}" for k, x in v.items()) + "}"
        if base == "JSON" and isinstance(v, dict):
            return json_object(v, _typed_paths(t))
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return format_float(v)
    if isinstance(v, decimal.Decimal):
        return format_decimal(v)
    if isinstance(v, (str, bytes)):
        return json_string(to_text(v))
    if isinstance(v, dt.datetime):
        # Type inference of JSON makes a DateTime64(9) of a date-time text with a fraction.
        return json_string(format_datetime(v, 0 if v.microsecond == 0 and not getattr(v, "nanosecond", 0) else 9))
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(json_value(x, None) for x in v) + "]"
    if isinstance(v, dict):
        # A JSON object without a type: for example an element of an Array(JSON) path.
        return json_object(v, {})
    return json_string(render_dynamic(v, False))


def _typed_paths(t: CHType) -> dict[str, CHType]:
    return {a.name: a.type for a in t.args if a.name and a.type is not None}


def _json_leaves(obj: dict, typed: dict[str, CHType], keys: tuple[str, ...], out: list) -> None:
    """Add (sort key, keys, JSON text) for each value of a nested JSON dict. A typed path, a value that is
    not a dict, and an empty object are values. A key can hold a dot (json_type_escape_dots_in_keys): the
    server sorts the path with the dot escaped as %2E."""
    for key, v in obj.items():
        path = (*keys, str(key))
        t = typed.get(".".join(path))
        if t is None:
            t = typed.get(".".join(k.replace(".", "%2E") for k in path))
        if t is None and isinstance(v, dict) and v:
            _json_leaves(v, typed, path, out)
        else:
            out.append((".".join(k.replace(".", "%2E") for k in path), path, json_value(v, t)))


def json_object(obj: dict, typed: dict[str, CHType]) -> str:
    """Write a JSON column value as the server does. The client gives the typed paths first, then the
    dynamic and the shared paths; the server writes all paths in sorted order. Paths with the same prefix
    go into one nested object; when a prefix comes again after another path, the server opens a second
    object with the same key."""
    leaves: list[tuple[str, tuple[str, ...], str]] = []
    _json_leaves(obj, typed, (), leaves)
    leaves.sort(key=lambda leaf: leaf[0])
    out = ["{"]
    open_keys: list[str] = []
    has_member = [False]
    for _, keys, text in leaves:
        parents = keys[:-1]
        common = 0
        while common < min(len(open_keys), len(parents)) and open_keys[common] == parents[common]:
            common += 1
        while len(open_keys) > common:
            out.append("}")
            open_keys.pop()
            has_member.pop()
        for key in parents[common:]:
            out.append(("," if has_member[-1] else "") + json_string(key) + ":{")
            has_member[-1] = True
            open_keys.append(key)
            has_member.append(False)
        out.append(("," if has_member[-1] else "") + json_string(keys[-1]) + ":" + text)
        has_member[-1] = True
    out.append("}" * (len(open_keys) + 1))
    return "".join(out)


def _json_renderer(t: CHType, nested: bool) -> Renderer:
    typed = _typed_paths(t)

    def json_(v: Any) -> str:
        if not isinstance(v, dict):
            raise _mismatch(t, v)
        return escape_string(json_object(v, typed), nested)

    return json_


_type_cache: dict[tuple[str, bool], Renderer] = {}


def renderer_for(type_name: str, nested: bool = False) -> Renderer:
    key = (type_name, nested)
    fn = _type_cache.get(key)
    if fn is None:
        fn = compile_renderer(parse_type(type_name), nested)
        _type_cache[key] = fn
    return fn


def _variant_renderer(t: CHType, nested: bool) -> Renderer:
    null_text = "NULL" if nested else "\\N"

    def variant(v: Any) -> str:
        if v is None:
            return null_text
        # The shim reads Variant and Geometry with the documented "typed" read format: TypedVariant(value,
        # type_name).
        type_name = getattr(v, "type_name", None)
        if type_name is None or not hasattr(v, "value"):
            raise _mismatch(t, v)
        return renderer_for(type_name, nested)(v.value)

    return variant


def render_dynamic(v: Any, nested: bool) -> str:
    """Write a Dynamic value (or a value of a JSON path). The client gives the Python value without its
    ClickHouse type, so the type comes from the Python type. A Float32, or a DateTime64 or Time64 of
    another precision than the one that the Python value shows, cannot be written correctly."""
    if v is None:
        return "NULL" if nested else "\\N"
    type_name = getattr(v, "type_name", None)
    if type_name is not None and hasattr(v, "value"):
        return renderer_for(type_name, nested)(v.value)
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return _dynamic_float(v)
    if isinstance(v, decimal.Decimal):
        return format_decimal(v)
    if isinstance(v, (str, bytes)):
        return escape_string(to_text(v), nested)
    if isinstance(v, dt.datetime):
        nanos = v.microsecond * 1000 + getattr(v, "nanosecond", 0)
        precision = 0 if nanos == 0 else (3 if nanos % 1_000_000 == 0 else (6 if nanos % 1000 == 0 else 9))
        return _quote_if(format_datetime(v, precision), nested)
    if isinstance(v, dt.date):
        return _quote_if(format_date(v), nested)
    if isinstance(v, dt.timedelta):
        return _quote_if(format_time(v, 0 if v.microseconds == 0 else 6), nested)
    if isinstance(v, uuid.UUID):
        return _quote_if(str(v), nested)
    if isinstance(v, ipaddress.IPv4Address):
        return _quote_if(str(v), nested)
    if isinstance(v, ipaddress.IPv6Address):
        return _quote_if(format_ipv6(v.packed), nested)
    if isinstance(v, list):
        return "[" + ",".join(render_dynamic(x, True) for x in v) + "]"
    if isinstance(v, tuple):
        return "(" + ",".join(render_dynamic(x, True) for x in v) + ")"
    if isinstance(v, dict):
        return "{" + ",".join(f"{render_dynamic(k, True)}:{render_dynamic(x, True)}" for k, x in v.items()) + "}"
    raise RenderError(f"clickhouse-test-runner: no TabSeparated renderer for the Dynamic value {v!r} ({type(v).__name__})")


def _dynamic_float(v: float) -> str:
    """A Float32 and a Float64 are both a Python float. A value that is exactly a Float32 and has a shorter
    Float32 text is taken as a Float32."""
    text64 = format_float(v, 64)
    if math.isfinite(v) and _f32(v) == v:
        text32 = format_float(v, 32)
        if len(text32) < len(text64):
            return text32
    return text64


def render_untyped(v: Any) -> str:
    """Write a value that came without a column type. Client.query gives a result without column types
    when it sends the statement through Client.command (for example CHECK TABLE)."""
    if v is None:
        return "\\N"
    if isinstance(v, (list, tuple)):
        return "\t".join(render_untyped(x) for x in v)
    return escape_string(to_text(str(v)) if not isinstance(v, bytes) else to_text(v), False)


def is_string_type(t: CHType) -> bool:
    while t.base in ("Nullable", "LowCardinality") and t.type_arg(0) is not None:
        t = t.type_arg(0)  # type: ignore[assignment]
    return t.base in _STRING_TYPES


class ColumnWriter:
    """Writes the values of one result column."""

    def __init__(self, type_name: str):
        self.type_name = type_name
        self.type = parse_type(type_name)
        self.render = renderer_for(type_name)
        self.raw_string = is_string_type(self.type)

    def write(self, v: Any, raw: bool = False) -> str:
        # The TSVRaw formats write a top-level string without escapes.
        if raw and self.raw_string and isinstance(v, (str, bytes)):
            return to_text(v)
        return self.render(v)
