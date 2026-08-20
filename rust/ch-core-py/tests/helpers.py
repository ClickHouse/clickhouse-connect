"""Tests for _ch_core Python bindings - Phase 1 types."""

import array
import datetime as dt
import decimal
import ipaddress
import math
import os
import struct
import subprocess
import sys
import textwrap
import uuid
from zoneinfo import ZoneInfo

import pytest

__all__ = [
    "ZoneInfo",
    "array",
    "decimal",
    "math",
    "os",
    "subprocess",
    "sys",
    "textwrap",
]

_ch_core = pytest.importorskip("_ch_core")

_EPOCH_DATE = dt.date(1970, 1, 1)
_EPOCH_NAIVE = dt.datetime(1970, 1, 1)


class _NdarrayLikeColumn:
    def __init__(self, values):
        self._values = values

    def __len__(self):
        return len(self._values)

    def __getitem__(self, index):
        return self._values[index]


class _SeriesLikeColumn:
    def __init__(self, values):
        self._values = values
        self.iloc = _NdarrayLikeColumn(values)

    def __len__(self):
        return len(self._values)

    def __getitem__(self, index):
        raise KeyError(index)


def _encode_varint(value: int) -> bytes:
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        result.append(byte)
        if value == 0:
            break
    return bytes(result)


def _encode_varint_string(s: str) -> bytes:
    encoded = s.encode("utf-8")
    return _encode_varint(len(encoded)) + encoded


# Type -> (struct format, byte width) for fixed-width types
_FIXED_TYPES = {
    "Bool": ("B", 1),
    "Int8": ("b", 1),
    "Int16": ("<h", 2),
    "Int32": ("<i", 4),
    "Int64": ("<q", 8),
    "UInt8": ("B", 1),
    "UInt16": ("<H", 2),
    "UInt32": ("<I", 4),
    "UInt64": ("<Q", 8),
    "Float32": ("<f", 4),
    "Float64": ("<d", 8),
}

# Type -> (wire byte width, signedness). Wide integers stay raw little-endian
# fixed-width values in the Native and Arrow representations.
_WIDE_TYPES = {
    "Int128": (16, True),
    "UInt128": (16, False),
    "Int256": (32, True),
    "UInt256": (32, False),
}

_INTERVAL_TYPES = (
    "IntervalYear",
    "IntervalQuarter",
    "IntervalMonth",
    "IntervalWeek",
    "IntervalDay",
    "IntervalHour",
    "IntervalMinute",
    "IntervalSecond",
    "IntervalMillisecond",
    "IntervalMicrosecond",
    "IntervalNanosecond",
)


def _temporal_struct_fmt(inner_type: str):
    """Wire struct format for a temporal type, or None if not temporal.

    Temporal columns are plain bulk integers on the wire; timezone and precision
    are type-name metadata only. Values are passed as raw units (Date/Date32 days,
    DateTime seconds, DateTime64 ticks, Time seconds, and Time64 ticks).
    """
    if inner_type == "Date":
        return "<H"  # u16 days
    if inner_type == "Date32":
        return "<i"  # i32 days
    if inner_type == "DateTime" or inner_type.startswith("DateTime("):
        return "<I"  # u32 seconds
    if inner_type.startswith("DateTime64("):
        return "<q"  # i64 ticks
    if inner_type == "Time":
        return "<i"  # i32 seconds
    if inner_type.startswith("Time64("):
        return "<q"  # i64 ticks
    if inner_type in _INTERVAL_TYPES:
        return "<q"  # i64 count in the named interval unit
    return None


def _decimal_wire_width(precision: int) -> int:
    """Wire byte width of Decimal(P, S), derived from the precision."""
    if precision <= 9:
        return 4
    if precision <= 18:
        return 8
    if precision <= 38:
        return 16
    return 32


def _uuid_wire_bytes(value: uuid.UUID) -> bytes:
    """UUID wire form: little-endian high half then little-endian low half."""
    return (value.int >> 64).to_bytes(8, "little") + (
        value.int & 0xFFFFFFFFFFFFFFFF
    ).to_bytes(8, "little")


def _bfloat16_bytes(value) -> bytes:
    """Truncate a Float32 to its upper 16 bits in Native little-endian order."""
    bits32 = struct.unpack("<I", struct.pack("<f", value))[0]
    return struct.pack("<H", bits32 >> 16)


def _bfloat16_value(value) -> float:
    """Return the Python float represented by ClickHouse's truncated BF16 word."""
    return struct.unpack("<f", b"\x00\x00" + _bfloat16_bytes(value))[0]


def _encode_lc_dict_values(dict_values, value_type):
    """Serialize a LowCardinality block dictionary (the inner type's bulk form)."""
    buf = bytearray()
    if value_type == "String":
        for v in dict_values:
            encoded = v if isinstance(v, bytes) else v.encode("utf-8")
            buf.extend(_encode_varint(len(encoded)))
            buf.extend(encoded)
    elif value_type in _FIXED_TYPES:
        fmt, _ = _FIXED_TYPES[value_type]
        for v in dict_values:
            buf.extend(struct.pack(fmt, v))
    elif value_type in _WIDE_TYPES:
        width, signed = _WIDE_TYPES[value_type]
        for v in dict_values:
            buf.extend(int(v).to_bytes(width, "little", signed=signed))
    elif value_type == "BFloat16":
        for v in dict_values:
            buf.extend(_bfloat16_bytes(v))
    elif (temporal_fmt := _temporal_struct_fmt(value_type)) is not None:
        for v in dict_values:
            buf.extend(struct.pack(temporal_fmt, v))
    elif value_type == "UUID":
        for v in dict_values:
            u = v if isinstance(v, uuid.UUID) else uuid.UUID(int=v)
            buf.extend(_uuid_wire_bytes(u))
    else:
        raise ValueError(f"_encode_lc_dict_values: unsupported inner type {value_type}")
    return buf


def _lc_key_version() -> bytes:
    """LowCardinality per-column state prefix: the u64 key version."""
    return struct.pack("<Q", 1)


def _build_low_cardinality_body_no_prefix(type_name, values):
    """LowCardinality(T) body without the hoisted key version.

    Per block: a u64 index-type word (HasAdditionalKeysBit and
    NeedUpdateDictionary set, UInt8 index width here), a u64 dictionary size,
    the dictionary values, a u64 row count, and the UInt8 index per row. For a
    Nullable inner type, dictionary slot 0 is the NULL sentinel and null rows
    index it. A zero-length run writes nothing at all (server limit == 0 early
    return), which is how an Array of all-empty rows encodes its LowCardinality
    element column.
    """
    inner = type_name[len("LowCardinality("):-1]
    nullable, value_type = _lc_dict_value_type(inner)

    if len(values) == 0:
        return b""

    dict_values = []
    slot_of = {}
    if nullable:
        dict_values.append(b"" if value_type == "String" else 0)  # slot 0 sentinel
    indices = []
    for v in values:
        if nullable and v is None:
            indices.append(0)
            continue
        if v not in slot_of:
            slot_of[v] = len(dict_values)
            dict_values.append(v)
        indices.append(slot_of[v])
    if len(dict_values) > 256 or any(i > 255 for i in indices):
        raise ValueError("_build_low_cardinality_body: test helper only emits UInt8 indices")

    buf = bytearray()
    # HasAdditionalKeysBit | NeedUpdateDictionary | UInt8 index width (tag 0).
    buf.extend(struct.pack("<Q", 0x600))
    buf.extend(struct.pack("<Q", len(dict_values)))
    buf.extend(_encode_lc_dict_values(dict_values, value_type))
    buf.extend(struct.pack("<Q", len(values)))
    for i in indices:
        buf.append(i)
    return bytes(buf)


def _build_low_cardinality_body(type_name, values):
    """Full LowCardinality(T) column body: the hoisted key version then the rest."""
    return _lc_key_version() + _build_low_cardinality_body_no_prefix(type_name, values)


def _encode_plain_body(inner_type, values):
    """Encode a plain (non-wrapper) column body; None renders as the type default."""
    buf = bytearray()
    if inner_type == "Nothing":
        buf.extend(b"0" * len(values))
    elif inner_type in _FIXED_TYPES:
        fmt, _ = _FIXED_TYPES[inner_type]
        default = 0 if "Int" in inner_type or "UInt" in inner_type else (
            0.0 if "Float" in inner_type else 0
        )
        for v in values:
            buf.extend(struct.pack(fmt, v if v is not None else default))
    elif inner_type == "BFloat16":
        for v in values:
            buf.extend(_bfloat16_bytes(v if v is not None else 0.0))
    elif inner_type in _WIDE_TYPES:
        width, signed = _WIDE_TYPES[inner_type]
        for v in values:
            buf.extend(int(v if v is not None else 0).to_bytes(width, "little", signed=signed))
    elif (temporal_fmt := _temporal_struct_fmt(inner_type)) is not None:
        for v in values:
            buf.extend(struct.pack(temporal_fmt, v if v is not None else 0))
    elif inner_type == "String":
        for v in values:
            s = v if v is not None else ""
            encoded = s if isinstance(s, bytes) else s.encode("utf-8")
            buf.extend(_encode_varint(len(encoded)))
            buf.extend(encoded)
    elif inner_type.startswith("FixedString("):
        width = int(inner_type[len("FixedString("):-1])
        for v in values:
            b = v if v is not None else b"\x00" * width
            if isinstance(b, str):
                b = b.encode("utf-8")
            buf.extend(bytes(b[:width]).ljust(width, b"\x00"))
    elif inner_type.startswith("Enum8(") or inner_type.startswith("Enum16("):
        fmt = "b" if inner_type.startswith("Enum8(") else "<h"
        for v in values:
            buf.extend(struct.pack(fmt, v if v is not None else 0))
    elif inner_type == "UUID":
        for v in values:
            buf.extend(_uuid_wire_bytes(v if v is not None else uuid.UUID(int=0)))
    elif inner_type == "IPv4":
        for v in values:
            addr = ipaddress.IPv4Address(v if v is not None else 0)
            buf.extend(int(addr).to_bytes(4, "little"))
    elif inner_type == "IPv6":
        for v in values:
            buf.extend(ipaddress.IPv6Address(v if v is not None else 0).packed)
    elif inner_type.startswith("Decimal("):
        precision = int(inner_type[len("Decimal("):-1].split(",")[0])
        width = _decimal_wire_width(precision)
        for v in values:
            buf.extend(int(v if v is not None else 0).to_bytes(width, "little", signed=True))
    else:
        raise ValueError(f"build_native_block: unsupported type {inner_type}")
    return bytes(buf)


def _split_top_level_commas(s):
    """Split on commas at paren-depth 0, skipping single-quote and backtick spans."""
    parts, start, depth, quote = [], 0, 0, None
    i = 0
    while i < len(s):
        ch = s[i]
        if quote is not None:
            if ch == "\\":
                i += 2
                continue
            if ch == quote:
                if quote == "`" and i + 1 < len(s) and s[i + 1] == "`":
                    i += 2
                    continue
                quote = None
        elif ch in "'`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(s[start:i])
            start = i + 1
        i += 1
    parts.append(s[start:])
    return [p.strip() for p in parts]


def _parse_tuple_element(part):
    """Parse one `Tuple(...)` element into (name_or_None, type_str)."""
    if part.startswith("`"):
        end = part.index("`", 1)
        return part[1:end], part[end + 1:].strip()
    # A named element has a top-level space before its type; an unnamed one
    # (including Decimal(9, 4) or DateTime64(3, 'tz')) has its spaces nested.
    depth, quote = 0, None
    for i, ch in enumerate(part):
        if quote is not None:
            if ch == quote:
                quote = None
        elif ch in "'`":
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == " " and depth == 0:
            return part[:i], part[i + 1:].strip()
    return None, part


def _parse_tuple_elements(type_name):
    """List of (name_or_None, type_str) for a `Tuple(...)` type string."""
    inner = type_name[len("Tuple("):-1].strip()
    if not inner:
        return []
    return [_parse_tuple_element(p) for p in _split_top_level_commas(inner)]


def _parse_map_types(type_name):
    """(key_type, value_type) for a `Map(K, V)` type string."""
    key, value = _split_top_level_commas(type_name[len("Map("):-1])
    return key, value


_POINT_PHYSICAL = "Tuple(Float64, Float64)"

# Geo alias -> physical nesting (unnamed Point tuple, one Array level per depth).
_GEO_PHYSICAL = {
    "Point": _POINT_PHYSICAL,
    "Ring": f"Array({_POINT_PHYSICAL})",
    "LineString": f"Array({_POINT_PHYSICAL})",
    "Polygon": f"Array(Array({_POINT_PHYSICAL}))",
    "MultiLineString": f"Array(Array({_POINT_PHYSICAL}))",
    "MultiPolygon": f"Array(Array(Array({_POINT_PHYSICAL})))",
}


def _expand_alias(type_name):
    """Physical type string of a name-decoration alias, else the input unchanged.

    SimpleAggregateFunction, the six geo aliases, and Nested carry a custom name
    over a physical type whose wire body is byte-identical, so the helper emits
    the alias header but the physical body. Recurses through Nullable
    (Nullable(Point) is legal); container recursion is left to the body helpers,
    which expand each inner type in turn.
    """
    if type_name.startswith("Nullable("):
        return f"Nullable({_expand_alias(type_name[len('Nullable('):-1])})"
    if type_name in _GEO_PHYSICAL:
        return _GEO_PHYSICAL[type_name]
    if type_name.startswith("SimpleAggregateFunction("):
        return _expand_alias(
            _split_top_level_commas(type_name[len("SimpleAggregateFunction("):-1])[1]
        )
    if type_name.startswith("Nested("):
        return f"Array(Tuple({type_name[len('Nested('):-1]}))"
    return type_name


def _strip_saf(type_name):
    """Strip a SimpleAggregateFunction name-decoration chain to its inner type."""
    while type_name.startswith("SimpleAggregateFunction("):
        type_name = _split_top_level_commas(type_name[len("SimpleAggregateFunction("):-1])[1]
    return type_name


def _lc_dict_value_type(inner):
    """(nullable, physical_value_type) for a LowCardinality inner, mirroring the
    core: strip the SAF chain, unwrap an optional Nullable, strip the SAF chain
    again. LowCardinality(SAF(anyLast, Nullable(String))) is index-level nullable."""
    inner = _strip_saf(inner)
    if inner.startswith("Nullable("):
        return True, _strip_saf(inner[len("Nullable("):-1])
    return False, inner


def _element_state_prefix(type_name):
    """State prefix hoisted to the front of a column, recursing into containers.

    Only LowCardinality contributes bytes (its u64 key version). Array, Tuple,
    Map, and Nullable recurse into their inner types in serialization order;
    every leaf writes nothing. Matches the core's read_state_prefix chain.
    """
    type_name = _expand_alias(type_name)
    if type_name.startswith("Array("):
        return _element_state_prefix(type_name[len("Array("):-1])
    if type_name.startswith("Nullable("):
        return _element_state_prefix(type_name[len("Nullable("):-1])
    if type_name.startswith("Tuple("):
        return b"".join(_element_state_prefix(t) for _, t in _parse_tuple_elements(type_name))
    if type_name.startswith("Map("):
        key, value = _parse_map_types(type_name)
        return _element_state_prefix(key) + _element_state_prefix(value)
    if type_name.startswith("LowCardinality("):
        return _lc_key_version()
    return b""


def _build_body_no_prefix(type_name, values):
    """Column body with its state prefix omitted (already hoisted by the caller)."""
    type_name = _expand_alias(type_name)
    if type_name.startswith("Array("):
        return _build_array_body(type_name, values)
    if type_name.startswith("Map("):
        return _build_map_body(type_name, values)
    if type_name.startswith("Tuple("):
        return _build_tuple_body(type_name, values, nullable=False)
    if type_name.startswith("LowCardinality("):
        return _build_low_cardinality_body_no_prefix(type_name, values)
    if type_name.startswith("Nullable("):
        inner = type_name[len("Nullable("):-1]
        # Nullable(Tuple) is the only nullable container: the tuple-level null
        # map precedes the tuple body (a null row still carries placeholder
        # element values on the wire).
        if inner.startswith("Tuple("):
            return _build_tuple_body(inner, values, nullable=True)
        buf = bytearray()
        for v in values:
            buf.append(0x01 if v is None else 0x00)
        buf.extend(_encode_plain_body(inner, values))
        return bytes(buf)
    return _encode_plain_body(type_name, values)


def _build_array_body(type_name, rows):
    """Array(T) body: absolute end-offsets (no leading zero) then the element body.

    The element body is the flattened element column written WITHOUT its state
    prefix, which is hoisted to the front of the whole column.
    """
    inner = type_name[len("Array("):-1]
    flat = []
    offsets = bytearray()
    for row in rows:
        flat.extend(row)
        offsets.extend(struct.pack("<Q", len(flat)))
    return bytes(offsets) + _build_body_no_prefix(inner, flat)


def _build_tuple_body(type_name, rows, nullable):
    """Tuple(T1, ...) body: optional tuple null map, then each element column.

    Each row is a sequence of element values in declaration order. A None row
    (only for Nullable(Tuple)) sets the null map bit and writes each element's
    default as a placeholder, exactly as the server serializes a null tuple row.
    The zero-element Tuple() writes one placeholder byte ('0') per row.
    """
    elements = _parse_tuple_elements(type_name)
    buf = bytearray()
    if nullable:
        for row in rows:
            buf.append(0x01 if row is None else 0x00)
    if not elements:
        buf.extend(b"0" * len(rows))
        return bytes(buf)
    columns = [[] for _ in elements]
    for row in rows:
        for ix in range(len(elements)):
            columns[ix].append(None if row is None else row[ix])
    for (_, etype), column in zip(elements, columns):
        buf.extend(_build_body_no_prefix(etype, column))
    return bytes(buf)


def _build_map_body(type_name, rows):
    """Map(K, V) body: absolute end-offsets (no leading zero), then the flattened
    keys column and the flattened values column. Each row is a dict."""
    key_type, value_type = _parse_map_types(type_name)
    keys, values = [], []
    offsets = bytearray()
    for row in rows:
        for k, v in row.items():
            keys.append(k)
            values.append(v)
        offsets.extend(struct.pack("<Q", len(keys)))
    buf = bytearray(offsets)
    buf.extend(_build_body_no_prefix(key_type, keys))
    buf.extend(_build_body_no_prefix(value_type, values))
    return bytes(buf)


def build_native_block(columns, *, block_info=False):
    """Build a ClickHouse Native format block from column specs.

    Each column is (name, type_name, values).
    Supports: Nothing, Bool, Int8/16/32/64/128/256, UInt8/16/32/64/128/256, Float32,
    Float64, String, FixedString(N),
    UUID, IPv4, IPv6, Enum8/16, Decimal, temporal, Nullable(*), LowCardinality(*),
    and Array(*) over those inner types. For Array(T) each value is a sequence of
    elements.
    """
    buf = bytearray()
    if block_info:
        # Old two-field BlockInfo preamble:
        # field 1 is_overflows=false, field 2 bucket_num=0, terminator=0.
        buf.extend(b"\x01\x00\x02\x00\x00\x00\x00\x00")
    num_cols = len(columns)
    num_rows = len(columns[0][2]) if columns else 0
    buf.extend(_encode_varint(num_cols))
    buf.extend(_encode_varint(num_rows))
    for name, type_name, values in columns:
        buf.extend(_encode_varint_string(name))
        buf.extend(_encode_varint_string(type_name))

        # A zero-row block carries only column headers, no body (server rows>0 gate).
        if num_rows == 0:
            continue

        # Emit the alias header but the physical type's body: a name-decoration
        # alias (geo, Nested, SimpleAggregateFunction) is byte-identical on the
        # wire to its physical type.
        physical = _expand_alias(type_name)

        if physical.startswith("LowCardinality("):
            buf.extend(_build_low_cardinality_body(physical, values))
            continue

        if physical.startswith("Array("):
            buf.extend(_element_state_prefix(physical))
            buf.extend(_build_array_body(physical, values))
            continue

        nullable_tuple = (
            physical.startswith("Nullable(")
            and physical[len("Nullable("):-1].startswith("Tuple(")
        )
        if physical.startswith("Tuple(") or physical.startswith("Map(") or nullable_tuple:
            buf.extend(_element_state_prefix(physical))
            buf.extend(_build_body_no_prefix(physical, values))
            continue

        is_nullable = physical.startswith("Nullable(")
        inner_type = physical[len("Nullable("):-1] if is_nullable else physical
        if is_nullable:
            for v in values:
                buf.append(0x01 if v is None else 0x00)
        buf.extend(_encode_plain_body(inner_type, values))
    return bytes(buf)


def build_native_block_from_bodies(columns, row_count):
    """Build a Native block from already-encoded column bodies."""
    buf = bytearray()
    buf.extend(_encode_varint(len(columns)))
    buf.extend(_encode_varint(row_count))
    for name, type_name, body in columns:
        buf.extend(_encode_varint_string(name))
        buf.extend(_encode_varint_string(type_name))
        buf.extend(body)
    return bytes(buf)


# ---------------------------------------------------------------------------
# Native insert encoding
# ---------------------------------------------------------------------------
