"""Tests for _ch_core Python bindings - Phase 1 types."""

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


class TestEncodeNativeBlock:
    def test_indexable_non_sequence_columns_match_native_helper(self):
        names = ["i", "s"]
        type_names = ["Int32", "String"]
        columns = [
            _NdarrayLikeColumn([13, 79]),
            _SeriesLikeColumn(["user_1", "user_2"]),
        ]

        encoded = _ch_core.encode_native_block(names, type_names, columns, 2)
        expected = build_native_block(
            [
                ("i", "Int32", [13, 79]),
                ("s", "String", ["user_1", "user_2"]),
            ]
        )
        assert encoded == expected

    @pytest.mark.parametrize("bad_values", ["xy", b"xy", bytearray(b"xy")])
    def test_bare_string_or_bytes_column_rejected(self, bad_values):
        with pytest.raises(ValueError, match="bare str or bytes"):
            _ch_core.encode_native_block(["s"], ["String"], [bad_values], len(bad_values))

    def test_common_types_match_native_helper(self):
        enum_type = "Enum8('red' = 1, 'green' = 2)"
        names = ["i", "s", "n", "fs", "d", "ts", "e"]
        type_names = [
            "Int32",
            "String",
            "Nullable(UInt16)",
            "FixedString(4)",
            "Date",
            "DateTime64(3)",
            enum_type,
        ]
        aware_ts = dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)
        columns = [
            [13, 79],
            ["user_1", b"\xff"],
            [13, None],
            ["ab", b"xy\x00\x00"],
            [dt.date(1970, 1, 2), 19737],
            [aware_ts, 0],
            ["red", 2],
        ]
        encoded = _ch_core.encode_native_block(names, type_names, columns, 2)
        expected = build_native_block(
            [
                ("i", "Int32", [13, 79]),
                ("s", "String", ["user_1", b"\xff"]),
                ("n", "Nullable(UInt16)", [13, None]),
                ("fs", "FixedString(4)", [b"ab", b"xy"]),
                ("d", "Date", [1, 19737]),
                ("ts", "DateTime64(3)", [1705322096789, 0]),
                ("e", enum_type, [1, 2]),
            ]
        )
        assert encoded == expected

    def test_datetime64_pre_epoch_fractional_encode(self):
        values = [
            dt.datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=dt.timezone.utc),
            dt.datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=dt.timezone.utc),
            dt.datetime(1970, 1, 1, 0, 0, 0, 999000, tzinfo=dt.timezone.utc),
        ]
        encoded = _ch_core.encode_native_block(["ts"], ["DateTime64(3)"], [values], len(values))
        expected = build_native_block([("ts", "DateTime64(3)", [-500, -1, 999])])
        assert encoded == expected
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            dt.datetime(1969, 12, 31, 23, 59, 59, 500000),
            dt.datetime(1969, 12, 31, 23, 59, 59, 999000),
            dt.datetime(1970, 1, 1, 0, 0, 0, 999000),
        ]

    @pytest.mark.parametrize("type_name", _INTERVAL_TYPES)
    def test_interval_round_trip_preserves_kind_and_signed_i64(self, type_name):
        values = [-(2**63), -79, 0, 13, 2**63 - 1]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))

        assert encoded == build_native_block([("v", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert batch.to_python_rows() == [(value,) for value in values]

    def test_interval_wrappers_and_containers_round_trip(self):
        columns = [
            ("scalar", "IntervalDay", [-13, 0, 79]),
            ("nullable", "Nullable(IntervalHour)", [-13, None, 79]),
            ("array", "Array(IntervalMinute)", [[-13, 79], [], [0]]),
            ("tuple", "Tuple(IntervalSecond, String)", [(-13, "x"), (0, "y"), (79, "z")]),
            (
                "array_tuple",
                "Array(Tuple(IntervalMillisecond, IntervalMonth))",
                [[(-13, 1), (79, -2)], [], [(0, 3)]],
            ),
            ("map", "Map(IntervalDay, String)", [{-13: "x"}, {}, {79: "z"}]),
            ("low_cardinality", "LowCardinality(IntervalHour)", [13, 79, 13]),
        ]
        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in columns],
            [type_name for _, type_name, _ in columns],
            [values for _, _, values in columns],
            3,
        )

        assert encoded == build_native_block(columns)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name for _, type_name, _ in columns]
        assert batch.to_python_columns() == [values for _, _, values in columns]

    @pytest.mark.parametrize(
        "type_name",
        ["intervalDay", "Intervalday", "INTERVALDAY", "IntervalDays", "IntervalDay()"],
    )
    def test_interval_type_names_are_exact(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[13]], 1)

    def test_prefix_is_prepended(self):
        payload = _ch_core.encode_native_block(["v"], ["Int8"], [[13]], 1)
        prefix = b"INSERT INTO t FORMAT Native\n"
        encoded = _ch_core.encode_native_block(["v"], ["Int8"], [[13]], 1, prefix=prefix)
        assert encoded == prefix + payload

    def test_low_cardinality_round_trip(self):
        vals = ["x", None, "y", "x", None, ""]
        encoded = _ch_core.encode_native_block(["c"], ["LowCardinality(Nullable(String))"], [vals], len(vals))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == ["LowCardinality(Nullable(String))"]
        assert list(batch.column_data(0)) == vals

    def test_uuid_bytes_match_python_serializer_order(self):
        value_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        encoded = _ch_core.encode_native_block(["u"], ["UUID"], [[value_uuid.bytes]], 1)
        expected_body = bytearray()
        expected_body.extend(bytes(reversed(value_uuid.bytes[:8])))
        expected_body.extend(bytes(reversed(value_uuid.bytes[8:])))
        expected = build_native_block_from_bodies([("u", "UUID", expected_body)], 1)
        assert encoded == expected

    def test_special_binary_types_exact_bytes(self):
        value_uuid = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        ipv4_values = ["192.0.2.1", ipaddress.IPv4Address("198.51.100.7")]
        ipv6_values = [ipaddress.IPv6Address("2001:db8::1"), "192.0.2.9"]
        decimals = [decimal.Decimal("123.4567"), "-1.5"]

        encoded = _ch_core.encode_native_block(
            ["u", "v4", "v6", "dec"],
            ["UUID", "IPv4", "IPv6", "Decimal(20, 4)"],
            [[value_uuid, 0], ipv4_values, ipv6_values, decimals],
            2,
        )

        uuid_body = bytearray()
        uuid_int = value_uuid.int
        uuid_body.extend((uuid_int >> 64).to_bytes(8, "little"))
        uuid_body.extend((uuid_int & 0xFFFFFFFFFFFFFFFF).to_bytes(8, "little"))
        uuid_body.extend(bytes(16))

        ipv4_body = bytearray()
        for value in ipv4_values:
            ipv4_body.extend(int(ipaddress.IPv4Address(value)).to_bytes(4, "little"))

        ipv6_body = bytearray()
        ipv6_body.extend(ipv6_values[0].packed)
        ipv6_body.extend(b"\x00" * 10 + b"\xff\xff" + ipaddress.IPv4Address(ipv6_values[1]).packed)

        decimal_body = bytearray()
        decimal_body.extend((1234567).to_bytes(16, "little", signed=True))
        decimal_body.extend((-15000).to_bytes(16, "little", signed=True))

        expected = build_native_block_from_bodies(
            [
                ("u", "UUID", uuid_body),
                ("v4", "IPv4", ipv4_body),
                ("v6", "IPv6", ipv6_body),
                ("dec", "Decimal(20, 4)", decimal_body),
            ],
            2,
        )
        assert encoded == expected

    def test_decimal_precision_boundary_and_overflow(self):
        encoded = _ch_core.encode_native_block(
            ["d"],
            ["Decimal(3, 1)"],
            [[decimal.Decimal("99.9"), decimal.Decimal("-99.9")]],
            2,
        )
        body = bytearray()
        body.extend((999).to_bytes(4, "little", signed=True))
        body.extend((-999).to_bytes(4, "little", signed=True))
        assert encoded == build_native_block_from_bodies([("d", "Decimal(3, 1)", body)], 2)

        for value in (decimal.Decimal("100.0"), decimal.Decimal("-100.0"), "999"):
            with pytest.raises(ValueError, match="exceeds precision 3"):
                _ch_core.encode_native_block(["d"], ["Decimal(3, 1)"], [[value]], 1)

    def test_decimal_str_failure_is_value_error_with_context(self):
        class BadDecimalStr:
            def __str__(self):
                raise RuntimeError("cannot render")

        with pytest.raises(ValueError, match='column "d" row 0 Decimal value cannot be stringified'):
            _ch_core.encode_native_block(["d"], ["Decimal(9, 2)"], [[BadDecimalStr()]], 1)

    def test_encode_errors_are_specific(self):
        with pytest.raises(ValueError, match="row_count"):
            _ch_core.encode_native_block(["v"], ["Int8"], [[13, 79]], 1)
        with pytest.raises(ValueError, match="not Nullable"):
            _ch_core.encode_native_block(["v"], ["Int8"], [[None]], 1)
        with pytest.raises(ValueError, match="FixedString binary value"):
            _ch_core.encode_native_block(["fs"], ["FixedString(4)"], [[b"xy"]], 1)
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], ["JSON"], [[{"a": 1}]], 1)
        with pytest.raises(ValueError, match="label"):
            _ch_core.encode_native_block(["e"], ["Enum8('ok' = 1)"], [["missing"]], 1)


class TestNothing:
    @pytest.mark.parametrize(
        ("type_name", "values", "expected"),
        [
            ("Nothing", [13, None, "ignored"], [None, None, None]),
            ("Nullable(Nothing)", [13, None, "ignored"], [None, None, None]),
            (
                "Array(Nothing)",
                [[13, None], [], ["ignored"]],
                [[None, None], [], [None]],
            ),
            (
                "Array(Nullable(Nothing))",
                [[None, None], [], [None]],
                [[None, None], [], [None]],
            ),
            (
                "Tuple(Nothing, UInt8)",
                [(13, 13), (None, 79), ("ignored", 5)],
                [(None, 13), (None, 79), (None, 5)],
            ),
            (
                "Tuple(Nullable(Nothing), UInt8)",
                [(None, 13), (None, 79), (None, 5)],
                [(None, 13), (None, 79), (None, 5)],
            ),
            (
                "Array(Tuple(Nothing, UInt8))",
                [[(13, 13), (None, 79)], [], [("ignored", 5)]],
                [[(None, 13), (None, 79)], [], [(None, 5)]],
            ),
            (
                "Map(Nothing, UInt8)",
                [{13: 13}, {}, {"ignored": 79}],
                [{None: 13}, {}, {None: 79}],
            ),
            (
                "Map(UInt8, Nothing)",
                [{13: 13}, {}, {79: "ignored"}],
                [{13: None}, {}, {79: None}],
            ),
            (
                "Map(UInt8, Nullable(Nothing))",
                [{13: None}, {}, {79: None}],
                [{13: None}, {}, {79: None}],
            ),
        ],
    )
    def test_encode_decode_type_matrix_and_all_object_exits(self, type_name, values, expected):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_noncanonical_wire_placeholder_bytes_are_ignored(self):
        data = build_native_block_from_bodies(
            [
                ("v", "Nothing", b"\x00\x7f\xff"),
                ("sentinel", "UInt8", bytes([13, 79, 5])),
            ],
            3,
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [None, None, None]
        assert list(batch.column_data(1)) == [13, 79, 5]

    def test_nullable_nothing_with_structurally_valid_row_is_still_none(self):
        data = build_native_block_from_bodies([("v", "Nullable(Nothing)", b"\x00\x01" + b"01")], 2)
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [None, None]

    @pytest.mark.parametrize("type_name", ["Nothing", "Nullable(Nothing)", "Array(Nothing)"])
    def test_zero_rows(self, type_name):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [[]], 0)
        assert encoded == build_native_block([("v", type_name, [])])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == []

    @pytest.mark.parametrize("container", [list, tuple, _NdarrayLikeColumn])
    def test_python_values_only_affect_nullable_structural_mask(self, container):
        values = container([13, None, "ignored"])
        plain = _ch_core.encode_native_block(["v"], ["Nothing"], [values], 3)
        nullable = _ch_core.encode_native_block(["v"], ["Nullable(Nothing)"], [values], 3)
        assert plain == build_native_block([("v", "Nothing", [13, None, "ignored"])])
        assert nullable == build_native_block([("v", "Nullable(Nothing)", [13, None, "ignored"])])

    def test_exact_bytes(self):
        # Pin the canonical 0x30 marker run independently of build_native_block.
        plain = _ch_core.encode_native_block(["v"], ["Nothing"], [[None, None, None]], 3)
        assert plain == build_native_block_from_bodies([("v", "Nothing", b"\x30\x30\x30")], 3)
        nullable = _ch_core.encode_native_block(["v"], ["Nullable(Nothing)"], [[None, 13, None]], 3)
        assert nullable == build_native_block_from_bodies(
            [("v", "Nullable(Nothing)", b"\x01\x00\x01" + b"\x30\x30\x30")], 3
        )

    def test_multi_entry_map_nothing_keys_collapse(self):
        # Dict representation: all-None keys collide, keeping the last value.
        data = build_native_block_from_bodies(
            [("v", "Map(Nothing, UInt8)", b"\x02\x00\x00\x00\x00\x00\x00\x00" + b"00" + bytes([13, 79]))],
            1,
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [{None: 79}]

    @pytest.mark.parametrize("type_name", ["nothing", "NOTHING", "Nothing "])
    def test_type_name_is_case_sensitive(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[]], 0)

    @pytest.mark.parametrize(
        "type_name",
        ["LowCardinality(Nothing)", "LowCardinality(Nullable(Nothing))"],
    )
    def test_low_cardinality_nothing_rejected(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported LowCardinality"):
            _ch_core.encode_native_block(["v"], [type_name], [[]], 0)

    def test_truncated_marker_run_rejected(self):
        data = build_native_block_from_bodies([("v", "Nothing", b"00")], 3)
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(data)

    def test_arrow_null_type(self):
        pa = pytest.importorskip("pyarrow")
        encoded = _ch_core.encode_native_block(
            ["v", "n", "a"],
            ["Nothing", "Nullable(Nothing)", "Array(Nothing)"],
            [[None, None], [None, None], [[None], []]],
            2,
        )
        table = pa.RecordBatchReader.from_stream(_ch_core.ColBatch.decode_native(encoded)).read_all()
        assert table.schema.types == [pa.null(), pa.null(), pa.large_list(pa.null())]
        assert table.schema.field("v").nullable
        assert table.schema.field("n").nullable
        assert table.column("v").to_pylist() == [None, None]
        assert table.column("n").to_pylist() == [None, None]
        assert table.column("a").to_pylist() == [[None], []]


class TestEncodeFastPaths:
    """Exact list/tuple and buffer-protocol fast paths for primitive columns."""

    def _encode(self, type_name, values, row_count=None):
        n = len(values) if row_count is None else row_count
        return _ch_core.encode_native_block(["v"], [type_name], [values], n)

    @pytest.mark.parametrize(
        "type_name,values",
        [
            ("Int8", [-128, -1, 0, 127]),
            ("Int16", [-32768, 0, 32767]),
            ("Int32", [-(2**31), 0, 2**31 - 1]),
            ("Int64", [-(2**63), -1, 0, 2**63 - 1]),
            ("UInt8", [0, 255]),
            ("UInt16", [0, 65535]),
            ("UInt32", [0, 2**32 - 1]),
            ("UInt64", [0, 1, 2**64 - 1]),
            ("Float32", [0.0, -1.5, 3.25]),
            ("Float64", [0.0, -1.5, 1e300]),
            ("Bool", [True, False, True]),
        ],
    )
    def test_edge_values_match_helper_and_container_kinds_agree(self, type_name, values):
        from_list = self._encode(type_name, list(values))
        assert from_list == build_native_block([("v", type_name, values)])
        assert self._encode(type_name, tuple(values), len(values)) == from_list
        # Non-list, non-buffer container takes the generic path; same bytes.
        assert self._encode(type_name, _NdarrayLikeColumn(values), len(values)) == from_list

    @pytest.mark.parametrize(
        "type_name,values",
        [
            ("Int8", [0, 128]),
            ("Int8", [-129]),
            ("Int64", [2**63]),
            ("Int64", [-(2**63) - 1]),
            ("UInt8", [256]),
            ("UInt64", [-1]),
            ("UInt64", [2**64]),
            ("Float64", [10**400]),
            ("IntervalDay", [2**63]),
            ("IntervalDay", [-(2**63) - 1]),
        ],
    )
    def test_out_of_range_raises_conversion_error(self, type_name, values):
        with pytest.raises(ValueError, match=f"row {len(values) - 1} cannot be converted to {type_name}"):
            self._encode(type_name, values)

    @pytest.mark.parametrize("value", [1.5, "5", dt.timedelta(days=5)])
    def test_interval_rejects_non_int_values(self, value):
        with pytest.raises(ValueError, match="row 0 cannot be converted to IntervalDay"):
            self._encode("IntervalDay", [value])

    def test_none_in_non_nullable_raises(self):
        with pytest.raises(ValueError, match='column "v" row 1 is None but Int64 is not Nullable'):
            self._encode("Int64", [3, None, 5])

    @pytest.mark.parametrize("make", [list, tuple])
    def test_nullable_list_and_tuple(self, make):
        values = [3, None, -(2**63), None, 2**63 - 1]
        encoded = self._encode("Nullable(Int64)", make(values), len(values))
        assert encoded == build_native_block([("v", "Nullable(Int64)", values)])

    def test_fallback_types_still_accepted(self):
        import enum

        class IntLike(enum.IntEnum):
            SEVEN = 7

        values = [True, IntLike.SEVEN, 3]
        assert self._encode("Int64", values) == build_native_block([("v", "Int64", [1, 7, 3])])
        # Exact ints take the fast path into floats; bool goes through the fallback.
        assert self._encode("Float64", [1, True, 2.5]) == build_native_block(
            [("v", "Float64", [1.0, 1.0, 2.5])]
        )

    def test_float32_overflow_becomes_inf(self):
        encoded = self._encode("Float32", [1e300])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_data(0)[0] == float("inf")

    def test_list_resized_during_fallback_raises(self):
        values = [1, None, 3, 4]

        class Evil:
            def __index__(self):
                del values[2:]
                return 7

        values[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode("Int64", values, 4)

    @pytest.mark.parametrize(
        "type_name,dtype,values",
        [
            ("Int8", "int8", [-128, 0, 127]),
            ("Int64", "int64", [-(2**63), 0, 2**63 - 1]),
            ("IntervalDay", "int64", [-(2**63), 0, 2**63 - 1]),
            ("UInt64", "uint64", [0, 2**64 - 1]),
            ("Float32", "float32", [0.0, -1.5, 3.25]),
            ("Float64", "float64", [0.0, -1.5, 1e300]),
        ],
    )
    def test_numpy_buffer_matches_list_path(self, type_name, dtype, values):
        np = pytest.importorskip("numpy")
        arr = np.array(values, dtype=dtype)
        assert self._encode(type_name, arr, len(values)) == self._encode(type_name, list(values))

    def test_numpy_strided_view_matches_list_path(self):
        np = pytest.importorskip("numpy")
        arr = np.arange(10, dtype="int64")[::2]
        assert self._encode("Int64", arr, 5) == self._encode("Int64", list(arr))

    def test_numpy_mismatched_dtype_falls_back(self):
        np = pytest.importorskip("numpy")
        arr = np.array([1, 2, 3], dtype="int32")
        assert self._encode("Int64", arr, 3) == self._encode("Int64", [1, 2, 3])

    def test_numpy_nullable_is_all_valid(self):
        np = pytest.importorskip("numpy")
        arr = np.array([1.5, 2.5], dtype="float64")
        expected = build_native_block([("v", "Nullable(Float64)", [1.5, 2.5])])
        assert self._encode("Nullable(Float64)", arr, 2) == expected

    @pytest.mark.parametrize(
        "type_name,dtype,values",
        [
            ("Int64", ">i8", [1, 2, 3]),
            ("Float64", ">f8", [1.5, -2.5, 1e300]),
        ],
    )
    def test_numpy_non_native_byte_order_matches_list_path(self, type_name, dtype, values):
        np = pytest.importorskip("numpy")
        arr = np.array(values, dtype=dtype)
        assert self._encode(type_name, arr, len(values)) == self._encode(type_name, list(values))

    def test_char_format_buffer_still_raises_for_uint8(self):
        view = memoryview(b"AB").cast("c")
        with pytest.raises(ValueError, match="cannot be converted to UInt8"):
            self._encode("UInt8", view, 2)

    def test_tuple_fallback_types_still_accepted(self):
        import enum

        class IntLike(enum.IntEnum):
            SEVEN = 7

        values = (True, IntLike.SEVEN, 3)
        assert self._encode("Int64", values, 3) == build_native_block([("v", "Int64", [1, 7, 3])])
        with pytest.raises(ValueError, match="row 1 cannot be converted to Int64"):
            self._encode("Int64", (1, "x", 3), 3)


class TestLowCardinalityDictCache:
    """LC read exits materialize each dictionary value once and reuse it."""

    def _batch(self, type_name, vals):
        encoded = _ch_core.encode_native_block(["lc"], [type_name], [vals], len(vals))
        return _ch_core.ColBatch.decode_native(encoded)

    def test_column_data_reuses_dictionary_objects(self):
        col = self._batch("LowCardinality(String)", ["a", "b", "a", "b", "a"]).column_data(0)
        assert list(col) == ["a", "b", "a", "b", "a"]
        assert col[0] is col[2] and col[2] is col[4]
        assert col[1] is col[3]

    def test_to_python_columns_and_rows_reuse(self):
        batch = self._batch("LowCardinality(String)", ["a", "b", "a", "b", "a"])
        col = batch.to_python_columns()[0]
        assert col[0] is col[2]
        rows = batch.to_python_rows()
        assert [r[0] for r in rows] == ["a", "b", "a", "b", "a"]
        assert rows[0][0] is rows[2][0]

    def test_nullable_lc_read_values_and_reuse(self):
        vals = ["x", None, "y", "x", None]
        col = self._batch("LowCardinality(Nullable(String))", vals).column_data(0)
        assert list(col) == vals
        assert col[0] is col[3]

    def test_lc_non_string_inner(self):
        vals = [7, 9, 7, 9, 7]
        col = self._batch("LowCardinality(Int64)", vals).column_data(0)
        assert list(col) == vals

    def test_all_null_lc_never_materializes_dictionary(self):
        # Null rows never reference the dictionary, so its slots stay
        # unmaterialized on every exit.
        batch = self._batch("LowCardinality(Nullable(UUID))", [None, None])
        assert list(batch.column_data(0)) == [None, None]
        assert list(batch.to_python_columns()[0]) == [None, None]
        assert [r[0] for r in batch.to_python_rows()] == [None, None]


class TestLowCardinalityInsertFastPath:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["lc"], [type_name], [vals], n)

    def test_containers_agree_and_round_trip(self):
        uniq = [f"tag_{i}" for i in range(5)]
        vals = [uniq[i % 5] for i in range(20)] + ["solo"]
        # Runtime-built distinct objects with equal content must dedupe by content.
        vals += ["tag_%d" % (i % 3) for i in range(6)]
        from_list = self._encode("LowCardinality(String)", vals)
        assert from_list == self._encode("LowCardinality(String)", tuple(vals), len(vals))
        assert from_list == self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert list(_ch_core.ColBatch.decode_native(from_list).column_data(0)) == vals

    def test_nullable_none_and_empty_string(self):
        vals = ["x", None, "", "x", None, ""]
        fast = self._encode("LowCardinality(Nullable(String))", vals)
        generic = self._encode("LowCardinality(Nullable(String))", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic
        batch = _ch_core.ColBatch.decode_native(fast)
        assert list(batch.column_data(0)) == vals

    def test_bytes_values_use_fallback(self):
        vals = [b"\xff", "x", b"\xff", "x"]
        fast = self._encode("LowCardinality(String)", vals)
        generic = self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic

    def test_none_in_non_nullable_lc_raises(self):
        with pytest.raises(ValueError, match="row 1 is None but LowCardinality\\(String\\) is not nullable"):
            self._encode("LowCardinality(String)", ["a", None])

    def test_bad_value_reports_row(self):
        with pytest.raises(ValueError, match="row 2 cannot be converted to String"):
            self._encode("LowCardinality(String)", ["a", "b", 13])

    @pytest.mark.parametrize("size", [4, 8, 16, 32, 64])
    def test_item_replacement_during_fallback_invalidates_ptr_cache(self, size):
        # A fallback __buffer__ drops the last ref to an already-scanned str;
        # the allocator can hand its address to a new same-size str, which
        # must not false-hit the pointer-identity cache.
        vals = ["A" * size, None, "C" * size]

        class EvilBuf:
            def __buffer__(self, flags):
                vals[0] = "x"  # drop the sole ref to the scanned "A"*size
                vals[2] = "B" * size  # same size class, may reuse its address
                return memoryview(b"EV")

            def __release_buffer__(self, view):
                pass

        vals[1] = EvilBuf()
        encoded = self._encode("LowCardinality(String)", vals, 3)
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert decoded == ["A" * size, "EV", "B" * size]

    def test_distinct_values_beyond_ptr_cache_cap(self):
        vals = [f"v_{i}" for i in range(70_000)]
        encoded = self._encode("LowCardinality(String)", vals)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals

    def test_str_subclass_uses_fallback(self):
        class S(str):
            pass

        vals = [S("a"), "b", S("a"), "b"]
        fast = self._encode("LowCardinality(String)", vals)
        generic = self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))
        assert fast == generic

    def test_lone_surrogate_raises_like_generic(self):
        vals = ["ok", "\ud800"]
        with pytest.raises(UnicodeEncodeError):
            self._encode("LowCardinality(String)", vals)
        with pytest.raises(UnicodeEncodeError):
            self._encode("LowCardinality(String)", _NdarrayLikeColumn(vals), len(vals))


class TestTemporalInsertFastPath:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["t"], [type_name], [vals], n)

    @pytest.mark.parametrize("type_name", ["DateTime", "DateTime64(3)", "DateTime64(6)"])
    def test_datetime_inputs_match_generic_container(self, type_name):
        class SubDT(dt.datetime):
            pass

        vals = [
            dt.datetime(2024, 5, 4, 3, 2, 1),
            dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc),
            SubDT(2024, 5, 4, 3, 2, 1, 123456),
            1700000000,
        ]
        fast = self._encode(type_name, vals)
        assert fast == self._encode(type_name, tuple(vals), len(vals))
        assert fast == self._encode(type_name, _NdarrayLikeColumn(vals), len(vals))

    def test_datetime64_string_fallback_matches_generic(self):
        vals = ["2024-01-15T12:34:56.789000+00:00", 5]
        fast = self._encode("DateTime64(3)", vals)
        assert fast == self._encode("DateTime64(3)", _NdarrayLikeColumn(vals), 2)

    def test_date_inputs_match_generic_and_helper(self):
        vals = [dt.date(2024, 1, 2), 19737, dt.date(1970, 1, 1)]
        fast = self._encode("Date", vals)
        assert fast == self._encode("Date", _NdarrayLikeColumn(vals), 3)
        expected_days = [dt.date(2024, 1, 2).toordinal() - 719163, 19737, 0]
        assert fast == build_native_block([("t", "Date", expected_days)])

    def test_nullable_datetime_with_none(self):
        vals = [dt.datetime(2024, 1, 15, 12, 0, 0, tzinfo=dt.timezone.utc), None, 5]
        fast = self._encode("Nullable(DateTime)", vals)
        assert fast == self._encode("Nullable(DateTime)", _NdarrayLikeColumn(vals), 3)

    def test_out_of_range_errors_unchanged(self):
        with pytest.raises(ValueError, match="outside UInt32 range"):
            self._encode("DateTime", [2**32])
        with pytest.raises(ValueError, match="outside UInt16 range"):
            self._encode("Date", [65536])
        with pytest.raises(ValueError, match="row 0 is None but DateTime is not Nullable"):
            self._encode("DateTime", [None])

    def test_negative_ints(self):
        # Negative is out of range for DateTime (falls back to the specific
        # range error) but a valid pre-epoch value for Date32.
        with pytest.raises(ValueError, match="outside UInt32 range"):
            self._encode("DateTime", [-1])
        fast = self._encode("Date32", [-100, 0, 100])
        assert fast == self._encode("Date32", _NdarrayLikeColumn([-100, 0, 100]), 3)
        decoded = list(_ch_core.ColBatch.decode_native(fast).column_data(0))
        assert decoded == [dt.date(1969, 9, 23), dt.date(1970, 1, 1), dt.date(1970, 4, 11)]


class TestTimeInsert:
    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["t"], [type_name], [vals], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_time_accepted_values_and_fast_raw_ints(self):
        values = [
            13,
            dt.timedelta(seconds=-1, microseconds=-500_000),
            dt.time(1, 2, 3, 999_999),
            "002:03:04.999",
            79.9,
        ]
        ticks = [13, -1, 3_723, 7_384, 79]
        encoded = self._encode("Time", values)
        assert encoded == build_native_block([("t", "Time", ticks)])
        assert encoded == self._encode("Time", tuple(values), len(values))
        assert encoded == self._encode(
            "Time", _NdarrayLikeColumn(values), len(values)
        )
        assert self._decode(encoded) == [dt.timedelta(seconds=v) for v in ticks]

    def test_established_inputs_skip_numpy_scalar_probe(self):
        probes = {"count": 0}

        def dtype_probe():
            probes["count"] += 1
            return "not-a-numpy-dtype"

        class StringValue(str):
            @property
            def dtype(self):
                return dtype_probe()

        class FloatValue(float):
            @property
            def dtype(self):
                return dtype_probe()

        class IntValue(int):
            @property
            def dtype(self):
                return dtype_probe()

        values = [
            dt.timedelta(seconds=1, microseconds=234_567),
            dt.time(0, 0, 1, 234_567),
            StringValue("000:00:01.234567"),
            FloatValue(79),
            IntValue(-13),
        ]
        expected_ticks = [1_234_567, 1_234_567, 1_234_567, 79, -13]
        direct = self._encode("Nullable(Time64(6))", [*values, None])
        generic = self._encode(
            "Nullable(Time64(6))", _NdarrayLikeColumn([*values, None]), 6
        )
        assert direct == generic == build_native_block(
            [
                (
                    "t",
                    "Nullable(Time64(6))",
                    [*expected_ticks, None],
                )
            ]
        )
        self._encode("Array(Time64(6))", [values])
        self._encode(
            "LowCardinality(Time)",
            [values[0], values[1], values[2], values[3], values[4]],
        )
        assert probes["count"] == 0

    def test_delta_and_time_subclasses_probe_then_fall_back(self):
        # Subclasses go through the numpy scalar probe (pd.Timedelta needs it);
        # a non-dtype attribute falls back to the struct-field conversion.
        class DeltaValue(dt.timedelta):
            @property
            def dtype(self):
                return "not-a-numpy-dtype"

        class TimeValue(dt.time):
            @property
            def dtype(self):
                return "not-a-numpy-dtype"

        values = [
            DeltaValue(seconds=1, microseconds=234_567),
            TimeValue(0, 0, 1, 234_567),
        ]
        encoded = self._encode("Nullable(Time64(6))", values)
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(6))", [1_234_567, 1_234_567])]
        )

    @pytest.mark.parametrize(
        "precision,values,ticks",
        [
            (
                0,
                [
                    dt.timedelta(seconds=13, microseconds=999_999),
                    dt.time(1, 2, 3, 999_999),
                    "-002:03:04.9",
                ],
                [13, 3_723, -7_384],
            ),
            (
                3,
                [
                    dt.timedelta(seconds=1, microseconds=234_567),
                    dt.time(1, 2, 3, 456_789),
                    "-002:03:04.56789",
                ],
                [1_234, 3_723_456, -7_384_567],
            ),
            (
                6,
                [
                    dt.timedelta(microseconds=79),
                    dt.time(0, 0, 1, 234_567),
                    "000:00:01.2",
                ],
                [79, 1_234_567, 1_200_000],
            ),
            (
                9,
                [
                    dt.timedelta(microseconds=79),
                    dt.time(0, 0, 1, 234_567),
                    "000:00:01.000000079",
                ],
                [79_000, 1_234_567_000, 1_000_000_079],
            ),
        ],
    )
    def test_time64_precisions(self, precision, values, ticks):
        type_name = f"Time64({precision})"
        encoded = self._encode(type_name, values)
        assert encoded == build_native_block([("t", type_name, ticks)])
        assert self._decode(encoded) == [
            dt.timedelta(
                microseconds=(abs(v) * 1_000_000 // (10**precision))
                * (-1 if v < 0 else 1)
            )
            for v in ticks
        ]

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time64(0)", dt.timedelta(milliseconds=-999)),
            ("Time64(3)", dt.timedelta(microseconds=-999)),
        ],
    )
    def test_negative_timedelta_sub_tick_truncates_toward_zero(
        self, type_name, value
    ):
        encoded = self._encode(type_name, [value])
        assert encoded == build_native_block([("t", type_name, [0])])

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time64(0)", ("timedelta64[ms]", -999)),
            ("Time64(3)", ("timedelta64[us]", -999)),
            ("Time64(6)", ("timedelta64[ns]", -999)),
            ("Time64(9)", ("timedelta64[ps]", -999)),
        ],
    )
    def test_numpy_scalar_negative_sub_tick_truncates_toward_zero(
        self, type_name, value
    ):
        np = pytest.importorskip("numpy")
        dtype, raw = value
        scalar = np.array(raw, dtype=dtype)[()]
        encoded = self._encode(type_name, [scalar])
        assert encoded == build_native_block([("t", type_name, [0])])

    @pytest.mark.parametrize(
        "type_name,expected_ticks",
        [
            ("Time", [1, -1, 0]),
            ("Time64(3)", [1_234, -1_234, 0]),
            ("Time64(6)", [1_234_567, -1_234_567, 0]),
            ("Time64(9)", [1_234_567_890, -1_234_567_890, 0]),
        ],
    )
    def test_numpy_timedelta64_ndarray_bulk_path(self, type_name, expected_ticks):
        np = pytest.importorskip("numpy")
        values = np.array(
            [1_234_567_890, -1_234_567_890, 0], dtype="timedelta64[ns]"
        )
        encoded = self._encode(type_name, values)
        assert encoded == build_native_block([("t", type_name, expected_ticks)])

    def test_numpy_timedelta64_two_dimensional_array_rejected(self):
        np = pytest.importorskip("numpy")
        values = np.array([13, 79], dtype="timedelta64[ns]").reshape(2, 1)
        with pytest.raises(ValueError, match="must be one-dimensional"):
            self._encode("Time64(9)", values)

    def test_numpy_timedelta64_byte_order_multiplier_and_general_ratios(self):
        np = pytest.importorskip("numpy")

        big_endian = np.array([1_234, -1_234], dtype=">m8[ms]")
        assert self._encode("Time64(3)", big_endian) == build_native_block(
            [("t", "Time64(3)", [1_234, -1_234])]
        )

        multiplied = np.array([13, -13], dtype="timedelta64[10us]")
        assert self._encode("Time64(9)", multiplied) == build_native_block(
            [("t", "Time64(9)", [130_000, -130_000])]
        )

        general = np.array([334, -334], dtype="timedelta64[3ps]")
        assert self._encode("Time64(9)", general) == build_native_block(
            [("t", "Time64(9)", [1, -1])]
        )

    def test_numpy_timedelta64_nullable_nat_array_and_scalars(self):
        np = pytest.importorskip("numpy")
        values = np.array([1_234_567, "NaT", -1_234_567], dtype="timedelta64[us]")
        encoded = self._encode("Nullable(Time64(6))", values)
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(6))", [1_234_567, None, -1_234_567])]
        )
        assert self._decode(encoded) == [
            dt.timedelta(microseconds=1_234_567),
            None,
            dt.timedelta(microseconds=-1_234_567),
        ]

        scalar_values = [np.timedelta64(13, "ms"), np.timedelta64("NaT")]
        scalar_encoded = self._encode("Nullable(Time64(3))", scalar_values)
        assert scalar_encoded == build_native_block(
            [("t", "Nullable(Time64(3))", [13, None])]
        )

    def test_pandas_timedelta_series_bulk_path(self):
        pd = pytest.importorskip("pandas")
        values = pd.Series(
            pd.to_timedelta(["1.234567890s", None, "-1.234567890s"])
        )
        encoded = self._encode("Nullable(Time64(9))", values)
        assert encoded == build_native_block(
            [
                (
                    "t",
                    "Nullable(Time64(9))",
                    [1_234_567_890, None, -1_234_567_890],
                )
            ]
        )

    def test_pandas_timedelta_scalar_ns_precision_in_list(self):
        pd = pytest.importorskip("pandas")
        values = [pd.Timedelta("1s 123456789ns"), pd.Timedelta("-1s")]
        encoded = self._encode("Time64(9)", values)
        assert encoded == build_native_block(
            [("t", "Time64(9)", [1_123_456_789, -1_000_000_000])]
        )
        sub_tick = self._encode("Time64(0)", [pd.Timedelta("-999ms")])
        assert sub_tick == build_native_block([("t", "Time64(0)", [0])])

    def test_pandas_nat_nullable_and_non_nullable(self):
        pd = pytest.importorskip("pandas")
        encoded = self._encode("Nullable(Time64(9))", [pd.Timedelta("1us"), pd.NaT])
        assert encoded == build_native_block(
            [("t", "Nullable(Time64(9))", [1_000, None])]
        )
        with pytest.raises(ValueError, match="row 0 is NaT but Time is not Nullable"):
            self._encode("Time", [pd.NaT])
        with pytest.raises(ValueError, match="row 1 is NaT"):
            self._encode("Time64(3)", _NdarrayLikeColumn([13, pd.NaT]), 2)

    def test_numpy_nat_non_nullable_and_range_errors(self):
        np = pytest.importorskip("numpy")
        with pytest.raises(ValueError, match="row 0 is NaT.*not Nullable"):
            self._encode("Time", np.array(["NaT"], dtype="timedelta64[ns]"))
        with pytest.raises(ValueError, match="outside logical range"):
            self._encode("Time", np.array([1_000], dtype="timedelta64[h]"))

    def test_late_day_time64_nested_value_avoids_i64_overflow(self):
        value = dt.time(23, 59, 59, 999_999)
        type_name = "Array(Tuple(Time64(9)))"
        encoded = self._encode(type_name, [[(value,)]])
        expected_ticks = 86_399_999_999_000
        assert encoded == build_native_block(
            [("t", type_name, [[(expected_ticks,)]])]
        )
        assert self._decode(encoded) == [
            [(dt.timedelta(seconds=86_399, microseconds=999_999),)]
        ]

    def test_nullable_and_recursive_shapes(self):
        nullable = [
            dt.timedelta(seconds=1, microseconds=250_000),
            None,
            "-000:00:00.001",
        ]
        encoded = self._encode("Nullable(Time64(3))", nullable)
        assert self._decode(encoded) == [
            dt.timedelta(milliseconds=1_250),
            None,
            dt.timedelta(milliseconds=-1),
        ]

        array_rows = [[dt.timedelta(milliseconds=13), "000:00:00.079"], [], [-1]]
        encoded = self._encode("Array(Time64(3))", array_rows)
        assert self._decode(encoded) == [
            [dt.timedelta(milliseconds=13), dt.timedelta(milliseconds=79)],
            [],
            [dt.timedelta(milliseconds=-1)],
        ]

        tuple_rows = [
            (dt.time(0, 0, 13), dt.timedelta(microseconds=79)),
            (-13, None),
        ]
        encoded = self._encode("Tuple(Time, Nullable(Time64(6)))", tuple_rows)
        assert self._decode(encoded) == [
            (dt.timedelta(seconds=13), dt.timedelta(microseconds=79)),
            (dt.timedelta(seconds=-13), None),
        ]

        array_tuple_rows = [
            [("000:00:13", 79_000)],
            [],
            [(dt.time(0, 1, 19), -1_000)],
        ]
        encoded = self._encode(
            "Array(Tuple(Time, Time64(6)))", array_tuple_rows
        )
        assert self._decode(encoded) == [
            [(dt.timedelta(seconds=13), dt.timedelta(microseconds=79_000))],
            [],
            [(dt.timedelta(seconds=79), dt.timedelta(microseconds=-1_000))],
        ]

    def test_low_cardinality_time_and_time64_rejection(self):
        encoded = self._encode("LowCardinality(Time)", [13, 79, 13, -1])
        assert self._decode(encoded) == [
            dt.timedelta(seconds=13),
            dt.timedelta(seconds=79),
            dt.timedelta(seconds=13),
            dt.timedelta(seconds=-1),
        ]
        with pytest.raises(
            NotImplementedError, match="unsupported LowCardinality inner type"
        ):
            self._encode("LowCardinality(Time64(3))", [13])

    @pytest.mark.parametrize(
        "type_name,value",
        [
            ("Time", 3_600_000),
            ("Time", -3_600_000),
            ("Time64(3)", 3_600_000_000),
            ("Time64(3)", -3_600_000_000),
            ("Time", "1000:00:00"),
            ("Time64(6)", "001:60:00"),
            ("Time64(6)", float("inf")),
        ],
    )
    def test_invalid_values(self, type_name, value):
        with pytest.raises(ValueError, match="column.*row 0.*Time"):
            self._encode(type_name, [value])

    def test_none_requires_nullable(self):
        with pytest.raises(ValueError, match="row 0 is None but Time is not Nullable"):
            self._encode("Time", [None])


class TestScalarObjectInsertFastPath:
    """UUID, IPv4, and Enum8/16 fast paths over exact lists and tuples."""

    _E8 = "Enum8('alpha' = 1, 'beta' = 2, 'gamma' = 3)"
    _E16 = "Enum16('alpha' = -5, 'beta' = 0, 'gamma' = 1000)"

    def _encode(self, type_name, vals, n=None):
        n = len(vals) if n is None else n
        return _ch_core.encode_native_block(["v"], [type_name], [vals], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_uuid_containers_agree_and_round_trip(self):
        vals = [
            uuid.UUID(int=0),
            uuid.UUID("00112233-4455-6677-8899-aabbccddeeff"),
            uuid.UUID(int=(1 << 128) - 1),
        ]
        fast = self._encode("UUID", vals)
        assert fast == self._encode("UUID", tuple(vals), len(vals))
        assert fast == self._encode("UUID", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_uuid_mixed_types_use_fallback(self):
        class SubUUID(uuid.UUID):
            pass

        known = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        vals = [known, str(known), known.int, known.bytes, SubUUID(int=known.int)]
        fast = self._encode("UUID", vals)
        assert fast == self._encode("UUID", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == [known] * 5

    def test_nullable_uuid(self):
        vals = [uuid.UUID(int=9), None, uuid.UUID(int=0), None]
        fast = self._encode("Nullable(UUID)", vals)
        assert fast == self._encode("Nullable(UUID)", tuple(vals), len(vals))
        assert fast == self._encode("Nullable(UUID)", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_uuid_errors_unchanged(self):
        with pytest.raises(ValueError, match="row 1 cannot be converted to UUID"):
            self._encode("UUID", [uuid.UUID(int=1), 1.5])
        with pytest.raises(ValueError, match="row 1 is None but UUID is not Nullable"):
            self._encode("UUID", [uuid.UUID(int=1), None])

    def test_ipv4_containers_agree_and_round_trip(self):
        vals = [
            ipaddress.IPv4Address("0.0.0.0"),
            ipaddress.IPv4Address("255.255.255.255"),
            ipaddress.IPv4Address("192.0.2.1"),
        ]
        fast = self._encode("IPv4", vals)
        assert fast == self._encode("IPv4", tuple(vals), len(vals))
        assert fast == self._encode("IPv4", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_ipv4_mixed_types_without_object_wrappers(self):
        import enum

        class IntLike(enum.IntEnum):
            ADDR = 16909060

        vals = [ipaddress.IPv4Address("1.2.3.4"), "1.2.3.4", 16909060, IntLike.ADDR]
        fast = self._encode("IPv4", vals)
        assert fast == self._encode("IPv4", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == [ipaddress.IPv4Address("1.2.3.4")] * 4

    def test_nullable_ipv4(self):
        vals = [ipaddress.IPv4Address("1.2.3.4"), None, ipaddress.IPv4Address("0.0.0.0")]
        fast = self._encode("Nullable(IPv4)", vals)
        assert fast == self._encode("Nullable(IPv4)", tuple(vals), len(vals))
        assert fast == self._encode("Nullable(IPv4)", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    def test_ipv4_errors_unchanged(self):
        with pytest.raises(ValueError, match="row 0 cannot be converted to IPv4"):
            self._encode("IPv4", ["1.2.3.999"])
        with pytest.raises(ValueError, match="row 0 cannot be converted to IPv4"):
            self._encode("IPv4", [_NdarrayLikeColumn([1])])
        with pytest.raises(ValueError, match="row 1 cannot be converted to IPv4"):
            self._encode("IPv4", [1, 2**32])

    def test_enum8_labels_and_codes_round_trip(self):
        vals = ["alpha", "beta", 3, "alpha", 99]
        fast = self._encode(self._E8, vals)
        assert fast == self._encode(self._E8, tuple(vals), len(vals))
        assert fast == self._encode(self._E8, _NdarrayLikeColumn(vals), len(vals))
        # Unknown raw codes pass through and read back as None.
        assert self._decode(fast) == ["alpha", "beta", "gamma", "alpha", None]

    def test_enum16_labels_and_codes_round_trip(self):
        vals = ["gamma", -5, "beta", "gamma"]
        fast = self._encode(self._E16, vals)
        assert fast == self._encode(self._E16, _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == ["gamma", "alpha", "beta", "gamma"]

    def test_nullable_enum8(self):
        vals = ["alpha", None, "gamma", None]
        fast = self._encode(f"Nullable({self._E8})", vals)
        assert fast == self._encode(f"Nullable({self._E8})", tuple(vals), len(vals))
        assert fast == self._encode(f"Nullable({self._E8})", _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == vals

    @pytest.mark.parametrize("type_name,enum_name", [(_E8, "Enum8"), (_E16, "Enum16")])
    def test_enum_unknown_label_raises(self, type_name, enum_name):
        expected = f'row 1 {enum_name} label "delta" is not defined'
        with pytest.raises(ValueError, match=expected):
            self._encode(type_name, ["alpha", "delta"])
        with pytest.raises(ValueError, match=expected):
            self._encode(type_name, _NdarrayLikeColumn(["alpha", "delta"]), 2)

    def test_enum_str_subclass_uses_fallback(self):
        class S(str):
            pass

        vals = [S("alpha"), "beta", S("gamma")]
        fast = self._encode(self._E8, vals)
        assert fast == self._encode(self._E8, _NdarrayLikeColumn(vals), len(vals))
        assert self._decode(fast) == ["alpha", "beta", "gamma"]

    @pytest.mark.parametrize("size", [4, 8, 16, 32, 64])
    def test_enum_item_replacement_during_fallback_invalidates_ptr_cache(self, size):
        # A fallback __index__ drops the last ref to an already-scanned label;
        # the allocator can hand its address to a new same-size str, which
        # must not false-hit the pointer-identity cache.
        a, b = "A" * size, "B" * size
        tname = f"Enum8('{a}' = 1, '{b}' = 2, 'EV' = 3)"
        vals = ["A" * size, None, "C" * size]

        class Evil:
            def __index__(self):
                vals[0] = "x"  # drop the sole ref to the scanned label
                vals[2] = "B" * size  # same size class, may reuse its address
                return 3

        vals[1] = Evil()
        assert self._decode(self._encode(tname, vals, 3)) == [a, "EV", b]

    def test_enum_list_resized_during_fallback_raises(self):
        vals = ["alpha", None, "beta", "gamma"]

        class Evil:
            def __index__(self):
                del vals[2:]
                return 2

        vals[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode(self._E8, vals, 4)


# ---------------------------------------------------------------------------
# Integer types
# ---------------------------------------------------------------------------

class TestDecodeInt8:
    def test_basic(self):
        data = build_native_block([("v", "Int8", [1, -1, 127])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        assert batch.column_type_names == ["Int8"]
        assert list(batch.column_data(0)) == [1, -1, 127]


class TestDecodeInt16:
    def test_basic(self):
        data = build_native_block([("v", "Int16", [256, -256, 32767])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [256, -256, 32767]


class TestDecodeInt32:
    def test_basic(self):
        data = build_native_block([("v", "Int32", [70000, -70000])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [70000, -70000]


class TestDecodeInt64:
    def test_basic(self):
        data = build_native_block([("id", "Int64", [10, 20, 30])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        assert batch.column_names == ["id"]
        assert batch.column_type_names == ["Int64"]
        assert list(batch.column_data(0)) == [10, 20, 30]

    def test_negative_values(self):
        data = build_native_block([("n", "Int64", [-1, 0, 9223372036854775807])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [-1, 0, 9223372036854775807]


class TestDecodeUInt8:
    def test_basic(self):
        data = build_native_block([("v", "UInt8", [0, 128, 255])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 128, 255]


class TestDecodeUInt16:
    def test_basic(self):
        data = build_native_block([("v", "UInt16", [0, 65535])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 65535]


class TestDecodeUInt32:
    def test_basic(self):
        data = build_native_block([("v", "UInt32", [0, 4_000_000_000])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 4_000_000_000]


class TestDecodeUInt64:
    def test_basic(self):
        data = build_native_block([("v", "UInt64", [0, 2**64 - 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [0, 2**64 - 1]


_WIDE_CASES = [
    ("Int128", [-(2**127), -1, 0, 2**127 - 1]),
    ("UInt128", [0, 13, 2**127, 2**128 - 1]),
    ("Int256", [-(2**255), -1, 0, 2**255 - 1]),
    ("UInt256", [0, 79, 2**255, 2**256 - 1]),
]

# Values straddling the i64/u64 fast-path boundary plus one true wide value;
# type min/max extremes are covered by _WIDE_CASES. Negative word boundaries
# exercise the decode word-scan where high words are all-ones.
_WIDE_FAST_SLOW_CASES = [
    (
        "Int128",
        [0, 1, -1, 2**63 - 1, -(2**63), -(2**63) - 1, 2**63, 2**64 - 1, 2**64,
         2**64 + 1, -(2**64), -(2**64) - 1, -(2**64) + 1, 2**100 + 13, -(2**100)],
    ),
    ("UInt128", [0, 1, 2**63 - 1, 2**63, 2**64 - 1, 2**64, 2**64 + 1, 2**100 + 13]),
    (
        "Int256",
        [0, 1, -1, 2**63 - 1, -(2**63), -(2**63) - 1, 2**63, 2**64 - 1, 2**64,
         2**64 + 1, -(2**64), -(2**64) - 1, -(2**64) + 1, -(2**128), -(2**192),
         -(2**192) - 1, 2**200 + 13, -(2**200)],
    ),
    ("UInt256", [0, 1, 2**63 - 1, 2**63, 2**64 - 1, 2**64, 2**64 + 1, 2**200 + 13]),
]


class _IndexValue:
    def __init__(self, value):
        self.value = value

    def __index__(self):
        return self.value


class _IntSubclass(int):
    pass


class TestWideIntegers:
    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_scalar_boundaries_and_all_object_exits(self, type_name, values):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert [row[0] for row in batch.to_python_rows()] == values

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_index_values(self, type_name, values):
        indexed = [_IndexValue(value) for value in values]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [indexed], len(indexed))
        assert encoded == build_native_block([("v", type_name, values)])

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_generic_column_matches_direct_buffer_builder(self, type_name, values):
        generic = _NdarrayLikeColumn(values)
        encoded = _ch_core.encode_native_block(["v"], [type_name], [generic], len(values))
        assert encoded == build_native_block([("v", type_name, values)])

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    @pytest.mark.parametrize(
        "wrapper", ["{}", "Nullable({})", "LowCardinality(Nullable({}))"]
    )
    def test_numeric_strings_match_integer_values(self, type_name, values, wrapper):
        type_name = wrapper.format(type_name)
        integer_values = values if wrapper == "{}" else [None, *values, None]
        string_values = [None if value is None else str(value) for value in integer_values]
        encoded = _ch_core.encode_native_block(
            ["v"], [type_name], [string_values], len(string_values)
        )
        assert encoded == build_native_block([("v", type_name, integer_values)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == integer_values

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    @pytest.mark.parametrize("wrapper", ["Nullable({})", "LowCardinality(Nullable({}))"])
    def test_nullable_and_low_cardinality(self, type_name, values, wrapper):
        type_name = wrapper.format(type_name)
        nullable_values = [None, values[0], values[-1], values[0], None]
        encoded = _ch_core.encode_native_block(
            ["v"], [type_name], [nullable_values], len(nullable_values)
        )
        assert encoded == build_native_block([("v", type_name, nullable_values)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == nullable_values

    def test_nested_containers(self):
        columns = [
            (
                "a",
                "Array(Nullable(Int128))",
                [[-(2**127), None], [], [13, 2**127 - 1]],
            ),
            (
                "t",
                "Tuple(UInt128, Int256)",
                [(2**128 - 1, -(2**255)), (13, -1), (0, 2**255 - 1)],
            ),
            (
                "m",
                "Map(UInt128, UInt256)",
                [{13: 2**256 - 1}, {}, {2**128 - 1: 79}],
            ),
        ]
        names = [name for name, _, _ in columns]
        types = [type_name for _, type_name, _ in columns]
        values = [column for _, _, column in columns]
        encoded = _ch_core.encode_native_block(names, types, values, 3)
        assert encoded == build_native_block(columns)

        batch = _ch_core.ColBatch.decode_native(encoded)
        assert [list(batch.column_data(index)) for index in range(len(columns))] == values
        assert [list(column) for column in batch.to_python_columns()] == values
        assert list(batch.to_python_rows()) == list(zip(*values))

    def test_numeric_strings_in_array_and_tuple(self):
        string_columns = [
            ("a", "Array(Int128)", [[str(-(2**127)), "13"], [], [str(2**127 - 1)]]),
            (
                "t",
                "Tuple(UInt128, Int256)",
                [
                    (str(2**128 - 1), str(-(2**255))),
                    ("13", str(2**255 - 1)),
                    ("0", "-1"),
                ],
            ),
        ]
        integer_values = [
            [[-(2**127), 13], [], [2**127 - 1]],
            [(2**128 - 1, -(2**255)), (13, 2**255 - 1), (0, -1)],
        ]
        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in string_columns],
            [type_name for _, type_name, _ in string_columns],
            [values for _, _, values in string_columns],
            3,
        )
        expected_columns = [
            (name, type_name, values)
            for (name, type_name, _), values in zip(string_columns, integer_values)
        ]
        assert encoded == build_native_block(expected_columns)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert [list(column) for column in batch.to_python_columns()] == integer_values

    def test_numeric_strings_in_array_low_cardinality_wide(self):
        type_name = "Array(LowCardinality(Int256))"
        string_rows = [[str(-(2**255)), "13", str(-(2**255))], [], [str(2**255 - 1)]]
        integer_rows = [[-(2**255), 13, -(2**255)], [], [2**255 - 1]]
        encoded = _ch_core.encode_native_block(
            ["v"], [type_name], [string_rows], len(string_rows)
        )
        assert encoded == build_native_block([("v", type_name, integer_rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == integer_rows

    @pytest.mark.parametrize("bad_index", ["raises", "non_int"])
    def test_bad_index_protocol_has_context(self, bad_index):
        class BadIndex:
            def __index__(self):
                if bad_index == "raises":
                    raise RuntimeError("index failed")
                return "13"

        with pytest.raises(
            ValueError,
            match=r'column "v" row 1 cannot be converted to Int256',
        ):
            _ch_core.encode_native_block(["v"], ["Int256"], [[13, BadIndex()]], 2)

    def test_index_mutating_source_list_is_rejected(self):
        values = [None, 13, 79]

        class MutatingIndex:
            def __index__(self):
                values.pop()
                return 5

        values[0] = MutatingIndex()
        with pytest.raises(ValueError, match=r'column "v" was resized during encoding'):
            _ch_core.encode_native_block(["v"], ["UInt256"], [values], 3)

    def test_empty_columns(self):
        names = [f"v{index}" for index in range(len(_WIDE_CASES))]
        types = [type_name for type_name, _ in _WIDE_CASES]
        values = [[] for _ in types]
        encoded = _ch_core.encode_native_block(names, types, values, 0)
        assert encoded == build_native_block(
            [(name, type_name, []) for name, type_name in zip(names, types)]
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == types
        assert [list(column) for column in batch.to_python_columns()] == values
        assert list(batch.to_python_rows()) == []

    @pytest.mark.parametrize(
        ("type_name", "invalid"),
        [
            ("Int128", -(2**127) - 1),
            ("Int128", 2**127),
            ("UInt128", -1),
            ("UInt128", -(2**63)),
            ("UInt128", -(2**63) - 1),
            ("UInt128", 2**128),
            ("Int256", -(2**255) - 1),
            ("Int256", 2**255),
            ("UInt256", -1),
            ("UInt256", -(2**63)),
            ("UInt256", -(2**63) - 1),
            ("UInt256", 2**256),
            ("Int128", str(2**127)),
            ("UInt128", "-1"),
            ("Int256", str(2**255)),
            ("UInt256", str(2**256)),
            ("Int128", "not-an-integer"),
            ("Int128", 1.5),
            ("Int256", object()),
            ("UInt256", b"79"),
        ],
    )
    def test_range_and_type_errors(self, type_name, invalid):
        with pytest.raises(
            ValueError,
            match=rf'column "v" row 0 cannot be converted to {type_name}',
        ):
            _ch_core.encode_native_block(["v"], [type_name], [[invalid]], 1)

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_FAST_SLOW_CASES)
    def test_fast_slow_boundary_round_trip(self, type_name, values):
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        assert encoded == build_native_block([("v", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert list(batch.column_data(0)) == values
        assert list(batch.to_python_columns()[0]) == values
        assert [row[0] for row in batch.to_python_rows()] == values

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_FAST_SLOW_CASES)
    def test_nullable_mixes_none_fast_and_slow_values(self, type_name, values):
        wrapped = f"Nullable({type_name})"
        mixed = [None, *values[:2], None, *values[2:], None]
        encoded = _ch_core.encode_native_block(["v"], [wrapped], [mixed], len(mixed))
        assert encoded == build_native_block([("v", wrapped, mixed)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == mixed

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    def test_int_subclass_encodes_via_index_protocol(self, type_name):
        # An int subclass is not an exact int; PyNumber_Index returns the
        # instance itself, so the slow path converts the subclass directly.
        values = [_IntSubclass(13), _IntSubclass(2**100)]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], 2)
        assert encoded == build_native_block([("v", type_name, [13, 2**100])])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [13, 2**100]

    @pytest.mark.parametrize("type_name", [type_name for type_name, _ in _WIDE_CASES])
    def test_bool_encodes_via_index_protocol(self, type_name):
        # bool is an int subclass, not an exact int, so it converts through
        # the index protocol to 1/0.
        encoded = _ch_core.encode_native_block(["v"], [type_name], [[True, False]], 2)
        assert encoded == build_native_block([("v", type_name, [1, 0])])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [1, 0]

    @pytest.mark.parametrize(("type_name", "values"), _WIDE_CASES)
    def test_arrow_fixed_binary_bytes(self, type_name, values):
        pa = pytest.importorskip("pyarrow")
        width, signed = _WIDE_TYPES[type_name]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("v", type_name, values)])
        )
        column = pa.RecordBatchReader.from_stream(batch).read_all().column("v")
        assert column.type == pa.binary(width)
        assert column.to_pylist() == [
            value.to_bytes(width, "little", signed=signed) for value in values
        ]


# ---------------------------------------------------------------------------
# Float types
# ---------------------------------------------------------------------------

class TestDecodeFloat32:
    def test_basic(self):
        data = build_native_block([("v", "Float32", [1.5, -2.25])])
        batch = _ch_core.ColBatch.decode_native(data)
        vals = list(batch.column_data(0))
        assert vals[0] == pytest.approx(1.5)
        assert vals[1] == pytest.approx(-2.25)


class TestDecodeFloat64:
    def test_basic(self):
        data = build_native_block([("val", "Float64", [1.5, 2.7, -0.1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 3
        vals = list(batch.column_data(0))
        assert vals[0] == pytest.approx(1.5)
        assert vals[1] == pytest.approx(2.7)
        assert vals[2] == pytest.approx(-0.1)


class TestBFloat16:
    def test_encode_decode_type_matrix_and_all_object_exits(self):
        columns = [
            ("scalar", "BFloat16", [1.1, -1.1, 13.0]),
            ("nullable", "Nullable(BFloat16)", [1.1, None, -1.1]),
            ("array", "Array(BFloat16)", [[1.1, -1.1], [], [13.0]]),
            (
                "tuple",
                "Tuple(BFloat16, String)",
                [(1.1, "user_1"), (-1.1, "user_2"), (13.0, "user_3")],
            ),
            (
                "array_tuple",
                "Array(Tuple(BFloat16, UInt8))",
                [[(1.1, 13), (-1.1, 79)], [], [(13.0, 5)]],
            ),
            (
                "map",
                "Map(BFloat16, String)",
                [{1.1: "user_1"}, {}, {-1.1: "user_2"}],
            ),
            (
                "low_cardinality",
                "LowCardinality(BFloat16)",
                [1.1, -1.1, 1.1],
            ),
            (
                "low_cardinality_nullable",
                "LowCardinality(Nullable(BFloat16))",
                [1.1, None, 1.1],
            ),
        ]
        expected = [
            [_bfloat16_value(1.1), _bfloat16_value(-1.1), 13.0],
            [_bfloat16_value(1.1), None, _bfloat16_value(-1.1)],
            [[_bfloat16_value(1.1), _bfloat16_value(-1.1)], [], [13.0]],
            [
                (_bfloat16_value(1.1), "user_1"),
                (_bfloat16_value(-1.1), "user_2"),
                (13.0, "user_3"),
            ],
            [
                [(_bfloat16_value(1.1), 13), (_bfloat16_value(-1.1), 79)],
                [],
                [(13.0, 5)],
            ],
            [{_bfloat16_value(1.1): "user_1"}, {}, {_bfloat16_value(-1.1): "user_2"}],
            [_bfloat16_value(1.1), _bfloat16_value(-1.1), _bfloat16_value(1.1)],
            [_bfloat16_value(1.1), None, _bfloat16_value(1.1)],
        ]

        encoded = _ch_core.encode_native_block(
            [name for name, _, _ in columns],
            [type_name for _, type_name, _ in columns],
            [values for _, _, values in columns],
            3,
        )

        assert encoded == build_native_block(columns)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert list(batch.to_python_columns()) == expected
        assert [list(column) for column in zip(*batch.to_python_rows())] == expected
        assert [list(batch.column_data(index)) for index in range(len(columns))] == expected

    @pytest.mark.parametrize("make", [list, tuple, _NdarrayLikeColumn])
    def test_scalar_container_paths_match_golden_bytes(self, make):
        values = [3.141592, -2.71828, 13]
        encoded = _ch_core.encode_native_block(
            ["v"],
            ["BFloat16"],
            [make(values)],
            len(values),
        )
        assert encoded == build_native_block([("v", "BFloat16", values)])

    @pytest.mark.parametrize("dtype", ["float32", "float64"])
    def test_numpy_buffer_matches_list_path(self, dtype):
        np = pytest.importorskip("numpy")
        values = [3.141592, -2.71828, 13.0]
        array = np.array(values, dtype=dtype)
        from_buffer = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], len(array))
        from_list = _ch_core.encode_native_block(["v"], ["BFloat16"], [list(array)], len(array))
        assert from_buffer == from_list == build_native_block([("v", "BFloat16", values)])

    def test_numpy_strided_buffer_matches_list_path(self):
        np = pytest.importorskip("numpy")
        array = np.array([1.1, 0.0, -1.1, 0.0, 13.0], dtype="float32")[::2]
        from_buffer = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], len(array))
        from_list = _ch_core.encode_native_block(["v"], ["BFloat16"], [list(array)], len(array))
        assert from_buffer == from_list

    def test_float32_buffer_signaling_nan_encodes_nan_word(self):
        np = pytest.importorskip("numpy")
        # sNaN whose payload lives only in the low 16 mantissa bits; plain
        # truncation would produce 0x7F80 (+inf).
        array = np.array([0x7F800001], dtype=np.uint32).view(np.float32)
        encoded = _ch_core.encode_native_block(["v"], ["BFloat16"], [array], 1)
        word = struct.unpack("<H", encoded[-2:])[0]
        assert word & 0x7F80 == 0x7F80 and word & 0x007F, hex(word)
        assert word == 0x7FC0
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert math.isnan(decoded[0])

    def test_special_values_and_arrow_raw_words(self):
        pa = pytest.importorskip("pyarrow")
        values = [0.0, -0.0, float("inf"), float("-inf"), float("nan"), 2**-133]
        encoded = _ch_core.encode_native_block(["v"], ["BFloat16"], [values], len(values))
        assert encoded == build_native_block([("v", "BFloat16", values)])

        batch = _ch_core.ColBatch.decode_native(encoded)
        decoded = list(batch.column_data(0))
        assert decoded[:4] == [0.0, -0.0, float("inf"), float("-inf")]
        assert math.copysign(1.0, decoded[1]) == -1.0
        assert math.isnan(decoded[4])
        assert decoded[5] == 2**-133

        arrow_column = pa.RecordBatchReader.from_stream(batch).read_all().column("v")
        assert arrow_column.type == pa.binary(2)
        assert arrow_column.to_pylist() == [_bfloat16_bytes(value) for value in values]

    def test_nullable_all_null_and_zero_rows(self):
        values = [None, None, None]
        encoded = _ch_core.encode_native_block(
            ["v"], ["Nullable(BFloat16)"], [values], len(values)
        )
        assert encoded == build_native_block([("v", "Nullable(BFloat16)", values)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == values

        empty = _ch_core.encode_native_block(["v"], ["BFloat16"], [[]], 0)
        assert empty == build_native_block([("v", "BFloat16", [])])
        assert list(_ch_core.ColBatch.decode_native(empty).column_data(0)) == []

    @pytest.mark.parametrize(
        "type_name",
        ["bfloat16", "Bfloat16", "BFLOAT16", "BFloat16()", "BFloat32"],
    )
    def test_type_name_is_exact(self, type_name):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["v"], [type_name], [[13.0]], 1)

    def test_invalid_value_names_row_and_type(self):
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [[13.0, "invalid"]], 2)

    @pytest.mark.parametrize("make", [list, tuple, _NdarrayLikeColumn])
    def test_finite_float32_overflow_is_rejected(self, make):
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [make([13.0, 1e300])], 2)

    def test_float64_buffer_finite_float32_overflow_is_rejected(self):
        np = pytest.importorskip("numpy")
        values = np.array([13.0, 1e300], dtype="float64")
        with pytest.raises(ValueError, match='column "v" row 1 cannot be converted to BFloat16'):
            _ch_core.encode_native_block(["v"], ["BFloat16"], [values], len(values))


# ---------------------------------------------------------------------------
# Bool
# ---------------------------------------------------------------------------

class TestDecodeBool:
    def test_basic(self):
        data = build_native_block([("b", "Bool", [1, 0, 1, 0, 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 5
        assert batch.column_type_names == ["Bool"]
        assert list(batch.column_data(0)) == [True, False, True, False, True]

    def test_nullable(self):
        data = build_native_block([("b", "Nullable(Bool)", [1, None, 0])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result == [True, None, False]


# ---------------------------------------------------------------------------
# String types
# ---------------------------------------------------------------------------

class TestDecodeString:
    def test_basic(self):
        data = build_native_block([("s", "String", ["hello", "", "world!"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["hello", "", "world!"]

    def test_unicode(self):
        data = build_native_block([("s", "String", ["héllo", "日本語"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["héllo", "日本語"]


class TestStringInvalidUtf8:
    def test_hex_fallback_on_all_paths(self):
        # Invalid UTF-8 renders as the lowercase hex of the raw bytes on every
        # materialization path, matching clickhouse-connect's String fallback.
        data = build_native_block([("s", "String", ["ok", b"\xff\xfe", b"\x80abc"])])
        batch = _ch_core.ColBatch.decode_native(data)
        expected = ["ok", "fffe", "80616263"]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_hex_fallback_nullable(self):
        data = build_native_block([("s", "Nullable(String)", [b"\xc3\x28", None, "u1"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["c328", None, "u1"]


class TestDecodeFixedString:
    def test_basic(self):
        data = build_native_block([("fs", "FixedString(3)", [b"abc", b"xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 2
        assert batch.column_type_names == ["FixedString(3)"]
        result = list(batch.column_data(0))
        assert result[0] == b"abc"
        assert result[1] == b"xyz"

    def test_null_padded(self):
        data = build_native_block([("fs", "FixedString(4)", [b"ab\x00\x00"])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] == b"ab\x00\x00"


# ---------------------------------------------------------------------------
# Nullable
# ---------------------------------------------------------------------------

class TestDecodeNullable:
    def test_nullable_int64(self):
        data = build_native_block([("n", "Nullable(Int64)", [100, None, 300, None])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 4
        assert batch.column_type_names == ["Nullable(Int64)"]
        result = list(batch.column_data(0))
        assert result == [100, None, 300, None]

    def test_nullable_int32(self):
        data = build_native_block([("n", "Nullable(Int32)", [1, None, 3])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == [1, None, 3]

    def test_nullable_float32(self):
        data = build_native_block([("f", "Nullable(Float32)", [None, 42.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] is None
        assert result[1] == pytest.approx(42.5)

    def test_nullable_float64(self):
        data = build_native_block([("f", "Nullable(Float64)", [None, 42.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = list(batch.column_data(0))
        assert result[0] is None
        assert result[1] == pytest.approx(42.5)

    def test_nullable_string(self):
        data = build_native_block([("s", "Nullable(String)", ["abc", None, "xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["abc", None, "xyz"]


# ---------------------------------------------------------------------------
# Temporal types
# ---------------------------------------------------------------------------

class TestDecodeDate:
    def test_date(self):
        days = [0, 19737, 100, 65535]
        data = build_native_block([("d", "Date", days)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Date"]
        expected = [_EPOCH_DATE + dt.timedelta(days=x) for x in days]
        assert list(batch.column_data(0)) == expected

    def test_date32_pre_epoch(self):
        days = [-25567, -1, 0, 19737]  # -25567 ~= 1900-01-01, signed days
        data = build_native_block([("d", "Date32", days)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Date32"]
        expected = [_EPOCH_DATE + dt.timedelta(days=x) for x in days]
        assert list(batch.column_data(0)) == expected


class TestDecodeDateTime:
    def test_datetime_naive(self):
        secs = [0, 1705322096, 961056000]
        data = build_native_block([("dt", "DateTime", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime"]
        expected = [_EPOCH_NAIVE + dt.timedelta(seconds=s) for s in secs]
        result = list(batch.column_data(0))
        assert result == expected
        assert all(v.tzinfo is None for v in result)

    def test_datetime_utc_is_naive(self):
        # A UTC-equivalent timezone renders naive, matching clickhouse-connect.
        secs = [1705322096]
        data = build_native_block([("dt", "DateTime('UTC')", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime('UTC')"]
        v = list(batch.column_data(0))[0]
        assert v == _EPOCH_NAIVE + dt.timedelta(seconds=secs[0])
        assert v.tzinfo is None

    def test_datetime_named_zone_is_aware(self):
        secs = [1705322096]  # 2024-01-15 12:34:56 UTC
        data = build_native_block([("dt", "DateTime('America/New_York')", secs)])
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v.tzinfo == ZoneInfo("America/New_York")
        # Same instant as the source UTC seconds, expressed in New York time.
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, tzinfo=dt.timezone.utc)
        assert (v.hour, v.minute, v.second) == (7, 34, 56)


class TestDateTimeTzSubSecond:
    _NY = ZoneInfo("America/New_York")

    def _decode_one(self, type_name, tick):
        data = build_native_block([("ts", type_name, [tick])])
        return list(_ch_core.ColBatch.decode_native(data).column_data(0))[0]

    def test_dst_fall_back_fold(self):
        # America/New_York 2020-11-01: 1:00:00.5 wall time exists twice. The
        # epoch values pin each side of the fold; sub-second micros must not
        # disturb the UTC offset.
        for epoch_secs, offset_hours in ((1604206800, -4), (1604210400, -5)):
            v = self._decode_one(
                "DateTime64(6, 'America/New_York')", epoch_secs * 1_000_000 + 500_000
            )
            assert v.microsecond == 500_000
            assert v.utcoffset() == dt.timedelta(hours=offset_hours)
            assert (v.hour, v.minute, v.second) == (1, 0, 0)

    def test_microsecond_exactness(self):
        # Sweep awkward microsecond values at a recent epoch and assert exact
        # round-trips through the tz-aware path.
        for secs in (1705322096, -1, -86_400):
            for micros in (1, 3, 333_333, 499_999, 500_000, 500_001, 999_999):
                v = self._decode_one(
                    "DateTime64(6, 'America/New_York')", secs * 1_000_000 + micros
                )
                expected = dt.datetime.fromtimestamp(secs, self._NY).replace(
                    microsecond=micros
                )
                assert v == expected
                assert v.microsecond == micros

    def test_far_future_exactness(self):
        # Year 2200 is beyond f64 sub-microsecond precision for epoch seconds;
        # the exact path must still produce the precise microsecond.
        secs = 7_258_204_800
        micros = 123_457
        v = self._decode_one("DateTime64(6, 'America/New_York')", secs * 1_000_000 + micros)
        expected = dt.datetime.fromtimestamp(secs, self._NY).replace(microsecond=micros)
        assert v == expected
        assert v.microsecond == micros


class TestDecodeDateTime64:
    def test_dt64_millis(self):
        ticks = [0, 1705322096789]  # milliseconds
        data = build_native_block([("ts", "DateTime64(3)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["DateTime64(3)"]
        expected = [_EPOCH_NAIVE + dt.timedelta(milliseconds=t) for t in ticks]
        assert list(batch.column_data(0)) == expected

    def test_dt64_nanos_truncate_to_micros(self):
        # Precision 9 (ns); Python datetime resolves to microseconds, so the
        # sub-microsecond digits are truncated.
        ticks = [1705322096789012345]
        data = build_native_block([("ts", "DateTime64(9)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, 789012)

    def test_dt64_nullable(self):
        ticks = [1705322096789, None, 0]
        data = build_native_block([("ts", "Nullable(DateTime64(3))", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["Nullable(DateTime64(3))"]
        result = list(batch.column_data(0))
        assert result[1] is None
        assert result[0] == _EPOCH_NAIVE + dt.timedelta(milliseconds=ticks[0])

    def test_temporal_rows_and_columns(self):
        days = [0, 19737]
        ticks = [0, 1705322096789]
        data = build_native_block([("d", "Date", days), ("ts", "DateTime64(3)", ticks)])
        batch = _ch_core.ColBatch.decode_native(data)
        rows = list(batch.to_python_rows())
        assert rows[1] == (
            _EPOCH_DATE + dt.timedelta(days=days[1]),
            _EPOCH_NAIVE + dt.timedelta(milliseconds=ticks[1]),
        )
        cols = list(batch.to_python_columns())
        assert list(cols[0])[0] == _EPOCH_DATE


class TestDecodeTime:
    @pytest.mark.parametrize(
        "type_name,ticks,expected",
        [
            (
                "Time",
                [-3_599_999, -13, 0, 3_599_999],
                [
                    dt.timedelta(seconds=v)
                    for v in [-3_599_999, -13, 0, 3_599_999]
                ],
            ),
            (
                "Time64(0)",
                [-13, 0, 79],
                [dt.timedelta(seconds=v) for v in [-13, 0, 79]],
            ),
            (
                "Time64(3)",
                [-1_500, -1, 0, 79_999],
                [
                    dt.timedelta(milliseconds=-1_500),
                    dt.timedelta(milliseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(milliseconds=79_999),
                ],
            ),
            (
                "Time64(6)",
                [-1_500_001, -1, 0, 79_999_999],
                [
                    dt.timedelta(microseconds=-1_500_001),
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(microseconds=79_999_999),
                ],
            ),
            (
                "Time64(9)",
                [-1_999, -1_001, -999, 1_999],
                [
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(microseconds=-1),
                    dt.timedelta(0),
                    dt.timedelta(microseconds=1),
                ],
            ),
        ],
    )
    def test_plain_precisions_and_truncation(self, type_name, ticks, expected):
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("t", type_name, ticks)])
        )
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected

    def test_nullable_and_all_object_exits(self):
        ticks = [-1_001, None, 1_999]
        expected = [
            dt.timedelta(microseconds=-1),
            None,
            dt.timedelta(microseconds=1),
        ]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("t", "Nullable(Time64(9))", ticks)])
        )
        assert batch.column_type_names == ["Nullable(Time64(9))"]
        assert list(batch.column_data(0)) == expected
        assert list(batch.to_python_columns()[0]) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_raw_time_ticks_scalar_nullable_and_low_cardinality(self):
        data = build_native_block(
            [
                ("t", "Time", [-13, 0, 79]),
                ("t64", "Nullable(Time64(9))", [-1_001, None, 1_999]),
                ("lc", "LowCardinality(Time)", [13, 79, 13]),
            ]
        )
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0, raw_time_ticks=True)) == [-13, 0, 79]
        assert list(batch.column_data(1, raw_time_ticks=True)) == [
            -1_001,
            None,
            1_999,
        ]
        assert list(batch.column_data(2, raw_time_ticks=True)) == [13, 79, 13]
        assert list(batch.column_data(0)) == [
            dt.timedelta(seconds=-13),
            dt.timedelta(0),
            dt.timedelta(seconds=79),
        ]

    def test_raw_time_ticks_recursive_array_tuple_and_low_cardinality(self):
        type_name = (
            "Array(Tuple(Time, Nullable(Time64(9)), "
            "Array(LowCardinality(Time))))"
        )
        rows = [
            [(13, -1_001, [13, 13, 79]), (-79, None, [])],
            [],
            [(0, 1_999, [-1])],
        ]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("v", type_name, rows)])
        )
        assert list(batch.column_data(0, True)) == rows
        assert list(batch.column_data(0)) == [
            [
                (
                    dt.timedelta(seconds=13),
                    dt.timedelta(microseconds=-1),
                    [dt.timedelta(seconds=13)] * 2 + [dt.timedelta(seconds=79)],
                ),
                (dt.timedelta(seconds=-79), None, []),
            ],
            [],
            [
                (
                    dt.timedelta(0),
                    dt.timedelta(microseconds=1),
                    [dt.timedelta(seconds=-1)],
                )
            ],
        ]

    def test_raw_time_ticks_map_duplicate_key_last_value_wins(self):
        type_name = "Map(Time, String)"
        body = bytearray(struct.pack("<Q", 3))
        body.extend(struct.pack("<iii", 13, 13, 79))
        body.extend(_encode_plain_body("String", ["first", "second", "third"]))
        data = build_native_block_from_bodies([("m", type_name, bytes(body))], 1)
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0, raw_time_ticks=True)) == [
            {13: "second", 79: "third"}
        ]
        assert list(batch.column_data(0)) == [
            {
                dt.timedelta(seconds=13): "second",
                dt.timedelta(seconds=79): "third",
            }
        ]

    @pytest.mark.parametrize("type_name", ["time", "time64(3)", "Time64"])
    def test_noncanonical_direct_headers_rejected(self, type_name):
        payload = build_native_block([("t", type_name, [])])
        with pytest.raises(ValueError, match="Unsupported ClickHouse type"):
            _ch_core.ColBatch.decode_native(payload)


class TestArrowTemporal:
    def test_arrow_temporal_types(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("d", "Date", [0, 19737]),
            ("d32", "Date32", [-25567, 19737]),
            ("dt", "DateTime", [0, 1705322096]),
            ("ts", "DateTime64(3)", [0, 1705322096789]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        # Zero-copy export keeps native widths: Date is raw uint16 days,
        # DateTime is raw uint32 seconds; Date32 and DateTime64(3) map to real
        # Arrow temporal types.
        assert result.schema.field("d").type == pa.uint16()
        assert result.schema.field("d32").type == pa.date32()
        assert result.schema.field("dt").type == pa.uint32()
        assert result.schema.field("ts").type == pa.timestamp("ms")
        assert result.column("d32").to_pylist() == [
            _EPOCH_DATE + dt.timedelta(days=-25567),
            _EPOCH_DATE + dt.timedelta(days=19737),
        ]


# ---------------------------------------------------------------------------
# LowCardinality
# ---------------------------------------------------------------------------

class TestDecodeLowCardinality:
    def test_string_basic(self):
        vals = ["red", "green", "red", "blue", "green", "red"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["LowCardinality(String)"]
        assert list(batch.column_data(0)) == vals

    def test_nullable_string(self):
        vals = ["x", None, "y", "x", None, "y"]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["LowCardinality(Nullable(String))"]
        assert list(batch.column_data(0)) == vals

    def test_empty_string_distinct_from_null(self):
        # A real empty string is its own dictionary entry, not the null sentinel.
        vals = ["", None, "", "a"]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_uint32_inner(self):
        # A non-String inner type exercises the dictionary -> primitive recursion.
        vals = [100, 200, 100, 4_000_000_000, 200]
        data = build_native_block([("c", "LowCardinality(UInt32)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_datetime_named_zone(self):
        # LowCardinality(DateTime(tz)) must still apply the timezone policy, which
        # means prepare_temporal has to see through the LowCardinality wrapper.
        secs = 1705322096  # 2024-01-15 12:34:56 UTC
        data = build_native_block(
            [("c", "LowCardinality(DateTime('America/New_York'))", [secs, secs])]
        )
        batch = _ch_core.ColBatch.decode_native(data)
        v = list(batch.column_data(0))[0]
        assert v.tzinfo == ZoneInfo("America/New_York")
        assert v == dt.datetime(2024, 1, 15, 12, 34, 56, tzinfo=dt.timezone.utc)

    def test_invalid_utf8_hex_fallback(self):
        vals = ["ok", b"\xff\xfe", "ok"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["ok", "fffe", "ok"]

    def test_paths_agree(self):
        vals = ["a", "a", "b", "c", "b", "a", None]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == vals

    def test_arrow_dictionary(self):
        pa = pytest.importorskip("pyarrow")
        vals = ["red", "green", "red", "blue"]
        data = build_native_block([("c", "LowCardinality(String)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert pa.types.is_dictionary(result.schema.field("c").type)
        assert result.column("c").to_pylist() == vals

    def test_arrow_nullable_dictionary(self):
        pa = pytest.importorskip("pyarrow")
        vals = ["x", None, "y", "x", None]
        data = build_native_block([("c", "LowCardinality(Nullable(String))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        col = pa.RecordBatchReader.from_stream(batch).read_all().column("c")
        assert pa.types.is_dictionary(col.type)
        assert col.to_pylist() == vals
        assert col.null_count == 2

    def test_across_chunks(self):
        # Each Native block carries its own dictionary; the two chunks must
        # concatenate correctly even with different per-block dictionaries.
        first = build_native_block([("c", "LowCardinality(String)", ["a", "b", "a"])])
        second = build_native_block([("c", "LowCardinality(String)", ["c", "c", "d"])])
        batch = _ch_core.ColBatch.decode_native(first + second)
        assert batch.num_chunks == 2
        assert list(batch.column_data(0)) == ["a", "b", "a", "c", "c", "d"]

    def test_with_block_info(self):
        # The shape clickhouse-connect actually receives: client_protocol_version
        # 54405 emits a BlockInfo preamble and, being below 54454, no per-column
        # custom-serialization marker. The LowCardinality state prefix follows.
        vals = ["red", "green", "red", None, "green"]
        data = build_native_block(
            [("c", "LowCardinality(Nullable(String))", vals)], block_info=True
        )
        batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
        assert list(batch.column_data(0)) == vals


# ---------------------------------------------------------------------------
# Enum
# ---------------------------------------------------------------------------

class TestDecodeEnum:
    _E8 = "Enum8('red' = 1, 'green' = 2, 'blue' = 3)"
    _E16 = "Enum16('alpha' = -5, 'beta' = 0, 'gamma' = 1000)"

    def test_enum8_basic(self):
        # Wire carries the integer codes; the Python exit yields the labels.
        data = build_native_block([("c", self._E8, [1, 2, 3, 1, 2])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [self._E8]
        assert list(batch.column_data(0)) == ["red", "green", "blue", "red", "green"]

    def test_enum16_negative_and_large(self):
        data = build_native_block([("c", self._E16, [-5, 0, 1000, 0, -5])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [self._E16]
        assert list(batch.column_data(0)) == ["alpha", "beta", "gamma", "beta", "alpha"]

    def test_nullable_enum8(self):
        data = build_native_block([("c", f"Nullable({self._E8})", [1, None, 3, None])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [f"Nullable({self._E8})"]
        assert list(batch.column_data(0)) == ["red", None, "blue", None]

    def test_unknown_code_is_none(self):
        # A code with no defined label materializes as None, matching
        # clickhouse-connect's int_map.get(code, None).
        data = build_native_block([("c", self._E8, [1, 99, 2])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == ["red", None, "green"]

    def test_paths_agree(self):
        data = build_native_block([("c", self._E16, [1000, -5, 0, 1000, -5])])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        expected = ["gamma", "alpha", "beta", "gamma", "alpha"]
        assert via_column_data == via_columns == via_rows == expected

    def test_arrow_exports_int_codes(self):
        # The core exports Enum as the raw signed-int codes (no per-cell label
        # remapping); the label policy lives only on the Python object exit.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("c", self._E8, [1, 2, 3]), ("d", self._E16, [-5, 0, 1000])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert result.schema.field("c").type == pa.int8()
        assert result.schema.field("d").type == pa.int16()
        assert result.column("c").to_pylist() == [1, 2, 3]
        assert result.column("d").to_pylist() == [-5, 0, 1000]


# ---------------------------------------------------------------------------
# UUID
# ---------------------------------------------------------------------------

class TestDecodeUUID:
    _KNOWN = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
    _VALUES = [uuid.UUID(int=0), _KNOWN, uuid.UUID(int=(1 << 128) - 1)]

    def test_values(self):
        data = build_native_block([("u", "UUID", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["UUID"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        assert all(type(v) is uuid.UUID for v in got)
        assert got[1] == uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
        assert all(v.is_safe is uuid.SafeUUID.unsafe for v in got)

    def test_paths_agree(self):
        data = build_native_block([("u", "UUID", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [self._KNOWN, None, uuid.UUID(int=0), None]
        data = build_native_block([("u", "Nullable(UUID)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_low_cardinality(self):
        vals = [self._KNOWN, uuid.UUID(int=0), self._KNOWN, self._KNOWN]
        data = build_native_block([("u", "LowCardinality(UUID)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        got = list(batch.column_data(0))
        assert got == vals
        assert all(v.is_safe is uuid.SafeUUID.unsafe for v in got)

    def test_low_cardinality_nullable(self):
        vals = [self._KNOWN, None, self._KNOWN, None]
        data = build_native_block([("u", "LowCardinality(Nullable(UUID))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_low_cardinality_all_null(self):
        vals = [None, None, None]
        data = build_native_block([("u", "LowCardinality(Nullable(UUID))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals

    def test_round_trip(self):
        vals = [self._KNOWN, uuid.UUID(int=79)]
        encoded = _ch_core.encode_native_block(["u"], ["UUID"], [vals], len(vals))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals


# ---------------------------------------------------------------------------
# IPv4
# ---------------------------------------------------------------------------

class TestDecodeIPv4:
    _VALUES = [
        ipaddress.IPv4Address("0.0.0.0"),
        ipaddress.IPv4Address("255.255.255.255"),
        ipaddress.IPv4Address("1.2.3.4"),
    ]

    def test_values(self):
        data = build_native_block([("v", "IPv4", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["IPv4"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        assert all(type(v) is ipaddress.IPv4Address for v in got)
        assert got[2] == ipaddress.ip_address("1.2.3.4")

    def test_paths_agree(self):
        data = build_native_block([("v", "IPv4", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [ipaddress.IPv4Address("1.2.3.4"), None, ipaddress.IPv4Address("0.0.0.0")]
        data = build_native_block([("v", "Nullable(IPv4)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_round_trip(self):
        vals = [ipaddress.IPv4Address("192.0.2.1"), ipaddress.IPv4Address("198.51.100.7")]
        encoded = _ch_core.encode_native_block(["v"], ["IPv4"], [vals], len(vals))
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == vals


# ---------------------------------------------------------------------------
# IPv6
# ---------------------------------------------------------------------------

class TestDecodeIPv6:
    _VALUES = [
        ipaddress.IPv6Address("::"),
        ipaddress.IPv6Address("::1"),
        ipaddress.IPv6Address("2001:db8:85a3:8d3:1319:8a2e:370:7348"),
        ipaddress.IPv6Address("::ffff:1.2.3.4"),
    ]

    def test_values(self):
        data = build_native_block([("v", "IPv6", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == ["IPv6"]
        got = list(batch.column_data(0))
        assert got == self._VALUES
        # Always IPv6Address, even for a v4-mapped value.
        assert all(type(v) is ipaddress.IPv6Address for v in got)
        # str() reads _scope_id, so it verifies the attribute was set.
        assert [str(v) for v in got] == [str(v) for v in self._VALUES]

    def test_paths_agree(self):
        data = build_native_block([("v", "IPv6", self._VALUES)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == self._VALUES

    def test_nullable(self):
        vals = [ipaddress.IPv6Address("::1"), None, ipaddress.IPv6Address("::ffff:1.2.3.4")]
        data = build_native_block([("v", "Nullable(IPv6)", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert list(batch.column_data(0)) == vals
        assert [row[0] for row in batch.to_python_rows()] == vals

    def test_round_trip(self):
        vals = [ipaddress.IPv6Address("2001:db8::1"), ipaddress.IPv6Address("::ffff:192.0.2.9")]
        encoded = _ch_core.encode_native_block(["v"], ["IPv6"], [vals], len(vals))
        got = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert got == vals
        assert all(type(v) is ipaddress.IPv6Address for v in got)


# ---------------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------------

class TestDecodeDecimal:
    # (precision, scale, unscaled values); precision picks the wire width
    # (<=9 -> 32-bit, <=18 -> 64, <=38 -> 128, <=76 -> 256).
    _CASES = [
        (9, 4, [0, 5, -5, -12000, 999_999_999, -999_999_999]),
        (18, 6, [0, 7, -123456, 123456, 10**18 - 1, -(10**18 - 1)]),
        (38, 10, [0, -3, 10**9 + 1, 10**38 - 1, -(10**38 - 1)]),
        (76, 20, [0, 42, -42, 10**19, -(10**41 + 7), 10**76 - 1, -(10**76 - 1)]),
        (9, 0, [0, -13, 999_999_999]),
        (76, 0, [0, 10**76 - 1, -(10**76 - 1)]),
    ]

    @staticmethod
    def _reference(unscaled, precision, scale):
        with decimal.localcontext() as ctx:
            ctx.prec = precision
            return decimal.Decimal(unscaled).scaleb(-scale)

    @pytest.mark.parametrize("precision,scale,unscaled", _CASES)
    def test_matches_python_reference(self, precision, scale, unscaled):
        type_name = f"Decimal({precision}, {scale})"
        data = build_native_block([("d", type_name, unscaled)])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.column_type_names == [type_name]
        got = list(batch.column_data(0))
        expected = [self._reference(u, precision, scale) for u in unscaled]
        assert got == expected
        assert [v.as_tuple() for v in got] == [e.as_tuple() for e in expected]

    def test_paths_agree(self):
        unscaled = [0, -3, 10**38 - 1]
        data = build_native_block([("d", "Decimal(38, 10)", unscaled)])
        batch = _ch_core.ColBatch.decode_native(data)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [row[0] for row in batch.to_python_rows()]
        expected = [self._reference(u, 38, 10) for u in unscaled]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable(self):
        vals = [12345, None, -12345, None]
        data = build_native_block([("d", "Nullable(Decimal(18, 4))", vals)])
        batch = _ch_core.ColBatch.decode_native(data)
        expected = [None if v is None else self._reference(v, 18, 4) for v in vals]
        assert list(batch.column_data(0)) == expected
        assert [row[0] for row in batch.to_python_rows()] == expected

    def test_round_trip(self):
        vals = [decimal.Decimal("123.4567"), decimal.Decimal("-1.5")]
        encoded = _ch_core.encode_native_block(["d"], ["Decimal(20, 4)"], [vals], len(vals))
        got = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        expected = [decimal.Decimal("123.4567"), decimal.Decimal("-1.5000")]
        assert got == expected
        assert [v.as_tuple() for v in got] == [e.as_tuple() for e in expected]


# ---------------------------------------------------------------------------
# Array
# ---------------------------------------------------------------------------

_ARR_KNOWN_UUID = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")
_ARR_NY = ZoneInfo("America/New_York")

# DateTime element: tz-aware New York input round-trips to tz-aware New York.
_ARR_DT_TZ_ROWS = [
    [dt.datetime(2024, 1, 15, 7, 34, 56, tzinfo=_ARR_NY)],
    [],
    [
        dt.datetime(2000, 6, 15, 12, 0, 0, tzinfo=_ARR_NY),
        dt.datetime(1970, 1, 1, 0, 0, 0, tzinfo=_ARR_NY),
    ],
]

# DateTime64(3) has no timezone, so tz-aware UTC input decodes to the naive UTC
# wall clock; passing UTC keeps the expectation independent of the host locale.
_ARR_DT64_UTC_ROWS = [
    [dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)],
    [],
    [
        dt.datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=dt.timezone.utc),
        dt.datetime(2000, 6, 15, 12, 0, 0, 500000, tzinfo=dt.timezone.utc),
    ],
]
_ARR_DT64_EXPECTED = [
    [dt.datetime(2024, 1, 15, 12, 34, 56, 789000)],
    [],
    [
        dt.datetime(1970, 1, 1, 0, 0, 0, 1000),
        dt.datetime(2000, 6, 15, 12, 0, 0, 500000),
    ],
]

# (type_name, py_rows, expected) with expected=None meaning it equals py_rows.
# Every case has multiple rows, at least one empty array, ragged lengths, and
# nullable element types mix None with values.
_ARR_ROUND_TRIP = [
    ("Array(Int32)", [[13, 79], [], [-1, 0, 2147483647], [7]], None),
    ("Array(Int64)", [[13], [], [9223372036854775807, -9223372036854775808], [5, 5]], None),
    ("Array(UInt64)", [[0, 18446744073709551615], [], [13, 79, 5]], None),
    ("Array(Float64)", [[1.5, -2.5], [], [0.0, 1e300]], None),
    ("Array(Bool)", [[True, False, True], [], [False]], None),
    ("Array(String)", [["user_1", "user_2"], [], ["", "sventon"]], None),
    ("Array(FixedString(3))", [[b"abc", b"xyz"], [], [b"a\x00\x00"]], None),
    ("Array(Date)", [[dt.date(2024, 1, 2), dt.date(1970, 1, 1)], [], [dt.date(2000, 6, 15)]], None),
    ("Array(DateTime('America/New_York'))", _ARR_DT_TZ_ROWS, None),
    ("Array(DateTime64(3))", _ARR_DT64_UTC_ROWS, _ARR_DT64_EXPECTED),
    ("Array(UUID)", [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]], None),
    (
        "Array(IPv4)",
        [
            [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("1.2.3.4")],
            [],
            [ipaddress.IPv4Address("255.255.255.255")],
        ],
        None,
    ),
    (
        "Array(IPv6)",
        [
            [ipaddress.IPv6Address("::1")],
            [],
            [ipaddress.IPv6Address("2001:db8::1"), ipaddress.IPv6Address("::ffff:1.2.3.4")],
        ],
        None,
    ),
    (
        "Array(Decimal(9, 2))",
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("9999999.99")]],
        None,
    ),
    (
        "Array(Decimal(38, 10))",
        [[decimal.Decimal("1.5"), decimal.Decimal("-2.25")], [], [decimal.Decimal("0")]],
        None,
    ),
    ("Array(Enum8('a' = 1, 'b' = 2))", [["a", "b"], [], ["b", "a", "b"]], None),
    ("Array(Nullable(Int32))", [[13, None, 79], [], [None], [7]], None),
    ("Array(Nullable(String))", [["user_1", None], [], [None, "x"]], None),
    ("Array(LowCardinality(String))", [["red", "green", "red"], [], ["blue"]], None),
    ("Array(LowCardinality(Nullable(String)))", [["x", None, "x"], [], [None, "y"]], None),
    ("Array(Array(Int32))", [[[13, 79], [5]], [], [[7]], [[], [1, 2, 3]]], None),
]

# (type_name, py_rows, wire_rows, expected): wire_rows is the raw wire form the
# helper serializes, py_rows is what the encoder is given, expected is the decode.
# Excludes LowCardinality-in-array because the encoder sets the index word's
# NeedUpdateDictionary bit that the helper does not, so their bytes differ.
_ARR_GOLDEN = [
    ("Array(Int32)", [[13, 79], [], [7]], [[13, 79], [], [7]], [[13, 79], [], [7]]),
    ("Array(String)", [["user_1", "user_2"], [], ["x"]], [["user_1", "user_2"], [], ["x"]], [["user_1", "user_2"], [], ["x"]]),
    ("Array(FixedString(3))", [[b"abc"], [], [b"xyz", b"a\x00\x00"]], [[b"abc"], [], [b"xyz", b"a\x00\x00"]], [[b"abc"], [], [b"xyz", b"a\x00\x00"]]),
    ("Array(UInt64)", [[0, 18446744073709551615], [], [13]], [[0, 18446744073709551615], [], [13]], [[0, 18446744073709551615], [], [13]]),
    ("Array(Nullable(Int32))", [[13, None], [], [7]], [[13, None], [], [7]], [[13, None], [], [7]]),
    ("Array(Array(Int32))", [[[13, 79], [5]], [], [[7]]], [[[13, 79], [5]], [], [[7]]], [[[13, 79], [5]], [], [[7]]]),
    (
        "Array(UUID)",
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
        [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]],
    ),
    (
        "Array(IPv4)",
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
        [[ipaddress.IPv4Address("1.2.3.4")], [], [ipaddress.IPv4Address("0.0.0.0"), ipaddress.IPv4Address("255.255.255.255")]],
    ),
    (
        "Array(IPv6)",
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
        [[ipaddress.IPv6Address("::1")], [], [ipaddress.IPv6Address("2001:db8::1")]],
    ),
    (
        "Array(Date)",
        [[dt.date(2024, 1, 2)], [], [dt.date(1970, 1, 1), dt.date(2000, 6, 15)]],
        [
            [dt.date(2024, 1, 2).toordinal() - 719163],
            [],
            [0, dt.date(2000, 6, 15).toordinal() - 719163],
        ],
        [[dt.date(2024, 1, 2)], [], [dt.date(1970, 1, 1), dt.date(2000, 6, 15)]],
    ),
    (
        "Array(DateTime64(3))",
        [
            [dt.datetime(2024, 1, 15, 12, 34, 56, 789000, tzinfo=dt.timezone.utc)],
            [],
            [dt.datetime(1970, 1, 1, 0, 0, 0, 1000, tzinfo=dt.timezone.utc)],
        ],
        [[1705322096789], [], [1]],
        [
            [dt.datetime(2024, 1, 15, 12, 34, 56, 789000)],
            [],
            [dt.datetime(1970, 1, 1, 0, 0, 0, 1000)],
        ],
    ),
    (
        "Array(Decimal(9, 2))",
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("0.00")]],
        [[100, -350], [], [0]],
        [[decimal.Decimal("1.00"), decimal.Decimal("-3.50")], [], [decimal.Decimal("0.00")]],
    ),
    (
        "Array(Enum8('a' = 1, 'b' = 2))",
        [["a", "b"], [], ["a"]],
        [[1, 2], [], [1]],
        [["a", "b"], [], ["a"]],
    ),
]


class TestArray:
    @pytest.mark.parametrize("type_name,py_rows,expected", _ARR_ROUND_TRIP)
    def test_round_trip(self, type_name, py_rows, expected):
        if expected is None:
            expected = py_rows
        encoded = _ch_core.encode_native_block(["a"], [type_name], [py_rows], len(py_rows))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == expected

    @pytest.mark.parametrize("type_name,py_rows,wire_rows,expected", _ARR_GOLDEN)
    def test_golden_bytes(self, type_name, py_rows, wire_rows, expected):
        encoded = _ch_core.encode_native_block(["a"], [type_name], [py_rows], len(py_rows))
        built = build_native_block([("a", type_name, wire_rows)])
        assert encoded == built
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == expected

    def test_golden_decode_low_cardinality_in_array(self):
        # encode == build is not asserted: the encoder sets the index word's
        # NeedUpdateDictionary bit while the helper does not, so their bytes
        # differ. Cover it by decode-of-helper-bytes plus an encode round-trip.
        rows = [["red", "green", "red"], [], ["blue"]]
        built = build_native_block([("c", "Array(LowCardinality(String))", rows)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == rows
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_low_cardinality_in_array_all_empty(self):
        # Every array empty (total_elements == 0): the LowCardinality element
        # body is absent entirely, so only the hoisted key version and the zero
        # offsets remain. With no index word to differ, encode == build holds.
        rows = [[], [], []]
        built = build_native_block([("c", "Array(LowCardinality(String))", rows)])
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        assert encoded == built
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    @pytest.mark.parametrize(
        "type_name,rows",
        [
            ("Array(Int32)", [[13, 79], [], [7, 5, 5]]),
            ("Array(Nullable(String))", [["user_1", None], [], [None, "x"]]),
            ("Array(Enum8('a' = 1, 'b' = 2))", [["a", "b"], [], ["b", "a", "b"]]),
            ("Array(DateTime('America/New_York'))", _ARR_DT_TZ_ROWS),
            ("Array(UUID)", [[uuid.UUID(int=0), _ARR_KNOWN_UUID], [], [uuid.UUID(int=79)]]),
            ("Array(LowCardinality(Nullable(String)))", [["x", None, "x"], [], [None, "y"]]),
            ("Array(Array(Int64))", [[[13, 79], [5]], [], [[7]], [[], [1, 2, 3]]]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, rows):
        encoded = _ch_core.encode_native_block(["a"], [type_name], [rows], len(rows))
        batch = _ch_core.ColBatch.decode_native(encoded)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_multi_block_concatenation(self):
        rows_a = [[13, 79], [], [7]]
        rows_b = [[5], [1, 2], []]
        block_a = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [rows_a], len(rows_a))
        block_b = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [rows_b], len(rows_b))
        merged = _ch_core.ColBatch.from_batches(
            [
                _ch_core.ColBatch.decode_native(block_a),
                _ch_core.ColBatch.decode_native(block_b),
            ]
        )
        assert merged.num_chunks == 2
        assert list(merged.column_data(0)) == rows_a + rows_b
        concat = _ch_core.ColBatch.decode_native(block_a + block_b)
        assert list(concat.column_data(0)) == rows_a + rows_b

    def test_zero_rows(self):
        encoded = _ch_core.encode_native_block(["a"], ["Array(Int32)"], [[]], 0)
        assert encoded == build_native_block([("a", "Array(Int32)", [])])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.num_rows == 0
        assert batch.column_type_names == ["Array(Int32)"]
        assert list(batch.column_data(0)) == []

    def test_nullable_array_type_rejected(self):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["a"], ["Nullable(Array(Int32))"], [[[13]]], 1)

    def test_none_row_rejected(self):
        with pytest.raises(ValueError, match="is None but Array"):
            _ch_core.encode_native_block(["a"], ["Array(Int32)"], [[None]], 1)

    def test_bare_str_row_rejected(self):
        with pytest.raises(ValueError, match="is a str, not an Array sequence"):
            _ch_core.encode_native_block(["a"], ["Array(Int32)"], [["abc"]], 1)

    def test_bytes_like_rows_flatten_as_int_elements(self):
        rows = [b"\x01\x02", bytearray(b"\x03"), memoryview(b"\x04\x05"), []]
        expected = _ch_core.encode_native_block(
            ["a"], ["Array(UInt8)"], [[[1, 2], [3], [4, 5], []]], 4
        )
        assert _ch_core.encode_native_block(["a"], ["Array(UInt8)"], [rows], 4) == expected

    def test_bytes_row_for_string_elements_raises_conversion_error(self):
        with pytest.raises(ValueError, match="row 0 element 0 cannot be converted to String"):
            _ch_core.encode_native_block(["a"], ["Array(String)"], [[b"ab"]], 1)

    def test_element_error_reports_outer_row_and_element(self):
        for outer in ([[1, 2], ["x"]], _NdarrayLikeColumn([[1, 2], ["x"]])):
            with pytest.raises(
                ValueError, match='column "v" row 1 element 0 cannot be converted to Int32'
            ):
                _ch_core.encode_native_block(["v"], ["Array(Int32)"], [outer], 2)
        with pytest.raises(
            ValueError, match='column "v" row 0 element 1 is None but Int32 is not Nullable'
        ):
            _ch_core.encode_native_block(["v"], ["Array(Int32)"], [[[1, None]]], 1)

    def test_nested_element_error_reports_outer_path(self):
        rows = [[[1]], [[2], ["x"]]]
        with pytest.raises(
            ValueError,
            match='column "v" row 1 element 1 element 0 cannot be converted to Int32',
        ):
            _ch_core.encode_native_block(["v"], ["Array(Array(Int32))"], [rows], 2)

    def test_array_lc_reuses_dictionary_objects(self):
        rows = [["red", "red", "green"], [], ["red", "green"]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(String))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[0][1]
        assert col[0][0] is col[2][0]
        cols = batch.to_python_columns()[0]
        assert cols[0][0] is cols[0][1] and cols[0][0] is cols[2][0]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0][0] is out_rows[0][0][1]
        assert out_rows[0][0][0] is out_rows[2][0][0]

    def test_nested_array_lc_reuses_dictionary_objects(self):
        rows = [[["a", "a"], ["a"]], [], [["a"]]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(Array(LowCardinality(String)))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(encoded)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0][0] is col[0][1][0]
        assert col[0][0][0] is col[2][0][0]

    def test_array_lc_nullable_reuse_and_values(self):
        rows = [["x", None, "x"], [], [None, "x"]]
        encoded = _ch_core.encode_native_block(
            ["c"], ["Array(LowCardinality(Nullable(String)))"], [rows], len(rows)
        )
        col = _ch_core.ColBatch.decode_native(encoded).column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[0][2]
        assert col[0][0] is col[2][1]

    def test_unsupported_element_rejected(self):
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["a"], ["Array(JSON)"], [[[{"a": 1}]]], 1)

    def test_deeply_nested_type_rejected_not_crash(self):
        # Past the parser depth cap the type is rejected without a stack overflow.
        deep = "Array(" * 200 + "Int32" + ")" * 200
        with pytest.raises(NotImplementedError):
            _ch_core.encode_native_block(["c"], [deep], [[]], 0)

    def test_unordered_row_rejected(self):
        for bad in ({3, 1, 2}, {13: 0, 79: 0}):
            with pytest.raises(ValueError):
                _ch_core.encode_native_block(["v"], ["Array(Int32)"], [[bad]], 1)

    def test_malformed_offsets_decode_error(self):
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(2))
        buf.extend(_encode_varint_string("a"))
        buf.extend(_encode_varint_string("Array(Int32)"))
        buf.extend(struct.pack("<Q", 2))  # row 0 end offset
        buf.extend(struct.pack("<Q", 1))  # row 1 end offset decreases -> invalid
        with pytest.raises(ValueError, match="Invalid Array layout"):
            _ch_core.ColBatch.decode_native(bytes(buf))


class TestArrayInsertFastPath:
    """Exact list/tuple row flattening into one flat element run."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["v"], [type_name], [rows], n)

    def test_row_and_outer_container_kinds_agree(self):
        rows = [[13, 79], [], [7, 5, 5]]
        from_lists = self._encode("Array(Int64)", rows)
        assert from_lists == build_native_block([("v", "Array(Int64)", rows)])
        assert self._encode("Array(Int64)", tuple(rows), 3) == from_lists
        assert self._encode("Array(Int64)", [tuple(r) for r in rows]) == from_lists
        assert self._encode("Array(Int64)", _NdarrayLikeColumn(rows), 3) == from_lists

    def test_exotic_row_containers_match_list_rows(self):
        expected = self._encode("Array(Int64)", [[0, 1, 2], [7], [], [3, 4]])
        rows = [range(3), _NdarrayLikeColumn([7]), (x for x in []), (3, 4)]
        assert self._encode("Array(Int64)", rows, 4) == expected

    def test_mixed_rows_agree_across_outer_containers(self):
        rows = [[1, 2], (3,), range(2)]
        expected = self._encode("Array(Int64)", [[1, 2], [3], [0, 1]])
        assert self._encode("Array(Int64)", rows, 3) == expected
        assert self._encode("Array(Int64)", _NdarrayLikeColumn(rows), 3) == expected

    def test_nested_tuple_rows(self):
        rows = [((1, 2), [3]), [], [(7,)], [[], (1, 2, 3)]]
        expected_rows = [[[1, 2], [3]], [], [[7]], [[], [1, 2, 3]]]
        encoded = self._encode("Array(Array(Int64))", rows, 4)
        assert encoded == self._encode("Array(Array(Int64))", expected_rows)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == expected_rows

    def test_nullable_element_tuple_rows(self):
        rows = [(13, None), (), (None, 7)]
        encoded = self._encode("Array(Nullable(Int64))", rows, 3)
        assert encoded == self._encode("Array(Nullable(Int64))", [list(r) for r in rows])
        expected = [[13, None], [], [None, 7]]
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == expected

    def test_string_and_lc_tuple_rows(self):
        rows = [("a", "bb"), (), ("a",)]
        for type_name in ("Array(String)", "Array(LowCardinality(String))"):
            fast = self._encode(type_name, rows, 3)
            assert fast == self._encode(type_name, [list(r) for r in rows])

    def test_lc_non_string_element_round_trip(self):
        rows = [[13, 79, 13], [], [79]]
        encoded = self._encode("Array(LowCardinality(UInt32))", rows)
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_empty_rows_tuple_outer(self):
        assert self._encode("Array(Int64)", (), 0) == build_native_block(
            [("v", "Array(Int64)", [])]
        )
        assert self._encode("Array(Int64)", ((), (), ()), 3) == build_native_block(
            [("v", "Array(Int64)", [[], [], []])]
        )

    def test_invalid_row_in_fallback_reports_row(self):
        with pytest.raises(ValueError, match="row 1 is not a valid Array value"):
            self._encode("Array(Int64)", [[1], 5, [3]], 3)

    def test_row_fallback_preserves_cause(self):
        class Boom(Exception):
            pass

        class EvilRow:
            def __iter__(self):
                raise Boom("iteration exploded")

        with pytest.raises(ValueError, match="row 1 is not a valid Array value") as excinfo:
            self._encode("Array(Int64)", [[1], EvilRow()], 2)
        assert isinstance(excinfo.value.__cause__, Boom)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [[shared, shared], [], [shared]]
        before = sys.getrefcount(shared)
        self._encode("Array(Int64)", rows, 3)
        assert sys.getrefcount(shared) == before

        rows = [[shared], ["x"]]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="element 0 cannot be converted"):
            self._encode("Array(Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [[shared], 5]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="not a valid Array value"):
            self._encode("Array(Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

    def test_outer_list_resized_during_row_fallback_raises(self):
        rows = [[1], None, [3], [4]]

        class Evil:
            def __iter__(self):
                del rows[2:]
                return iter([7])

        rows[1] = Evil()
        with pytest.raises(ValueError, match="resized during encoding"):
            self._encode("Array(Int64)", rows, 4)

    def test_outer_list_resized_during_element_fallback_encodes_snapshot(self):
        # Element conversion runs Python that clears the outer list after the
        # flatten pass; the flat run holds strong references, so the original
        # rows still encode.
        rows = [[1], [], [3]]

        class Evil:
            def __index__(self):
                rows.clear()
                return 9

        rows[1] = [Evil()]
        expected = self._encode("Array(Int64)", [[1], [9], [3]])
        assert self._encode("Array(Int64)", rows, 3) == expected


class TestTupleInsert:
    """Tuple(T1, ...) encode: positional and named-dict rows."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["t"], [type_name], [rows], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_unnamed_rows_match_golden_and_round_trip(self):
        rows = [(13, "a"), (-1, ""), (79, "sventon")]
        encoded = self._encode("Tuple(Int32, String)", rows)
        assert encoded == build_native_block([("t", "Tuple(Int32, String)", rows)])
        assert self._decode(encoded) == rows

    def test_row_and_outer_container_kinds_agree(self):
        rows = [(13, "a"), (79, "b")]
        expected = self._encode("Tuple(Int32, String)", rows)
        assert self._encode("Tuple(Int32, String)", [list(r) for r in rows]) == expected
        assert self._encode("Tuple(Int32, String)", tuple(rows), 2) == expected
        assert self._encode("Tuple(Int32, String)", _NdarrayLikeColumn(rows), 2) == expected

    def test_generic_iterable_rows(self):
        expected = self._encode("Tuple(Int64, Int64)", [(0, 1), (5, 6)])
        assert self._encode("Tuple(Int64, Int64)", [range(2), (5, 6)], 2) == expected

    def test_named_tuple_positional_rows(self):
        rows = [(13, "x"), (79, "y")]
        encoded = self._encode("Tuple(a Int32, b String)", rows)
        assert encoded == build_native_block([("t", "Tuple(a Int32, b String)", rows)])
        assert self._decode(encoded) == [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]

    def test_named_tuple_dict_rows(self):
        dict_rows = [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]
        expected = self._encode("Tuple(a Int32, b String)", [(13, "x"), (79, "y")])
        assert self._encode("Tuple(a Int32, b String)", dict_rows) == expected
        # Extra keys are ignored.
        dict_rows = [{"a": 13, "b": "x", "zz": 5}, {"b": "y", "a": 79}]
        assert self._encode("Tuple(a Int32, b String)", dict_rows) == expected

    def test_backtick_named_tuple_dict_rows(self):
        type_name = "Tuple(`a b` Int32, c String)"
        expected = self._encode(type_name, [(13, "x")])
        assert expected == build_native_block([("t", type_name, [(13, "x")])])
        assert self._encode(type_name, [{"a b": 13, "c": "x"}]) == expected

    def test_dict_row_missing_key_nullable_becomes_none(self):
        type_name = "Tuple(a Int32, b Nullable(String))"
        assert self._encode(type_name, [{"a": 13}]) == self._encode(type_name, [(13, None)])

    def test_dict_row_missing_key_non_nullable_raises(self):
        with pytest.raises(ValueError, match='column "t" row 0 element "b" is None'):
            self._encode("Tuple(a Int32, b String)", [{"a": 13}])

    def test_dict_subclass_rows_read_via_get(self):
        import collections

        rows = [collections.OrderedDict([("a", 13), ("b", "x")])]
        assert self._encode("Tuple(a Int32, b String)", rows) == self._encode(
            "Tuple(a Int32, b String)", [(13, "x")]
        )

    def test_non_dict_row_in_dict_mode_raises(self):
        rows = [{"a": 13, "b": "x"}, (79, "y")]
        with pytest.raises(ValueError, match="row 1 cannot be read as a dict"):
            self._encode("Tuple(a Int32, b String)", rows, 2)

    def test_dict_row_in_positional_mode_raises(self):
        rows = [(13, "x"), {"a": 79, "b": "y"}]
        with pytest.raises(ValueError, match="row 1 is a dict but Tuple rows are read"):
            self._encode("Tuple(a Int32, b String)", rows, 2)
        with pytest.raises(ValueError, match="row 0 is a dict but Tuple rows are read"):
            self._encode("Tuple(Int32, String)", [{"a": 1}], 1)

    def test_arity_mismatch_raises(self):
        with pytest.raises(
            ValueError, match='column "t" row 1 has 3 elements but the Tuple declares 2'
        ):
            self._encode("Tuple(Int32, String)", [(1, "a"), (1, "a", "extra")], 2)
        with pytest.raises(ValueError, match="row 0 has 1 elements but the Tuple declares 2"):
            self._encode("Tuple(Int32, String)", [[1]], 1)

    def test_str_row_rejected(self):
        with pytest.raises(ValueError, match="row 0 is a str, not a Tuple row"):
            self._encode("Tuple(String, String)", ["ab"], 1)

    def test_none_row_non_nullable_raises(self):
        with pytest.raises(ValueError, match="row 1 is None but Tuple"):
            self._encode("Tuple(Int32, String)", [(1, "a"), None], 2)

    def test_element_error_names_column_row_and_element(self):
        with pytest.raises(ValueError, match='column "t" row 1 element 0 cannot be converted'):
            self._encode("Tuple(Int64, String)", [(1, "a"), ("x", "b")], 2)
        with pytest.raises(ValueError, match='column "t" row 0 element "b" cannot be converted'):
            self._encode("Tuple(a Int32, b Int32)", [{"a": 1, "b": "x"}])

    def test_lc_string_element(self):
        # The core encoder's LC flags word differs from the test helper's
        # (both valid), so LC types round-trip instead of golden-comparing.
        type_name = "Tuple(LowCardinality(String), Int64)"
        rows = [("red", 1), ("red", 2), ("blue", 3)]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_array_and_nullable_elements(self):
        type_name = "Tuple(Array(Int64), Nullable(String))"
        rows = [([13, 79], "a"), ([], None)]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_nested_tuple_and_map_elements(self):
        rows = [(1, (2, "x")), (3, (4, "y"))]
        encoded = self._encode("Tuple(UInt8, Tuple(UInt8, String))", rows)
        assert self._decode(encoded) == rows
        rows = [({"k1": 1, "k2": 2}, 10), ({}, 20)]
        encoded = self._encode("Tuple(Map(String, UInt8), Int32)", rows)
        assert self._decode(encoded) == rows

    def test_nullable_tuple_none_rows_match_golden_and_round_trip(self):
        type_name = "Nullable(Tuple(Nullable(Int32), String))"
        rows = [(13, "a"), None, (None, "b"), None]
        encoded = self._encode(type_name, rows)
        assert encoded == build_native_block([("t", type_name, rows)])
        assert self._decode(encoded) == rows

    def test_nullable_tuple_default_placeholders_cover_scalar_types(self):
        type_name = "Nullable(Tuple(UUID, IPv4, Date, Decimal(9, 2), Bool, Float64))"
        rows = [None, (uuid.UUID(int=13), ipaddress.IPv4Address("1.2.3.4"), 79, 5, True, 1.5)]
        encoded = self._encode(type_name, rows, 2)
        decoded = self._decode(encoded)
        assert decoded[0] is None
        assert decoded[1] == (
            uuid.UUID(int=13),
            ipaddress.IPv4Address("1.2.3.4"),
            dt.date(1970, 3, 21),
            decimal.Decimal("5.00"),
            True,
            1.5,
        )

    def test_nullable_named_tuple_dict_rows_with_none(self):
        type_name = "Nullable(Tuple(a UInt64, b String))"
        rows = [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]
        encoded = self._encode(type_name, rows, 3)
        assert self._decode(encoded) == [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]

    def test_empty_tuple(self):
        rows = [(), (), ()]
        encoded = self._encode("Tuple()", rows)
        assert encoded == build_native_block([("t", "Tuple()", rows)])
        assert self._decode(encoded) == rows
        with pytest.raises(ValueError, match="has 1 elements but the Tuple declares 0"):
            self._encode("Tuple()", [(1,)], 1)

    def test_array_of_tuple(self):
        type_name = "Array(Tuple(Int64, String))"
        rows = [[(1, "a"), (2, "b")], [], [(3, "c")]]
        encoded = self._encode(type_name, rows)
        assert self._decode(encoded) == rows

    def test_zero_row_probe(self):
        encoded = _ch_core.encode_native_block(
            ["t", "m"], ["Tuple(a Int32, b String)", "Map(String, UInt8)"], [[], []], 0
        )
        assert encoded == build_native_block(
            [("t", "Tuple(a Int32, b String)", []), ("m", "Map(String, UInt8)", [])]
        )

    def test_lc_tuple_and_nullable_map_still_rejected(self):
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(
                ["v"], ["LowCardinality(Tuple(Int32, String))"], [[(1, "a")]], 1
            )
        with pytest.raises(NotImplementedError, match="unsupported"):
            _ch_core.encode_native_block(["v"], ["Nullable(Map(String, UInt8))"], [[{}]], 1)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [(shared, "a"), (shared, "b")]
        before = sys.getrefcount(shared)
        self._encode("Tuple(Int64, String)", rows)
        assert sys.getrefcount(shared) == before

        rows = [(shared, "a"), (shared, 5)]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="element 1"):
            self._encode("Tuple(Int64, String)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [(shared, "a"), (shared,)]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="declares 2"):
            self._encode("Tuple(Int64, String)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [{"a": shared, "b": "x"}, {"a": shared}]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match='element "b" is None'):
            self._encode("Tuple(a Int64, b String)", rows, 2)
        assert sys.getrefcount(shared) == before


class TestMapInsert:
    """Map(K, V) encode: dict rows flattened into key and value runs."""

    def _encode(self, type_name, rows, row_count=None):
        n = len(rows) if row_count is None else row_count
        return _ch_core.encode_native_block(["m"], [type_name], [rows], n)

    def _decode(self, encoded):
        return list(_ch_core.ColBatch.decode_native(encoded).column_data(0))

    def test_dict_rows_match_golden_and_round_trip(self):
        rows = [{"k1": 1, "k2": 2}, {}, {"x": 255}]
        encoded = self._encode("Map(String, UInt8)", rows)
        assert encoded == build_native_block([("m", "Map(String, UInt8)", rows)])
        assert self._decode(encoded) == rows

    def test_outer_container_kinds_agree(self):
        rows = [{13: "a", 79: "b"}, {}, {5: "c"}]
        expected = self._encode("Map(UInt64, String)", rows)
        assert self._encode("Map(UInt64, String)", tuple(rows), 3) == expected
        assert self._encode("Map(UInt64, String)", _NdarrayLikeColumn(rows), 3) == expected

    def test_all_empty_dict_rows(self):
        rows = [{}, {}, {}]
        encoded = self._encode("Map(String, UInt8)", rows)
        assert encoded == build_native_block([("m", "Map(String, UInt8)", rows)])
        assert self._decode(encoded) == rows

    def test_dict_subclass_rows_via_items(self):
        import collections

        rows = [collections.OrderedDict([("a", 13), ("b", 79)]), {}]
        assert self._encode("Map(String, UInt8)", rows) == self._encode(
            "Map(String, UInt8)", [{"a": 13, "b": 79}, {}]
        )

    def test_non_dict_row_rejected(self):
        with pytest.raises(ValueError, match="row 1 is not a dict for Map"):
            self._encode("Map(String, UInt8)", [{"a": 1}, [("b", 2)]], 2)
        with pytest.raises(ValueError, match="row 0 is not a dict for Map"):
            self._encode("Map(String, UInt8)", ["ab"], 1)

    def test_none_row_rejected(self):
        with pytest.raises(ValueError, match="row 1 is None but Map"):
            self._encode("Map(String, UInt8)", [{"a": 1}, None], 2)

    def test_nullable_value_type(self):
        rows = [{"a": 13, "b": None}, {}, {"c": 79}]
        encoded = self._encode("Map(String, Nullable(Int32))", rows)
        assert encoded == build_native_block([("m", "Map(String, Nullable(Int32))", rows)])
        assert self._decode(encoded) == rows

    def test_array_value_type(self):
        rows = [{"k": [13, 79], "e": []}, {}, {"s": [5]}]
        encoded = self._encode("Map(String, Array(Int64))", rows)
        assert self._decode(encoded) == rows

    def test_low_cardinality_string_key(self):
        rows = [{"red": 1, "blue": 2}, {}, {"red": 3}]
        encoded = self._encode("Map(LowCardinality(String), UInt64)", rows)
        assert self._decode(encoded) == rows

    def test_map_of_map_and_tuple_values(self):
        rows = [{1: {2: "a"}}, {}, {3: {}}]
        encoded = self._encode("Map(UInt64, Map(UInt64, String))", rows)
        assert self._decode(encoded) == rows
        rows = [{"k": (1, "x")}, {}]
        encoded = self._encode("Map(String, Tuple(UInt8, String))", rows)
        assert self._decode(encoded) == rows

    def test_key_and_value_errors_name_row_and_entry(self):
        with pytest.raises(ValueError, match='column "m" row 1 key 0 cannot be converted'):
            self._encode("Map(UInt8, UInt8)", [{1: 1}, {"x": 2}], 2)
        with pytest.raises(ValueError, match='column "m" row 0 value 1 cannot be converted'):
            self._encode("Map(String, UInt8)", [{"a": 1, "b": "x"}], 1)

    def test_flat_refs_refcount_balance(self):
        shared = 1 << 40

        rows = [{shared: shared}, {}, {1: shared}]
        before = sys.getrefcount(shared)
        self._encode("Map(UInt64, Int64)", rows)
        assert sys.getrefcount(shared) == before

        rows = [{shared: shared}, {shared: "x"}]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="value 0 cannot be converted"):
            self._encode("Map(UInt64, Int64)", rows, 2)
        assert sys.getrefcount(shared) == before

        rows = [{shared: shared}, "boom"]
        before = sys.getrefcount(shared)
        with pytest.raises(ValueError, match="is not a dict for Map"):
            self._encode("Map(UInt64, Int64)", rows, 2)
        assert sys.getrefcount(shared) == before


# ---------------------------------------------------------------------------
# Tuple
# ---------------------------------------------------------------------------

_TUP_KNOWN_UUID = uuid.UUID("00112233-4455-6677-8899-aabbccddeeff")

# (type_name, wire_rows, expected). Each wire row is a sequence of element
# values; expected None means the decoded column equals wire_rows (as tuples).
_TUPLE_UNNAMED = [
    ("Tuple(Int32, String)", [(13, "user_1"), (-1, ""), (79, "sventon")], None),
    ("Tuple(UInt64, Float64, Bool)",
     [(0, 1.5, True), (18446744073709551615, -2.5, False)], None),
    ("Tuple(Int32)", [(13,), (79,), (-2147483648,)], None),
    ("Tuple(Nullable(Int32), String)", [(13, "a"), (None, "b"), (79, "c")], None),
    ("Tuple(UUID, IPv4)",
     [(uuid.UUID(int=0), ipaddress.IPv4Address("1.2.3.4")),
      (_TUP_KNOWN_UUID, ipaddress.IPv4Address("255.255.255.255"))], None),
    ("Tuple(LowCardinality(String), Int32)",
     [("red", 1), ("red", 2), ("blue", 3)], None),
    ("Tuple(Array(Int64), String)",
     [([13, 79], "a"), ([], "b"), ([5, 5, 5], "c")], None),
    ("Tuple(UInt8, Tuple(UInt8, String))",
     [(1, (2, "x")), (3, (4, "y"))], None),
    ("Tuple(Map(String, UInt8), Int32)",
     [({"k1": 1, "k2": 2}, 10), ({}, 20)], None),
]

_TUPLE_NAMED = [
    ("Tuple(a Int32, b String)",
     [(13, "x"), (79, "y")], [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]),
    ("Tuple(id UInt64, val Nullable(Int32))",
     [(1, 13), (2, None)], [{"id": 1, "val": 13}, {"id": 2, "val": None}]),
    ("Tuple(`a b` Int32, c String)",
     [(13, "x")], [{"a b": 13, "c": "x"}]),
]


class TestTuple:
    @pytest.mark.parametrize("type_name,wire_rows,expected", _TUPLE_UNNAMED)
    def test_unnamed_to_tuple(self, type_name, wire_rows, expected):
        if expected is None:
            expected = [tuple(r) for r in wire_rows]
        built = build_native_block([("t", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, tuple) for v in decoded)

    @pytest.mark.parametrize("type_name,wire_rows,expected", _TUPLE_NAMED)
    def test_named_to_dict(self, type_name, wire_rows, expected):
        built = build_native_block([("t", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, dict) for v in decoded)

    @pytest.mark.parametrize(
        "type_name,wire_rows,expected",
        [
            ("Tuple(Int32, String)", [(13, "a"), (79, "b")], [(13, "a"), (79, "b")]),
            ("Tuple(x Int32, y String)", [(13, "a")], [{"x": 13, "y": "a"}]),
            ("Tuple(Array(Int64), Nullable(String))",
             [([13], "a"), ([], None)], [([13], "a"), ([], None)]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows, expected):
        built = build_native_block([("t", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable_tuple_null_rows_are_none(self):
        # Nullable(Tuple) decodes null rows to None and non-null rows to their
        # value, across all exit paths. A nullable field inside a non-null tuple
        # keeps its own None. (clickhouse-connect mis-decodes this rare type, so
        # the Rust path is the correct reference, not a parity target.)
        type_name = "Nullable(Tuple(Nullable(Int32), String))"
        wire_rows = [(13, "a"), None, (None, "b"), None]
        expected = [(13, "a"), None, (None, "b"), None]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("t", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_nullable_named_tuple_null_rows_are_none(self):
        type_name = "Nullable(Tuple(a UInt64, b String))"
        wire_rows = [(13, "x"), None, (5, "y")]
        expected = [{"a": 13, "b": "x"}, None, {"a": 5, "b": "y"}]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("t", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_empty_tuple_is_empty_tuple_per_row(self):
        # Tuple() has one placeholder byte per row and decodes to an empty tuple.
        built = build_native_block([("t", "Tuple()", [(), (), ()])])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [(), (), ()]

    def test_decimal_and_date_element_machinery(self):
        # Decimal/Date elements carry raw wire values in the helper (unscaled
        # integer, epoch days) and decode through the recursive per-field
        # context: the Decimal field builds its scaled decimal.Decimal.
        day = (dt.date(2024, 1, 2) - dt.date(1970, 1, 1)).days
        wire_rows = [(100, day), (-350, 0)]
        built = build_native_block([("t", "Tuple(Decimal(9, 2), Date)", wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [
            (decimal.Decimal("1.00"), dt.date(2024, 1, 2)),
            (decimal.Decimal("-3.50"), dt.date(1970, 1, 1)),
        ]

    def test_multi_block_concatenation(self):
        type_name = "Tuple(Int32, String)"
        rows_a = [(13, "a"), (79, "b")]
        rows_b = [(5, "c")]
        block_a = build_native_block([("t", type_name, rows_a)])
        block_b = build_native_block([("t", type_name, rows_b)])
        decoded = list(_ch_core.ColBatch.decode_native(block_a + block_b).column_data(0))
        assert decoded == [(13, "a"), (79, "b"), (5, "c")]

    def test_truncated_tuple_raises_eof(self):
        built = build_native_block([("t", "Tuple(Int32, Int32)", [(13, 79)])])
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(built[:-2])


# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

_MAP_DECODE = [
    ("Map(String, UInt8)", [{"k1": 1, "k2": 2}, {}, {"x": 255}], None),
    ("Map(UInt64, String)", [{13: "a", 79: "b"}, {}, {5: "c"}], None),
    ("Map(String, Nullable(Int32))", [{"a": 13, "b": None}, {}, {"c": 79}], None),
    ("Map(LowCardinality(String), UInt64)",
     [{"red": 1, "blue": 2}, {}, {"red": 3}], None),
    ("Map(UInt8, Array(Int32))", [{1: [13, 79], 2: []}, {}, {3: [5]}], None),
    ("Map(String, Tuple(UInt8, String))", [{"k": (1, "x")}, {}], None),
    ("Map(UInt64, Map(UInt64, String))", [{1: {2: "a"}}, {}, {3: {}}], None),
]


class TestMap:
    @pytest.mark.parametrize("type_name,wire_rows,expected", _MAP_DECODE)
    def test_round_trip(self, type_name, wire_rows, expected):
        if expected is None:
            expected = wire_rows
        built = build_native_block([("m", type_name, wire_rows)])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == expected
        assert all(isinstance(v, dict) for v in decoded)

    @pytest.mark.parametrize(
        "type_name,wire_rows",
        [
            ("Map(String, UInt8)", [{"a": 13, "b": 79}, {}, {"c": 5}]),
            ("Map(UInt64, Nullable(String))", [{13: "x", 79: None}, {}]),
            ("Map(String, Array(Int32))", [{"k": [13, 79]}, {}]),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows):
        built = build_native_block([("m", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == wire_rows

    def test_duplicate_keys_last_value_wins(self):
        # dict input cannot express duplicate keys, so build the entries by hand:
        # one row with keys [1, 1] and values [10, 20]. dict(zip(...)) keeps the
        # first position and the last value.
        body = struct.pack("<Q", 2) + b"\x01\x01" + b"\x0a\x14"
        block = build_native_block_from_bodies([("m", "Map(UInt8, UInt8)", body)], 1)
        decoded = list(_ch_core.ColBatch.decode_native(block).column_data(0))
        assert decoded == [{1: 20}]

    def test_decimal_value_machinery(self):
        # A Decimal value carries a raw unscaled integer in the helper and
        # decodes through the value field's context into a scaled decimal.Decimal.
        built = build_native_block(
            [("m", "Map(String, Decimal(9, 2))", [{"a": 100, "b": -350}, {}])]
        )
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [{"a": decimal.Decimal("1.00"), "b": decimal.Decimal("-3.50")}, {}]

    def test_empty_maps(self):
        built = build_native_block([("m", "Map(String, UInt8)", [{}, {}, {}])])
        decoded = list(_ch_core.ColBatch.decode_native(built).column_data(0))
        assert decoded == [{}, {}, {}]

    def test_multi_block_concatenation(self):
        type_name = "Map(String, UInt8)"
        rows_a = [{"a": 13}, {}]
        rows_b = [{"b": 79, "c": 5}]
        block_a = build_native_block([("m", type_name, rows_a)])
        block_b = build_native_block([("m", type_name, rows_b)])
        decoded = list(_ch_core.ColBatch.decode_native(block_a + block_b).column_data(0))
        assert decoded == [{"a": 13}, {}, {"b": 79, "c": 5}]

    def test_non_monotonic_offsets_rejected(self):
        # A Map's offsets share the Array offset validation: a decreasing run is
        # rejected by the core decoder.
        body = struct.pack("<Q", 2) + struct.pack("<Q", 1) + b"\x01\x01" + b"\x0a\x14"
        block = build_native_block_from_bodies([("m", "Map(UInt8, UInt8)", body)], 2)
        with pytest.raises(ValueError, match="Invalid Array layout"):
            _ch_core.ColBatch.decode_native(block)


# ---------------------------------------------------------------------------
# Geo aliases (Point/Ring/LineString/Polygon/MultiLineString/MultiPolygon)
# ---------------------------------------------------------------------------

# A Point decodes to an (x, y) tuple; each array-based kind wraps it in one more
# list level. Rows round-trip unchanged (tuples in, tuples out).
_GEO_DECODE = [
    ("Point", [(1.5, 2.5), (-3.25, 4.0), (0.0, 0.0)]),
    ("Ring", [[(1.0, 2.0), (3.0, 4.0)], [], [(5.5, 6.5)]]),
    ("LineString", [[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)], [], [(7.5, 8.5)]]),
    ("Polygon", [[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)], [(0.25, 0.25)]], []]),
    ("MultiLineString", [[[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0)]], []]),
    ("MultiPolygon", [[[[(0.0, 0.0), (1.0, 0.0)]], [[(2.0, 2.0)], [(3.0, 3.0)]]], []]),
]


class TestGeo:
    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_decode_shape_and_type_name(self, type_name, rows):
        batch = _ch_core.ColBatch.decode_native(build_native_block([("g", type_name, rows)]))
        assert batch.column_type_names == [type_name]
        # `==` distinguishes tuple from list, so equality enforces the Point ->
        # tuple, array-level -> list nesting exactly.
        assert list(batch.column_data(0)) == rows

    def test_point_leaves_are_tuples(self):
        decoded = list(
            _ch_core.ColBatch.decode_native(
                build_native_block([("g", "Point", [(1.5, 2.5), (-3.25, 4.0)])])
            ).column_data(0)
        )
        assert all(isinstance(v, tuple) and len(v) == 2 for v in decoded)

    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_encode_round_trip_and_golden(self, type_name, rows):
        encoded = _ch_core.encode_native_block(["g"], [type_name], [rows], len(rows))
        assert encoded == build_native_block([("g", type_name, rows)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == rows

    @pytest.mark.parametrize("type_name,rows", _GEO_DECODE)
    def test_all_exit_paths_agree(self, type_name, rows):
        batch = _ch_core.ColBatch.decode_native(build_native_block([("g", type_name, rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_nullable_point_null_rows_are_none(self):
        # Nullable(Point) parses as Nullable(Geo(Point)); the delegate is a Tuple,
        # so the decoded column carries tuple-level validity and null rows read
        # as None across all exit paths.
        rows = [(1.5, 2.5), None, (-3.25, 4.0), None]
        batch = _ch_core.ColBatch.decode_native(
            build_native_block([("g", "Nullable(Point)", rows)])
        )
        assert batch.column_type_names == ["Nullable(Point)"]
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == rows

    def test_nullable_point_encode_round_trip(self):
        rows = [(1.5, 2.5), None, (0.0, -1.0)]
        encoded = _ch_core.encode_native_block(["g"], ["Nullable(Point)"], [rows], len(rows))
        assert encoded == build_native_block([("g", "Nullable(Point)", rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows


# ---------------------------------------------------------------------------
# Nested (physical Array(Tuple(named ...)))
# ---------------------------------------------------------------------------

class TestNested:
    def test_decode_to_list_of_dicts(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        expected = [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("n", type_name, wire_rows)]))
        assert batch.column_type_names == [type_name]
        decoded = list(batch.column_data(0))
        assert decoded == expected
        assert all(isinstance(item, dict) for row in decoded for item in row)

    def test_all_exit_paths_agree(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north")], [], [(5, "east"), (7, "west")]]
        expected = [[{"a": 13, "b": "north"}], [], [{"a": 5, "b": "east"}, {"a": 7, "b": "west"}]]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("n", type_name, wire_rows)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == expected

    def test_encode_positional_golden_and_round_trip(self):
        type_name = "Nested(a UInt32, b String)"
        wire_rows = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [wire_rows], len(wire_rows))
        assert encoded == build_native_block([("n", type_name, wire_rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]

    def test_encode_dict_rows_match_positional(self):
        type_name = "Nested(a UInt32, b String)"
        positional = [[(13, "north"), (79, "south")], [], [(5, "east")]]
        dict_rows = [
            [{"a": 13, "b": "north"}, {"a": 79, "b": "south"}],
            [],
            [{"a": 5, "b": "east"}],
        ]
        expected = _ch_core.encode_native_block(["n"], [type_name], [positional], 3)
        assert _ch_core.encode_native_block(["n"], [type_name], [dict_rows], 3) == expected

    def test_nested_low_cardinality_field_round_trip(self):
        # A LowCardinality Nested field resolves through the LC path; the core's
        # LC index word differs from the helper's, so this round-trips instead of
        # golden-comparing.
        type_name = "Nested(city LowCardinality(String), pop UInt32)"
        dict_rows = [
            [{"city": "harbor", "pop": 13}, {"city": "harbor", "pop": 79}],
            [],
            [{"city": "ridge", "pop": 5}],
        ]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [dict_rows], 3)
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == dict_rows

    def test_nested_point_field_golden_and_round_trip(self):
        # A geo alias (Point) nested inside a non-alias container: Nested expands
        # to Array(Tuple(p Point, q UInt32)) and the Point field expands again.
        type_name = "Nested(p Point, q UInt32)"
        wire_rows = [[((1.5, 2.5), 13), ((3.0, 4.0), 79)], [], [((5.5, 6.5), 5)]]
        encoded = _ch_core.encode_native_block(["n"], [type_name], [wire_rows], len(wire_rows))
        assert encoded == build_native_block([("n", type_name, wire_rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == [
            [{"p": (1.5, 2.5), "q": 13}, {"p": (3.0, 4.0), "q": 79}],
            [],
            [{"p": (5.5, 6.5), "q": 5}],
        ]


# ---------------------------------------------------------------------------
# SimpleAggregateFunction (physical == the inner type)
# ---------------------------------------------------------------------------

class TestSimpleAggregateFunction:
    def test_sum_uint64_golden_and_round_trip(self):
        type_name = "SimpleAggregateFunction(sum, UInt64)"
        values = [13, 79, 0, 18446744073709551615]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        assert encoded == build_native_block([("s", type_name, values)])
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        decoded = list(batch.column_data(0))
        assert decoded == values
        assert all(isinstance(v, int) for v in decoded)

    def test_any_string_golden_and_round_trip(self):
        type_name = "SimpleAggregateFunction(any, String)"
        values = ["red", "", "green"]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        assert encoded == build_native_block([("s", type_name, values)])
        decoded = list(_ch_core.ColBatch.decode_native(encoded).column_data(0))
        assert decoded == values
        assert all(isinstance(v, str) for v in decoded)

    def test_anylast_low_cardinality_string_round_trip(self):
        # The inner LowCardinality resolves through the LC path; the core's LC
        # index word differs from the helper's, so this round-trips.
        type_name = "SimpleAggregateFunction(anyLast, LowCardinality(String))"
        values = ["red", "blue", "red", "red", "green"]
        encoded = _ch_core.encode_native_block(["s"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values

    def test_all_exit_paths_agree(self):
        type_name = "SimpleAggregateFunction(sum, UInt64)"
        values = [13, 79, 5]
        batch = _ch_core.ColBatch.decode_native(build_native_block([("s", type_name, values)]))
        via_column_data = list(batch.column_data(0))
        via_columns = list(batch.to_python_columns()[0])
        via_rows = [r[0] for r in batch.to_python_rows()]
        assert via_column_data == via_columns == via_rows == values

    def test_array_of_saf_golden_and_round_trip(self):
        # A SAF alias nested inside a non-alias Array container.
        type_name = "Array(SimpleAggregateFunction(sum, UInt64))"
        rows = [[13, 79], [], [5, 5, 18446744073709551615]]
        encoded = _ch_core.encode_native_block(["a"], [type_name], [rows], len(rows))
        assert encoded == build_native_block([("a", type_name, rows)])
        assert list(_ch_core.ColBatch.decode_native(encoded).column_data(0)) == rows

    def test_low_cardinality_string_round_trip(self):
        # LowCardinality(SAF(anyLast, String)): the SAF chain resolves through the
        # LC path. Server round-trips this live; the python codec cannot. The
        # core's LC index word differs from the helper's, so no golden compare.
        type_name = "LowCardinality(SimpleAggregateFunction(anyLast, String))"
        values = ["red", "blue", "red", "red", "green"]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        built = build_native_block([("v", type_name, values)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == values

    def test_low_cardinality_nullable_string_round_trip(self):
        # LowCardinality(SAF(anyLast, Nullable(String))) is index-level nullable:
        # the SAF chain is stripped before the Nullable is detected.
        type_name = "LowCardinality(SimpleAggregateFunction(anyLast, Nullable(String)))"
        values = ["red", None, "red", None, "blue"]
        encoded = _ch_core.encode_native_block(["v"], [type_name], [values], len(values))
        batch = _ch_core.ColBatch.decode_native(encoded)
        assert batch.column_type_names == [type_name]
        assert list(batch.column_data(0)) == values
        built = build_native_block([("v", type_name, values)])
        assert list(_ch_core.ColBatch.decode_native(built).column_data(0)) == values


class TestAliasRejections:
    @pytest.mark.parametrize(
        "type_name",
        ["Nullable(Ring)", "Nullable(Nested(a UInt32, b String))"],
    )
    def test_server_illegal_nullable_alias_rejected(self, type_name):
        # Ring and Nested expand to an Array, which is not nullable-able, so the
        # core parser rejects the type and encode raises at the header stage.
        with pytest.raises(NotImplementedError, match="unsupported ClickHouse type"):
            _ch_core.encode_native_block(["x"], [type_name], [[None]], 1)


# ---------------------------------------------------------------------------
# Tuple/Map column-major fill
# ---------------------------------------------------------------------------

class TestTupleMapFill:
    """The buffered exits decode top-level Tuple/Map columns with a hoisted
    column-major fill; Tuple/Map nested inside Array stay on the per-cell path."""

    @staticmethod
    def _all_exits(batch):
        return (
            list(batch.column_data(0)),
            list(batch.to_python_columns()[0]),
            [r[0] for r in batch.to_python_rows()],
        )

    @pytest.mark.parametrize(
        "type_name,wire_rows,expected",
        [
            ("Tuple(LowCardinality(String), Int64)",
             [("x", 1), ("y", 2), ("x", 3)], [("x", 1), ("y", 2), ("x", 3)]),
            ("Tuple(a LowCardinality(String), b Int64)",
             [("x", 1), ("y", 2)], [{"a": "x", "b": 1}, {"a": "y", "b": 2}]),
            ("Map(String, Int64)", [{"a": 1, "b": 2}, {}, {"c": 3}], None),
            ("Map(String, Array(Int64))", [{"k": [1, 2], "e": []}, {}, {"z": [3]}], None),
            ("Nullable(Tuple(Int64, String))", [(1, "a"), None, (2, "b")], None),
            ("Tuple()", [(), (), ()], None),
            ("Map(String, UInt8)", [{}, {}, {}], None),
        ],
    )
    def test_all_exit_paths_agree(self, type_name, wire_rows, expected):
        if expected is None:
            expected = wire_rows
        built = build_native_block([("c", type_name, wire_rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        via_column_data, via_columns, via_rows = self._all_exits(batch)
        assert via_column_data == via_columns == via_rows == expected

    def test_tuple_lc_field_identity_within_chunk(self):
        rows = [("red", 1), ("blue", 2), ("red", 3), ("red", 4)]
        built = build_native_block([("t", "Tuple(LowCardinality(String), Int64)", rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0][0] is col[2][0]
        assert col[0][0] is col[3][0]
        cols = batch.to_python_columns()[0]
        assert cols[0][0] is cols[2][0]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0][0] is out_rows[2][0][0]

    def test_map_lc_value_identity_within_chunk(self):
        rows = [{"a": "red", "b": "blue"}, {}, {"c": "red"}]
        built = build_native_block([("m", "Map(String, LowCardinality(String))", rows)])
        batch = _ch_core.ColBatch.decode_native(built)
        col = batch.column_data(0)
        assert list(col) == rows
        assert col[0]["a"] is col[2]["c"]
        cols = batch.to_python_columns()[0]
        assert cols[0]["a"] is cols[2]["c"]
        out_rows = batch.to_python_rows()
        assert out_rows[0][0]["a"] is out_rows[2][0]["c"]

    def test_map_lc_key_identity_within_chunk(self):
        rows = [{"red": 1, "blue": 2}, {}, {"red": 3}]
        built = build_native_block([("m", "Map(LowCardinality(String), UInt64)", rows)])
        col = _ch_core.ColBatch.decode_native(built).column_data(0)
        assert list(col) == rows
        key0 = next(k for k in col[0] if k == "red")
        assert key0 is next(iter(col[2]))

    def test_nullable_tuple_lc_field_null_rows_and_identity(self):
        # Null rows discard their field values; valid rows still share the
        # dictionary object.
        rows = [("x", 1), None, ("x", 2), ("y", 3)]
        enc = _ch_core.encode_native_block(
            ["c"], ["Nullable(Tuple(LowCardinality(String), Int64))"], [rows], len(rows)
        )
        batch = _ch_core.ColBatch.decode_native(enc)
        via_column_data, via_columns, via_rows = self._all_exits(batch)
        assert via_column_data == via_columns == via_rows == rows
        col = batch.column_data(0)
        assert col[0][0] is col[2][0]

    def test_fill_path_matches_per_cell_array_element_path(self):
        # The same values decode through the column-major fill at top level and
        # through the per-cell path one Array level down; they must agree.
        tup_rows = [(1, "a"), (2, "b"), (3, "c")]
        map_rows = [{"a": 1, "b": 2}, {}, {"c": 3}]
        top = _ch_core.ColBatch.decode_native(build_native_block([
            ("t", "Tuple(Int64, String)", tup_rows),
            ("m", "Map(String, Int64)", map_rows),
        ]))
        wrapped = _ch_core.ColBatch.decode_native(build_native_block([
            ("t", "Array(Tuple(Int64, String))", [[r] for r in tup_rows]),
            ("m", "Array(Map(String, Int64))", [[r] for r in map_rows]),
        ]))
        assert [[v] for v in top.column_data(0)] == list(wrapped.column_data(0))
        assert [[v] for v in top.column_data(1)] == list(wrapped.column_data(1))

    def test_multi_chunk_named_tuple_and_map(self):
        tn = "Tuple(a Int64, b String)"
        mp = "Map(String, Int64)"
        block_a = build_native_block([("t", tn, [(1, "x")]), ("m", mp, [{"a": 1}])])
        block_b = build_native_block(
            [("t", tn, [(2, "y"), (3, "z")]), ("m", mp, [{}, {"b": 2}])]
        )
        batch = _ch_core.ColBatch.decode_native(block_a + block_b)
        assert list(batch.column_data(0)) == [
            {"a": 1, "b": "x"}, {"a": 2, "b": "y"}, {"a": 3, "b": "z"}
        ]
        assert list(batch.column_data(1)) == [{"a": 1}, {}, {"b": 2}]
        assert list(batch.to_python_rows()) == [
            ({"a": 1, "b": "x"}, {"a": 1}),
            ({"a": 2, "b": "y"}, {}),
            ({"a": 3, "b": "z"}, {"b": 2}),
        ]


# ---------------------------------------------------------------------------
# Multi-column
# ---------------------------------------------------------------------------

class TestMultiColumn:
    def test_mixed_types(self):
        data = build_native_block([
            ("i", "Int32", [1, 2]),
            ("f", "Float64", [1.1, 2.2]),
            ("s", "String", ["a", "bb"]),
            ("b", "Bool", [1, 0]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 2
        assert batch.num_columns == 4
        assert batch.column_names == ["i", "f", "s", "b"]
        assert list(batch.column_data(0)) == [1, 2]
        assert list(batch.column_data(2)) == ["a", "bb"]
        assert list(batch.column_data(3)) == [True, False]


# ---------------------------------------------------------------------------
# Python access
# ---------------------------------------------------------------------------

class TestPythonAccess:
    def test_to_python_rows(self):
        data = build_native_block([
            ("a", "Int32", [10, 20]),
            ("b", "String", ["x", "y"]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        rows = list(batch.to_python_rows())
        assert rows == [(10, "x"), (20, "y")]

    def test_to_python_columns(self):
        data = build_native_block([
            ("a", "Int64", [1, 2, 3]),
            ("b", "Float64", [1.0, 2.0, 3.0]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        cols = list(batch.to_python_columns())
        assert list(cols[0]) == [1, 2, 3]

    def test_column_data_out_of_range(self):
        data = build_native_block([("a", "Int64", [1])])
        batch = _ch_core.ColBatch.decode_native(data)
        with pytest.raises(ValueError, match="out of range"):
            batch.column_data(5)


# ---------------------------------------------------------------------------
# Path equivalence
# ---------------------------------------------------------------------------

class TestPathEquivalence:
    _COLUMNS = [
        ("b", "Bool", [1, 0, 1]),
        ("i8", "Int8", [-5, 0, 127]),
        ("i16", "Int16", [-300, 0, 32767]),
        ("i32", "Int32", [-70000, 0, 70000]),
        ("i64", "Int64", [-1, 0, 2**62]),
        ("u8", "UInt8", [0, 128, 255]),
        ("u16", "UInt16", [0, 13, 65535]),
        ("u32", "UInt32", [0, 79, 4_000_000_000]),
        ("u64", "UInt64", [0, 1, 2**64 - 1]),
        ("f32", "Float32", [1.5, -2.25, 0.0]),
        ("f64", "Float64", [1.5, -0.1, 1e300]),
        ("s", "String", ["u1", "", b"\xff"]),
        ("fs", "FixedString(2)", [b"ab", b"cd", b"ef"]),
        ("d", "Date", [0, 100, 19737]),
        ("d32", "Date32", [-25567, 0, 19737]),
        ("dt", "DateTime", [0, 961056000, 1705322096]),
        ("dtz", "DateTime('America/New_York')", [0, 961056000, 1705322096]),
        ("ts", "DateTime64(3)", [0, 13, 1705322096789]),
        ("tsz", "DateTime64(6, 'Asia/Istanbul')", [0, 79, 1705322096789012]),
        ("nb", "Nullable(Bool)", [1, None, 0]),
        ("ni", "Nullable(Int64)", [13, None, 79]),
        ("nf", "Nullable(Float64)", [1.5, None, -2.5]),
        ("ns", "Nullable(String)", ["x", None, b"\x80"]),
        ("nts", "Nullable(DateTime64(3))", [1705322096789, None, 0]),
    ]

    def _assert_paths_agree(self, batch):
        via_columns = [list(c) for c in batch.to_python_columns()]
        via_column_data = [list(batch.column_data(i)) for i in range(batch.num_columns)]
        via_rows = [list(col) for col in zip(*batch.to_python_rows())]
        assert via_columns == via_column_data
        assert via_columns == via_rows

    def test_all_types_agree_across_paths(self):
        data = build_native_block(self._COLUMNS)
        self._assert_paths_agree(_ch_core.ColBatch.decode_native(data))

    def test_mid_column_error_raises_on_all_paths(self):
        # A timestamp beyond datetime.MAXYEAR fails partway through the
        # column; every path must surface a clean ValueError, not crash or
        # return a partial result.
        data = build_native_block([("ts", "DateTime64(0)", [0, 300_000_000_000])])
        batch = _ch_core.ColBatch.decode_native(data)
        with pytest.raises(ValueError):
            batch.column_data(0)
        with pytest.raises(ValueError):
            batch.to_python_columns()
        with pytest.raises(ValueError):
            batch.to_python_rows()

    def test_paths_agree_across_chunks(self):
        # Two Native blocks in one buffer decode as two chunks; every path
        # must concatenate them identically.
        first = build_native_block(self._COLUMNS)
        second = build_native_block(
            [(name, type_name, values[::-1]) for name, type_name, values in self._COLUMNS]
        )
        batch = _ch_core.ColBatch.decode_native(first + second)
        assert batch.num_chunks == 2
        assert batch.num_rows == 6
        self._assert_paths_agree(batch)


# ---------------------------------------------------------------------------
# Arrow export
# ---------------------------------------------------------------------------

class TestArrowExport:
    def test_arrow_c_stream(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("i", "Int32", [10, 20, 30]),
            ("f", "Float64", [1.5, 2.5, 3.5]),
            ("s", "String", ["a", "b", "c"]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()

        assert result.num_rows == 3
        assert result.schema.field("i").type == pa.int32()
        assert result.schema.field("f").type == pa.float64()
        assert result.schema.field("s").type == pa.utf8()
        assert result.column("i").to_pylist() == [10, 20, 30]

    def test_arrow_bool(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("b", "Bool", [1, 0, 1])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("b").type == pa.bool_()
        assert result.column("b").to_pylist() == [True, False, True]

    def test_arrow_int_widths(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([
            ("a", "Int8", [1]),
            ("b", "Int16", [2]),
            ("c", "UInt32", [3]),
            ("d", "UInt64", [4]),
        ])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("a").type == pa.int8()
        assert result.schema.field("b").type == pa.int16()
        assert result.schema.field("c").type == pa.uint32()
        assert result.schema.field("d").type == pa.uint64()

    def test_arrow_float32(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("f", "Float32", [1.5])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("f").type == pa.float32()

    def test_arrow_fixed_binary(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("fs", "FixedString(3)", [b"abc", b"xyz"])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        assert result.schema.field("fs").type == pa.binary(3)
        assert result.column("fs").to_pylist() == [b"abc", b"xyz"]

    def test_arrow_nullable(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("n", "Nullable(Int32)", [1, None, 3])])
        batch = _ch_core.ColBatch.decode_native(data)
        reader = pa.RecordBatchReader.from_stream(batch)
        result = reader.read_all()
        col = result.column("n")
        assert col.null_count == 1
        assert col.to_pylist() == [1, None, 3]

    def test_arrow_tuple(self):
        # Tuple exports as an Arrow struct; a named tuple keeps its element names,
        # an unnamed one uses the core's positional field names.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("t", "Tuple(a Int32, b String)", [(13, "x"), (79, "y")])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert pa.types.is_struct(result.schema.field("t").type)
        assert result.column("t").to_pylist() == [{"a": 13, "b": "x"}, {"a": 79, "b": "y"}]

    def test_arrow_map(self):
        # Map exports as an Arrow large_list of a key/value struct.
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("m", "Map(String, UInt8)", [{"k1": 1, "k2": 2}, {}])])
        batch = _ch_core.ColBatch.decode_native(data)
        result = pa.RecordBatchReader.from_stream(batch).read_all()
        assert result.column("m").to_pylist() == [
            [{"key": "k1", "value": 1}, {"key": "k2", "value": 2}],
            [],
        ]


# ---------------------------------------------------------------------------
# PipeDecoder fd ownership
# ---------------------------------------------------------------------------

@pytest.mark.skipif(sys.platform == "win32", reason="PipeDecoder is unix-only")
class TestPipeDecoderFd:
    def test_invalid_fd_raises(self):
        with pytest.raises(OSError):
            _ch_core.PipeDecoder(999999)

    def test_negative_fd_raises(self):
        with pytest.raises(OSError):
            _ch_core.PipeDecoder(-1)

    def test_caller_closing_fd_is_safe(self):
        # The decoder reads from its own duplicate, so the caller closing both
        # pipe ends must not abort the process when the decoder is dropped.
        read_fd, write_fd = os.pipe()
        decoder = _ch_core.PipeDecoder(read_fd)
        os.close(write_fd)
        os.close(read_fd)
        assert list(decoder) == []
        del decoder

    def test_streams_blocks_from_pipe(self):
        read_fd, write_fd = os.pipe()
        os.write(write_fd, build_native_block([("v", "Int64", [13, 79])]))
        os.close(write_fd)
        decoder = _ch_core.PipeDecoder(read_fd)
        batches = [list(b.column_data(0)) for b in decoder]
        os.close(read_fd)
        assert batches == [[13, 79]]


# ---------------------------------------------------------------------------
# Arrow capsule lifecycle
# ---------------------------------------------------------------------------

class TestArrowCapsuleLifecycle:
    def test_unconsumed_capsule_does_not_leak(self):
        # Run in a subprocess so ru_maxrss reflects only this workload and not
        # the pytest process high-water mark.
        script = textwrap.dedent("""
            import resource
            import sys

            import _ch_core

            data = sys.stdin.buffer.read()

            def rss_kb():
                usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                # ru_maxrss is bytes on macOS, kilobytes on Linux.
                return usage // 1024 if sys.platform == "darwin" else usage

            for _ in range(1000):
                _ch_core.ColBatch.decode_native(data).__arrow_c_stream__()
            base = rss_kb()
            for _ in range(50000):
                _ch_core.ColBatch.decode_native(data).__arrow_c_stream__()
            print(rss_kb() - base)
        """)
        data = build_native_block([("v", "Int64", [13, 79, 5])])
        result = subprocess.run(
            [sys.executable, "-c", script],
            input=data,
            capture_output=True,
            check=True,
        )
        delta_kb = int(result.stdout)
        # The leak this guards against grew tens of MB over this loop.
        assert delta_kb < 16 * 1024

    def test_consumed_capsule_no_double_free(self):
        pa = pytest.importorskip("pyarrow")
        data = build_native_block([("v", "Int64", [13, 79])])
        for _ in range(10000):
            table = pa.RecordBatchReader.from_stream(
                _ch_core.ColBatch.decode_native(data)
            ).read_all()
            assert table.num_rows == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestUnsupportedType:
    def test_raises_value_error(self):
        # JSON is not a supported column type, so it surfaces as a clean
        # UnsupportedType -> ValueError. (UUID, IPv4/IPv6, Enum, Array, Tuple,
        # and Map are decoded by the core now, so they no longer exercise this
        # path.)
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint_string("id"))
        buf.extend(_encode_varint_string("JSON"))
        with pytest.raises(ValueError, match="Unsupported ClickHouse type 'JSON'"):
            _ch_core.ColBatch.decode_native(bytes(buf))


class TestBufferInputs:
    def test_decode_native_accepts_buffer_types(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        for view in (data, bytearray(data), memoryview(data)):
            batch = _ch_core.ColBatch.decode_native(view)
            assert list(batch.column_data(0)) == [13, 79]

    def test_feed_accepts_buffer_types(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        for view in (data, bytearray(data), memoryview(data)):
            decoder = _ch_core.StreamDecoder()
            batches = list(decoder.feed(view)) + list(decoder.finish())
            assert [list(b.column_data(0)) for b in batches] == [[13, 79]]


class TestMismatchedBlocks:
    def test_mismatched_second_block_rejected_at_decode(self):
        # Every block of a result shares one schema; the core rejects a
        # mismatched later block at decode time. The binding also guards its
        # raw list and tuple fills as defense in depth.
        wide = build_native_block([("a", "Int64", [13]), ("b", "Int64", [79])])
        narrow = build_native_block([("a", "Int64", [5])])
        with pytest.raises(ValueError, match="schema differs"):
            _ch_core.ColBatch.decode_native(wide + narrow)

    def test_mismatched_type_rejected_at_decode(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        with pytest.raises(ValueError, match="schema differs"):
            _ch_core.ColBatch.decode_native(first + second)

    def test_stream_decoder_rejects_mismatch(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        decoder = _ch_core.StreamDecoder()
        assert len(decoder.feed(first)) == 1
        with pytest.raises(ValueError, match="schema differs"):
            decoder.feed(second)

    def test_block_decoder_rejects_mismatch(self):
        first = build_native_block([("a", "Int64", [13])])
        second = build_native_block([("a", "String", ["u1"])])
        decoder = _ch_core.BlockDecoder(first + second)
        batch = next(decoder)
        assert list(batch.column_data(0)) == [13]
        with pytest.raises(ValueError, match="schema differs"):
            next(decoder)

    @pytest.mark.skipif(sys.platform == "win32", reason="PipeDecoder is unix-only")
    def test_pipe_decoder_rejects_mismatch(self):
        read_fd, write_fd = os.pipe()
        os.write(write_fd, build_native_block([("a", "Int64", [13])]))
        os.write(write_fd, build_native_block([("a", "String", ["u1"])]))
        os.close(write_fd)
        decoder = _ch_core.PipeDecoder(read_fd)
        batches = []
        with pytest.raises(ValueError, match="schema differs"):
            for batch in decoder:
                batches.append(list(batch.column_data(0)))
        os.close(read_fd)
        # Whether the first good block is yielded depends on how the pipe
        # reads chunk the feed; only the rejection is guaranteed.
        assert batches in ([], [[13]])


class TestTruncatedData:
    def test_decode_native_truncated_raises_eof(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        with pytest.raises(EOFError):
            _ch_core.ColBatch.decode_native(data[:-3])

    def test_stream_decoder_truncated_finish_raises_eof(self):
        data = build_native_block([("v", "Int64", [13, 79])])
        decoder = _ch_core.StreamDecoder()
        assert list(decoder.feed(data[:-3])) == []
        with pytest.raises(EOFError):
            decoder.finish()


class TestEmptyBatch:
    def test_zero_rows(self):
        data = build_native_block([("a", "Int32", []), ("b", "String", [])])
        batch = _ch_core.ColBatch.decode_native(data)
        assert batch.num_rows == 0
        assert batch.num_columns == 2
        assert list(batch.to_python_rows()) == []


class TestFromBatches:
    _COLUMNS_A = [("id", "Int64", [13, 79]), ("name", "String", ["user_1", "user_2"])]
    _COLUMNS_B = [("id", "Int64", [5]), ("name", "String", ["user_3"])]

    def test_merge_matches_decode_native(self):
        first = build_native_block(self._COLUMNS_A)
        second = build_native_block(self._COLUMNS_B)
        parts = list(_ch_core.BlockDecoder(first)) + list(_ch_core.BlockDecoder(second))
        merged = _ch_core.ColBatch.from_batches(parts)
        reference = _ch_core.ColBatch.decode_native(first + second)
        assert merged.num_rows == 3
        assert merged.num_chunks == 2
        assert list(merged.to_python_rows()) == list(reference.to_python_rows())
        pa = pytest.importorskip("pyarrow")
        merged_table = pa.RecordBatchReader.from_stream(merged).read_all()
        reference_table = pa.RecordBatchReader.from_stream(reference).read_all()
        assert merged_table == reference_table

    def test_mismatched_schema_raises(self):
        first = _ch_core.ColBatch.decode_native(
            build_native_block([("id", "Int64", [13])])
        )
        wrong_type = _ch_core.ColBatch.decode_native(
            build_native_block([("id", "String", ["user_1"])])
        )
        wrong_name = _ch_core.ColBatch.decode_native(
            build_native_block([("other", "Int64", [79])])
        )
        with pytest.raises(ValueError, match="Batch 1 schema differs"):
            _ch_core.ColBatch.from_batches([first, wrong_type])
        with pytest.raises(ValueError, match="Batch 2 schema differs"):
            _ch_core.ColBatch.from_batches([first, first, wrong_name])

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="at least one batch"):
            _ch_core.ColBatch.from_batches([])

    def test_zero_row_trailer_dropped(self):
        rows = _ch_core.ColBatch.decode_native(build_native_block(self._COLUMNS_A))
        # BlockDecoder keeps a zero-row block as a chunk, so the trailer batch
        # carries one zero-row chunk into the merge.
        (trailer,) = _ch_core.BlockDecoder(
            build_native_block([("id", "Int64", []), ("name", "String", [])])
        )
        assert trailer.num_chunks == 1
        merged = _ch_core.ColBatch.from_batches([rows, trailer])
        assert merged.num_rows == 2
        assert merged.num_chunks == 1
        assert list(merged.to_python_rows()) == [(13, "user_1"), (79, "user_2")]

    def test_all_zero_rows(self):
        empty_block = build_native_block([("id", "Int64", []), ("name", "String", [])])
        reference = _ch_core.ColBatch.decode_native(empty_block)
        # Each part holds one zero-row chunk; the merge must drop them all.
        (part,) = _ch_core.BlockDecoder(empty_block)
        merged = _ch_core.ColBatch.from_batches([part, part])
        assert merged.num_rows == 0
        assert merged.num_chunks == 0
        assert merged.num_chunks == reference.num_chunks
        assert list(merged.to_python_rows()) == list(reference.to_python_rows()) == []
        merged_cols = [list(c) for c in merged.to_python_columns()]
        reference_cols = [list(c) for c in reference.to_python_columns()]
        assert merged_cols == reference_cols == [[], []]
        pa = pytest.importorskip("pyarrow")
        table = pa.RecordBatchReader.from_stream(merged).read_all()
        assert table.schema.names == ["id", "name"]
        assert table.schema == pa.RecordBatchReader.from_stream(reference).read_all().schema
        assert table.num_rows == 0


class TestMaterializeFastPath:
    """Edge values and multi-chunk slot accounting for the typed fill loops."""

    _EDGE_COLUMNS = [
        ("i8", "Int8", [-128, -1, 127]),
        ("i64", "Int64", [-(2**63), 0, 2**63 - 1]),
        ("u32", "UInt32", [0, 2**31, 2**32 - 1]),
        ("u64", "UInt64", [0, 2**63, 2**64 - 1]),
        ("f64", "Float64", [-0.5, 0.0, 1.5]),
        ("b", "Bool", [True, False, True]),
        ("n", "Nullable(Int64)", [-(2**63), None, 2**63 - 1]),
    ]

    def test_edge_values_rows_columns_and_column_data(self):
        batch = _ch_core.ColBatch.decode_native(build_native_block(self._EDGE_COLUMNS))
        expected_cols = [values for _, _, values in self._EDGE_COLUMNS]
        assert [list(c) for c in batch.to_python_columns()] == expected_cols
        assert list(batch.to_python_rows()) == list(zip(*expected_cols))
        for idx, col in enumerate(expected_cols):
            assert list(batch.column_data(idx)) == col

    def test_multi_chunk_mixed_fast_and_fallback_columns(self):
        block = build_native_block(
            [
                ("v", "Int64", [-(2**63), 2**63 - 1]),
                ("s", "String", ["user_1", "user_2"]),
                ("u", "UInt32", [2**32 - 1, 7]),
            ]
        )
        # The same batch three times stresses the per-chunk row-offset
        # bookkeeping with duplicate Arc chunks.
        (part,) = _ch_core.BlockDecoder(block)
        merged = _ch_core.ColBatch.from_batches([part, part, part])
        assert merged.num_chunks == 3
        expected_rows = [(-(2**63), "user_1", 2**32 - 1), (2**63 - 1, "user_2", 7)] * 3
        assert list(merged.to_python_rows()) == expected_rows
        assert [list(c) for c in merged.to_python_columns()] == [
            [-(2**63), 2**63 - 1] * 3,
            ["user_1", "user_2"] * 3,
            [2**32 - 1, 7] * 3,
        ]


class TestBlockInfo:
    def test_with_block_info(self):
        data = build_native_block(
            [("v", "Int64", [77, 88])],
            block_info=True,
        )
        batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
        assert batch.num_rows == 2
        assert list(batch.column_data(0)) == [77, 88]
