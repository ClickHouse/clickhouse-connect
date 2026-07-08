"""Tests for _ch_core Python bindings - Phase 1 types."""

import datetime as dt
import decimal
import ipaddress
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


def _temporal_struct_fmt(inner_type: str):
    """Wire struct format for a temporal type, or None if not temporal.

    Temporal columns are plain bulk integers on the wire; timezone and precision
    are type-name metadata only. Values are passed as raw epoch units (Date/Date32
    days, DateTime seconds, DateTime64 ticks).
    """
    if inner_type == "Date":
        return "<H"  # u16 days
    if inner_type == "Date32":
        return "<i"  # i32 days
    if inner_type == "DateTime" or inner_type.startswith("DateTime("):
        return "<I"  # u32 seconds
    if inner_type.startswith("DateTime64("):
        return "<q"  # i64 ticks
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

    Per block: a u64 index-type word (HasAdditionalKeysBit set, UInt8 index width
    here), a u64 dictionary size, the dictionary values, a u64 row count, and the
    UInt8 index per row. For a Nullable inner type, dictionary slot 0 is the NULL
    sentinel and null rows index it. A zero-length run writes nothing at all
    (server limit == 0 early return), which is how an Array of all-empty rows
    encodes its LowCardinality element column.
    """
    inner = type_name[len("LowCardinality("):-1]
    nullable = inner.startswith("Nullable(")
    value_type = inner[len("Nullable("):-1] if nullable else inner

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
    buf.extend(struct.pack("<Q", 0x200))  # HasAdditionalKeysBit | UInt8 index width (tag 0)
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
    if inner_type in _FIXED_TYPES:
        fmt, _ = _FIXED_TYPES[inner_type]
        default = 0 if "Int" in inner_type or "UInt" in inner_type else (
            0.0 if "Float" in inner_type else 0
        )
        for v in values:
            buf.extend(struct.pack(fmt, v if v is not None else default))
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


def _element_state_prefix(type_name):
    """State prefix hoisted to the front of a column, recursing through Array.

    Only LowCardinality contributes bytes (its u64 key version); Array recurses
    into its element, every leaf/Nullable writes nothing. Matches the core's
    write_state_prefix.
    """
    if type_name.startswith("Array("):
        return _element_state_prefix(type_name[len("Array("):-1])
    if type_name.startswith("LowCardinality("):
        return _lc_key_version()
    return b""


def _build_body_no_prefix(type_name, values):
    """Column body with its state prefix omitted (already hoisted by the caller)."""
    if type_name.startswith("Array("):
        return _build_array_body(type_name, values)
    if type_name.startswith("LowCardinality("):
        return _build_low_cardinality_body_no_prefix(type_name, values)
    if type_name.startswith("Nullable("):
        inner = type_name[len("Nullable("):-1]
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


def build_native_block(columns, *, block_info=False):
    """Build a ClickHouse Native format block from column specs.

    Each column is (name, type_name, values).
    Supports: Bool, Int8-64, UInt8-64, Float32, Float64, String, FixedString(N),
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

        if type_name.startswith("LowCardinality("):
            buf.extend(_build_low_cardinality_body(type_name, values))
            continue

        if type_name.startswith("Array("):
            buf.extend(_element_state_prefix(type_name))
            buf.extend(_build_array_body(type_name, values))
            continue

        is_nullable = type_name.startswith("Nullable(")
        inner_type = type_name[len("Nullable("):-1] if is_nullable else type_name
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
            _ch_core.encode_native_block(["v"], ["Tuple(Int8, Int8)"], [[(13, 79)]], 1)
        with pytest.raises(ValueError, match="label"):
            _ch_core.encode_native_block(["e"], ["Enum8('ok' = 1)"], [["missing"]], 1)


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
        ],
    )
    def test_out_of_range_raises_conversion_error(self, type_name, values):
        with pytest.raises(ValueError, match=f"row {len(values) - 1} cannot be converted to {type_name}"):
            self._encode(type_name, values)

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
            _ch_core.encode_native_block(["a"], ["Array(Tuple(Int8, Int8))"], [[[(1, 2)]]], 1)

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
        # Tuple is not a supported column type, so it surfaces as a clean
        # UnsupportedType -> ValueError. (UUID, IPv4/IPv6, Enum, and Array are
        # decoded by the core now, so they no longer exercise this path.)
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint_string("id"))
        buf.extend(_encode_varint_string("Tuple(UInt8, UInt8)"))
        with pytest.raises(ValueError, match="Unsupported ClickHouse type 'Tuple\\(UInt8, UInt8\\)'"):
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
