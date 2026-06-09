"""Tests for _ch_core Python bindings - Phase 1 types."""

import datetime as dt
import os
import struct
import subprocess
import sys
import textwrap
from zoneinfo import ZoneInfo

import pytest

_ch_core = pytest.importorskip("_ch_core")

_EPOCH_DATE = dt.date(1970, 1, 1)
_EPOCH_NAIVE = dt.datetime(1970, 1, 1)


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


def build_native_block(columns, *, block_info=False):
    """Build a ClickHouse Native format block from column specs.

    Each column is (name, type_name, values).
    Supports: Bool, Int8-64, UInt8-64, Float32, Float64, String, FixedString(N), Nullable(*).
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

        is_nullable = type_name.startswith("Nullable(")
        inner_type = type_name
        if is_nullable:
            inner_type = type_name[len("Nullable("):-1]
            for v in values:
                buf.append(0x01 if v is None else 0x00)

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
                buf.extend(b[:width].ljust(width, b"\x00"))
        else:
            raise ValueError(f"build_native_block: unsupported type {inner_type}")
    return bytes(buf)


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
        buf = bytearray()
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint(1))
        buf.extend(_encode_varint_string("id"))
        buf.extend(_encode_varint_string("UUID"))
        with pytest.raises(ValueError, match="Unsupported ClickHouse type 'UUID'"):
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


class TestBlockInfo:
    def test_with_block_info(self):
        data = build_native_block(
            [("v", "Int64", [77, 88])],
            block_info=True,
        )
        batch = _ch_core.ColBatch.decode_native(data, has_block_info=True)
        assert batch.num_rows == 2
        assert list(batch.column_data(0)) == [77, 88]
