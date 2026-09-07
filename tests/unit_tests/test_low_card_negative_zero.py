import struct

import pytest

from clickhouse_connect.datatypes.registry import get_from_name as gfn
from clickhouse_connect.driver.insert import InsertContext

# Native LowCardinality layout written by ClickHouseType.write_column:
#   uint64 low cardinality version
#   uint64 serialization flags (low byte is log2 of the key width in bytes)
#   uint64 dictionary length
#   dictionary values
#   uint64 key count
#   keys
_HEADER = 24


def _write_low_card(type_name: str, column: list) -> bytes:
    dest = bytearray()
    gfn(type_name).write_column(column, dest, InsertContext("", [], []))
    return bytes(dest)


def _dictionary_and_keys(raw: bytes, value_fmt: str) -> tuple[list[bytes], list[int]]:
    flags = int.from_bytes(raw[8:16], "little")
    dict_len = int.from_bytes(raw[16:24], "little")
    value_width = struct.calcsize(value_fmt)
    dictionary = [raw[_HEADER + i * value_width : _HEADER + (i + 1) * value_width] for i in range(dict_len)]

    offset = _HEADER + dict_len * value_width
    key_count = int.from_bytes(raw[offset : offset + 8], "little")
    key_width = 1 << (flags & 0xFF)
    keys_start = offset + 8
    keys = [int.from_bytes(raw[keys_start + i * key_width : keys_start + (i + 1) * key_width], "little") for i in range(key_count)]
    return dictionary, keys


@pytest.mark.parametrize(
    "type_name, value_fmt",
    [("LowCardinality(Float32)", "<f"), ("LowCardinality(Float64)", "<d")],
)
def test_low_card_keeps_negative_zero(type_name: str, value_fmt: str):
    """0.0 and -0.0 are equal and hash equal in Python, but encode to different bytes."""
    dictionary, keys = _dictionary_and_keys(_write_low_card(type_name, [0.0, -0.0]), value_fmt)

    assert dictionary == [struct.pack(value_fmt, 0.0), struct.pack(value_fmt, -0.0)]
    assert keys == [0, 1]


@pytest.mark.parametrize(
    "type_name, value_fmt",
    [("LowCardinality(Float32)", "<f"), ("LowCardinality(Float64)", "<d")],
)
def test_low_card_still_dedups_repeated_negative_zero(type_name: str, value_fmt: str):
    """Negative zero remains a single dictionary entry, so dedup is preserved."""
    dictionary, keys = _dictionary_and_keys(_write_low_card(type_name, [-0.0, 0.0, -0.0]), value_fmt)

    assert dictionary == [struct.pack(value_fmt, -0.0), struct.pack(value_fmt, 0.0)]
    assert keys == [0, 1, 0]


def test_low_card_nullable_keeps_negative_zero():
    raw = _write_low_card("LowCardinality(Nullable(Float64))", [None, 0.0, -0.0])
    dictionary, keys = _dictionary_and_keys(raw, "<d")

    # Index 0 is the null placeholder written by the nullable branch.
    assert dictionary[1:] == [struct.pack("<d", 0.0), struct.pack("<d", -0.0)]
    assert keys == [0, 1, 2]


def test_low_card_ordinary_floats_still_dedup():
    dictionary, keys = _dictionary_and_keys(_write_low_card("LowCardinality(Float64)", [1.5, 1.5, 2.5]), "<d")

    assert dictionary == [struct.pack("<d", 1.5), struct.pack("<d", 2.5)]
    assert keys == [0, 0, 1]


def test_low_card_strings_unaffected():
    """Non-float columns keep taking the plain value as the dictionary key."""
    raw = _write_low_card("LowCardinality(String)", ["a", "b", "a"])

    # Strings are variable width, so only the dictionary length is checked here.
    assert int.from_bytes(raw[16:24], "little") == 2
