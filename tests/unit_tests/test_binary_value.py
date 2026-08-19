from datetime import date, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest

from clickhouse_connect.datatypes.binary_value import (
    MAX_ZERO_WIDTH_ELEMENTS,
    UnsupportedBinaryTypeError,
    _decode_binary_value,
)
from clickhouse_connect.datatypes.dynamic import decode_shared_data_value
from clickhouse_connect.driver.exceptions import StreamCompleteException
from clickhouse_connect.driver.query import QueryContext


def _leb128(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _i32(value: int) -> bytes:
    return value.to_bytes(4, "little", signed=True)


def _i64(value: int) -> bytes:
    return value.to_bytes(8, "little", signed=True)


CTX = QueryContext()

DECODE_CASES = [
    pytest.param(b"\x01\x05", 5, id="uint8"),
    pytest.param(b"\x15\x05hello", "hello", id="string"),
    pytest.param(b"\x0f\x19\x4d", date(2024, 1, 15), id="date"),
    pytest.param(
        b"\x13\x03" + _i64(1705321845123),
        datetime(2024, 1, 15, 12, 30, 45, 123000),
        id="datetime64",
    ),
    pytest.param(
        b"\x12\x10America/New_York" + (1705321845).to_bytes(4, "little"),
        datetime(2024, 1, 15, 7, 30, 45, tzinfo=ZoneInfo("America/New_York")),
        id="datetime-tz",
    ),
    pytest.param(
        b"\x14\x03\x10America/New_York" + _i64(1705321845123),
        datetime(2024, 1, 15, 7, 30, 45, 123000, tzinfo=ZoneInfo("America/New_York")),
        id="datetime64-tz",
    ),
    pytest.param(
        b"\x34\x09" + _i64(3723123456789),
        timedelta(seconds=3723, microseconds=123456),
        id="time64",
    ),
    pytest.param(
        b"\x1d" + bytes(range(7, -1, -1)) + bytes(range(15, 7, -1)),
        UUID("00010203-0405-0607-0809-0a0b0c0d0e0f"),
        id="uuid-word-order",
    ),
    pytest.param(
        b"\x1e\x23\x09" + b"\x03" + b"\x00" + _i32(1) + b"\x01" + b"\x00" + _i32(3),
        [1, None, 3],
        id="array-nullable-int32",
    ),
    pytest.param(
        b"\x1e\x26\x23\x15" + b"\x02" + b"\x01" + b"\x00\x01x",
        [None, "x"],
        id="array-lowcardinality-nullable-string",
    ),
    pytest.param(
        b"\x27\x15\x04" + b"\x02" + b"\x01a" + (1).to_bytes(8, "little") + b"\x01b" + (2).to_bytes(8, "little"),
        {"a": 1, "b": 2},
        id="map-string-uint64",
    ),
    pytest.param(
        b"\x1f\x02\x09\x15" + _i32(13) + b"\x01x",
        (13, "x"),
        id="tuple-unnamed",
    ),
    pytest.param(
        b"\x20\x02\x01n\x09\x01s\x15" + _i32(13) + b"\x01x",
        {"n": 13, "s": "x"},
        id="tuple-named",
    ),
    pytest.param(
        b"\x1f\x01\x23\x15" + b"\x01",
        (None,),
        id="tuple-nullable-null-element",
    ),
    # Array(Dynamic) [7, "a", None]: each element self-describing, null as
    # the bare Nothing type, exactly as SerializationDynamic writes it
    pytest.param(
        b"\x1e\x2b\x20" + b"\x03" + b"\x0a" + _i64(7) + b"\x15\x01a" + b"\x00",
        [7, "a", None],
        id="array-dynamic-with-null",
    ),
    # the exact bytes from issue #897: Array(JSON) [{"k": "v"}]
    pytest.param(
        b"\x1e\x30\x00\x00\x10\x00\x00\x00" + b"\x01\x01\x01k\x15\x01v",
        [{"k": "v"}],
        id="array-json-dynamic-path",
    ),
    # nested JSON with a typed path (a Int32): the value is NOT self
    # describing. Unreachable via a live server (typed paths normalize away
    # before reaching shared data), so covered by crafted bytes only.
    pytest.param(
        b"\x30\x00\x00\x10\x01\x01a\x09\x00\x00" + b"\x01\x01a" + _i32(13),
        {"a": 13},
        id="json-typed-path",
    ),
    # JSON type encoding carrying SKIP and SKIP REGEXP entries, which the
    # parser must consume even though they do not affect the value
    pytest.param(
        b"\x30\x00\x00\x10\x00\x01\x01s\x01\x01r" + b"\x01\x01k\x15\x01v",
        {"k": "v"},
        id="json-skip-lists",
    ),
    pytest.param(b"\x1e\x00\x00", [], id="array-nothing-empty"),
    pytest.param(b"\x1e\x1e\x1f\x00" + b"\x02\x02\x01", [[(), ()], [()]], id="array-array-empty-tuple"),
]


@pytest.mark.parametrize("encoded,expected", DECODE_CASES)
def test_decode_binary_value(encoded: bytes, expected):
    assert _decode_binary_value(encoded, CTX) == expected


REJECT_CASES = [
    # Array(Tuple()) with a count past the zero-width bound: elements occupy
    # zero bytes, so the count must be bounded explicitly
    pytest.param(b"\x1e\x1f\x00" + _leb128(MAX_ZERO_WIDTH_ELEMENTS + 1), id="zero-width-bomb"),
    # Array(Array(Tuple())) where no single count exceeds the bound but the
    # nested counts together exhaust the shared zero-width budget
    pytest.param(
        b"\x1e\x1e\x1f\x00" + b"\x02" + _leb128(1) + _leb128(MAX_ZERO_WIDTH_ELEMENTS),
        id="nested-zero-width-bomb",
    ),
    # Array(UInt8) count far past the remaining bytes
    pytest.param(b"\x1e\x01" + _leb128(2**40), id="count-exceeds-input"),
    # DateTime64 with a sub-second scale past 9
    pytest.param(b"\x13\x0a", id="scale-too-large"),
    # the server never serializes Nothing values
    pytest.param(b"\x1e\x00\x03", id="array-nothing-nonzero-count"),
    pytest.param(b"\x00", id="bare-nothing"),
    # trailing garbage after a complete value
    pytest.param(b"\x01\x05\xff", id="trailing-bytes"),
    # unknown type index (0x36 QBit)
    pytest.param(b"\x36\x0d\x08", id="unsupported-index"),
]


@pytest.mark.parametrize("encoded", REJECT_CASES)
def test_decode_binary_value_rejects(encoded: bytes):
    with pytest.raises(UnsupportedBinaryTypeError):
        _decode_binary_value(encoded, CTX)


@pytest.mark.parametrize(
    "encoded",
    [
        # Array(Int32) declaring 2 elements but carrying 1
        pytest.param(b"\x1e\x09\x02" + _i32(1), id="truncated-value"),
        # Array type encoding with no element type
        pytest.param(b"\x1e", id="truncated-type"),
    ],
)
def test_decode_binary_value_truncated(encoded: bytes):
    with pytest.raises(StreamCompleteException):
        _decode_binary_value(encoded, CTX)


@pytest.mark.parametrize(
    "encoded",
    [
        pytest.param(b"\x1e\x1f\x00" + _leb128(MAX_ZERO_WIDTH_ELEMENTS + 1), id="zero-width-bomb"),
        pytest.param(b"\x36\x0d\x08", id="unsupported-index"),
        pytest.param(b"\x1e\x09\x02" + _i32(1), id="truncated-value"),
    ],
)
def test_shared_data_value_falls_back_to_raw_bytes(encoded: bytes):
    # the public entry point never raises; undecodable input comes back verbatim
    assert decode_shared_data_value(encoded, CTX) == encoded
