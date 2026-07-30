import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.exceptions import StreamCompleteException, StreamFailureError
from clickhouse_connect.driver.transform import NativeTransform
from tests.helpers import TAGGED_EXCEPTION_BODY, TAGGED_EXCEPTION_TAG, bytes_source, to_bytes


def test_read_ints():
    for cls in CResponseBuffer, PyResponseBuffer:
        buff = bytes_source("05 20 00 00 00 00 00 00 68 10 83 03 77", cls=cls)
        assert buff.read_uint64() == 8197
        assert buff.read_leb128() == 104
        assert buff.read_leb128() == 16
        assert buff.read_leb128() == 387
        assert buff.read_byte() == 0x77
        try:
            buff.read_byte()
        except StreamCompleteException:
            pass


def _encode_leb128(value: int) -> bytes:
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


@pytest.mark.parametrize("value", [2**31, 2**32, 2**35, 2**63])
def test_read_leb128_large_values(value):
    data = _encode_leb128(value)
    c_result = bytes_source(data, cls=CResponseBuffer).read_leb128()
    py_result = bytes_source(data, cls=PyResponseBuffer).read_leb128()
    assert c_result == value
    assert py_result == value
    assert c_result == py_result


def test_read_strings():
    for cls in CResponseBuffer, PyResponseBuffer:
        buff = bytes_source("04 43 44 4d 41", cls=cls)
        assert buff.read_leb128_str() == "CDMA"
        try:
            buff.read_str_col(2, "utf8")
        except StreamCompleteException:
            pass


def test_read_bytes():
    for cls in (
        CResponseBuffer,
        PyResponseBuffer,
    ):
        buff = bytes_source("04 43 44 4d 41 22 44 66 88 AA", cls=cls)
        buff.read_byte()
        assert buff.read_bytes(5) == to_bytes("43 44 4d 41 22")
        try:
            buff.read_bytes(10)
        except StreamCompleteException:
            pass


def test_fixed_string_strips_padding():
    data = bytes.fromhex("41 00 00 00 42 43 00 00")
    expected = ["A", "BC"]
    for cls in CResponseBuffer, PyResponseBuffer:
        buff = bytes_source(data, cls=cls)
        assert list(buff.read_fixed_str_col(4, 2, "utf8")) == expected


def test_tagged_exception_extracts_clean_message():
    class TaggedSource:
        def __init__(self):
            self.gen = iter([TAGGED_EXCEPTION_BODY])
            self.exception_tag = TAGGED_EXCEPTION_TAG

        def close(self, ex: Exception | None = None):
            pass

    for cls in CResponseBuffer, PyResponseBuffer:
        with pytest.raises(StreamFailureError) as ex:
            NativeTransform.parse_response(cls(TaggedSource()))
        assert str(ex.value) == "Big bam occurred right while reading the data"


@pytest.mark.parametrize("split_point", ["closing_marker", "opening_marker"])
def test_tagged_exception_split_across_chunks(split_point):
    # The server can split the in-band exception block across transport chunks. Chunk it so that no
    # single chunk holds the whole block, which defeats the last-chunk fallback in
    # NativeTransform.parse_response: a clean error can only come from the cross-chunk scan in
    # ResponseBuffer._check_for_exception, which must reassemble the block from both chunks.
    body = TAGGED_EXCEPTION_BODY
    if split_point == "closing_marker":
        # Opening marker and message in the first chunk, closing marker alone in the last chunk. The
        # closing marker (tag then __exception__) only appears in the footer, so split at the footer tag.
        # Exercises the exception_buf accumulation branch.
        split = body.rfind(TAGGED_EXCEPTION_TAG.encode())
    else:
        # Split inside the opening __exception__ marker itself, so neither chunk contains the whole
        # marker. Exercises the _carryover branch, which must rejoin the marker across the two chunks.
        split = body.find(b"__exception__") + 7
    chunks = [body[:split], body[split:]]

    class ChunkedSource:
        def __init__(self):
            self.gen = iter(chunks)
            self.exception_tag = TAGGED_EXCEPTION_TAG

        def close(self, ex: Exception | None = None):
            pass

    for cls in CResponseBuffer, PyResponseBuffer:
        with pytest.raises(StreamFailureError) as ex:
            NativeTransform.parse_response(cls(ChunkedSource()))
        assert str(ex.value) == "Big bam occurred right while reading the data"
