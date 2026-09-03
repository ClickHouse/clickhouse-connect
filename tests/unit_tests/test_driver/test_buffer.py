import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer

from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.exceptions import (
    GENERIC_CLICKHOUSE_ERROR,
    OperationalError,
    StreamCompleteException,
    StreamFailureError,
)
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.transform import NativeTransform
from tests.helpers import TAGGED_EXCEPTION_BODY, TAGGED_EXCEPTION_TAG, bytes_source, to_bytes


def test_gen_and_exception_tag_exposed():
    class Source:
        def __init__(self):
            self.gen = iter([b"chunk_1", b"chunk_2"])
            self.exception_tag = "TAG13"

        def close(self, ex: Exception | None = None):
            pass

    for cls in CResponseBuffer, PyResponseBuffer:
        buff = cls(Source())
        assert next(buff.gen) == b"chunk_1"
        assert buff.exception_tag == "TAG13"


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


def _tagged_exception_body(message: str) -> bytes:
    encoded = message.encode()
    return (
        b"bodybody\r\n__exception__\r\n"
        + TAGGED_EXCEPTION_TAG.encode()
        + b"\r\n"
        + encoded
        + b"\n"
        + str(len(encoded)).encode()
        + b" "
        + TAGGED_EXCEPTION_TAG.encode()
        + b"\r\n__exception__\r\n"
    )


@pytest.mark.parametrize(
    "mode,expected,forbidden",
    [
        (
            "scrub",
            "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)",
            "version",
        ),
        (False, GENERIC_CLICKHOUSE_ERROR, "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
    ],
)
def test_tagged_exception_honors_show_clickhouse_errors(mode, expected, forbidden):
    class TaggedSource:
        def __init__(self):
            self.gen = iter(
                [
                    _tagged_exception_body(
                        "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. "
                        "(FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 26.2.4.23 (official build))"
                    )
                ]
            )
            self.exception_tag = TAGGED_EXCEPTION_TAG

        def close(self, ex: Exception | None = None):
            pass

    context = QueryContext()
    context.show_clickhouse_errors = mode
    for cls in CResponseBuffer, PyResponseBuffer:
        with pytest.raises(StreamFailureError) as ex:
            NativeTransform.parse_response(cls(TaggedSource()), context)
        assert str(ex.value) == expected
        assert forbidden not in str(ex.value)


def test_transport_failure_with_native_last_message_does_not_leak_binary_data():
    class FailedSource:
        last_message = b"\xff" * 80

        def read_leb128(self):
            raise OperationalError("transport failed")

        def close(self):
            pass

    context = QueryContext()
    context.show_clickhouse_errors = "scrub"
    with pytest.raises(StreamFailureError) as ex:
        NativeTransform.parse_response(FailedSource(), context)
    assert str(ex.value) == "Stream failed during read (connection closed by server)"

    context.show_clickhouse_errors = True
    with pytest.raises(StreamFailureError) as ex:
        NativeTransform.parse_response(FailedSource(), context)
    assert str(ex.value) == "unrecognized data found in stream: `" + "ff" * 16 + "`"


def test_scrub_mode_ignores_code_marker_outside_error_window():
    class FailedSource:
        last_message = b"Code: 395. " + (b"x" * 1100) + b"private_row=user_1"

        def read_leb128(self):
            raise OperationalError("transport failed")

        def close(self):
            pass

    context = QueryContext()
    context.show_clickhouse_errors = "scrub"
    with pytest.raises(StreamFailureError) as ex:
        NativeTransform.parse_response(FailedSource(), context)
    assert str(ex.value) == "Stream failed during read (connection closed by server)"
    assert "private_row" not in str(ex.value)


@pytest.mark.parametrize("mode", [True, "scrub", False])
def test_stream_complete_without_server_error_uses_connection_drop_message(mode):
    class ShortSource:
        last_message = b""

        def __init__(self):
            self.calls = 0

        def read_leb128(self):
            self.calls += 1
            if self.calls == 1:
                return 1
            raise StreamCompleteException

        def close(self):
            pass

    context = QueryContext()
    context.show_clickhouse_errors = mode
    with pytest.raises(StreamFailureError) as ex:
        NativeTransform.parse_response(ShortSource(), context)
    assert str(ex.value) == "Stream ended unexpectedly (connection closed by server)"


@pytest.mark.parametrize("mode", [True, "scrub", False])
def test_read_failure_without_server_error_uses_connection_drop_message(mode):
    class FailedSource:
        last_message = b""

        def read_leb128(self):
            raise OperationalError("transport failed with no server message")

        def close(self):
            pass

    context = QueryContext()
    context.show_clickhouse_errors = mode
    with pytest.raises(StreamFailureError) as ex:
        NativeTransform.parse_response(FailedSource(), context)
    assert str(ex.value) == "Stream failed during read (connection closed by server)"
