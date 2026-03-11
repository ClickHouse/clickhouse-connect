import pytest

from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.exceptions import StreamFailureError
from clickhouse_connect.driver.exceptions import StreamCompleteException
from clickhouse_connect.driver.transform import NativeTransform
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer  # pylint: disable=no-name-in-module
from tests.helpers import bytes_source, to_bytes


def test_read_ints():
    for cls in CResponseBuffer, PyResponseBuffer:
        buff = bytes_source('05 20 00 00 00 00 00 00 68 10 83 03 77', cls=cls)
        assert buff.read_uint64() == 8197
        assert buff.read_leb128() == 104
        assert buff.read_leb128() == 16
        assert buff.read_leb128() == 387
        assert buff.read_byte() == 0x77
        try:
            buff.read_byte()
        except StreamCompleteException:
            pass


def test_read_strings():
    for cls in CResponseBuffer, PyResponseBuffer:
        buff = bytes_source('04 43 44 4d 41', cls=cls)
        assert buff.read_leb128_str() == 'CDMA'
        try:
            buff.read_str_col(2, 'utf8')
        except StreamCompleteException:
            pass


def test_read_bytes():
    for cls in CResponseBuffer, PyResponseBuffer, :
        buff = bytes_source('04 43 44 4d 41 22 44 66 88 AA', cls=cls)
        buff.read_byte()
        assert buff.read_bytes(5) == to_bytes('43 44 4d 41 22')
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
    exception_tag = "PU1FNUFH98"
    response_body = (
        b"bodybodybodybody\r\n"
        b"__exception__PU1FNUFH98\r\n"
        b"Big bam occurred right while reading the data\r\n"
        b"46 PU1FNUFH98__exception__\r\n"
    )

    class TaggedSource:
        def __init__(self):
            self.gen = iter([response_body])
            self.exception_tag = exception_tag

        def close(self, ex: Exception | None = None):
            pass

    for cls in CResponseBuffer, PyResponseBuffer:
        with pytest.raises(StreamFailureError) as ex:
            NativeTransform.parse_response(cls(TaggedSource()))
        assert str(ex.value) == "Big bam occurred right while reading the data"
