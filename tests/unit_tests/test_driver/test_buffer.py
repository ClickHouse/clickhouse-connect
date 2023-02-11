from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.exceptions import StreamCompleteException
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
            buff.read_str_col(2)
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
