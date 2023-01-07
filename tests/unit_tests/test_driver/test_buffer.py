from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
# pylint: disable=no-name-in-module
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer
from tests.helpers import bytes_source, to_bytes


def test_read_ints():
    for cls in PyResponseBuffer, CResponseBuffer:
        buff = bytes_source('05 20 00 00 00 00 00 00 68 10 83 03 77', cls=cls)
        assert buff.read_uint64() == 8197

        assert buff.read_leb128() == 104
        assert buff.read_leb128() == 16
        assert buff.read_leb128() == 387

        assert buff.read_byte() == 0x77
        try:
            buff.read_byte()
        except StopIteration:
            pass


def test_read_strings():
    for cls in PyResponseBuffer, CResponseBuffer:
        buff = bytes_source('04 43 44 4d 41 22 44 66 88 AA', cls=cls)
        assert buff.read_leb128_str() == 'CDMA'


def test_read_bytes():
    for cls in PyResponseBuffer, CResponseBuffer:
        buff = bytes_source('04 43 44 4d 41 22 44 66 88 AA', cls=cls)
        buff.read_byte()
        assert buff.read_bytes(5) == to_bytes('43 44 4d 41 22')
