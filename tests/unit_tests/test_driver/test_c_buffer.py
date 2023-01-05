from clickhouse_connect.driverc.buffer import ResponseBuffer
from tests.helpers import bytes_gen, to_bytes


def test_read_ints():
    gen = bytes_gen('05 20 00 00 00 00 00 00 68 10 83 03 77')
    buff = ResponseBuffer(gen)
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
    gen = bytes_gen('04 43 44 4d 41 22 44 66 88 AA')
    buff = ResponseBuffer(gen)
    assert buff.read_leb128_str() == 'CDMA'


def test_read_bytes():
    gen = bytes_gen('04 43 44 4d 41 22 44 66 88 AA')
    buff = ResponseBuffer(gen)
    buff.read_byte()
    assert buff.read_bytes(5) == to_bytes('43 44 4d 41 22')
