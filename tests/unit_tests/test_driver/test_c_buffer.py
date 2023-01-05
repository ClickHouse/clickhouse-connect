from clickhouse_connect.driverc.buffer import ResponseBuffer
from tests.helpers import bytes_gen


def test_read_ints():
    gen = bytes_gen('05 20 00 00 00 00 00 00 68 10 83 03 77')
    buff = ResponseBuffer(gen)
    assert buff.read_uint64() == 8197

    assert buff.read_leb128() == 104
    assert buff.read_leb128() == 16
    assert buff.read_leb128() == 387

    assert buff.read_byte() == 0x77
