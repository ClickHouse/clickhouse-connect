from tests.helpers import bytes_response, to_bytes


def test_read_ints():
    buff = bytes_response('05 20 00 00 00 00 00 00 68 10 83 03 77')
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
    buff = bytes_response('04 43 44 4d 41 22 44 66 88 AA')
    assert buff.read_leb128_str() == 'CDMA'


def test_read_bytes():
    buff = bytes_response('04 43 44 4d 41 22 44 66 88 AA')
    buff.read_byte()
    assert buff.read_bytes(5) == to_bytes('43 44 4d 41 22')
