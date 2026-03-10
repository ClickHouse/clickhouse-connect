import struct

import pytest

from clickhouse_connect.driver.bytesource import ByteArraySource


def test_read_byte():
    src = ByteArraySource(b"\x42\xff")
    assert src.read_byte() == 0x42
    assert src.read_byte() == 0xFF


def test_read_byte_eof():
    src = ByteArraySource(b"")
    with pytest.raises(EOFError):
        src.read_byte()


def test_read_bytes():
    src = ByteArraySource(b"\x01\x02\x03\x04\x05")
    assert src.read_bytes(3) == b"\x01\x02\x03"
    assert src.read_bytes(2) == b"\x04\x05"


def test_read_bytes_eof():
    src = ByteArraySource(b"\x01\x02")
    with pytest.raises(EOFError):
        src.read_bytes(5)


def test_read_leb128_single_byte():
    src = ByteArraySource(b"\x7f")
    assert src.read_leb128() == 127


def test_read_leb128_multi_byte():
    # 300 = 0b100101100 => LEB128: 0xAC 0x02
    src = ByteArraySource(b"\xac\x02")
    assert src.read_leb128() == 300


def test_read_leb128_zero():
    src = ByteArraySource(b"\x00")
    assert src.read_leb128() == 0


def test_read_leb128_str():
    # "hello" = 5 bytes, LEB128 length = 0x05
    src = ByteArraySource(b"\x05hello")
    assert src.read_leb128_str() == "hello"


def test_read_uint64():
    val = 18000000000000000000
    data = val.to_bytes(8, "little", signed=False)
    src = ByteArraySource(data)
    assert src.read_uint64() == val


def test_read_uint64_zero():
    src = ByteArraySource(b"\x00" * 8)
    assert src.read_uint64() == 0


def test_read_array_uint8():
    src = ByteArraySource(b"\x01\x02\x03")
    assert src.read_array("B", 3) == [1, 2, 3]


def test_read_array_int8():
    # -1 as signed byte = 0xFF
    src = ByteArraySource(b"\xff\x80\x01")
    result = src.read_array("b", 3)
    assert result == [-1, -128, 1]


def test_read_array_uint16():
    data = struct.pack("<HH", 1000, 60000)
    src = ByteArraySource(data)
    assert src.read_array("H", 2) == [1000, 60000]


def test_read_array_uint32():
    data = struct.pack("<II", 100000, 4000000000)
    src = ByteArraySource(data)
    assert src.read_array("I", 2) == [100000, 4000000000]


def test_read_array_uint64():
    data = struct.pack("<QQ", 0, 2**64 - 1)
    src = ByteArraySource(data)
    assert src.read_array("Q", 2) == [0, 2**64 - 1]


def test_read_array_int64():
    data = struct.pack("<qq", -9000000000000000000, 42)
    src = ByteArraySource(data)
    assert src.read_array("q", 2) == [-9000000000000000000, 42]


def test_read_array_float32():
    data = struct.pack("<f", 3.14)
    src = ByteArraySource(data)
    result = src.read_array("f", 1)
    assert result[0] == pytest.approx(3.14, rel=1e-5)


def test_read_array_float64():
    data = struct.pack("<d", 2.718281828459045)
    src = ByteArraySource(data)
    result = src.read_array("d", 1)
    assert result[0] == pytest.approx(2.718281828459045)


def test_read_array_unsupported():
    src = ByteArraySource(b"\x00")
    with pytest.raises(NotImplementedError):
        src.read_array("Z", 1)


def test_read_str_col():
    # LEB128 length 5 + "hello"
    src = ByteArraySource(b"\x05hello")
    result = src.read_str_col(1, "utf-8")
    assert result == ["hello"]


def test_read_str_col_raw_bytes():
    src = ByteArraySource(b"\x03abc")
    result = src.read_str_col(1, None)
    assert result == [b"abc"]


def test_read_str_col_multi_row_raises():
    src = ByteArraySource(b"\x00")
    with pytest.raises(NotImplementedError):
        src.read_str_col(2, "utf-8")


def test_read_bytes_col_raises():
    src = ByteArraySource(b"\x00")
    with pytest.raises(NotImplementedError):
        src.read_bytes_col(1, 1)


def test_read_fixed_str_col_raises():
    src = ByteArraySource(b"\x00")
    with pytest.raises(NotImplementedError):
        src.read_fixed_str_col(1, 1, "utf-8")


def test_close_is_noop():
    src = ByteArraySource(b"\x00")
    src.close()  # should not raise


def test_sequential_reads():
    """Test that position tracking works correctly across mixed reads."""
    data = b"\x42"  # read_byte
    data += b"\x03abc"  # read_leb128_str
    data += struct.pack("<Q", 999)  # read_uint64
    src = ByteArraySource(data)

    assert src.read_byte() == 0x42
    assert src.read_leb128_str() == "abc"
    assert src.read_uint64() == 999
