import decimal
from datetime import date

from clickhouse_connect.datatypes.registry import get_from_name as gfn


def to_bytes(hex_str):
    return bytearray.fromhex(hex_str)


def test_string():
    str_type = gfn('String')
    source = to_bytes('0f 41 20 6c 6f 76 65 6c  79 20 73 74 72 69 6e 67')
    value, loc = str_type.from_row_binary(source, 0)
    assert value == 'A lovely string'


def test_array():
    str_array = gfn('Array(LowCardinality(String))')
    source = to_bytes('02 07 73 74 72 69 6e 67  31 07 73 74 72 69 6e 67 32')
    value, loc = str_array.from_row_binary(source, 0)
    assert value == ['string1', 'string2']


def test_nullable():
    str_array = gfn('Array(Nullable(String))')
    source = to_bytes('04 00 07 73 74 72 69 6e 67 31 00 07 73 74 72 69 6e 67 32 01 00 03 73 74 34')
    value, loc = str_array.from_row_binary(source, 0)
    assert value == ['string1', 'string2', None, 'st4']


def test_uuid():
    uuid = gfn('UUID')
    source = to_bytes('6c 4a 9b 63 ad 80 a6 c4  97 e7 d6 75 33 71 5a ad')
    value, loc = uuid.from_row_binary(source, 0)
    assert str(value) == 'c4a680ad-639b-4a6c-ad5a-713375d6e797'


def test_tuple():
    ch_tuple = gfn('Tuple(Boolean, String, Bool, Int16)')
    source = to_bytes('01 0f 41 20 6c 6f 76 65 6c  79 20 73 74 72 69 6e 67 00 77 23')
    value, loc = ch_tuple.from_row_binary(source, 0)
    assert value == (True, 'A lovely string', False, 9079)


def test_ip():
    ipv6 = gfn('IPv6')
    source = to_bytes('00 00 00 00 00 00 00 00 00 00 ff ff 58 34 00 01')
    value, loc = ipv6.from_row_binary(source, 0)
    assert value == '88.52.0.1'
    source = to_bytes('fd 78 dd 5e 6f ce 73 92  04 4a 87 53 a9 07 26 b2')
    value, loc = ipv6.from_row_binary(source, 0)
    assert value == 'fd78:dd5e:6fce:7392:44a:8753:a907:26b2'


def test_decimal():
    dec_type = gfn('Decimal128(5)')
    source = to_bytes('b8 6a 05 00 00 00 00 00  00 00 00 00 00 00 00 00')
    value, loc = dec_type.from_row_binary(source, 0)
    assert value == decimal.Decimal('3.55000')


def test_date():
    dt = gfn('Date32')
    source = to_bytes('fd f9 ff ff')
    value, loc = dt.from_row_binary(source, 0)
    assert value == date(1965, 10, 15)
    source = to_bytes('7a b9 00 00 ')
    value, loc = dt.from_row_binary(source, 0)
    assert value == date(2100, 1, 1)


def test_datetime():
    dt = gfn("DateTime64(6, 'Europe/Moscow')")
    source = to_bytes('80 0b af 48 aa 8d 03 00')
    value, loc = dt.from_row_binary(source, 0)
    assert value.tzinfo.zone == 'Europe/Moscow'
    assert value.microsecond == 520000
    assert value.year == 2001

