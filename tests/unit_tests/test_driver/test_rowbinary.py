import decimal
import uuid
from datetime import date
from ipaddress import IPv4Address, IPv6Address
from typing import Union, Any

from clickhouse_connect.datatypes.registry import get_from_name as gfn
from clickhouse_connect.datatypes.base import ClickHouseType
from tests.helpers import to_bytes


def assert_frb(ch_type: Union[str, ClickHouseType], source: Union[memoryview, bytes, str],
               expected: Any, start: int = 0, expected_loc: int = -1):
    if isinstance(ch_type, str):
        ch_type = gfn(ch_type)
    if isinstance(source, str):
        source = to_bytes(source)
    value, loc = ch_type.from_row_binary(source, start)
    assert value == expected
    if expected_loc != -1:
        assert loc == expected_loc
    return value, loc


def assert_trb(ch_type: Union[str, ClickHouseType], value: Any, expected: Union[memoryview, bytes, str]):
    if isinstance(ch_type, str):
        ch_type = gfn(ch_type)
    dest = bytearray()
    ch_type.to_row_binary(value, dest)
    if isinstance(expected, str):
        expected = to_bytes(expected)
    assert dest == expected


def round_trip(type_name: str, source: str, value: Any):
    ch_type = gfn(type_name)
    source = to_bytes(source)
    assert_frb(ch_type, source, value)
    assert_trb(ch_type, value, source)


def test_ints():
    assert_frb('UInt8', '50 22 86', 0x22, 1, 2)
    source = '33 99'
    round_trip('Int16', source, -26317)


def test_string():
    source = '1F 41 20 6c 6f 76 65 6c 79 20 73 74 72 69 6e 67 20 77 69 74 68 20 66 72 75 69 74 20 f0 9f a5 9d'
    value = 'A lovely string with fruit 🥝'
    round_trip('String', source, value)


def test_array():
    arr_type = 'Array(LowCardinality(String))'
    source = '02 07 73 74 72 69 6e 67  31 07 73 74 72 69 6e 67 32'
    value = ['string1', 'string2']
    round_trip(arr_type, source, value)


def test_nullable():
    arr_type = 'Array(Nullable(String))'
    source = '04 00 07 73 74 72 69 6e 67 31 00 07 73 74 72 69 6e 67 32 01 00 03 73 74 34'
    value = ['string1', 'string2', None, 'st4']
    round_trip(arr_type, source, value)


def test_uuid():
    uuid_type = 'UUID'
    source = '6c 4a 9b 63 ad 80 a6 c4  97 e7 d6 75 33 71 5a ad'
    value = uuid.UUID('c4a680ad-639b-4a6c-ad5a-713375d6e797')
    round_trip(uuid_type, source, value)


def test_tuple():
    tuple_type = 'Tuple(Boolean, String, Bool, Int16)'
    source = '01 0f 41 20 6c 6f 76 65 6c  79 20 73 74 72 69 6e 67 00 77 23'
    value = (True, 'A lovely string', False, 9079)
    round_trip(tuple_type, source, value)


def test_ip():
    round_trip('IPv4', '16 05 04 cf', IPv4Address('207.4.5.22'))
    round_trip('IPv6', '00 00 00 00 00 00 00 00  00 00 ff ff 58 34 fe 01', IPv4Address('88.52.254.1'))
    round_trip('IPv6', 'fd 78 dd 5e 6f ce 73 92  04 4a 87 53 a9 07 26 b2',
               IPv6Address('fd78:dd5e:6fce:7392:44a:8753:a907:26b2'))


def test_decimal():
    round_trip('Decimal128(5)', 'b8 6a 05 00 00 00 00 00 00 00 00 00 00 00 00 00', decimal.Decimal('3.5500'))
    #dec_type = gfn('Decimal64(8)')
    #assert dec_type.to_row_binary(float(3.55)) == to_bytes('c0 de 28 15 00 00 00 00')


def test_date():
    round_trip('Date32', 'fd f9 ff ff', date(1965, 10, 15))
    round_trip('Date32', '7a b9 00 00', date(2100, 1, 1))
    round_trip('Date32', 'aa 88 00 00', date(2065, 10, 15))


def test_datetime64():
    dt = gfn("DateTime64(6, 'Europe/Moscow')")
    source = to_bytes('80 0b af 48 aa 8d 03 00')
    value, loc = dt.from_row_binary(source, 0)
    assert value.tzinfo.zone == 'Europe/Moscow'
    assert value.microsecond == 520000
    assert value.year == 2001
    assert_trb(dt, value, source)
