from ipaddress import IPv4Address
from uuid import UUID

from clickhouse_connect.datatypes import registry
from clickhouse_connect.driver.native import parse_response
from tests.helpers import to_bytes

UINT16_NULLS = """
    0104 0969 6e74 5f76 616c 7565 104e 756c
    6c61 626c 6528 5549 6e74 3136 2901 0001
    0000 0014 0000 0028 00
"""

LOW_CARDINALITY = """
    0102 026c 6316 4c6f 7743 6172 6469 6e61
    6c69 7479 2853 7472 696e 6729 0100 0000
    0000 0000 0006 0000 0000 0000 0300 0000
    0000 0000 0004 4344 4d41 0347 534d 0200
    0000 0000 0000 0102 0101 026c 6316 4c6f
    7743 6172 6469 6e61 6c69 7479 2853 7472
    696e 6729 0100 0000 0000 0000 0006 0000
    0000 0000 0200 0000 0000 0000 0004 554d
    5453 0100 0000 0000 0000 01
 """

LOW_CARD_ARRAY = """
    0102 066c 6162 656c 731d 4172 7261 7928
    4c6f 7743 6172 6469 6e61 6c69 7479 2853
    7472 696e 6729 2901 0000 0000 0000 0000
    0000 0000 0000 0000 0000 0000 0000 00
"""

SIMPLE_MAP = """
    0101 066e 6e5f 6d61 7013 4d61 7028 5374
    7269 6e67 2c20 5374 7269 6e67 2902 0000
    0000 0000 0004 6b65 7931 046b 6579 3206
    7661 6c75 6531 0676 616c 7565 32
"""

LOW_CARD_MAP = """
    0102 086d 6170 5f6e 756c 6c2b 4d61 7028
    4c6f 7743 6172 6469 6e61 6c69 7479 2853
    7472 696e 6729 2c20 4e75 6c6c 6162 6c65
    2855 5549 4429 2901 0000 0000 0000 0002
    0000 0000 0000 0004 0000 0000 0000 0000
    0600 0000 0000 0003 0000 0000 0000 0000
    0469 676f 7206 6765 6f72 6765 0400 0000
    0000 0000 0102 0102 0100 0000 0000 0000
    0000 0000 0000 0000 0000 0000 235f 7dc5
    799f 431d a9e1 93ca ccff c652 235f 7dc5
    799f 437f a9e1 93ca ccff 0052 235f 7dc5
    799f 431d a9e1 93ca ccff c652
"""

NESTED = """
0104 066e 6573 7465 6421 4e65 7374 6564  
2873 7472 3120 5374 7269 6e67 2c20 696e  
7433 3220 5549 6e74 3332 2900 0000 0000 
0000 0002 0000 0000 0000 0004 0000 0000  
0000 0006 0000 0000 0000 0005 7468 7265  
6504 6669 7665 036f 6e65 0374 776f 036f  
6e65 0374 776f 0500 0000 4d00 0000 0500  
0000 3700 0000 0500 0000 3700 0000  
"""


def check_result(result, expected, row_num=0, col_num=0):
    result_set = result[0]
    row = result_set[row_num]
    value = row[col_num]
    assert value == expected


def test_uint16_nulls():
    result = parse_response(to_bytes(UINT16_NULLS))
    assert result[0] == [(None,), (20,), (None,), (40,)]


def test_low_cardinality():
    result = parse_response(to_bytes(LOW_CARDINALITY))
    assert result[0] == [('CDMA',), ('GSM',), ('UMTS',)]


def test_low_card_array():
    result = parse_response(to_bytes(LOW_CARD_ARRAY))
    assert result[0][0] == ([],), ([],)


def test_map():
    result = parse_response(to_bytes(SIMPLE_MAP))
    check_result(result, {'key1': 'value1', 'key2': 'value2'})
    result = parse_response(to_bytes(LOW_CARD_MAP))
    check_result(result, {'george': UUID('1d439f79-c57d-5f23-52c6-ffccca93e1a9'), 'igor': None})


def test_ip():
    ips = ['192.168.5.3', '202.44.8.25', '0.0.2.2']
    ipv4_type = registry.get_from_name('IPv4')
    dest = bytearray()
    ipv4_type.write_native_column(ips, dest)
    python, _ = ipv4_type.read_native_column(dest, 0, 3)
    assert python == [IPv4Address(ip) for ip in ips]


def test_nested():
    result = parse_response(to_bytes(NESTED))
    check_result(result, [{"str1": "one", "int32": 5}, {"str1": "two", "int32": 55}], 2, 0)
