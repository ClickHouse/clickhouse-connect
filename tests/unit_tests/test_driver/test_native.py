from clickhouse_connect.driver.native import parse_response
from tests.helpers import to_bytes

uint16_nulls = ("0104 0969 6e74 5f76 616c 7565 104e 756c"
                "6c61 626c 6528 5549 6e74 3136 2901 0001"
                "0000 0014 0000 0028 00")

low_cardinality = ("0102 026c 6316 4c6f 7743 6172 6469 6e61"
                   "6c69 7479 2853 7472 696e 6729 0100 0000"
                   "0000 0000 0006 0000 0000 0000 0300 0000"
                   "0000 0000 0004 4344 4d41 0347 534d 0200"
                   "0000 0000 0000 0102 0101 026c 6316 4c6f"
                   "7743 6172 6469 6e61 6c69 7479 2853 7472"
                   "696e 6729 0100 0000 0000 0000 0006 0000"
                   "0000 0000 0200 0000 0000 0000 0004 554d"
                   "5453 0100 0000 0000 0000 01"
                   )


def test_uint16_nulls():
    result = parse_response(to_bytes(uint16_nulls))
    assert result[0] == ((None,), (20,), (None,), (40,))


def test_low_cardinality():
    result = parse_response(to_bytes(low_cardinality))
    assert result[0] == (('CDMA',), ('GSM',), ('UMTS',))
