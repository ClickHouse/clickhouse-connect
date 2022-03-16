import clickhouse_connect
from clickhouse_connect.driver.native import parse_raw, parse_response
from tests.helpers import to_bytes

uint16_nulls = ("0104 0969 6e74 5f76 616c 7565 104e 756c"
                "6c61 626c 6528 5549 6e74 3136 2901 0001"
                "0000 0014 0000 0028 00")


def test_uint16_column():
    result = parse_response(to_bytes(uint16_nulls))
    print (result)


def test_local():
    client = clickhouse_connect.client()
    result = client.query('SELECT * FROM system.tables')
