from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.native import build_insert
from tests.helpers import to_bytes


LOW_CARD_OUTPUT = """
0101 0576 616c 7565 204c 6f77 4361 7264
696e 616c 6974 7928 4e75 6c6c 6162 6c65
2853 7472 696e 6729 2901 0000 0000 0000
0000 0600 0000 0000 0002 0000 0000 0000
0000 0574 6872 6565 0100 0000 0000 0000
01
"""


def test_low_card_null():
    data = [['three']]
    names = ['value']
    types = [get_from_name('LowCardinality(Nullable(String))')]
    output = build_insert(data, column_names=names, column_types=types)
    assert to_bytes(LOW_CARD_OUTPUT) == output
