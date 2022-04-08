from clickhouse_connect.driverc.creaders import read_string_column
from tests.helpers import to_bytes


def test_read_string():
    source = '1F 41 20 6c 6f 76 65 6c 79 20 73 74 72 69 6e 67 20 77 69 74 68 20 66 72 75 69 74 20 f0 9f a5 9d'
    column, loc = read_string_column(to_bytes(source), 0, 1, 'UTF-8')
    value = 'A lovely string with fruit 🥝'
    assert column[0] == value
    assert loc == 32
