from clickhouse_connect.datatypes.format import clear_all_formats, set_default_formats
from clickhouse_connect.datatypes.network import IPv6
from clickhouse_connect.datatypes.numeric import Int32
from clickhouse_connect.datatypes.string import FixedString


def test_default_formats():
    clear_all_formats()
    set_default_formats('Int32', 'string', 'IP*', 'string')
    assert IPv6.read_format() == 'string'
    assert Int32.read_format() == 'string'
    assert FixedString.read_format() == 'native'
