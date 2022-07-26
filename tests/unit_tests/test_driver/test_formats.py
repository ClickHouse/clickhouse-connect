from clickhouse_connect.datatypes.format import set_default_formats, set_write_format
from clickhouse_connect.datatypes.network import IPv6
from clickhouse_connect.datatypes.numeric import Int32
from clickhouse_connect.datatypes.string import FixedString


def test_default_formats():
    set_default_formats('Int32', 'string', 'IP*', 'string')
    assert IPv6.read_format() == 'string'
    assert Int32.read_format() == 'string'
    assert FixedString.read_format() == 'native'


def test_fixed_str_format():
    set_write_format('FixedString', 'string')
    assert FixedString.write_format() == 'string'
