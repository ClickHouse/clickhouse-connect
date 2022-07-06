from clickhouse_connect.datatypes.network import IPv6
from clickhouse_connect.datatypes.numeric import Int32
from clickhouse_connect.datatypes.string import FixedString
from clickhouse_connect.driver.transform import FormatControl


def test_format_control():
    fmt_ctl = FormatControl(default_formats={'Int32': 'string'}, read_formats={'IP*': 'string'})
    assert fmt_ctl.read_format(IPv6) == 'string'
    assert fmt_ctl.write_format(Int32) == 'string'
    assert fmt_ctl.read_format(FixedString) == 'native'
