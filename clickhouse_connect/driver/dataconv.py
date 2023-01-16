from ipaddress import IPv4Address

from clickhouse_connect.driver.types import ByteSource


def read_ipv4_col(source: ByteSource, num_rows: int):
    column = source.read_array('I', num_rows)
    fast_ip_v4 = IPv4Address.__new__
    new_col = []
    app = new_col.append
    for x in column:
        ipv4 = fast_ip_v4(IPv4Address)
        ipv4._ip = x  # pylint: disable=protected-access
        app(ipv4)
    return new_col
