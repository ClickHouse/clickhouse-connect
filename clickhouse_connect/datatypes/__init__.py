import clickhouse_connect.datatypes.container
import clickhouse_connect.datatypes.network
import clickhouse_connect.datatypes.numeric
import clickhouse_connect.datatypes.registry
import clickhouse_connect.datatypes.special
import clickhouse_connect.datatypes.string
import clickhouse_connect.datatypes.temporal


def fixed_string_format(fmt: str, encoding: str = 'utf8'):
    clickhouse_connect.datatypes.string.FixedString.format(fmt, encoding)


def big_int_format(fmt: str):
    clickhouse_connect.datatypes.numeric.BigInt.format(fmt)


def uint64_format(fmt: str):
    clickhouse_connect.datatypes.numeric.UInt64.format(fmt)


def uuid_format(fmt: str):
    clickhouse_connect.datatypes.special.UUID.format(fmt)


def ip_format(fmt: str):
    clickhouse_connect.datatypes.network.IPv4.format(fmt)
    clickhouse_connect.datatypes.network.IPv6.format(fmt)
