import clickhouse_connect.datatypes.registry
import clickhouse_connect.datatypes.standard
import clickhouse_connect.datatypes.temporal
import clickhouse_connect.datatypes.special
import clickhouse_connect.datatypes.network


def fixed_string_format(fmt: str, encoding:str = 'utf8'):
    clickhouse_connect.datatypes.special.FixedString.format(fmt, encoding)


def big_int_format(fmt: str):
    clickhouse_connect.datatypes.standard.BigInt.format(fmt)


def uint64_format(fmt: str):
    clickhouse_connect.datatypes.standard.UInt64.format(fmt)


def uuid_format(fmt: str):
    clickhouse_connect.datatypes.special.ChUUID.format(fmt)


def ip_format(fmt: str):
    clickhouse_connect.datatypes.network.ip_format(fmt)