import clickhouse_connect.datatypes.registry
import clickhouse_connect.datatypes.standard
import clickhouse_connect.datatypes.special
import clickhouse_connect.datatypes.network

from clickhouse_connect.datatypes.standard import String, UInt64


def fixed_string_format(method: str, encoding: str = 'utf8', encoding_error:str = 'hex'):
    special.fixed_string_format(method, encoding, encoding_error)


def uint64_format(method: str):
    if method.lower() == 'signed':
        pass


def string_encoding(encoding: str):
    String._encoding = encoding


def ip_format(fmt: str):
    network.ip_format(fmt)
