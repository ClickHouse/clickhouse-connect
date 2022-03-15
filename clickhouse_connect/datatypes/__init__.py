import clickhouse_connect.datatypes.registry
import clickhouse_connect.datatypes.standard
import clickhouse_connect.datatypes.special
import clickhouse_connect.datatypes.network

from clickhouse_connect.datatypes.standard import String, UInt64, int64_signed, int64_unsigned


def fixed_string_format(method: str, encoding: str = 'utf8', encoding_error:str = 'hex'):
    special.fixed_string_format(method, encoding, encoding_error)


def uint64_format(method: str):
    if method.lower() == 'signed':
        UInt64.from_row_binary = int64_signed
    elif method.lower() == 'unsigned':
        UInt64.from_row_binary = int64_unsigned
    else:
        raise ValueError(f"Unknown UInt64 format option {method}")


def string_encoding(encoding: str):
    String._encoding = encoding


def ip_format(fmt: str):
    network.ip_format(fmt)
