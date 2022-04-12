import logging

import clickhouse_connect.datatypes.container
import clickhouse_connect.datatypes.network as dt_network
import clickhouse_connect.datatypes.numeric as dt_numeric
import clickhouse_connect.datatypes.special as dt_special
import clickhouse_connect.datatypes.string as dt_string
import clickhouse_connect.datatypes.temporal
import clickhouse_connect.datatypes.registry

from clickhouse_connect.driver.exceptions import ProgrammingError

# pylint: disable=protected-access
try:
    from clickhouse_connect.driverc import creaders

    dt_string.String._from_native_impl = creaders.read_string_column
    dt_string.FixedString._from_native_str = creaders.read_fixed_string_str
    dt_string.FixedString._from_native_bytes = creaders.read_fixed_string_bytes
except ImportError:
    logging.warning('Unable to connect optimized C driver functions, falling back to pure Python', exc_info=True)


def fixed_string_format(fmt: str, encoding: str = 'utf8'):
    if fmt == 'string':
        dt_string.FixedString.format = 'string'
        dt_string.FixedString.encoding = encoding
    elif fmt == 'bytes':
        dt_string.FixedString.format = 'bytes'
        dt_string.FixedString.encoding = 'utf8'
    else:
        raise ProgrammingError(f'Unrecognized fixed string default format {fmt}')


def big_int_format(fmt: str):
    if fmt in ('string', 'int'):
        dt_numeric.BigInt.format = fmt
    else:
        raise ProgrammingError(f'Unrecognized Big Integer default format {fmt}')


def uint64_format(fmt: str):
    if fmt == 'unsigned':
        dt_numeric.UInt64.format = 'unsigned'
        dt_numeric.UInt64._array_type = 'Q'
        dt_numeric.UInt64.np_format = 'u8'
    elif fmt == 'signed':
        dt_numeric.UInt64.format = 'signed'
        dt_numeric.UInt64._array_type = 'q'
        dt_numeric.UInt64.np_format = 'i8'
    else:
        raise ProgrammingError(f'Unrecognized UInt64 default format {fmt}')


def uuid_format(fmt: str):
    if fmt in ('uuid', 'string'):
        dt_special.UUID.format = fmt
    else:
        raise ProgrammingError(f'Unrecognized UUID default format {fmt}')


def ip_format(fmt: str):
    if fmt in ('string', 'ip'):
        dt_network.IPv4.format = fmt
        dt_network.IPv6.format = fmt
    else:
        raise ProgrammingError(f'Unrecognized IPv4/IPv6 default format {fmt}')
