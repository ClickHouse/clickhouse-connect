import logging

import clickhouse_connect.datatypes.container
import clickhouse_connect.datatypes.network as dt_network
import clickhouse_connect.datatypes.numeric as dt_numeric
import clickhouse_connect.datatypes.special as dt_special
import clickhouse_connect.datatypes.string as dt_string
import clickhouse_connect.datatypes.temporal
import clickhouse_connect.datatypes.registry


logger = logging.getLogger(__name__)

# pylint: disable=protected-access
try:
    from clickhouse_connect.driverc import creaders

    dt_string.String._read_native_impl = creaders.read_string_column
    dt_string.FixedString._read_native_str = creaders.read_fixed_string_str
    dt_string.FixedString._read_native_bytes = creaders.read_fixed_string_bytes
except ImportError:
    logger.warning('Unable to connect optimized C driver functions, falling back to pure Python', exc_info=True)
