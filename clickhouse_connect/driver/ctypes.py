import logging
import os

import clickhouse_connect.driver.dataconv as pydc
import clickhouse_connect.driver.npconv as pync
from clickhouse_connect.driver.buffer import ResponseBuffer
from clickhouse_connect.driver.common import coerce_bool

logger = logging.getLogger(__name__)

RespBuffCls = ResponseBuffer
data_conv = pydc
numpy_conv = pync

if coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_USE_C', True)):
    try:
        from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer
        import clickhouse_connect.driverc.dataconv as cdc
        data_conv = cdc
        RespBuffCls = CResponseBuffer
        logger.info('Successfully imported ClickHouse Connect C data optimizations')
    except ImportError:
        CResponseBuffer = None
        logger.warning('Unable to connect optimized C data functions, falling back to pure Python', exc_info=True)
    try:
        import clickhouse_connect.driverc.npconv as cnc
        numpy_conv = cnc
        logger.info('Successfully import ClickHouse Connect C/Numpy optimizations')
    except ImportError:
        logger.warning('Unable to connect ClickHouse Connect C to Numpy API, falling back to Pure Python',
                       exc_info=True)
else:
    logger.info('ClickHouse Connect C optimizations disabled')
