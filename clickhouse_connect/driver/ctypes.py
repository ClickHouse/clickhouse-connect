import logging

import clickhouse_connect.driver.dataconv as pydc
from clickhouse_connect.driver.buffer import ResponseBuffer

logger = logging.getLogger(__name__)

RespBuffCls = ResponseBuffer
data_conv = pydc

try:
    from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer
    #import clickhouse_connect.driverc.dataconv as cdc
    #data_conv = cdc
    RespBuffCls = CResponseBuffer
except ImportError:
    CResponseBuffer = None
    logger.warning('Unable to connect optimized C driver functions, falling back to pure Python', exc_info=True)
