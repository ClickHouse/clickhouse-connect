import sys

from clickhouse_connect.driver import create_client, create_async_client


if sys.version_info < (3, 10):
    raise RuntimeError(
        "clickhouse-connect 1.0+ requires Python 3.10 or later. "
        "Python 3.9 users should pin to clickhouse-connect<1.0."
    )


driver_name = 'clickhousedb'

get_client = create_client
get_async_client = create_async_client
