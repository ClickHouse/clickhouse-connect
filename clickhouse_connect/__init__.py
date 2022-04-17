from clickhouse_connect.driver import create_client

# pylint: disable=invalid-name
driver_name = 'clickhousedb'


def client(**kwargs):
    return create_client(**kwargs)
