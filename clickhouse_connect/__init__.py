from clickhouse_connect.driver import create_driver

# pylint: disable=invalid-name
driver_name = 'clickhousedb'


def client(**kwargs):
    return create_driver(**kwargs)
