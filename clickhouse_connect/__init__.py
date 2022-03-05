from clickhouse_connect.driver import create_driver

driver_name = 'clickhousedb'


def client(**kwargs):
    return create_driver(**kwargs)
