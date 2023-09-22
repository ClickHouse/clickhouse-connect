from clickhouse_connect.driver import create_client


driver_name = 'clickhousedb'


def get_client(**kwargs):
    return create_client(**kwargs)
