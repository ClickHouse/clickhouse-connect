import pkg_resources

from clickhouse_connect.driver import create_client
from clickhouse_connect.entry_points import validate_entrypoints

driver_name = 'clickhousedb'


def get_client(**kwargs):
    return create_client(**kwargs)


def check_ep():
    assert validate_entrypoints() == 0


def version():
    return pkg_resources.get_distribution('clickhouse-connect').version
