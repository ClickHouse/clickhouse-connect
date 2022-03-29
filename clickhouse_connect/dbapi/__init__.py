from clickhouse_connect.dbapi.connection import Connection


apilevel = '2.0'         # PEP 249  DB API level
threadsafety = 2         # PEP 249  Threads may share the module and connections.
paramstyle = 'pyformat'  # PEP 249  Python extended format codes, e.g. ...WHERE name=%(name)s


class Error(Exception):
    pass


def connect(**kwargs):
    return Connection(**kwargs)
