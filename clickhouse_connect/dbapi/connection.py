from urllib.parse import urlparse, parse_qs

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver import create_client

# pylint: disable=too-many-arguments
from clickhouse_connect.driver.query import QueryResult


class Connection:
    """
    See :ref:`https://peps.python.org/pep-0249/`
    """
    def __init__(self, dsn: str = None, username: str = None, password: str = None, host: str = None,
                 database: str = None, interface: str = None, port: int = 0, **kwargs):
        settings = kwargs.copy()
        if dsn:
            parsed = urlparse(dsn)
            username = username or parsed.username
            password = password or parsed.password
            host = host or parsed.hostname
            port = port or parsed.port
            if parsed.path and not database:
                database = parsed.path[1:].split('/')[0]
            database = database or parsed.path
            settings = dict(parse_qs(parsed.query)).update(settings)
        self.client = create_client(host, username, password, database, interface, port, **settings)
        self.timezone = self.client.server_tz

    def close(self):
        self.client.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def command(self, cmd: str):
        return self.client.command(cmd)

    def raw_query(self, query: str) -> QueryResult:
        return self.client.query(query)

    def cursor(self):
        return Cursor(self.client)
