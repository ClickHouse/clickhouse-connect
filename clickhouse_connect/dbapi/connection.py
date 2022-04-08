from urllib.parse import urlparse, parse_qs

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver import create_driver

# pylint: disable=too-many-arguments
class Connection:
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
        self.driver = create_driver(host, username, password, database, interface, port, **settings)

    def close(self):
        self.driver.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def command(self, cmd: str):
        result = self.driver.command(cmd)
        try:
            return int(result)
        except ValueError:
            return result

    def cursor(self):
        return Cursor(self.driver)
