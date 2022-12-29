from typing import Dict, Any, Union

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver import create_client
from clickhouse_connect.driver.query import QueryResult


class Connection:
    """
    See :ref:`https://peps.python.org/pep-0249/`
    """
    def __init__(self,
                 dsn: str = None,
                 username: str = None,
                 password: str = None,
                 host: str = None,
                 database: str = None,
                 interface: str = None,
                 port: int = 0,
                 secure: Union[bool, str] = False,
                 settings: Dict[str, Any] = None):
        self.client = create_client(host=host,
                                    username=username,
                                    password=password,
                                    database=database,
                                    interface=interface,
                                    port=port,
                                    secure=secure,
                                    dsn=dsn,
                                    settings=settings or {})
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
