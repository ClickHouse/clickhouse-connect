from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.httpdriver import HttpDriver


class Connection:

    def __init__(self, *args, **kwargs):
        self.driver = HttpDriver(*args, **kwargs)

    def close(self):
        self.driver.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self.driver)
