from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver import create_driver


class Connection:
    def __init__(self, **kwargs):
        self.driver = create_driver(**kwargs)

    def close(self):
        self.driver.close()

    def commit(self):
        pass

    def rollback(self):
        pass

    def cursor(self):
        return Cursor(self.driver)
