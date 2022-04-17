from collections.abc import Sequence
from typing import Optional

from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver import BaseClient


class Cursor:
    def __init__(self, driver: BaseClient):
        self.driver = driver
        self.arraysize = 1
        self.data: Optional[Sequence] = None
        self.names = []
        self.types = []
        self._rowcount = 0
        self._ix = 0

    def check_valid(self):
        if self.data is None:
            raise ProgrammingError("Cursor is not valid")

    @property
    def description(self):
        return [(n, t.name, None, None, None, None, True) for n, t in zip(self.names, self.types)]

    @property
    def rowcount(self):
        return self._rowcount

    def close(self):
        self.data = None

    def execute(self, operation, *_):
        query_result = self.driver.query(operation)
        self.data = query_result.result_set
        self.names = query_result.column_names
        self.types = query_result.column_types
        self._rowcount = len(self.data)

    def fetchall(self):
        self.check_valid()
        ret = self.data
        self._ix = self._rowcount
        return ret

    def fetchone(self):
        self.check_valid()
        if self._ix >= self._rowcount:
            return None
        val = self.data[self._ix]
        self._ix += 1
        return val

    def fetchmany(self, size: int = -1):
        self.check_valid()
        end = self._ix + max(size, self._rowcount - self._ix)
        ret = self.data[self._ix: end]
        self._ix = end
        return ret

    def nextset(self):
        raise NotImplementedError

    def callproc(self, *args, **kwargs):
        raise NotImplementedError
