import logging

from typing import Optional, Sequence

from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver import BaseClient
from clickhouse_connect.driver.parser import parse_callable


class Cursor:
    def __init__(self, client: BaseClient):
        self.client = client
        self.arraysize = 1
        self.data: Optional[Sequence] = None
        self.names = []
        self.types = []
        self._rowcount = 0
        self._ix = 0

    def check_valid(self):
        if self.data is None:
            raise ProgrammingError('Cursor is not valid')

    @property
    def description(self):
        return [(n, t.name, None, None, None, None, True) for n, t in zip(self.names, self.types)]

    @property
    def rowcount(self):
        return self._rowcount

    def close(self):
        self.data = None

    def execute(self, operation, parameters=None):
        query_result = self.client.query(operation, parameters)
        self.data = query_result.result_set
        self.names = query_result.column_names
        self.types = query_result.column_types
        self._rowcount = len(self.data)

    def _check_insert(self, operation, data):
        if not operation.upper().startswith('INSERT INTO '):
            return False
        temp = operation[11:].strip()
        table_end = min(temp.find(' '), temp.find('('))
        table = temp[:table_end].strip()
        temp = temp[table_end:].strip()
        if temp[0] == '(':
            _, col_names, temp = parse_callable(temp)
        else:
            col_names = '*'
        if 'VALUES' not in temp.upper():
            return False
        self.client.insert(table, data, col_names)
        return True

    def executemany(self, operation, parameters):
        #if self._check_insert(operation, parameters):
            #return
        self.data = []
        try:
            for param_row in parameters:
                query_result = self.client.query(operation, param_row)
                self.data.extend(query_result.result_set)
                if self.names or self.types:
                    if query_result.column_names != self.names:
                        logging.warning('Inconsistent column names %s : %s for operation %s in cursor executemany',
                                        self.names, query_result.column_names, operation)
                else:
                    self.names = query_result.column_names
                    self.types = query_result.column_types
        except TypeError as ex:
            raise ProgrammingError(f'Invalid parameters {parameters} passed to cursor executemany') from ex
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
