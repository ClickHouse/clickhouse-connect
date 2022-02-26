from clickhouse_connect.dbapi.exceptions import ProgrammingError


class Cursor:
    def __init__(self, driver):
        self.driver = driver
        self.arraysize = 1
        self.data= None
        self.names = []
        self.types = []
        self._rowcount = -1

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

    def execute(self, operation, parameters=None, context=None):
        self.data, self.names, self.types = self.driver.query(operation)
        self._rowcount = len(self.data)

    def fetchall(self):
        self.check_valid()
        ret = self.data
        self.data = []
        return ret

    def fetchone(self):
        self.check_valid()
        if not self.data:
            return None
        return self.data.pop(0)

    def fetchmany(self, size:int = -1):
        self.check_valid()
        sz = max(size, self.arraysize)
        ret = self.data[:sz]
        self.data = self.data[sz:]
        return ret

    def nextset(self):
        raise NotImplementedError

    def callproc(self, *args, **kwargs):
        raise NotImplementedError
