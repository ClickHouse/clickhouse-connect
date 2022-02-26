class Cursor:
    def __init__(self, driver):
        self.driver = driver
        self._reset()

    def _reset(self):
        self.arraysize = 1
        self._data= None
        self._names = None
        self._types = None
        self._index = 0
        self._operation = None
        self._rowcount = -1

    def _check_valid(self):
        if self._data is None:
            raise Error("Cursor is not valid")

    @property
    def description(self):
        return [(n, t.name, None, None, None, None, True) for n, t in zip(self._names, self._types)]
    
    @property
    def rowcount(self):
        return self._rowcount

    def close(self):
        self._reset()

    def execute(self, operation, parameters=None, context=None):
        self._reset()
        self._operation = operation
        self._data, self._names, self._types = self.driver.query(operation)
        self._rowcount = len(self._data)

    def fetchall(self):
        self._check_valid()
        ret = self._data[self._index:self._rowcount]
        self._index = self._rowcount
        return ret

    def fetchone(self):
        self._check_valid()
        if self._index >= self._rowcount:
            return None
        ret = self._data[self._index]
        self._index += 1
        return ret

    def fetchmany(self, size:int = -1):
        self._check_valid()
        sz = max(size, self.arraysize)
        end = max(self._index + sz, self._rowcount)
        ret = self[self.index:end]
        self._index = end
        return ret

    def nextset(self):
        raise NotImplementedError

    def callproc(self, *args, **kwargs):
        raise NotImplementedError
