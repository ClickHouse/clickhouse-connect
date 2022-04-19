from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable


class TableEngine(SchemaEventTarget, Visitable):

    def compile(self):
        return f'Engine {self.__class__.__name__}{self._engine_params()}'

    def _set_parent(self, parent):
        parent.engine = self

    def _engine_params(self):
        raise NotImplementedError


class MergeTree(TableEngine):
    def __init__(self, order_by):
        self.order_by = [order_by] if isinstance(order_by, str) else order_by

    def _engine_params(self):
        return f" ORDER BY ({','.join(self.order_by)})"
