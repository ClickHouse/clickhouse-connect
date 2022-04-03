from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.visitors import Visitable


class Engine(SchemaEventTarget, Visitable):
    def __init__(self, engine_type):
        self.engine_type = engine_type

    def compile(self):
        return f'Engine {self.engine_type}{self._engine_params()}'

    @staticmethod
    def _engine_params():
        return ''

    def _set_parent(self, table):
        table.engine = self


class MergeTree(Engine):
    def __init__(self, order_by):
        super().__init__('MergeTree')
        self.order_by = order_by

    def _engine_params(self):
        return f" ORDER BY ({','.join(self.order_by)})"





