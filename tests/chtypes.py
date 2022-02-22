import types
from sqlalchemy.types import TypeEngine
from unittest import TestCase

from sqlalchemy import DATE

from click_alchemy.chtypes.typelist import ClickHouseType


class TestIntClass(ClickHouseType):
    @classmethod
    def from_row_binary(cls, source, loc):
        return source[loc], loc + 1

class TestDateClass(DATE):
    __visit_name__ = 'test_date'


class ClickHouseTypeTest(TestCase):

    def test_rtti(self):
        tc = TestDateClass()
        assert (isinstance(tc, TypeEngine))

    def test_inheritance(self):
        cls = TestIntClass
        sqla_type = TestDateClass()

        class Wrapper(type(sqla_type)):
            def literal_processor(self, dialect):
                def process(value):
                    return super().process(value)

                return process

        Wrapper()

        sqla_cls = types.new_class('TestSQLA', (TestDateClass, ), {})

        class DynamicWrapper(type(sqla_cls())):
            pass

        DynamicWrapper()
