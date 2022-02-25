from unittest import TestCase

from sqlalchemy import String

from clickhouse_connect.superset.engine import ClickHouseEngineSpec


class TestSupersetTypes(TestCase):

    def test_column_mapping(self):
        spec = ClickHouseEngineSpec
        column_type, gen_type = spec.get_sqla_column_type('Nullable(LowCardinality(String)')
        assert(isinstance(column_type, String))


