from sqlalchemy import String
from superset.app import SupersetApp


def test_column_mapping(superset_app: SupersetApp):

    from clickhouse_connect.cc_superset.engine import ClickHouseEngineSpec
    spec = ClickHouseEngineSpec
    column_type, gen_type = spec.get_sqla_column_type('LowCardinality(Nullable(String))')
    assert(isinstance(column_type, String))


