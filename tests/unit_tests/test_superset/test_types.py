from sqlalchemy import String
from superset.app import SupersetApp


def test_column_mapping(superset_app: SupersetApp):

    from clickhouse_connect.superset.engine import ClickHouseEngineSpec
    spec = ClickHouseEngineSpec
    column_type, gen_type = spec.get_sqla_column_type('Nullable(LowCardinality(String))')
    assert(isinstance(column_type, String))


