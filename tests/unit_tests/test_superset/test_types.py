from sqlalchemy import String
from superset.app import SupersetApp
from superset.utils.core import GenericDataType


# pylint: disable=import-outside-toplevel,unused-argument
def test_column_mapping(superset_app: SupersetApp):

    from clickhouse_connect.cc_superset.engine import ClickHouseEngineSpec
    spec = ClickHouseEngineSpec
    column_type, gen_type = spec.get_sqla_column_type('LowCardinality(Nullable(String))')
    assert isinstance(column_type, String)
    assert gen_type == GenericDataType.STRING
