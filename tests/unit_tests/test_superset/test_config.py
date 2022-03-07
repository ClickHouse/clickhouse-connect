from superset.app import SupersetApp


def test_build_uri(superset_app: SupersetApp):
    from clickhouse_connect.superset.engine import ClickHouseEngineSpec
    spec = ClickHouseEngineSpec
    parameters = {
        'username': 'ClickHouse',
        'password': 'password',
        'host': 'localhost'
    }
    url = spec.build_sqlalchemy_uri(parameters)
    assert url == 'clickhousedb+connect://ClickHouse:password@localhost/__default__'


def test_json_schema(superset_app: SupersetApp):
    from clickhouse_connect.superset.engine import ClickHouseEngineSpec
    spec = ClickHouseEngineSpec
    json_schema = spec.parameters_json_schema()
    assert json_schema['properties']['port']['maximum'] == 65535


