from typing import Callable

from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig


def test_form_encode_query_basic(client_factory, call, test_config: TestConfig, table_context: Callable):
    """Test that form_encode_query sends parameters as form data"""
    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    with table_context('test_form_encode', ['id UInt32', 'name String', 'value Float64']):
        call(form_client.insert, 'test_form_encode',
                          [[1, 'test1', 10.5],
                           [2, 'test2', 20.3],
                           [3, 'test3', 30.7]])

        result = call(form_client.query,
            'SELECT * FROM test_form_encode WHERE id = {id:UInt32}',
            parameters={'id': 2}
        )
        assert result.row_count == 1
        assert result.first_row[1] == 'test2'

        result = call(form_client.query,
            'SELECT * FROM test_form_encode WHERE name = {name:String} AND value > {val:Float64}',
            parameters={'name': 'test3', 'val': 25.0}
        )
        assert result.row_count == 1
        assert result.first_row[0] == 3


def test_form_encode_with_arrays(client_factory, call, test_config: TestConfig, table_context: Callable):
    """Test form_encode_query with array parameters"""
    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    with table_context('test_form_arrays', ['id UInt32', 'tags Array(String)']):
        call(form_client.insert, 'test_form_arrays',
                          [[1, ['tag1', 'tag2']],
                           [2, ['tag2', 'tag3']],
                           [3, ['tag1', 'tag3']]])

        result = call(form_client.query,
            'SELECT * FROM test_form_arrays WHERE has(tags, {tag:String})',
            parameters={'tag': 'tag3'}
        )
        assert result.row_count == 2

        ids = [1, 3]
        result = call(form_client.query,
            'SELECT * FROM test_form_arrays WHERE id IN {ids:Array(UInt32)}',
            parameters={'ids': ids}
        )
        assert result.row_count == 2
        assert sorted([row[0] for row in result.result_rows]) == [1, 3]


def test_form_encode_raw_query(client_factory, call, test_config: TestConfig):
    """Test form_encode_query with raw_query method"""
    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    result = call(form_client.raw_query,
        'SELECT {a:Int32} + {b:Int32} as sum',
        parameters={'a': 10, 'b': 20}
    )

    assert b'30' in result


def test_form_encode_vs_regular(client_factory, param_client: Client, call, test_config: TestConfig, table_context: Callable):
    """Verify that form_encode_query produces same results as regular parameter handling"""
    regular_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=False
    )

    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    with table_context('test_comparison', ['id UInt32', 'text String', 'score Float64']):
        call(param_client.insert, 'test_comparison',
                          [[i, f'text_{i}', i * 1.5] for i in range(1, 11)])

        query = 'SELECT * FROM test_comparison WHERE id > {min_id:UInt32} AND score < {max_score:Float64} ORDER BY id'
        params = {'min_id': 3, 'max_score': 12.0}

        regular_result = call(regular_client.query, query, parameters=params)
        form_result = call(form_client.query, query, parameters=params)

        assert regular_result.result_rows == form_result.result_rows
        assert regular_result.row_count == form_result.row_count


def test_form_encode_nullable_params(client_factory, call, test_config: TestConfig):
    """Test form_encode_query with nullable parameters"""
    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    result = call(form_client.query,
        'SELECT {val:Nullable(String)} IS NULL as is_null',
        parameters={'val': None}
    )
    assert result.first_row[0] == 1

    result = call(form_client.query,
        'SELECT {val:Nullable(String)} as value',
        parameters={'val': 'test_value'}
    )
    assert result.first_row[0] == 'test_value'


def test_form_encode_schema_probe_query(client_factory, call, test_config: TestConfig, table_context: Callable):
    """Test that schema-probe queries (LIMIT 0) work correctly with form_encode_query_params"""
    form_client = client_factory(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        form_encode_query_params=True
    )

    # Test with a simple LIMIT 0 query
    result = call(form_client.query, 'SELECT name, database, NOW() as dt FROM system.tables LIMIT 0')
    assert result.column_names == ('name', 'database', 'dt')
    assert len(result.column_types) == 3
    assert len(result.result_set) == 0

    # Test with LIMIT 0 and parameters
    with table_context('test_schema_probe', ['id UInt32', 'name String', 'value Float64']):
        result = call(form_client.query,
            'SELECT * FROM test_schema_probe WHERE id = {id:UInt32} LIMIT 0',
            parameters={'id': 1}
        )
        assert result.column_names == ('id', 'name', 'value')
        assert len(result.column_types) == 3
        assert len(result.result_set) == 0

        # Test with complex query and parameters
        result = call(form_client.query,
            'SELECT id, name, value * {multiplier:Float64} as adjusted_value '
            'FROM test_schema_probe '
            'WHERE name = {filter_name:String} LIMIT 0',
            parameters={'multiplier': 2.5, 'filter_name': 'test'}
        )
        assert result.column_names == ('id', 'name', 'adjusted_value')
        assert len(result.column_types) == 3
        assert len(result.result_set) == 0
