from datetime import date
from typing import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import arrow


def test_arrow(test_client: Client, table_context: Callable):
    if not arrow:
        pytest.skip('PyArrow package not available')
    if not test_client.min_version('21'):
        pytest.skip(f'PyArrow is not supported in this server version {test_client.server_version}')
    with table_context('test_arrow_insert', ['animal String', 'legs Int64']):
        n_legs = arrow.array([2, 4, 5, 100])
        animals = arrow.array(['Flamingo', 'Horse', 'Brittle stars', 'Centipede'])
        names = ['legs', 'animal']
        insert_table = arrow.Table.from_arrays([n_legs, animals], names=names)
        test_client.insert_arrow('test_arrow_insert', insert_table)
        result_table = test_client.query_arrow('SELECT * FROM test_arrow_insert', use_strings=False)
        arrow_schema = result_table.schema
        assert arrow_schema.field(0).name == 'animal'
        assert arrow_schema.field(0).type.id == 14
        assert arrow_schema.field(1).type.bit_width == 64
        # pylint: disable=no-member
        assert arrow.compute.sum(result_table['legs']).as_py() == 111
        assert len(result_table.columns) == 2

    arrow_table = test_client.query_arrow('SELECT number from system.numbers LIMIT 500',
                                          settings={'max_block_size': 50})
    arrow_schema = arrow_table.schema
    assert arrow_schema.field(0).name == 'number'
    assert arrow_schema.field(0).type.id == 8
    assert arrow_table.num_rows == 500


def test_arrow_map(test_client: Client, table_context: Callable):
    if not arrow:
        pytest.skip('PyArrow package not available')
    if not test_client.min_version('21'):
        pytest.skip(f'PyArrow is not supported in this server version {test_client.server_version}')
    with table_context('test_arrow_map', ['trade_date Date, code String',
                                          'kdj Map(String, Float32)',
                                          'update_time DateTime DEFAULT now()']):
        data = [[date(2023, 10, 15), 'C1', {'k': 2.5, 'd': 0, 'j': 0}],
                [date(2023, 10, 16), 'C2', {'k': 3.5, 'd': 0, 'j': -.372}]]
        insert_result = test_client.insert('test_arrow_map', data, column_names=('trade_date', 'code', 'kdj'))
        assert 2 == insert_result.written_rows
        arrow_table = test_client.query_arrow('SELECT * FROM test_arrow_map ORDER BY trade_date',
                                              use_strings=True)
        print(arrow_table)
        assert isinstance(arrow_table.schema, arrow.Schema)
        insert_result = test_client.insert_arrow('test_arrow_map', arrow_table)
        assert 4 == test_client.command('SELECT count() FROM test_arrow_map')
        assert 2 == insert_result.written_rows
