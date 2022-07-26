import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS, HAS_ARROW


def test_arrow(test_client: Client):
    if not HAS_ARROW:
        pytest.skip('PyArrow package not available')
    arrow_table = test_client.query_arrow('SELECT database, name, total_rows FROM system.tables')
    arrow_schema = arrow_table.schema
    assert arrow_schema.field(0).name == 'database'
    assert arrow_schema.field(1).type.id == 13
    assert arrow_schema.field(2).type.bit_width == 64
    assert arrow_table.num_rows > 20
    assert len(arrow_table.columns) == 3

    arrow_table = test_client.query_arrow('SELECT number from system.numbers LIMIT 500',
                                          settings={'max_block_size': 50})
    arrow_schema = arrow_table.schema
    assert arrow_schema.field(0).name == 'number'
    assert arrow_schema.field(0).type.id == 8
    assert arrow_table.num_rows == 500


def test_numpy(test_client: Client):
    if not HAS_NUMPY:
        pytest.skip('Numpy package not available')
    np_array = test_client.query_np('SELECT * FROM system.tables')
    assert len(np_array['database']) > 10


def test_pandas(test_client: Client, test_table_engine: str):
    if not HAS_PANDAS:
        pytest.skip('Pandas package not available')
    df = test_client.query_df('SELECT * FROM system.tables')
    test_client.command('DROP TABLE IF EXISTS test_system_insert')
    test_client.command(f'CREATE TABLE test_system_insert as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert')
    assert new_df.columns.all() == df.columns.all()
