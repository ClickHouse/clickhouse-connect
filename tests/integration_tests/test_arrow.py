import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import arrow


def test_arrow(test_client: Client, test_table_engine: str):
    if not arrow:
        pytest.skip('PyArrow package not available')
    arrow_table = test_client.query_arrow('SELECT database, name, total_rows as total_rows FROM system.tables',
                                          use_strings=False)
    arrow_schema = arrow_table.schema
    assert arrow_schema.field(0).name == 'database'
    assert arrow_schema.field(1).type.id == 14
    assert arrow_schema.field(2).type.bit_width == 64
    assert arrow_table.num_rows > 20
    assert len(arrow_table.columns) == 3

    test_client.command('DROP TABLE IF EXISTS test_arrow_insert')
    test_client.command('CREATE TABLE test_arrow_insert (database String, name String, total_rows Nullable(UInt64))' +
                        f'ENGINE {test_table_engine} ORDER BY (database, name)')
    test_client.insert_arrow('test_arrow_insert', arrow_table)
    sum_total_rows = test_client.command('SELECT sum(total_rows) from test_arrow_insert')
    assert sum_total_rows > 5
    arrow_table = test_client.query_arrow('SELECT number from system.numbers LIMIT 500',
                                          settings={'max_block_size': 50})
    arrow_schema = arrow_table.schema
    assert arrow_schema.field(0).name == 'number'
    assert arrow_schema.field(0).type.id == 8
    assert arrow_table.num_rows == 500
