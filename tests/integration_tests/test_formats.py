from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS, HAS_ARROW


def test_arrow(test_client: Client):
    if not HAS_ARROW:
        return
    arrow_schema, arrow_batch = test_client.query_arrow('SELECT database, name, total_rows FROM system.tables')
    assert arrow_schema.field(0).name == 'database'
    assert arrow_schema.field(2).type.id == 8
    assert arrow_schema.field(2).type.bit_width == 64
    assert arrow_batch.num_rows > 20
    assert len(arrow_batch.columns) == 3


def test_numpy(test_client: Client):
    if HAS_NUMPY:
        np_array = test_client.query_np('SELECT * FROM system.tables')
        assert len(np_array['database']) > 10


def test_pandas(test_client: Client, test_table_engine: str):
    if not HAS_PANDAS:
        return
    df = test_client.query_df('SELECT * FROM system.tables')
    test_client.command('DROP TABLE IF EXISTS test_system_insert')
    test_client.command(f'CREATE TABLE test_system_insert as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert')
    assert new_df.columns.all() == df.columns.all()
