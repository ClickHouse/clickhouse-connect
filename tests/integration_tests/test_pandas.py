import math
from datetime import datetime, date

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import Client, ProgrammingError
from clickhouse_connect.driver.options import pd

str_type = get_from_name('String')
int32_type = get_from_name('Int32')
dt_type = get_from_name('DateTime')
float_type = get_from_name('Float32')


def test_pandas_basic(test_client: Client, test_table_engine: str):
    if not pd:
        pytest.skip('Pandas package not available')
    df = test_client.query_df('SELECT * FROM system.tables')
    test_client.command('DROP TABLE IF EXISTS test_system_insert')
    test_client.command(f'CREATE TABLE test_system_insert as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert')
    assert new_df.columns.all() == df.columns.all()


def test_pandas_nulls(test_client: Client, test_table_engine: str):
    if not pd:
        pytest.skip('Pandas package not available')
    nulls_data = [['key1', 1000, 77.3, 'value1', datetime(2022, 10, 15, 10, 3, 2)],
                  ['key2', 2000, 882.00, None, None],
                  ['key3', None, math.nan, 'value3', date(2022, 7, 4)]]
    df = pd.DataFrame(nulls_data, columns=['key', 'num', 'flt', 'str', 'dt'])
    test_client.command('CREATE TABLE test_pandas (key String, num Int32, flt Float32, str String, dt DateTime)' +
                        f' ENGINE {test_table_engine} ORDER BY (key)')
    try:
        test_client.insert_df('test_pandas', df)
    except ProgrammingError:
        pass
    test_client.command('DROP TABLE IF EXISTS test_pandas')
    test_client.command('CREATE TABLE test_pandas (key String, num Nullable(Int32), flt Nullable(Float32), '
                        'str Nullable(String), dt Nullable(DateTime)) ' +
                        f'ENGINE {test_table_engine} ORDER BY (key)')
    test_client.insert_df('test_pandas', df)
    result_df = test_client.query_df('SELECT * FROM test_pandas')
    print (result_df)
