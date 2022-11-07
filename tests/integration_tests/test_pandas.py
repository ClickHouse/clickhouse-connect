import math
from datetime import datetime, date

import pytest

from clickhouse_connect.driver import Client, ProgrammingError
from clickhouse_connect.driver.options import pd

pandas_data = [['key1', 1000, 77.3, 'value1', datetime(2022, 10, 15, 10, 3, 2), None],
               ['key2', 2000, 882.00, None, None, date(1976, 5, 5)],
               ['key3', None, math.nan, 'value3', date(2022, 7, 4), date(1999, 12, 31)]]


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
    df = pd.DataFrame(pandas_data, columns=['key', 'num', 'flt', 'str', 'dt', 'd'])
    test_client.command('CREATE TABLE test_pandas (key String, num Int32, flt Float32, str String, dt DateTime, ' +
                        f'day_col Date) ENGINE {test_table_engine} ORDER BY (key)')
    insert_columns = ['key', 'num', 'flt', 'str', 'dt', 'day_col']
    try:
        test_client.insert_df('test_pandas', df, insert_columns=insert_columns)
    except ProgrammingError:
        pass
    test_client.command('DROP TABLE IF EXISTS test_pandas')
    test_client.command('CREATE TABLE test_pandas (key String, num Nullable(Int32), flt Nullable(Float32), '
                        'str Nullable(String), dt Nullable(DateTime), day_col Nullable(Date)) ' +
                        f'ENGINE {test_table_engine} ORDER BY (key)')
    test_client.insert_df('test_pandas', df, insert_columns=insert_columns)
    result_df = test_client.query_df('SELECT * FROM test_pandas')
    assert result_df.iloc[0]['num'] == 1000
    assert result_df.iloc[1]['day_col'] == pd.Timestamp(year=1976, month=5, day=5)
    assert pd.isna(result_df.iloc[2]['flt'])
    assert result_df.iloc[2]['str'] == 'value3'


def test_pandas_context_inserts(test_client: Client, test_table_engine: str):
    if not pd:
        pytest.skip('Pandas package not available')
    test_client.command('CREATE TABLE test_pandas_multiple (key String, num Nullable(Int32), flt Nullable(Float32), '
                        'str Nullable(String), dt Nullable(DateTime), day_col Nullable(Date)) ' +
                        f'ENGINE {test_table_engine} ORDER BY (key)')
    # df = pd.DataFrame(pandas_data, columns=['key', 'num', 'flt', 'str', 'dt', 'day_col'])
