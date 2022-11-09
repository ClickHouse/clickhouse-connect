from datetime import datetime, date
from typing import Callable

import pytest

from clickhouse_connect.driver import Client, ProgrammingError
from clickhouse_connect.driver.options import pd
from tests.integration_tests.datasets import null_ds, null_ds_columns, null_ds_types

pytestmark = pytest.mark.skipif(pd is None, reason='Pandas package not installed')


def test_pandas_basic(test_client: Client, test_table_engine: str):
    df = test_client.query_df('SELECT * FROM system.tables')
    test_client.command('DROP TABLE IF EXISTS test_system_insert_pd')
    test_client.command(f'CREATE TABLE test_system_insert_pd as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert_pd', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert_pd')
    test_client.command('DROP TABLE IF EXISTS test_system_insert_pd')
    assert new_df.columns.all() == df.columns.all()


def test_pandas_nulls(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_pandas')
    df = pd.DataFrame(null_ds, columns=['key', 'num', 'flt', 'str', 'dt', 'd'])
    test_client.command('CREATE TABLE test_pandas (key String, num Int32, flt Float32, str String, dt DateTime, ' +
                        f'day_col Date) ENGINE {test_table_engine} ORDER BY (key)')
    insert_columns = ['key', 'num', 'flt', 'str', 'dt', 'day_col']
    try:
        test_client.insert_df('test_pandas', df, column_names=insert_columns)
    except ProgrammingError:
        pass
    test_client.command('DROP TABLE IF EXISTS test_pandas')
    test_client.command('CREATE TABLE test_pandas (key String, num Nullable(Int32), flt Nullable(Float32), '
                        'str Nullable(String), dt Nullable(DateTime), day_col Nullable(Date)) ' +
                        f'ENGINE {test_table_engine} ORDER BY (key)')
    test_client.insert_df('test_pandas', df, column_names=insert_columns)
    result_df = test_client.query_df('SELECT * FROM test_pandas')
    test_client.command('DROP TABLE IF EXISTS test_pandas')
    assert result_df.iloc[0]['num'] == 1000
    assert result_df.iloc[1]['day_col'] == pd.Timestamp(year=1976, month=5, day=5)
    assert pd.isna(result_df.iloc[2]['flt'])
    assert result_df.iloc[2]['str'] == 'value3'


def test_pandas_context_inserts(test_client: Client, table_context: Callable):
    with table_context('test_pandas_multiple', null_ds_columns, null_ds_types):
        df = pd.DataFrame(null_ds, columns=null_ds_columns)
        insert_context = test_client.create_insert_context('test_pandas_multiple', df.columns)
        insert_context.data = df
        test_client.data_insert(insert_context)
        assert test_client.command('SELECT count() FROM test_pandas_multiple') == 3
        next_df = pd.DataFrame(
            [['key4', -415, None, 'value4', datetime(2022, 7, 4, 15, 33, 4, 5233), date(1999, 12, 31)]],
            columns=null_ds_columns)
        test_client.insert_df(df=next_df, context=insert_context)
        assert test_client.command('SELECT count() FROM test_pandas_multiple') == 4


def test_pandas_large_types(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_pandas_big_int')
    test_client.command('CREATE TABLE IF NOT EXISTS test_pandas_big_int (key String, value Int256)' +
                        f' Engine {test_table_engine} ORDER BY key')
    df = pd.DataFrame([['key1', 2000, ], ['key2', 30000000000000000000000000000000000]], columns=['key', 'value'])
    test_client.insert_df('test_pandas_big_int', df)
    result_df = test_client.query_df('SELECT * FROM test_pandas_big_int')
    assert result_df.iloc[0]['value'] == 2000
    assert result_df.iloc[1]['value'] == 30000000000000000000000000000000000


def test_pandas_datetime64(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_pandas_dt64')
    test_client.command('CREATE TABLE IF NOT EXISTS test_pandas_dt64 (key String, value DateTime64(9))' +
                        f' Engine {test_table_engine} ORDER BY key')
    now = datetime.now()
    df = pd.DataFrame([['key1', now], ['key2', pd.Timestamp(1992, 11, 6, 12, 50, 40, 7420, 44)]],
                      columns=['key', 'value'])
    test_client.insert_df('test_pandas_dt64', df)
    result_df = test_client.query_df('SELECT * FROM test_pandas_dt64')
    assert result_df.iloc[0]['value'] == now
    # Note that nanoseconds are lost because of the Python datetime conversion
    assert result_df.iloc[1]['value'] == datetime(1992, 11, 6, 12, 50, 40, 7420)
