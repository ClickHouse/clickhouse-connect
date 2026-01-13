import os
import random
from datetime import datetime, date, timedelta, time
from typing import Callable
from io import StringIO

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.options import np, pd
from tests.helpers import random_query
from tests.integration_tests.conftest import TestConfig
from tests.integration_tests.datasets import null_ds, null_ds_columns, null_ds_types

pytestmark = pytest.mark.skipif(pd is None, reason='Pandas package not installed')


def test_pandas_basic(param_client: Client, call, test_table_engine: str):
    df = call(param_client.query_df, 'SELECT * FROM system.tables')
    source_df = df.copy()
    call(param_client.command, 'DROP TABLE IF EXISTS test_system_insert_pd')
    call(param_client.command, f'CREATE TABLE test_system_insert_pd as system.tables Engine {test_table_engine}'
                               f' ORDER BY (database, name)')
    call(param_client.insert_df, 'test_system_insert_pd', df)
    new_df = call(param_client.query_df, 'SELECT * FROM test_system_insert_pd')
    call(param_client.command, 'DROP TABLE IF EXISTS test_system_insert_pd')
    assert new_df.columns.all() == df.columns.all()
    assert df.equals(source_df)
    df = call(param_client.query_df, "SELECT * FROM system.tables WHERE engine = 'not_a_thing'")
    assert len(df) == 0
    assert isinstance(df, pd.DataFrame)


def test_pandas_nulls(param_client: Client, call, table_context: Callable):
    df = pd.DataFrame(null_ds, columns=['key', 'num', 'flt', 'str', 'dt', 'd'])
    source_df = df.copy()
    insert_columns = ['key', 'num', 'flt', 'str', 'dt', 'day_col']
    with table_context('test_pandas_nulls_bad', ['key String', 'num Int32', 'flt Float32',
                                                 'str String', 'dt DateTime', 'day_col Date']):

        with pytest.raises(DataError):
            call(param_client.insert_df, 'test_pandas_nulls_bad', df, column_names=insert_columns)

    with table_context('test_pandas_nulls_good',
                       ['key String', 'num Nullable(Int32)', 'flt Nullable(Float32)',
                        'str Nullable(String)', "dt Nullable(DateTime('America/Denver'))", 'day_col Nullable(Date)']):
        call(param_client.insert_df, 'test_pandas_nulls_good', df, column_names=insert_columns)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_nulls_good')
        assert result_df.iloc[0]['num'] == 1000
        assert pd.isna(result_df.iloc[2]['num'])
        assert result_df.iloc[1]['day_col'] == pd.Timestamp(year=1976, month=5, day=5)
        assert pd.isna(result_df.iloc[0]['day_col'])
        assert pd.isna(result_df.iloc[1]['dt'])
        assert pd.isna(result_df.iloc[2]['flt'])
        assert pd.isna(result_df.iloc[2]['num'])
        assert pd.isnull(result_df.iloc[3]['flt'])
        assert result_df['num'].dtype.name == 'Int32'
        if param_client.protocol_version:
            assert isinstance(result_df['dt'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        assert result_df.iloc[2]['str'] == 'value3'
        assert df.equals(source_df)


def test_pandas_all_null_float(param_client: Client, call):
    df = call(param_client.query_df, "SELECT number, cast(NULL, 'Nullable(Float64)') as flt FROM numbers(500)")
    assert df['flt'].dtype.name == 'float64'


def test_pandas_csv(param_client: Client, call, table_context: Callable):
    csv = """
key,num,flt,str,dt,d
key1,555,25.44,string1,2022-11-22 15:00:44,2001-02-14
key2,6666,,string2,,
"""
    csv_file = StringIO(csv)
    df = pd.read_csv(csv_file, parse_dates=['dt', 'd'])
    df = df[['num', 'flt']].astype('Float32')
    source_df = df.copy()
    with table_context('test_pandas_csv', null_ds_columns, null_ds_types):
        call(param_client.insert_df, 'test_pandas_csv', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_csv')
        assert np.isclose(result_df.iloc[0]['flt'], 25.44)
        assert pd.isna(result_df.iloc[1]['flt'])
        result_df = call(param_client.query, 'SELECT * FROM test_pandas_csv')
        assert pd.isna(result_df.result_set[1][2])
        assert df.equals(source_df)


def test_pandas_context_inserts(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_multiple', null_ds_columns, null_ds_types):
        df = pd.DataFrame(null_ds, columns=null_ds_columns)
        source_df = df.copy()
        insert_context = call(param_client.create_insert_context, 'test_pandas_multiple', df.columns)
        insert_context.data = df
        call(param_client.data_insert, insert_context)
        assert call(param_client.command, 'SELECT count() FROM test_pandas_multiple') == 4
        next_df = pd.DataFrame(
            [['key4', -415, None, 'value4', datetime(2022, 7, 4, 15, 33, 4, 5233), date(1999, 12, 31)]],
            columns=null_ds_columns)
        call(param_client.insert_df, df=next_df, context=insert_context)
        assert call(param_client.command, 'SELECT count() FROM test_pandas_multiple') == 5
        assert df.equals(source_df)


def test_pandas_low_card(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_low_card', ['key String',
                                                'value LowCardinality(Nullable(String))',
                                                'date_value LowCardinality(Nullable(DateTime))',
                                                'int_value LowCardinality(Nullable(Int32))']):
        df = pd.DataFrame([
            ['key1', 'test_string_0', datetime(2022, 10, 15, 4, 25), -372],
            ['key2', 'test_string_1', datetime.now(), 4777288],
            ['key3', None, datetime.now(), 4777288],
            ['key4', 'test_string', pd.NaT, -5837274],
            ['key5', pd.NA, pd.NA, None]
        ],
            columns=['key', 'value', 'date_value', 'int_value'])
        source_df = df.copy()
        call(param_client.insert_df, 'test_pandas_low_card', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_low_card', use_none=True)
        assert result_df.iloc[0]['value'] == 'test_string_0'
        assert result_df.iloc[1]['value'] == 'test_string_1'
        assert result_df.iloc[0]['date_value'] == pd.Timestamp(2022, 10, 15, 4, 25)
        assert result_df.iloc[1]['int_value'] == 4777288
        assert pd.isna(result_df.iloc[3]['date_value'])
        assert pd.isna(result_df.iloc[2]['value'])
        assert pd.api.types.is_datetime64_any_dtype(result_df['date_value'].dtype)
        assert df.equals(source_df)


def test_pandas_large_types(param_client: Client, call, table_context: Callable):
    columns = ['key String', 'value Int256', 'u_value UInt256'
               ]
    key2_value = 30000000000000000000000000000000000
    if not param_client.min_version('21'):
        columns = ['key String', 'value Int64']
        key2_value = 3000000000000000000
    with table_context('test_pandas_big_int', columns):
        df = pd.DataFrame([['key1', 2000, 50], ['key2', key2_value, 70], ['key3', -2350, 70]], columns=['key', 'value', 'u_value'])
        source_df = df.copy()
        call(param_client.insert_df, 'test_pandas_big_int', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_big_int')
        assert result_df.iloc[0]['value'] == 2000
        assert result_df.iloc[1]['value'] == key2_value
        assert df.equals(source_df)


def test_pandas_enums(param_client: Client, call, table_context: Callable):
    columns = ['key String', "value Enum8('Moscow' = 0, 'Rostov' = 1, 'Kiev' = 2)",
               "null_value Nullable(Enum8('red'=0,'blue'=5,'yellow'=10))"]
    with table_context('test_pandas_enums', columns):
        df = pd.DataFrame([['key1', 1, 0], ['key2', 0, None]], columns=['key', 'value', 'null_value'])
        source_df = df.copy()
        call(param_client.insert_df, 'test_pandas_enums', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_enums ORDER BY key')
        assert result_df.iloc[0]['value'] == 'Rostov'
        assert result_df.iloc[1]['value'] == 'Moscow'
        assert result_df.iloc[1]['null_value'] is None
        assert result_df.iloc[0]['null_value'] == 'red'
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', 'Rostov', 'blue'], ['key4', 'Moscow', None]], columns=['key', 'value', 'null_value'])
        call(param_client.insert_df, 'test_pandas_enums', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_enums ORDER BY key')
        assert result_df.iloc[2]['key'] == 'key3'
        assert result_df.iloc[2]['value'] == 'Rostov'
        assert result_df.iloc[3]['value'] == 'Moscow'
        assert result_df.iloc[2]['null_value'] == 'blue'
        assert result_df.iloc[3]['null_value'] is None


def test_pandas_datetime64(param_client: Client, call, table_context: Callable):
    if not param_client.min_version('20'):
        pytest.skip(f'DateTime64 not supported in this server version {param_client.server_version}')
    nano_timestamp = pd.Timestamp(1992, 11, 6, 12, 50, 40, 7420, 44)
    milli_timestamp = pd.Timestamp(2022, 5, 3, 10, 44, 10, 55000)
    chicago_timestamp = milli_timestamp.tz_localize('America/Chicago')
    with table_context('test_pandas_dt64', ['key String',
                                            'nanos DateTime64(9)',
                                            'millis DateTime64(3)',
                                            "chicago DateTime64(3, 'America/Chicago')"]):
        now = datetime.now()
        df = pd.DataFrame([['key1', now, now, now],
                           ['key2', nano_timestamp, milli_timestamp, chicago_timestamp]],
                          columns=['key', 'nanos', 'millis', 'chicago'])
        source_df = df.copy()
        call(param_client.insert_df, 'test_pandas_dt64', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_dt64')
        assert result_df.iloc[0]['nanos'] == now
        assert result_df.iloc[1]['nanos'] == nano_timestamp
        assert result_df.iloc[1]['millis'] == milli_timestamp
        assert result_df.iloc[1]['chicago'] == chicago_timestamp
        assert isinstance(result_df['chicago'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        test_dt = np.array(['2017-11-22 15:42:58.270000+00:00'][0])
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', pd.to_datetime(test_dt)]], columns=['key', 'nanos'])
        call(param_client.insert_df, 'test_pandas_dt64', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_dt64 WHERE key = %s', parameters=('key3',))
        assert result_df.iloc[0]['nanos'].second == 58


def test_pandas_streams(param_client: Client, call, consume_stream):
    if not param_client.min_version('22'):
        pytest.skip(f'generateRandom is not supported in this server version {param_client.server_version}')
    runs = os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250')
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows, date32=False)
        stream = call(param_client.query_df_stream, query, settings={'max_block_size': 5000})

        def process(df):
            nonlocal stream_count, row_count
            stream_count += 1
            row_count += len(df)

        consume_stream(stream, process)
        assert row_count == query_rows
        assert stream_count > 2


def test_pandas_date(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_date', ['key UInt32', 'dt Date', 'null_dt Nullable(Date)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        call(param_client.insert_df, 'test_pandas_date', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_date')
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)


def test_pandas_date32(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_date32', ['key UInt32', 'dt Date32', 'null_dt Nullable(Date32)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1968, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        call(param_client.insert_df, 'test_pandas_date32', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_date32')
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)
        assert result_df.iloc[2]['dt'] == pd.Timestamp(1968, 4, 15)


def test_pandas_row_df(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_row_df', ['key UInt64', 'dt DateTime64(6)', 'fs FixedString(5)']):
        df = pd.DataFrame({'key': [1, 2],
                          'dt': [pd.Timestamp(2023, 5, 4, 10, 20), pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)],
                           'fs': ['seven', 'bit']})
        df = df.iloc[1:]
        source_df = df.copy()
        call(param_client.insert_df, 'test_pandas_row_df', df)
        result_df = call(param_client.query_df, 'SELECT * FROM test_pandas_row_df', column_formats={'fs': 'string'})
        assert str(result_df.dtypes.iloc[2]) == 'string'
        assert result_df.iloc[0]['key'] == 2
        assert result_df.iloc[0]['dt'] == pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)
        assert result_df.iloc[0]['fs'] == 'bit'
        assert len(result_df) == 1
        assert source_df.equals(df)


def test_pandas_null_strings(param_client: Client, call, table_context: Callable):
    with table_context('test_pandas_null_strings', ['id String', 'test_col LowCardinality(String)']):
        row = {'id': 'id', 'test_col': None}
        df = pd.DataFrame([row])
        assert df['test_col'].isnull().values.all()
        with pytest.raises(DataError):
            call(param_client.insert_df, 'test_pandas_null_strings', df)
        row2 = {'id': 'id2', 'test_col': 'val'}
        df = pd.DataFrame([row, row2])
        with pytest.raises(DataError):
            call(param_client.insert_df, 'test_pandas_null_strings', df)


def test_pandas_small_blocks(test_config: TestConfig, param_client: Client, call):
    if test_config.cloud:
        pytest.skip('Skipping performance test in ClickHouse Cloud')
    res = call(param_client.query_df, 'SELECT number, randomString(512) FROM numbers(1000000)',
                               settings={'max_block_size': 250})
    assert len(res) == 1000000


def test_pandas_string_to_df_insert(param_client: Client, call, table_context: Callable):
    if not param_client.min_version('25.2'):
        pytest.skip(f'Nullable(JSON) type not available in this version: {param_client.server_version}')
    with table_context(
        "test_pandas_string_to_df_insert",
        [
            "id UInt32",
            "timestamp Nullable(DateTime)",
            "json_data Nullable(JSON)",
        ],
    ):

        df = pd.DataFrame(
            [[1, "simple"], [2, "with spaces"], [3, "特殊字符"], [4, ""]],
            columns=["id", "s"],
        )

        json_data_dict = {"vm": "", "App Name": "MKT"}
        json_data_dict2 = {"Room": "Leo"}

        data = [
            {
                "id": 1,
                "timestamp": datetime(year=2025, month=7, day=5, hour=12),
                "json_data": json_data_dict,
            },
            {
                "id": 2,
                "timestamp": datetime(year=2025, month=7, day=6, hour=12),
                "json_data": json_data_dict2,
            },
            {
                "id": 3,
                "timestamp": datetime(year=2025, month=7, day=7, hour=12),
                "json_data": None,
            },
        ]

        df = pd.DataFrame(data)
        call(param_client.insert_df, "test_pandas_string_to_df_insert", df)
        result_df = call(param_client.query_df,
            "SELECT * FROM test_pandas_string_to_df_insert ORDER BY id"
        )

        assert result_df.iloc[0]["json_data"] == json_data_dict
        assert result_df.iloc[1]["json_data"] == json_data_dict2
        assert result_df.iloc[2]["json_data"] is None


def test_pandas_time(
    test_config: TestConfig, param_client: Client, call, table_context: Callable
):
    """Round trip test for Time types"""
    if not param_client.min_version("25.6"):
        pytest.skip("Time and types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip(
            "Time types require settings change, but settings are locked in cloud, skipping tests."
        )

    table_name = "time_tests"

    with table_context(
        table_name,
        [
            "t Time",
            "nt Nullable(Time)",
        ],
        settings={"enable_time_time64_type": 1},
    ):
        test_data = {
            "t": [timedelta(seconds=1), timedelta(seconds=2)],
            "nt": ["00:01:00", None],
        }

        df = pd.DataFrame(test_data)
        call(param_client.insert, table_name, df)

        df_res = call(param_client.query_df, f"SELECT * FROM {table_name}")
        print(df_res)
        assert df_res["t"][0] == pd.Timedelta("0 days 00:00:01")
        assert df_res["t"][1] == pd.Timedelta("0 days 00:00:02")
        assert df_res["nt"][0] == pd.Timedelta("0 days 00:01:00")
        assert pd.isna(df_res["nt"][1])


def test_pandas_time64(
        test_config: TestConfig, param_client: Client, call, table_context: Callable
):
    """Round trip test for Time64 types"""
    if not param_client.min_version("25.6"):
        pytest.skip("Time64 types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip(
            "Time64 types require settings change, but settings are locked in cloud, skipping tests."
        )

    table_name = "time64_tests"

    with table_context(
        table_name,
        [
            "t64_3 Time64(3)",
            "nt64_3 Nullable(Time64(3))",
            "t64_6 Time64(6)",
            "nt64_6 Nullable(Time64(6))",
            "t64_9 Time64(9)",
            "nt64_9 Nullable(Time64(9))",
        ],
        settings={"enable_time_time64_type": 1},
    ):
        test_data = {
            "t64_3": [1, 2],
            "nt64_3": [time(second=45), None],
            "t64_6": ["00:00:10.5000000", "00:00:10"],
            "nt64_6": [60, None],
            "t64_9": ["00:00:01.1000000000", "00:10:00"],
            "nt64_9": [time(second=30, microsecond=500), None],
        }

        df = pd.DataFrame(test_data)
        call(param_client.insert, table_name, df)

        # Make sure the df insert worked correctly
        int_res = call(param_client.query,
            f"SELECT * FROM {table_name}",
            query_formats={"Time": "int", "Time64": "int"},
        )
        rows = int_res.result_rows
        assert rows[0] == (1, 45000, 10500000, 60, 1100000000, 30000500000)
        assert rows[1] == (2, None, 10000000, None, 600000000000, None)

        df_res = call(param_client.query_df, f"SELECT * FROM {table_name}")
        expected_row_0 = [
            pd.Timedelta(t)
            for t in [
                "0 days 00:00:00.001000",
                "0 days 00:00:45",
                "0 days 00:00:10.500000",
                "0 days 00:00:00.000060",
                "0 days 00:00:01.100000",
                "0 days 00:00:30.000500",
            ]
        ]

        assert expected_row_0 == df_res.iloc[0].tolist()

        expected_row_1 = [
            pd.Timedelta(t)
            for t in [
                "0 days 00:00:00.002000",
                "NaT",
                "0 days 00:00:10",
                "NaT",
                "0 days 00:10:00",
                "NaT",
            ]
        ]

        assert expected_row_1 == df_res.iloc[1].tolist()
