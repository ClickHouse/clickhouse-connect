import os
import random
from datetime import datetime, date, timedelta, time
from typing import Callable
from io import StringIO

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import DataError, ProgrammingError
from clickhouse_connect.driver.options import np, pd, arrow
from tests.helpers import random_query
from tests.integration_tests.conftest import TestConfig
from tests.integration_tests.datasets import null_ds, null_ds_columns, null_ds_types

pytestmark = pytest.mark.skipif(pd is None, reason='Pandas package not installed')


def _dtype_unit(dtype):
    """Extract time resolution unit from a numpy or pandas dtype."""
    return getattr(dtype, "unit", None) or np.datetime_data(dtype)[0]


def test_pandas_basic(test_client: Client, test_table_engine: str):
    df = test_client.query_df('SELECT * FROM system.tables')
    source_df = df.copy()
    test_client.command('DROP TABLE IF EXISTS test_system_insert_pd')
    test_client.command(f'CREATE TABLE test_system_insert_pd as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert_pd', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert_pd')
    test_client.command('DROP TABLE IF EXISTS test_system_insert_pd')
    assert new_df.columns.all() == df.columns.all()
    assert df.equals(source_df)
    df = test_client.query_df("SELECT * FROM system.tables WHERE engine = 'not_a_thing'")
    assert len(df) == 0
    assert isinstance(df, pd.DataFrame)


def test_pandas_nulls(test_client: Client, table_context: Callable):
    df = pd.DataFrame(null_ds, columns=['key', 'num', 'flt', 'str', 'dt', 'd'])
    source_df = df.copy()
    insert_columns = ['key', 'num', 'flt', 'str', 'dt', 'day_col']
    with table_context('test_pandas_nulls_bad', ['key String', 'num Int32', 'flt Float32',
                                                 'str String', 'dt DateTime', 'day_col Date']):

        with pytest.raises(DataError):
            test_client.insert_df('test_pandas_nulls_bad', df, column_names=insert_columns)

    with table_context('test_pandas_nulls_good',
                       ['key String', 'num Nullable(Int32)', 'flt Nullable(Float32)',
                        'str Nullable(String)', "dt Nullable(DateTime('America/Denver'))", 'day_col Nullable(Date)']):
        test_client.insert_df('test_pandas_nulls_good', df, column_names=insert_columns)
        result_df = test_client.query_df('SELECT * FROM test_pandas_nulls_good')
        assert result_df.iloc[0]['num'] == 1000
        assert pd.isna(result_df.iloc[2]['num'])
        assert result_df.iloc[1]['day_col'] == pd.Timestamp(year=1976, month=5, day=5)
        assert pd.isna(result_df.iloc[0]['day_col'])
        assert pd.isna(result_df.iloc[1]['dt'])
        assert pd.isna(result_df.iloc[2]['flt'])
        assert pd.isna(result_df.iloc[2]['num'])
        assert pd.isnull(result_df.iloc[3]['flt'])
        assert result_df['num'].dtype.name == 'Int32'
        if test_client.protocol_version:
            assert isinstance(result_df['dt'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        assert result_df.iloc[2]['str'] == 'value3'
        assert df.equals(source_df)


def test_pandas_all_null_float(test_client: Client):
    df = test_client.query_df("SELECT number, cast(NULL, 'Nullable(Float64)') as flt FROM numbers(500)")
    assert df['flt'].dtype.name == 'float64'


def test_pandas_csv(test_client: Client, table_context: Callable):
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
        test_client.insert_df('test_pandas_csv', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_csv')
        assert np.isclose(result_df.iloc[0]['flt'], 25.44)
        assert pd.isna(result_df.iloc[1]['flt'])
        result_df = test_client.query('SELECT * FROM test_pandas_csv')
        assert pd.isna(result_df.result_set[1][2])
        assert df.equals(source_df)


def test_pandas_context_inserts(test_client: Client, table_context: Callable):
    with table_context('test_pandas_multiple', null_ds_columns, null_ds_types):
        df = pd.DataFrame(null_ds, columns=null_ds_columns)
        source_df = df.copy()
        insert_context = test_client.create_insert_context('test_pandas_multiple', df.columns)
        insert_context.data = df
        test_client.data_insert(insert_context)
        assert test_client.command('SELECT count() FROM test_pandas_multiple') == 4
        next_df = pd.DataFrame(
            [['key4', -415, None, 'value4', datetime(2022, 7, 4, 15, 33, 4, 5233), date(1999, 12, 31)]],
            columns=null_ds_columns)
        test_client.insert_df(df=next_df, context=insert_context)
        assert test_client.command('SELECT count() FROM test_pandas_multiple') == 5
        assert df.equals(source_df)


def test_pandas_low_card(test_client: Client, table_context: Callable):
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
        test_client.insert_df('test_pandas_low_card', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_low_card', use_none=True)
        assert result_df.iloc[0]['value'] == 'test_string_0'
        assert result_df.iloc[1]['value'] == 'test_string_1'
        assert result_df.iloc[0]['date_value'] == pd.Timestamp(2022, 10, 15, 4, 25)
        assert result_df.iloc[1]['int_value'] == 4777288
        assert pd.isna(result_df.iloc[3]['date_value'])
        assert pd.isna(result_df.iloc[2]['value'])
        assert pd.api.types.is_datetime64_any_dtype(result_df['date_value'].dtype)
        assert df.equals(source_df)


def test_pandas_large_types(test_client: Client, table_context: Callable):
    columns = ['key String', 'value Int256', 'u_value UInt256'
               ]
    key2_value = 30000000000000000000000000000000000
    if not test_client.min_version('21'):
        columns = ['key String', 'value Int64']
        key2_value = 3000000000000000000
    with table_context('test_pandas_big_int', columns):
        df = pd.DataFrame([['key1', 2000, 50], ['key2', key2_value, 70], ['key3', -2350, 70]], columns=['key', 'value', 'u_value'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_big_int', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_big_int')
        assert result_df.iloc[0]['value'] == 2000
        assert result_df.iloc[1]['value'] == key2_value
        assert df.equals(source_df)


def test_pandas_enums(test_client: Client, table_context: Callable):
    columns = ['key String', "value Enum8('Moscow' = 0, 'Rostov' = 1, 'Kiev' = 2)",
               "null_value Nullable(Enum8('red'=0,'blue'=5,'yellow'=10))"]
    with table_context('test_pandas_enums', columns):
        df = pd.DataFrame([['key1', 1, 0], ['key2', 0, None]], columns=['key', 'value', 'null_value'])
        source_df = df.copy()
        test_client.insert_df('test_pandas_enums', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_enums ORDER BY key')
        assert result_df.iloc[0]['value'] == 'Rostov'
        assert result_df.iloc[1]['value'] == 'Moscow'
        assert pd.isna(result_df.iloc[1]['null_value'])
        assert result_df.iloc[0]['null_value'] == 'red'
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', 'Rostov', 'blue'], ['key4', 'Moscow', None]], columns=['key', 'value', 'null_value'])
        test_client.insert_df('test_pandas_enums', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_enums ORDER BY key')
        assert result_df.iloc[2]['key'] == 'key3'
        assert result_df.iloc[2]['value'] == 'Rostov'
        assert result_df.iloc[3]['value'] == 'Moscow'
        assert result_df.iloc[2]['null_value'] == 'blue'
        assert pd.isna(result_df.iloc[3]['null_value'])


def test_pandas_datetime64(test_client: Client, table_context: Callable):
    if not test_client.min_version('20'):
        pytest.skip(f'DateTime64 not supported in this server version {test_client.server_version}')
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
        test_client.insert_df('test_pandas_dt64', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_dt64')
        assert result_df.iloc[0]['nanos'] == now
        assert result_df.iloc[1]['nanos'] == nano_timestamp
        assert result_df.iloc[1]['millis'] == milli_timestamp
        assert result_df.iloc[1]['chicago'] == chicago_timestamp
        assert isinstance(result_df['chicago'].dtype, pd.core.dtypes.dtypes.DatetimeTZDtype)
        assert _dtype_unit(result_df['nanos'].dtype) == 'ns'
        assert _dtype_unit(result_df['millis'].dtype) == 'ms'
        assert _dtype_unit(result_df['chicago'].dtype) == 'ms'
        test_dt = np.array(['2017-11-22 15:42:58.270000+00:00'][0])
        assert df.equals(source_df)
        df = pd.DataFrame([['key3', pd.to_datetime(test_dt)]], columns=['key', 'nanos'])
        test_client.insert_df('test_pandas_dt64', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_dt64 WHERE key = %s', parameters=('key3',))
        assert result_df.iloc[0]['nanos'].second == 58


def test_pandas_streams(test_client: Client):
    if not test_client.min_version('22'):
        pytest.skip(f'generateRandom is not supported in this server version {test_client.server_version}')
    runs = os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250')
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows, date32=False)
        stream = test_client.query_df_stream(query, settings={'max_block_size': 5000})
        with stream:
            for df in stream:
                stream_count += 1
                row_count += len(df)
        assert row_count == query_rows
        assert stream_count > 2


def test_pandas_date(test_client: Client, table_context:Callable):
    with table_context('test_pandas_date', ['key UInt32', 'dt Date', 'null_dt Nullable(Date)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        test_client.insert_df('test_pandas_date', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_date')
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)
        for col in ['dt', 'null_dt']:
            assert _dtype_unit(result_df[col].dtype) == 's'


def test_pandas_date32(test_client: Client, table_context:Callable):
    with table_context('test_pandas_date32', ['key UInt32', 'dt Date32', 'null_dt Nullable(Date32)']):
        df = pd.DataFrame([[1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                           [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                           [3, pd.Timestamp(1968, 4, 15), pd.Timestamp(2101, 12, 31)]],
                          columns=['key', 'dt', 'null_dt'])
        test_client.insert_df('test_pandas_date32', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_date32')
        assert result_df.iloc[1]['dt'] == pd.Timestamp(2088, 1, 31)
        assert result_df.iloc[0]['dt'] == pd.Timestamp(1992, 10, 15)
        assert result_df.iloc[0]['null_dt'] == pd.Timestamp(2023, 5, 4)
        assert pd.isnull(result_df.iloc[1]['null_dt'])
        assert result_df.iloc[2]['null_dt'] == pd.Timestamp(2101, 12, 31)
        assert result_df.iloc[2]['dt'] == pd.Timestamp(1968, 4, 15)
        for col in ['dt', 'null_dt']:
            assert _dtype_unit(result_df[col].dtype) == 's'


def test_pandas_row_df(test_client: Client, table_context:Callable):
    with table_context('test_pandas_row_df', ['key UInt64', 'dt DateTime64(6)', 'fs FixedString(5)']):
        df = pd.DataFrame({'key': [1, 2],
                          'dt': [pd.Timestamp(2023, 5, 4, 10, 20), pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)],
                           'fs': ['seven', 'bit']})
        df = df.iloc[1:]
        source_df = df.copy()
        test_client.insert_df('test_pandas_row_df', df)
        result_df = test_client.query_df('SELECT * FROM test_pandas_row_df', column_formats={'fs': 'string'})
        assert str(result_df.dtypes.iloc[2]) == 'string'
        assert result_df.iloc[0]['key'] == 2
        assert result_df.iloc[0]['dt'] == pd.Timestamp(2023, 10, 15, 14, 50, 2, 4038)
        assert result_df.iloc[0]['fs'] == 'bit'
        assert len(result_df) == 1
        assert source_df.equals(df)


def test_pandas_null_strings(test_client: Client, table_context:Callable):
    with table_context('test_pandas_null_strings', ['id String', 'test_col LowCardinality(String)']):
        row = {'id': 'id', 'test_col': None}
        df = pd.DataFrame([row])
        assert df['test_col'].isnull().values.all()
        with pytest.raises(DataError):
            test_client.insert_df('test_pandas_null_strings', df)
        row2 = {'id': 'id2', 'test_col': 'val'}
        df = pd.DataFrame([row, row2])
        with pytest.raises(DataError):
            test_client.insert_df('test_pandas_null_strings', df)


def test_pandas_small_blocks(test_config: TestConfig, test_client: Client):
    if test_config.cloud:
        pytest.skip('Skipping performance test in ClickHouse Cloud')
    res = test_client.query_df('SELECT number, randomString(512) FROM numbers(1000000)',
                               settings={'max_block_size': 250})
    assert len(res) == 1000000


def test_pandas_string_to_df_insert(test_client: Client, table_context: Callable):
    if not test_client.min_version('25.2'):
        pytest.skip(f'Nullable(JSON) type not available in this version: {test_client.server_version}')
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
        test_client.insert_df("test_pandas_string_to_df_insert", df)
        result_df = test_client.query_df(
            "SELECT * FROM test_pandas_string_to_df_insert ORDER BY id"
        )

        assert result_df.iloc[0]["json_data"] == json_data_dict
        assert result_df.iloc[1]["json_data"] == json_data_dict2
        assert result_df.iloc[2]["json_data"] is None


def test_pandas_time(
    test_config: TestConfig, test_client: Client, table_context: Callable
):
    """Round trip test for Time types"""
    if not test_client.min_version("25.6"):
        pytest.skip("Time and types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip(
            "Time types require settings change, but settings are locked in cloud, skipping tests."
        )

    table_name = "time_tests"
    test_client.command("SET enable_time_time64_type = 1")

    with table_context(
        table_name,
        [
            "t Time",
            "nt Nullable(Time)",
        ],
    ):
        test_data = {
            "t": [timedelta(seconds=1), timedelta(seconds=2)],
            "nt": ["00:01:00", None],
        }

        df = pd.DataFrame(test_data)
        test_client.insert(table_name, df)

        df_res = test_client.query_df(f"SELECT * FROM {table_name}")
        print(df_res.to_string())
        print(df_res.dtypes)
        assert df_res["t"][0] == pd.Timedelta("0 days 00:00:01")
        assert df_res["t"][1] == pd.Timedelta("0 days 00:00:02")
        assert df_res["nt"][0] == pd.Timedelta("0 days 00:01:00")
        assert pd.isna(df_res["nt"][1])
        for col in ["t", "nt"]:
            assert _dtype_unit(df_res[col].dtype) == "s"


def test_pandas_time64(
    test_config: TestConfig, test_client: Client, table_context: Callable
):
    """Round trip test for Time64 types"""
    if not test_client.min_version("25.6"):
        pytest.skip("Time64 types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip(
            "Time64 types require settings change, but settings are locked in cloud, skipping tests."
        )

    table_name = "time64_tests"
    test_client.command("SET enable_time_time64_type = 1")

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
        test_client.insert(table_name, df)

        # Make sure the df insert worked correctly
        int_res = test_client.query(
            f"SELECT * FROM {table_name}",
            query_formats={"Time": "int", "Time64": "int"},
        )
        rows = int_res.result_rows
        assert rows[0] == (1, 45000, 10500000, 60, 1100000000, 30000500000)
        assert rows[1] == (2, None, 10000000, None, 600000000000, None)

        df_res = test_client.query_df(f"SELECT * FROM {table_name}")
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
        expected_units = ["ms", "ms", "us", "us", "ns", "ns"]
        for col, expected_unit in zip(df_res.columns, expected_units):
            assert _dtype_unit(df_res[col].dtype) == expected_unit


def test_pandas_query_df_arrow(test_client: Client, table_context: Callable):
    if not arrow:
        pytest.skip("PyArrow package not available")

    table_name = "df_pyarrow_query_test"

    with table_context(
        table_name,
        [
            "ui8 UInt8",
            "d Date",
            "nui8 Nullable(UInt8)",
            "bi BigInt",
            "f32 Float32",
            "s String",
            "ns Nullable(String)",
            "b Bool",
        ],
    ):
        data = (
            [1, pd.Timestamp(2023, 5, 4), 10, 123456789, 35.2, "string 1", None, 0],
            [2, pd.Timestamp(2023, 5, 5), None, -45678912, 8.5555588888, "string 2", None, 1],
            [3, pd.Timestamp(2023, 5, 6), 30, 789123456, 3.14159, "string 3", None, 1],
        )
        test_client.insert(table_name, data)
        result_df = test_client.query_df_arrow(f"SELECT * FROM {table_name}")
        for dt in list(result_df.dtypes):
            assert isinstance(dt, pd.ArrowDtype)


def test_pandas_insert_df_arrow(test_client: Client, table_context: Callable):
    if not arrow:
        pytest.skip("PyArrow package not available")

    table_name = "df_pyarrow_insert_test"
    data = [[78, pd.NA, "a"], [51, 421, "b"]]
    df = pd.DataFrame(data, columns=["i64", "ni64", "str"])

    with table_context(
        table_name,
        [
            "i64 Int64",
            "ni64 Nullable(Int64)",
            "str String",
        ],
    ):
        df = df.convert_dtypes(dtype_backend="pyarrow")
        test_client.insert_df_arrow(table_name, df)
        res_df = test_client.query(f"SELECT * from {table_name} ORDER BY i64")
        assert res_df.result_rows == [(51, 421, "b"), (78, None, "a")]

    with table_context(
        table_name,
        [
            "i64 Int64",
            "ni64 Nullable(Int64)",
            "str String",
        ],
    ):
        df = pd.DataFrame(data, columns=["i64", "ni64", "str"])
        df["i64"] = df["i64"].astype(pd.ArrowDtype(arrow.int64()))
        with pytest.raises(ProgrammingError, match="Non-Arrow columns found"):
            test_client.insert_df_arrow(table_name, df)


def test_date_resolution_roundtrip(test_client: Client, table_context: Callable):
    """Date columns should return datetime64[s] through insert_df/query_df."""
    with table_context("test_date_res_rt", ["key UInt8", "d Date", "nd Nullable(Date)"]):
        df = pd.DataFrame({
            "key": [1, 2],
            "d": [pd.Timestamp("2024-06-15"), pd.Timestamp("1985-03-22")],
            "nd": [pd.Timestamp("2024-06-15"), pd.NaT],
        })
        test_client.insert_df("test_date_res_rt", df)
        result = test_client.query_df("SELECT * FROM test_date_res_rt ORDER BY key")
        assert _dtype_unit(result["d"].dtype) == "s"
        assert _dtype_unit(result["nd"].dtype) == "s"
        assert result.iloc[0]["d"] == pd.Timestamp("2024-06-15")
        assert pd.isna(result.iloc[1]["nd"])


def test_datetime_resolution_roundtrip(test_client: Client, table_context: Callable):
    """DateTime columns should return datetime64[s] through insert_df/query_df."""
    with table_context("test_datetime_res_rt", ["key UInt8", "dt DateTime", "ndt Nullable(DateTime)"]):
        df = pd.DataFrame({
            "key": [1, 2],
            "dt": [pd.Timestamp("2024-06-15 10:30:00"), pd.Timestamp("1985-03-22 14:00:00")],
            "ndt": [pd.Timestamp("2024-06-15 10:30:00"), pd.NaT],
        })
        test_client.insert_df("test_datetime_res_rt", df)
        result = test_client.query_df("SELECT * FROM test_datetime_res_rt ORDER BY key")
        assert _dtype_unit(result["dt"].dtype) == "s"
        assert _dtype_unit(result["ndt"].dtype) == "s"
        assert result.iloc[0]["dt"] == pd.Timestamp("2024-06-15 10:30:00")


def test_datetime64_scale_resolution(test_client: Client, table_context: Callable):
    """DateTime64 columns should return resolution matching their scale."""
    with table_context("test_dt64_scale_res", [
        "key UInt8",
        "ms DateTime64(3)",
        "us DateTime64(6)",
        "ns DateTime64(9)",
        "nms Nullable(DateTime64(3))",
    ]):
        ts = pd.Timestamp("2024-06-15 10:30:00.123456789")
        df = pd.DataFrame({
            "key": [1, 2],
            "ms": [ts, ts],
            "us": [ts, ts],
            "ns": [ts, ts],
            "nms": [ts, pd.NaT],
        })
        test_client.insert_df("test_dt64_scale_res", df)
        result = test_client.query_df("SELECT * FROM test_dt64_scale_res ORDER BY key")
        assert _dtype_unit(result["ms"].dtype) == "ms"
        assert _dtype_unit(result["us"].dtype) == "us"
        assert _dtype_unit(result["ns"].dtype) == "ns"
        assert _dtype_unit(result["nms"].dtype) == "ms"
        assert pd.isna(result.iloc[1]["nms"])


def test_date_outside_ns_range(test_client: Client, table_context: Callable):
    """Dates outside datetime64[ns] range (~1677-2262) should work with [s] resolution."""
    with table_context("test_wide_date", ["key UInt8", "d Date"]):
        df = pd.DataFrame({
            "key": [1, 2],
            "d": [pd.Timestamp("2100-01-01"), pd.Timestamp("2149-06-06")],
        })
        test_client.insert_df("test_wide_date", df)
        result = test_client.query_df("SELECT * FROM test_wide_date ORDER BY key")
        assert result.iloc[0]["d"] == pd.Timestamp("2100-01-01")
        assert result.iloc[1]["d"] == pd.Timestamp("2149-06-06")


def test_date32_far_future(test_client: Client, table_context: Callable):
    """Date32 supports dates well beyond ns range."""
    with table_context("test_date32_wide", ["key UInt8", "d Date32"]):
        far = pd.Timestamp("2300-12-31")
        df = pd.DataFrame({"key": [1], "d": [far]})
        test_client.insert_df("test_date32_wide", df)
        result = test_client.query_df("SELECT * FROM test_date32_wide")
        assert result.iloc[0]["d"] == far


def test_tz_aware_datetime_resolution(test_client: Client, table_context: Callable):
    """Tz-aware DateTime64 should preserve resolution through round-trip."""
    with table_context("test_tz_res", [
        "key UInt8",
        "dt DateTime64(3, 'America/New_York')",
    ]):
        ts = pd.Timestamp("2024-06-15 10:30:00", tz="America/New_York")
        df = pd.DataFrame({"key": [1], "dt": [ts]})
        test_client.insert_df("test_tz_res", df)
        result = test_client.query_df("SELECT * FROM test_tz_res")
        assert _dtype_unit(result["dt"].dtype) == "ms"
        assert result.iloc[0]["dt"] == ts


def test_tz_aware_nullable_datetime(test_client: Client, table_context: Callable):
    """Nullable tz-aware columns should handle NaT correctly."""
    with table_context("test_tz_null", [
        "key UInt8",
        "dt Nullable(DateTime64(3, 'UTC'))",
    ]):
        ts = pd.Timestamp("2024-06-15 10:30:00", tz="UTC")
        df = pd.DataFrame({"key": [1, 2], "dt": [ts, pd.NaT]})
        test_client.insert_df("test_tz_null", df)
        result = test_client.query_df("SELECT * FROM test_tz_null ORDER BY key")
        actual = result.iloc[0]["dt"]
        # The driver may return tz-aware or naive depending on Nullable wrapping.
        # Compare the naive component to ensure the value is correct either way.
        expected_naive = pd.Timestamp("2024-06-15 10:30:00")
        if hasattr(actual, "tzinfo") and actual.tzinfo is not None:
            assert actual == ts
        else:
            assert actual == expected_naive
        assert pd.isna(result.iloc[1]["dt"])


def test_query_then_insert_roundtrip(test_client: Client, table_context: Callable):
    """A DataFrame from query_df should be insertable back into the same schema."""
    with table_context("test_rt_src", ["key UInt8", "dt DateTime", "val String"]):
        test_client.insert("test_rt_src", [[1, datetime(2024, 6, 15), "hello"]])
        df = test_client.query_df("SELECT * FROM test_rt_src")

        with table_context("test_rt_dst", ["key UInt8", "dt DateTime", "val String"]):
            test_client.insert_df("test_rt_dst", df)
            result = test_client.query_df("SELECT * FROM test_rt_dst")
            assert result.iloc[0]["key"] == 1
            assert result.iloc[0]["dt"] == pd.Timestamp("2024-06-15")
            assert result.iloc[0]["val"] == "hello"


def test_datetime64_roundtrip_preserves_precision(test_client: Client, table_context: Callable):
    """Round-trip through query_df -> insert_df should preserve sub-second precision."""
    with table_context("test_rt_prec", ["key UInt8", "dt DateTime64(6)"]):
        ts = datetime(2024, 6, 15, 10, 30, 0, 123456)
        test_client.insert("test_rt_prec", [[1, ts]])
        df = test_client.query_df("SELECT * FROM test_rt_prec")
        assert _dtype_unit(df["dt"].dtype) == "us"

        with table_context("test_rt_prec2", ["key UInt8", "dt DateTime64(6)"]):
            test_client.insert_df("test_rt_prec2", df)
            result = test_client.query_df("SELECT * FROM test_rt_prec2")
            assert result.iloc[0]["dt"] == pd.Timestamp("2024-06-15 10:30:00.123456")


def test_string_column_with_nulls(test_client: Client, table_context: Callable):
    """Nullable string columns should handle None correctly."""
    with table_context("test_str_null", ["key UInt8", "s Nullable(String)"]):
        df = pd.DataFrame({"key": [1, 2, 3], "s": ["hello", None, "world"]})
        test_client.insert_df("test_str_null", df)
        result = test_client.query_df("SELECT * FROM test_str_null ORDER BY key")
        assert result.iloc[0]["s"] == "hello"
        assert pd.isna(result.iloc[1]["s"])
        assert result.iloc[2]["s"] == "world"


def test_all_null_string_column(test_client: Client, table_context: Callable):
    """A fully null string column should insert without error."""
    with table_context("test_all_null_str", ["key UInt8", "s Nullable(String)"]):
        df = pd.DataFrame({"key": [1, 2], "s": [None, None]})
        test_client.insert_df("test_all_null_str", df)
        result = test_client.query_df("SELECT * FROM test_all_null_str ORDER BY key")
        assert pd.isna(result.iloc[0]["s"])
        assert pd.isna(result.iloc[1]["s"])


def test_lowcard_string_with_nulls(test_client: Client, table_context: Callable):
    """LowCardinality Nullable String should handle None in pandas 3."""
    with table_context("test_lc_str", ["key UInt8", "s LowCardinality(Nullable(String))"]):
        df = pd.DataFrame({"key": [1, 2, 3], "s": ["a", None, "b"]})
        test_client.insert_df("test_lc_str", df)
        result = test_client.query_df("SELECT * FROM test_lc_str ORDER BY key")
        assert result.iloc[0]["s"] == "a"
        assert pd.isna(result.iloc[1]["s"])
        assert result.iloc[2]["s"] == "b"


def test_empty_string_vs_null(test_client: Client, table_context: Callable):
    """Empty strings should not be confused with nulls."""
    with table_context("test_empty_str", ["key UInt8", "s Nullable(String)"]):
        df = pd.DataFrame({"key": [1, 2, 3], "s": ["", None, "x"]})
        test_client.insert_df("test_empty_str", df)
        result = test_client.query_df("SELECT * FROM test_empty_str ORDER BY key")
        assert result.iloc[0]["s"] == ""
        assert pd.isna(result.iloc[1]["s"])
        assert result.iloc[2]["s"] == "x"


def test_int_column_with_nan(test_client: Client, table_context: Callable):
    """Nullable int columns from pandas (which use NaN for missing) should insert correctly."""
    with table_context("test_int_nan", ["key UInt8", "n Nullable(Int32)"]):
        df = pd.DataFrame({"key": [1, 2, 3], "n": [10, None, 30]})
        test_client.insert_df("test_int_nan", df)
        result = test_client.query_df("SELECT * FROM test_int_nan ORDER BY key")
        assert result.iloc[0]["n"] == 10
        assert pd.isna(result.iloc[1]["n"])
        assert result.iloc[2]["n"] == 30


def test_float_column_with_nan(test_client: Client, table_context: Callable):
    """Nullable float columns should handle NaN correctly."""
    with table_context("test_float_nan", ["key UInt8", "f Nullable(Float64)"]):
        df = pd.DataFrame({"key": [1, 2], "f": [3.14, None]})
        test_client.insert_df("test_float_nan", df)
        result = test_client.query_df("SELECT * FROM test_float_nan ORDER BY key")
        assert abs(result.iloc[0]["f"] - 3.14) < 0.001
        assert pd.isna(result.iloc[1]["f"])


def test_time_resolution(
    test_config: TestConfig, test_client: Client, table_context: Callable
):
    """Time columns should return timedelta64[s]."""
    if not test_client.min_version("25.6"):
        pytest.skip("Time types require ClickHouse 25.6+")
    if test_config.cloud:
        pytest.skip("Time types require settings change")

    test_client.command("SET enable_time_time64_type = 1")
    with table_context("test_time_res", ["key UInt8", "t Time", "nt Nullable(Time)"]):
        test_client.insert("test_time_res", [
            [1, timedelta(hours=2, minutes=30), 3600],
            [2, timedelta(seconds=45), None],
        ])
        result = test_client.query_df("SELECT * FROM test_time_res ORDER BY key")
        assert _dtype_unit(result["t"].dtype) == "s"
        assert _dtype_unit(result["nt"].dtype) == "s"
        assert result.iloc[0]["t"] == pd.Timedelta(hours=2, minutes=30)
        assert pd.isna(result.iloc[1]["nt"])


def test_time64_resolution(
    test_config: TestConfig, test_client: Client, table_context: Callable
):
    """Time64 columns should return resolution matching their scale."""
    if not test_client.min_version("25.6"):
        pytest.skip("Time64 types require ClickHouse 25.6+")
    if test_config.cloud:
        pytest.skip("Time types require settings change")

    test_client.command("SET enable_time_time64_type = 1")
    with table_context("test_time64_res", [
        "key UInt8",
        "ms Time64(3)",
        "us Time64(6)",
        "ns Time64(9)",
    ]):
        test_client.insert("test_time64_res", [
            [1, 1500, 1500000, 1500000000],
        ])
        result = test_client.query_df("SELECT * FROM test_time64_res")
        assert _dtype_unit(result["ms"].dtype) == "ms"
        assert _dtype_unit(result["us"].dtype) == "us"
        assert _dtype_unit(result["ns"].dtype) == "ns"


def test_insert_datetime64s_into_datetime(test_client: Client, table_context: Callable):
    """Insert a datetime64[s] column into a DateTime column (both second precision)."""
    with table_context("test_s_to_dt", ["key UInt8", "dt DateTime"]):
        df = pd.DataFrame({
            "key": [1],
            "dt": pd.array([pd.Timestamp("2024-06-15 10:30:00")], dtype="datetime64[s]"),
        })
        assert _dtype_unit(df["dt"].dtype) == "s"
        test_client.insert_df("test_s_to_dt", df)
        result = test_client.query_df("SELECT * FROM test_s_to_dt")
        assert result.iloc[0]["dt"] == pd.Timestamp("2024-06-15 10:30:00")


def test_insert_datetime64ns_into_datetime64_3(test_client: Client, table_context: Callable):
    """Insert a datetime64[ns] column into DateTime64(3) -- should truncate to ms."""
    with table_context("test_ns_to_ms", ["key UInt8", "dt DateTime64(3)"]):
        ts = pd.Timestamp("2024-06-15 10:30:00.123456789")
        df = pd.DataFrame({
            "key": [1],
            "dt": pd.array([ts], dtype="datetime64[ns]"),
        })
        test_client.insert_df("test_ns_to_ms", df)
        result = test_client.query_df("SELECT * FROM test_ns_to_ms")
        # Microseconds and nanoseconds should be truncated
        assert result.iloc[0]["dt"] == pd.Timestamp("2024-06-15 10:30:00.123000")


def test_insert_datetime64us_into_datetime64_6(test_client: Client, table_context: Callable):
    """Insert a datetime64[us] column into DateTime64(6) -- exact match."""
    with table_context("test_us_to_us", ["key UInt8", "dt DateTime64(6)"]):
        ts = pd.Timestamp("2024-06-15 10:30:00.123456")
        df = pd.DataFrame({
            "key": [1],
            "dt": pd.array([ts], dtype="datetime64[us]"),
        })
        test_client.insert_df("test_us_to_us", df)
        result = test_client.query_df("SELECT * FROM test_us_to_us")
        assert result.iloc[0]["dt"] == ts


def test_large_datetime_insert(test_client: Client, table_context: Callable):
    """Insert enough rows to trigger multiple blocks, verify all come back."""
    n = 10000
    with table_context("test_large_dt", ["key UInt32", "dt DateTime64(3)", "s String"]):
        df = pd.DataFrame({
            "key": range(n),
            "dt": pd.date_range("2024-01-01", periods=n, freq="s"),
            "s": [f"row_{i}" for i in range(n)],
        })
        test_client.insert_df("test_large_dt", df)
        result = test_client.query_df("SELECT count() as cnt FROM test_large_dt")
        assert result.iloc[0]["cnt"] == n

        result = test_client.query_df("SELECT * FROM test_large_dt ORDER BY key")
        assert _dtype_unit(result["dt"].dtype) == "ms"
        assert len(result) == n
        assert result.iloc[0]["s"] == "row_0"
        assert result.iloc[n - 1]["s"] == f"row_{n - 1}"
