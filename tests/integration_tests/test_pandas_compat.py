import datetime
from typing import Callable

import pytest
from clickhouse_connect.driver.options import pd, PANDAS_VERSION
from clickhouse_connect.driver import Client
from clickhouse_connect.common import set_setting
from tests.integration_tests.conftest import TestConfig

IS_PANDAS_2 = PANDAS_VERSION >= (2, 0)
RES_SETTING_NAME = "preserve_pandas_datetime_resolution"
PYARROW_SETTING_NAME = "pandas_dtype_backend"

pytestmark = pytest.mark.skipif(pd is None, reason="Pandas package not installed")


def test_pandas_date_compat(test_client: Client, table_context: Callable):
    table_name = "test_date"
    with table_context(
        table_name,
        [
            "key UInt8",
            "dt Date",
            "ndt Nullable(Date)",
        ],
    ):
        df = pd.DataFrame(
            [
                [1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)],
            ],
            columns=["key", "dt", "ndt"],
        )
        test_client.insert_df(table_name, df)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            res = "[s]"
        else:
            res = "[ns]"

        for dt in list(result_df.dtypes)[1:]:
            assert res in str(dt)


def test_pandas_date32_compat(test_client: Client, table_context: Callable):
    table_name = "test_date32"
    with table_context(
        table_name,
        [
            "key UInt8",
            "dt Date32",
            "ndt Nullable(Date32)",
        ],
    ):
        df = pd.DataFrame(
            [
                [1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)],
            ],
            columns=["key", "dt", "ndt"],
        )
        test_client.insert_df(table_name, df)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            res = "[s]"
        else:
            res = "[ns]"

        for dt in list(result_df.dtypes)[1:]:
            assert res in str(dt)


def test_pandas_datetime_compat(test_client: Client, table_context: Callable):
    table_name = "test_datetime"
    with table_context(
        table_name,
        [
            "key UInt8",
            "dt DateTime",
            "ndt Nullable(DateTime)",
        ],
    ):
        df = pd.DataFrame(
            [
                [1, pd.Timestamp(1992, 10, 15), pd.Timestamp(2023, 5, 4)],
                [2, pd.Timestamp(2088, 1, 31), pd.NaT],
                [3, pd.Timestamp(1971, 4, 15), pd.Timestamp(2101, 12, 31)],
            ],
            columns=["key", "dt", "ndt"],
        )
        test_client.insert_df(table_name, df)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            res = "[s]"
        else:
            res = "[ns]"

        for dt in list(result_df.dtypes)[1:]:
            assert res in str(dt)


def test_pandas_datetime64_compat(test_client: Client, table_context: Callable):
    table_name = "test_datetime64"
    with table_context(
        table_name,
        [
            "key UInt8",
            "dt3 DateTime64(3)",
            "null_dt3 Nullable(DateTime64(3))",
            "dt6 DateTime64(6)",
            "null_dt6 Nullable(DateTime64(6))",
            "dt9 DateTime64(9)",
            "null_dt9 Nullable(DateTime64(9))",
        ],
    ):
        df = pd.DataFrame(
            [
                [
                    1,
                    pd.Timestamp(1992, 10, 15),
                    pd.Timestamp(2023, 5, 4),
                    pd.Timestamp(1992, 10, 15),
                    pd.Timestamp(2023, 5, 4),
                    pd.Timestamp(1992, 10, 15),
                    pd.Timestamp(2023, 5, 4),
                ],
                [
                    2,
                    pd.Timestamp(1992, 10, 15),
                    pd.NaT,
                    pd.Timestamp(1992, 10, 15),
                    pd.NaT,
                    pd.Timestamp(1992, 10, 15),
                    pd.NaT,
                ],
            ],
            columns=["key", "dt3", "null_dt3", "dt6", "null_dt6", "dt9", "null_dt9"],
        )
        test_client.insert_df(table_name, df)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            dts = list(result_df.dtypes)[1:]
            expected_res_list = ["[ms]", "[ms]", "[us]", "[us]", "[ns]", "[ns]"]
            for actual, expected in zip(dts, expected_res_list):
                assert expected in str(actual)
        else:
            res = "[ns]"
            for dt in list(result_df.dtypes)[1:]:
                assert res in str(dt)


def test_pandas_time_compat(
    test_config: TestConfig,
    test_client: Client,
    table_context: Callable,
):
    if not test_client.min_version("25.6"):
        pytest.skip("Time types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip("Time types require settings change, but settings are locked in cloud, skipping tests.")

    test_client.command("SET enable_time_time64_type = 1")
    table_name = "test_time"
    with table_context(
        table_name,
        [
            "key UInt8",
            "t Time",
            "null_t Nullable(Time)",
        ],
    ):
        data = (
            [1, datetime.timedelta(hours=5), 500],
            [2, datetime.timedelta(hours=1), None],
            [3, -datetime.timedelta(minutes=45), 600],
        )

        test_client.insert(table_name, data)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            res = "[s]"
        else:
            res = "[ns]"

        for dt in list(result_df.dtypes)[1:]:
            assert res in str(dt)


def test_pandas_time64_compat(
    test_config: TestConfig,
    test_client: Client,
    table_context: Callable,
):
    if not test_client.min_version("25.6"):
        pytest.skip("Time64 types require ClickHouse 25.6+")

    if test_config.cloud:
        pytest.skip("Time types require settings change, but settings are locked in cloud, skipping tests.")

    test_client.command("SET enable_time_time64_type = 1")
    table_name = "test_time64"
    with table_context(
        table_name,
        [
            "key UInt8",
            "t3 Time64(3)",
            "null_t3 Nullable(Time64(3))",
            "t6 Time64(6)",
            "null_t6 Nullable(Time64(6))",
            "t9 Time64(9)",
            "null_t9 Nullable(Time64(9))",
        ],
    ):
        data = (
            [1, 1, 2, 3, 4, 5, 6],
            [2, 10, None, 30, None, 50, None],
            [3, 100, 200, 300, 400, 500, 600],
        )
        test_client.insert(table_name, data)
        set_setting(RES_SETTING_NAME, False)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert "[ns]" in str(dt)

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        if IS_PANDAS_2:
            dts = list(result_df.dtypes)[1:]
            expected_res_list = ["[ms]", "[ms]", "[us]", "[us]", "[ns]", "[ns]"]
            for actual, expected in zip(dts, expected_res_list):
                assert expected in str(actual)
        else:
            res = "[ns]"
            for dt in list(result_df.dtypes)[1:]:
                assert res in str(dt)


# Dtype backend tests
def test_pandas_dtype_backends(test_client: Client, table_context: Callable):
    table_name = "pyarrow_tests"
    test_client.command("SET enable_time_time64_type = 1")
    set_setting(PYARROW_SETTING_NAME, "numpy")

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
            "t64_ms Time64(3)",
        ],
    ):
        data = (
            [1, pd.Timestamp(2023, 5, 4), 10, 123456789, 35.2, "string 1", None, 0, 1000],
            [2, pd.Timestamp(2023, 5, 5), None, -45678912, 8.5555588888, "string 2", None, 1, 10000],
            [3, pd.Timestamp(2023, 5, 6), 30, 789123456, 3.14159, "string 3", None, 1, 10000],
        )
        test_client.insert(table_name, data)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")
        for dt in list(result_df.dtypes):
            assert "[pyarrow]" not in str(dt)

        set_setting(PYARROW_SETTING_NAME, "pyarrow")

        if IS_PANDAS_2:
            result_df = test_client.query_df(f"SELECT * FROM {table_name}")
            for dt in list(result_df.dtypes):
                assert "[pyarrow]" in str(dt)
            assert "[ns][pyarrow]" in str(list(result_df.dtypes)[8])
        else:
            result_df = test_client.query_df(f"SELECT * FROM {table_name}")
            for dt in list(result_df.dtypes):
                assert "[pyarrow]" not in str(dt)
            assert "[ns][pyarrow]" in str(list(result_df.dtypes)[8])

        set_setting(RES_SETTING_NAME, True)
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")
        assert "[s][pyarrow]" in str(list(result_df.dtypes)[1])
        assert "[ms][pyarrow]" in str(list(result_df.dtypes)[8])
