import datetime
from typing import Callable

import pytest
import pandas as pd

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import pd

PANDAS_VERSION = tuple(map(int, pd.__version__.split(".")[:2]))
IS_PANDAS_2 = PANDAS_VERSION >= (2, 0)
EXPECTED_RES = "ns"

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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)


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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)


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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)


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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)


def test_pandas_time_compat(test_client: Client, table_context: Callable):
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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)


def test_pandas_time64_compat(test_client: Client, table_context: Callable):
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
        result_df = test_client.query_df(f"SELECT * FROM {table_name}")

        for dt in list(result_df.dtypes)[1:]:
            assert f"[{EXPECTED_RES}" in str(dt)
