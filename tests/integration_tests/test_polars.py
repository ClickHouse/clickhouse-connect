from datetime import datetime, date
import string
from typing import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import pl, arrow

pytestmark = [
    pytest.mark.skipif(pl is None, reason="polars package not installed"),
    pytest.mark.skipif(arrow is None, reason="pyarrow package not installed"),
]


def test_polars_insert(test_client: Client, table_context: Callable):
    with table_context(
        "test_polars",
        [
            "key String",
            "num Nullable(Int32)",
            "flt Float32",
            "str Nullable(String)",
            "dt DateTime('America/Denver')",
            "day_col Nullable(Date)",
        ],
    ) as ctx:
        df = pl.DataFrame(
            {
                "key": ["a", "b", "c"],
                "num": [1, None, 3],
                "flt": [1.2, 3.4, 5.6],
                "str": [None, "mystr", "another"],
                "dt": [datetime(2025, 7, 1, 10, 30, 0, 0), datetime(2025, 8, 1, 10, 30, 0, 0), datetime(2025, 8, 12, 10, 30, 1, 0)],
                "day_col": [date(2025, 7, 1), date(2025, 8, 1), date(2025, 8, 12)],
            }
        )
        test_client.insert_df_arrow(ctx.table, df)
        res = test_client.query(f"SELECT key FROM {ctx.table}")
        assert [i[0] for i in res.result_rows] == df["key"].to_list()


def test_bad_insert_fails(test_client: Client, table_context: Callable):
    with table_context(
        "test_polars",
        [
            "key String",
            "num Nullable(Int32)",
            "flt Float32",
            "str Nullable(String)",
            "dt DateTime('America/Denver')",
            "day_col Nullable(Date)",
        ],
    ):
        with pytest.raises(TypeError, match="got list"):
            test_client.insert_df_arrow("test_polars", [[1, 2, 3]])


def test_polars_query(test_client: Client, table_context: Callable):
    with table_context(
        "test_polars",
        [
            "key String",
            "num Nullable(Int32)",
            "flt Float32",
            "str Nullable(String)",
            "dt DateTime('America/Denver')",
            "day_col Nullable(Date)",
        ],
    ) as ctx:
        data = [
            ["a", "b", "c"],
            [1, None, 3],
            [1.2, 3.4, 5.6],
            [None, "mystr", "another"],
            [datetime(2025, 7, 1, 10, 30, 0, 0), datetime(2025, 8, 1, 10, 30, 0, 0), datetime(2025, 8, 12, 10, 30, 1, 0)],
            [date(2025, 7, 1), date(2025, 8, 1), date(2025, 8, 12)],
        ]
        test_client.insert(
            ctx.table,
            data,
            column_names=["key", "num", "flt", "str", "dt", "day_col"],
            column_oriented=True,
        )
        df = test_client.query_df_arrow(f"SELECT key FROM {ctx.table}", dataframe_library="polars")
        assert isinstance(df, pl.DataFrame)
        assert data[0] == df["key"].to_list()


def test_polars_arrow_stream(test_client: Client, table_context: Callable):
    if not arrow:
        pytest.skip("PyArrow package not available")
    if not test_client.min_version("21"):
        pytest.skip(f"PyArrow is not supported in this server version {test_client.server_version}")
    with table_context("test_arrow_insert", ["counter Int64", "letter String"]):
        counter = arrow.array(range(1000000))
        alphabet = string.ascii_lowercase
        letter = arrow.array([alphabet[x % 26] for x in range(1000000)])
        names = ["counter", "letter"]
        insert_table = arrow.Table.from_arrays([counter, letter], names=names)
        test_client.insert_arrow("test_arrow_insert", insert_table)
        stream = test_client.query_df_arrow_stream("SELECT * FROM test_arrow_insert", dataframe_library="polars")
        with stream:
            result_dfs = list(stream)

        assert len(result_dfs) > 1
        total_rows = 0
        for df in result_dfs:
            assert df.shape[1] == 2
            assert df["counter"].dtype == pl.Int64
            assert df["letter"].dtype == pl.String
            expected_letter = df["counter"].map_elements(lambda x: alphabet[x % 26], return_dtype=pl.String)
            assert df["letter"].equals(expected_letter)
            total_rows += len(df)
        assert total_rows == 1000000


def test_polars_utc_timestamp_naive(test_client: Client, table_context: Callable):
    """Test that polars DataFrames get naive timestamps when server is UTC.

    This test reproduces the bug where Arrow format preserves UTC timezone
    in timestamp columns instead of returning naive datetimes.
    """
    with table_context('test_polars_utc_tz', ['ts DateTime']) as ctx:
        test_client.command(f"INSERT INTO {ctx.table} VALUES (now())")
        df = test_client.query_df_arrow(
            f"SELECT * FROM {ctx.table}",
            dataframe_library="polars"
        )
        # BUG: Previously returned Datetime('us', 'UTC') instead of Datetime('us')
        # The timezone should be stripped for naive datetime output
        ts_dtype = df.schema['ts']
        assert ts_dtype.time_zone is None, f"Expected naive datetime, got timezone: {ts_dtype.time_zone}"
