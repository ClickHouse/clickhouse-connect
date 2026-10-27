import datetime
import logging
import os
import random
from collections.abc import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.options import np
from tests.helpers import list_equal, random_query
from tests.integration_tests.datasets import (
    basic_ds,
    basic_ds_columns,
    basic_ds_types,
    dt_ds,
    dt_ds_columns,
    dt_ds_types,
    null_ds,
    null_ds_columns,
    null_ds_types,
)

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.skipif(np is None, reason="Numpy package not installed")


def test_numpy_dates(param_client: Client, call, table_context: Callable):
    np_array = np.array(dt_ds, dtype="datetime64[s]").reshape(-1, 1)
    source_arr = np_array.copy()
    with table_context("test_numpy_dates", dt_ds_columns, dt_ds_types):
        call(param_client.insert, "test_numpy_dates", np_array)
        new_np_array = call(param_client.query_np, "SELECT * FROM test_numpy_dates")
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


def test_invalid_date(param_client: Client, call):
    try:
        sql = "SELECT cast(now64(), 'DateTime64(1)')"
        call(param_client.query_df, sql)
    except ProgrammingError as ex:
        assert "milliseconds" in str(ex)


def test_numpy_record_type(param_client: Client, call, table_context: Callable):
    dt_type = "datetime64[ns]"
    ds_types = basic_ds_types

    np_array = np.array(basic_ds, dtype=f"U20,int32,float,U20,{dt_type},U20")
    source_arr = np_array.copy()
    np_array.dtype.names = basic_ds_columns
    with table_context("test_numpy_basic", basic_ds_columns, ds_types):
        call(param_client.insert, "test_numpy_basic", np_array)
        new_np_array = call(param_client.query_np, "SELECT * FROM test_numpy_basic", max_str_len=20)
        assert np.array_equal(np_array, new_np_array)
        empty_np_array = call(param_client.query_np, "SELECT * FROM test_numpy_basic WHERE key = 'NOT A KEY' ")
        assert len(empty_np_array) == 0
        assert np.array_equal(source_arr, np_array)


def test_numpy_object_type(param_client: Client, call, table_context: Callable):
    dt_type = "datetime64[ns]"
    ds_types = basic_ds_types

    np_array = np.array(basic_ds, dtype=f"O,int32,float,O,{dt_type},O")
    np_array.dtype.names = basic_ds_columns
    source_arr = np_array.copy()
    with table_context("test_numpy_basic", basic_ds_columns, ds_types):
        call(param_client.insert, "test_numpy_basic", np_array)
        new_np_array = call(param_client.query_np, "SELECT * FROM test_numpy_basic")
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


@pytest.mark.parametrize(
    "select, expected",
    [
        ("[number, number + 1, number + 2] AS v", [[[x, x + 1, x + 2]] for x in range(13)]),
        ("(number, number + 1) AS t", [[(x, x + 1)] for x in range(13)]),
        ("(number, number + 1) AS t, [number] AS v", [[(x, x + 1), [x]] for x in range(13)]),
        ("arrayMap(x -> [number, x], range(number % 3 + 1)) AS v", [[[[x, y] for y in range(x % 3 + 1)]] for x in range(13)]),
        ("[(number, number + 1)] AS v", [[[(x, x + 1)]] for x in range(13)]),
    ],
)
def test_numpy_object_columns_keep_row_shape(param_client: Client, call, consume_stream, select, expected):
    query = f"SELECT {select} FROM numbers(13)"
    settings = {"max_block_size": 4, "max_threads": 1}
    np_array = call(param_client.query_np, query, settings=settings)
    assert np_array.shape == (13, len(expected[0]))
    assert np_array.dtype == np.dtype("O")
    assert np_array.tolist() == expected

    blocks = []
    consume_stream(call(param_client.query_np_stream, query, settings=settings), blocks.append)
    assert len(blocks) > 1
    assert all(block.ndim == 2 and block.dtype == np.dtype("O") for block in blocks)
    assert np.concatenate(blocks).tolist() == expected


@pytest.mark.parametrize("column_count", [1, 2])
@pytest.mark.parametrize("max_str_len", [0, 7])
def test_numpy_string_columns_keep_row_shape(param_client: Client, call, consume_stream, column_count, max_str_len):
    select = ", ".join(f"toString(number) AS s{x}" for x in range(column_count))
    query = f"SELECT {select} FROM numbers(13)"
    kwargs = {"settings": {"max_block_size": 4, "max_threads": 1}, "max_str_len": max_str_len}
    expected = [[str(x)] * column_count for x in range(13)]
    dtype = np.dtype(f"U{max_str_len}" if max_str_len else "O")
    np_array = call(param_client.query_np, query, **kwargs)
    assert np_array.shape == (13, column_count)
    assert np_array.dtype == dtype
    assert np_array.tolist() == expected

    blocks = []
    consume_stream(call(param_client.query_np_stream, query, **kwargs), blocks.append)
    assert len(blocks) > 1
    assert all(block.ndim == 2 and block.dtype == dtype for block in blocks)
    assert np.concatenate(blocks).tolist() == expected


@pytest.mark.parametrize("row_count", [1, 13])
@pytest.mark.parametrize("expression", ["13", "number % 3", "number"])
def test_numpy_numeric_low_card(param_client: Client, call, consume_stream, row_count, expression):
    query = f"SELECT toLowCardinality(toInt32({expression})) AS v FROM numbers({row_count})"
    expected = [[13 if expression == "13" else x % 3 if expression == "number % 3" else x] for x in range(row_count)]
    settings = {"max_block_size": 4, "max_threads": 1}
    np_array = call(param_client.query_np, query, settings=settings)
    assert np_array.tolist() == expected

    blocks = []
    consume_stream(call(param_client.query_np_stream, query, settings=settings), blocks.append)
    if row_count > 4:
        assert len(blocks) > 1
    assert np.concatenate(blocks).tolist() == expected


def test_numpy_nulls(param_client: Client, call, table_context: Callable):
    np_types = [(col_name, "O") for col_name in null_ds_columns]
    np_array = np.rec.fromrecords(null_ds, dtype=np_types)
    source_arr = np_array.copy()
    with table_context("test_numpy_nulls", null_ds_columns, null_ds_types):
        call(param_client.insert, "test_numpy_nulls", np_array)
        new_np_array = call(param_client.query_np, "SELECT * FROM test_numpy_nulls", use_none=True)
        assert list_equal(np_array.tolist(), new_np_array.tolist())
        assert list_equal(source_arr.tolist(), np_array.tolist())


def test_numpy_matrix(param_client: Client, call, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382, 1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype="int32")
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    with table_context("test_numpy_matrix", ["col1 Int32", "col2 Int32", "col3 Int32"]):
        call(param_client.insert, "test_numpy_matrix", matrix)
        py_result = call(param_client.query, "SELECT * FROM test_numpy_matrix").result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = call(param_client.query_np, "SELECT * FROM test_numpy_matrix")
        assert list(numpy_result[1]) == list(py_result[1])
        call(param_client.command, "TRUNCATE TABLE test_numpy_matrix")
        numpy_result = call(param_client.query_np, "SELECT * FROM test_numpy_matrix")
        assert np.size(numpy_result) == 0
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_matrix(param_client: Client, call, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382, 1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype="int64")
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    columns = ["col1 UInt256", "col2 Int64", "col3 Int128"]
    with table_context("test_numpy_bigint_matrix", columns):
        call(param_client.insert, "test_numpy_bigint_matrix", matrix)
        py_result = call(param_client.query, "SELECT * FROM test_numpy_bigint_matrix").result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = call(param_client.query_np, "SELECT * FROM test_numpy_bigint_matrix")
        assert list(numpy_result[1]) == list(py_result[1])
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_object(param_client: Client, call, table_context: Callable):
    source = [("key1", 347288, datetime.datetime(1999, 10, 15, 12, 3, 44)), ("key2", "348147832478", datetime.datetime.now())]
    np_array = np.array(source, dtype="O,uint64,datetime64[s]")
    source_arr = np_array.copy()
    columns = ["key String", "big_value UInt256", "dt DateTime"]
    with table_context("test_numpy_bigint_object", columns):
        call(param_client.insert, "test_numpy_bigint_object", np_array)
        py_result = call(param_client.query, "SELECT * FROM test_numpy_bigint_object").result_set
        assert list(py_result[0]) == list(source[0])
        numpy_result = call(param_client.query_np, "SELECT * FROM test_numpy_bigint_object")
        assert list(py_result[1]) == list(numpy_result[1])
        assert np.array_equal(source_arr, np_array)


def test_numpy_streams(param_client: Client, call, consume_stream):
    runs = os.environ.get("CLICKHOUSE_CONNECT_TEST_FUZZ", "250")
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows)
        stream = call(param_client.query_np_stream, query, settings={"max_block_size": 5000})

        def process(np_array):
            nonlocal stream_count, row_count
            stream_count += 1
            row_count += np_array.shape[0]

        consume_stream(stream, process)

        assert row_count == query_rows
        assert stream_count > 2


def test_numpy_limit_zero(param_client: Client, call, consume_stream):
    query = "SELECT 1 AS column1, 'x' AS s LIMIT 0"
    assert call(param_client.query_np, query).shape == (0,)

    blocks = []
    consume_stream(call(param_client.query_np_stream, query), blocks.append)
    assert blocks == []


@pytest.mark.asyncio
async def test_numpy_streams_async(test_native_async_client):
    """Async-only numpy streaming test."""
    runs = os.environ.get("CLICKHOUSE_CONNECT_TEST_FUZZ", "250")
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows)

        stream = await test_native_async_client.query_np_stream(query, settings={"max_block_size": 5000})
        async with stream:
            async for np_array in stream:
                stream_count += 1
                row_count += np_array.shape[0]
        assert row_count == query_rows
        assert stream_count > 2
