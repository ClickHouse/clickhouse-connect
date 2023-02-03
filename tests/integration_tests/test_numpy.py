import datetime
import logging
import os
import random
from typing import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import np
from tests.helpers import list_equal, random_query
from tests.integration_tests.datasets import basic_ds, basic_ds_columns, basic_ds_types, null_ds, null_ds_columns, \
    null_ds_types, dt_ds, dt_ds_columns, dt_ds_types


logger = logging.getLogger(__name__)
pytestmark = pytest.mark.skipif(np is None, reason='Numpy package not installed')


def test_numpy_dates(test_client: Client, table_context: Callable):
    np_array = np.array(dt_ds, dtype='datetime64[s]').reshape(-1, 1)
    source_arr = np_array.copy()
    with table_context('test_numpy_dates', dt_ds_columns, dt_ds_types):
        test_client.insert('test_numpy_dates', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_dates')
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


def test_numpy_record_type(test_client: Client, table_context: Callable):
    np_array = np.array(basic_ds, dtype='U20,int32,float,U20,datetime64[ns],U20')
    source_arr = np_array.copy()
    np_array.dtype.names = basic_ds_columns
    with table_context('test_numpy_basic', basic_ds_columns, basic_ds_types):
        test_client.insert('test_numpy_basic', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_basic', max_str_len=20)
        assert np.array_equal(np_array, new_np_array)
        empty_np_array = test_client.query_np("SELECT * FROM test_numpy_basic WHERE key = 'NOT A KEY' ")
        assert len(empty_np_array) == 0
        assert np.array_equal(source_arr, np_array)


def test_numpy_object_type(test_client: Client, table_context: Callable):
    np_array = np.array(basic_ds, dtype='O,int32,float,O,datetime64[ns],O')
    np_array.dtype.names = basic_ds_columns
    source_arr = np_array.copy()
    with table_context('test_numpy_basic', basic_ds_columns, basic_ds_types):
        test_client.insert('test_numpy_basic', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_basic')
        assert np.array_equal(np_array, new_np_array)
        assert np.array_equal(source_arr, np_array)


def test_numpy_nulls(test_client: Client, table_context: Callable):
    np_types = [(col_name, 'O') for col_name in null_ds_columns]
    np_array = np.rec.fromrecords(null_ds, dtype=np_types)
    source_arr = np_array.copy()
    with table_context('test_numpy_nulls', null_ds_columns, null_ds_types):
        test_client.insert('test_numpy_nulls', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_nulls', use_none=True)
        assert list_equal(np_array.tolist(), new_np_array.tolist())
        assert list_equal(source_arr.tolist(), np_array.tolist())


def test_numpy_matrix(test_client: Client, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382,
              1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype='int32')
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    with table_context('test_numpy_matrix', ['col1 Int32', 'col2 Int32', 'col3 Int32']):
        test_client.insert('test_numpy_matrix', matrix)
        py_result = test_client.query('SELECT * FROM test_numpy_matrix').result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = test_client.query_np('SELECT * FROM test_numpy_matrix')
        assert list(numpy_result[1]) == list(py_result[1])
        test_client.command('TRUNCATE TABLE test_numpy_matrix')
        numpy_result = test_client.query_np('SELECT * FROM test_numpy_matrix')
        assert np.size(numpy_result) == 0
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_matrix(test_client: Client, table_context: Callable):
    source = [25000, -37283, 4000, 25770, 40032, 33002, 73086, -403882, 57723, 77382,
              1213477, 2, 0, 5777732, 99827616]
    source_array = np.array(source, dtype='int64')
    matrix = source_array.reshape((5, 3))
    matrix_copy = matrix.copy()
    with table_context('test_numpy_bigint_matrix', ['col1 UInt256', 'col2 Int64', 'col3 Int128']):
        test_client.insert('test_numpy_bigint_matrix', matrix)
        py_result = test_client.query('SELECT * FROM test_numpy_bigint_matrix').result_set
        assert list(py_result[1]) == [25000, -37283, 4000]
        numpy_result = test_client.query_np('SELECT * FROM test_numpy_bigint_matrix')
        assert list(numpy_result[1]) == list(py_result[1])
        assert np.array_equal(matrix, matrix_copy)


def test_numpy_bigint_object(test_client: Client, table_context: Callable):
    source = [('key1', 347288, datetime.datetime(1999, 10, 15, 12, 3, 44)),
              ('key2', '348147832478', datetime.datetime.now())]
    np_array = np.array(source, dtype='O,uint64,datetime64[s]')
    source_arr = np_array.copy()
    with table_context('test_numpy_bigint_object', ['key String', 'big_value UInt256', 'dt DateTime']):
        test_client.insert('test_numpy_bigint_object', np_array)
        py_result = test_client.query('SELECT * FROM test_numpy_bigint_object').result_set
        assert list(py_result[0]) == list(source[0])
        numpy_result = test_client.query_np('SELECT * FROM test_numpy_bigint_object')
        assert list(py_result[1]) == list(numpy_result[1])
        assert np.array_equal(source_arr, np_array)


def test_numpy_streams(test_client: Client):
    runs = os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250')
    for _ in range(int(runs) // 2):
        query_rows = random.randint(0, 5000) + 20000
        stream_count = 0
        row_count = 0
        query = random_query(query_rows)
        stream = test_client.query_np_stream(query, settings={'max_block_size': 5000})
        with stream:
            for np_array in stream:
                stream_count += 1
                row_count += np_array.shape[0]
        assert row_count == query_rows
        assert stream_count > 2
