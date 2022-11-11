from typing import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.options import np
from tests.helpers import list_equal
from tests.integration_tests.datasets import basic_ds, basic_ds_columns, basic_ds_types, null_ds, null_ds_columns, \
    null_ds_types

pytestmark = pytest.mark.skipif(np is None, reason='Numpy package not installed')


def test_numpy_basic(test_client: Client, table_context: Callable):
    np_array = np.array(basic_ds, dtype='U20,int32,float,U20,datetime64[ns]')
    np_array.dtype.names = basic_ds_columns
    with table_context('test_numpy_basic', basic_ds_columns, basic_ds_types):
        test_client.insert('test_numpy_basic', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_basic', force_structured=True, max_str_len=20)
        assert np.array_equal(np_array, new_np_array)


def test_numpy_nulls(test_client: Client, table_context: Callable):
    np_types = [(col_name, 'O') for col_name in null_ds_columns]
    np_array = np.rec.fromrecords(null_ds, dtype=np_types)
    with table_context('test_numpy_nulls', null_ds_columns, null_ds_types):
        test_client.insert('test_numpy_nulls', np_array)
        new_np_array = test_client.query_np('SELECT * FROM test_numpy_nulls')
        assert list_equal(np_array.tolist(), new_np_array.tolist())
