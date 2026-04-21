import array

import numpy as np
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer
from clickhouse_connect.driverc.dataconv import build_lc_nullable_column as c_build_lc_nullable_column
from clickhouse_connect.driverc.dataconv import build_nullable_column as c_build_nullable_column
from clickhouse_connect.driverc.dataconv import read_nullable_array as c_read_nullable_array
from clickhouse_connect.driverc.npconv import read_numpy_array as c_read_numpy_array

from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.dataconv import build_lc_nullable_column as py_build_lc_nullable_column
from clickhouse_connect.driver.dataconv import build_nullable_column as py_build_nullable_column
from clickhouse_connect.driver.dataconv import read_nullable_array as py_read_nullable_array
from clickhouse_connect.driver.npconv import read_numpy_array as py_read_numpy_array
from tests.helpers import bytes_source


def test_build_nullable_column_parity():
    source = [1.5, 2.5, 3.5]
    null_map = bytes([0, 1, 0])
    expected = [1.5, None, 3.5]

    assert py_build_nullable_column(source, null_map, None) == expected
    assert c_build_nullable_column(source, null_map, None) == expected


def test_build_lc_nullable_column_parity():
    index = ["", "alpha", "beta", "gamma"]
    keys = array.array("H", [1, 0, 3, 2])
    expected = ["alpha", None, "gamma", "beta"]

    assert py_build_lc_nullable_column(index, keys, None) == expected
    assert c_build_lc_nullable_column(index, keys, None) == expected


def test_build_nullable_column_parity_with_non_none_null():
    source = [1.5, 2.5, 3.5]
    null_map = bytes([0, 1, 0])
    expected = [1.5, 0.0, 3.5]

    assert py_build_nullable_column(source, null_map, 0.0) == expected
    assert c_build_nullable_column(source, null_map, 0.0) == expected


def test_build_lc_nullable_column_parity_with_non_none_null():
    index = ["", "alpha", "beta", "gamma"]
    keys = array.array("H", [1, 0, 3, 2])
    expected = ["alpha", "", "gamma", "beta"]

    assert py_build_lc_nullable_column(index, keys, "") == expected
    assert c_build_lc_nullable_column(index, keys, "") == expected


def test_read_nullable_array_parity():
    payload = bytes([0, 1, 0]) + np.array([10, 20, 30], dtype=np.uint16).tobytes()
    py_source = bytes_source(payload, cls=PyResponseBuffer)
    c_source = bytes_source(payload, cls=CResponseBuffer)
    expected = [10, None, 30]

    assert py_read_nullable_array(py_source, "H", 3, None) == expected
    assert c_read_nullable_array(c_source, "H", 3, None) == expected


def test_numpy_read_parity_with_python_buffer():
    data = np.array([1, 255, 1024, 65535], dtype=np.uint16).tobytes()
    py_source = bytes_source(data, cls=PyResponseBuffer)
    c_source = bytes_source(data, cls=PyResponseBuffer)

    py_result = py_read_numpy_array(py_source, "<u2", 4)
    c_result = c_read_numpy_array(c_source, "<u2", 4)

    assert np.array_equal(py_result, c_result)


def test_numpy_read_parity_with_c_buffer():
    data = np.array([3, 7, 11, 13], dtype=np.uint16).tobytes()
    py_source = bytes_source(data, cls=CResponseBuffer)
    c_source = bytes_source(data, cls=CResponseBuffer)

    py_result = py_read_numpy_array(py_source, "<u2", 4)
    c_result = c_read_numpy_array(c_source, "<u2", 4)

    assert np.array_equal(py_result, c_result)
