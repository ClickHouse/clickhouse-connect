import array

import numpy as np
import pytest
from clickhouse_connect.driverc.buffer import ResponseBuffer as CResponseBuffer
from clickhouse_connect.driverc.dataconv import build_array_rows as c_build_array_rows
from clickhouse_connect.driverc.dataconv import build_lc_nullable_column as c_build_lc_nullable_column
from clickhouse_connect.driverc.dataconv import build_map_rows as c_build_map_rows
from clickhouse_connect.driverc.dataconv import build_nullable_column as c_build_nullable_column
from clickhouse_connect.driverc.dataconv import read_nullable_array as c_read_nullable_array
from clickhouse_connect.driverc.npconv import read_numpy_array as c_read_numpy_array

from clickhouse_connect.driver.buffer import ResponseBuffer as PyResponseBuffer
from clickhouse_connect.driver.dataconv import build_array_rows as py_build_array_rows
from clickhouse_connect.driver.dataconv import build_lc_nullable_column as py_build_lc_nullable_column
from clickhouse_connect.driver.dataconv import build_map_rows as py_build_map_rows
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


@pytest.mark.parametrize(
    "column, offsets, expected",
    [
        ([13, 79, None, "user_1"], [2, 2, 3, 4], [[13, 79], [], [None], ["user_1"]]),
        ([], [0, 0], [[], []]),
        ([], [], []),
        # Malformed offsets are clamped the way Python slicing clamps them
        ([13, 79], [2**63, 0, 1], [[13, 79], [], [13]]),
    ],
)
@pytest.mark.parametrize("offsets_cls", [lambda x: array.array("Q", x), list], ids=["array", "list"])
@pytest.mark.parametrize("build_array_rows", [py_build_array_rows, c_build_array_rows], ids=["python", "cython"])
def test_build_array_rows_parity(column, offsets, expected, offsets_cls, build_array_rows):
    rows = build_array_rows(column, offsets_cls(offsets))
    assert rows == expected
    assert all(row is not column for row in rows)


@pytest.mark.parametrize("build_array_rows", [py_build_array_rows, c_build_array_rows], ids=["python", "cython"])
def test_build_array_rows_parity_keeps_element_objects(build_array_rows):
    column = list(np.array([13, 79, 80], dtype=np.uint64))
    rows = build_array_rows(column, array.array("Q", [2, 3]))
    assert rows == [[13, 79], [80]]
    assert all(v is column[ix] for ix, v in enumerate(rows[0] + rows[1]))


@pytest.mark.parametrize(
    "keys, values, offsets, map_type, expected",
    [
        pytest.param(["a", "b", "c"], [1, 2, None], [2, 2, 3], dict, [{"a": 1, "b": 2}, {}, {"c": None}], id="dict"),
        pytest.param(["a", "a", "b"], [1, 2, 3], [2, 3], dict, [{"a": 2}, {"b": 3}], id="duplicate_keys_dict_keeps_last"),
        pytest.param(["a", "a", "b"], [1, 2, 3], [2, 3], list, [[("a", 1), ("a", 2)], [("b", 3)]], id="duplicate_keys_pairs_keeps_all"),
        pytest.param([], [], [0, 0], dict, [{}, {}], id="empty_maps"),
        pytest.param([], [], [], dict, [], id="no_rows"),
        pytest.param(["a", "b"], [1, 2], [2**63, 0, 1], dict, [{"a": 1, "b": 2}, {}, {"a": 1}], id="malformed_offsets_clamped_dict"),
        pytest.param(["a", "b"], [1, 2], [2**63, 0, 1], list, [[("a", 1), ("b", 2)], [], [("a", 1)]], id="malformed_offsets_clamped_pairs"),
    ],
)
@pytest.mark.parametrize("offsets_cls", [lambda x: array.array("Q", x), list], ids=["array", "list"])
@pytest.mark.parametrize("build_map_rows", [py_build_map_rows, c_build_map_rows], ids=["python", "cython"])
def test_build_map_rows_parity(keys, values, offsets, map_type, expected, offsets_cls, build_map_rows):
    assert build_map_rows(keys, values, offsets_cls(offsets), map_type) == expected


@pytest.mark.parametrize("map_type", [dict, list], ids=["dict", "pairs"])
@pytest.mark.parametrize("build_map_rows", [py_build_map_rows, c_build_map_rows], ids=["python", "cython"])
def test_build_map_rows_parity_keeps_element_objects(map_type, build_map_rows):
    keys = list(np.array([13, 79, 80], dtype=np.uint64))
    values = list(np.array([1.5, 2.5, 3.5]))
    rows = build_map_rows(keys, values, array.array("Q", [2, 3]), map_type)
    pairs = [pair for row in rows for pair in (row.items() if map_type is dict else row)]
    assert pairs == list(zip(keys, values))
    assert all(k is keys[ix] and v is values[ix] for ix, (k, v) in enumerate(pairs))


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
