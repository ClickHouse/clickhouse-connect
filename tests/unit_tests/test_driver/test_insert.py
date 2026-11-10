import datetime

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.tools.datagen import fixed_len_ascii_str


def test_block_size():
    data = [(1, (datetime.date(2020, 5, 2), datetime.datetime(2020, 5, 2, 10, 5, 2)))]
    ctx = InsertContext(
        "fake_table",
        ["key", "date_tuple"],
        [
            get_from_name("UInt64"),
            get_from_name("Tuple(Date, DateTime)"),
        ],
        data,
    )
    assert ctx.block_row_count == 262144

    data = [(x, fixed_len_ascii_str(400)) for x in range(5000)]
    ctx = InsertContext(
        "fake_table",
        ["key", "big_str"],
        [
            get_from_name("Int32"),
            get_from_name("String"),
        ],
        data,
    )
    assert ctx.block_row_count == 8192


def test_block_size_empty_array_sample():
    # An empty Array(Dynamic) row in the sample must not divide by zero.
    ctx = InsertContext(
        "fake_table",
        ["key", "dyn_array"],
        [
            get_from_name("UInt8"),
            get_from_name("Array(Dynamic)"),
        ],
        [(0, []), (1, [13, "user_1"])],
    )
    assert ctx.block_row_count > 0


def test_convert_pandas_enum_column_paths():
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    enum_type = get_from_name("Enum8('missing' = 0, 'one' = 1, 'two' = 2)")

    # NA-free int dtypes keep the vectorized object-array conversion.
    ctx = InsertContext("fake_table", ["e"], [enum_type], pd.DataFrame({"e": pd.Series([1, 2, 1], dtype="int64")}))
    column = ctx._block_columns[0]
    assert isinstance(column, np.ndarray)
    assert list(column) == [1, 2, 1]

    # Float dtypes take the per-row path that maps NaN to the zero code.
    ctx = InsertContext("fake_table", ["e"], [enum_type], pd.DataFrame({"e": [1.0, float("nan"), 2.0]}))
    column = ctx._block_columns[0]
    assert isinstance(column, list)
    assert column == [1, 0, 2]


@pytest.mark.parametrize("column_oriented", [False, True])
def test_convert_numpy_datetime64_2d_keeps_orientation(column_oriented):
    np = pytest.importorskip("numpy")
    types = [get_from_name("Date"), get_from_name("DateTime64(3)")]
    values = np.array(["2024-05-06T07:08:09.123456789", "1969-12-31T23:59:59.999999"], dtype="datetime64[ns]")
    ctx = InsertContext("fake_table", ["d", "dt64"], types, column_oriented=column_oriented)
    ctx.data = np.stack([values, values]) if column_oriented else np.stack([values, values], axis=1)
    expected_columns = [[19849, -1], [1714979289123, -1]]
    if column_oriented:
        assert ctx._block_columns == expected_columns
    else:
        assert ctx._block_rows == [list(row) for row in zip(*expected_columns)]
    assert ctx.column_oriented is column_oriented

    with pytest.raises(ProgrammingError, match="column count"):
        ctx.data = np.stack([values] * 3) if column_oriented else np.stack([values] * 3, axis=1)
