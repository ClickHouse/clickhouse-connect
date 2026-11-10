import datetime
from decimal import Decimal

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import DataError
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


@pytest.mark.parametrize("dtype", ["float32", "float64", "Float32", "Float64", "Int64"])
@pytest.mark.parametrize("type_name", ["Int32", "UInt64", "Nullable(Int32)", "Nullable(UInt64)"])
def test_convert_pandas_integer_missing_values(dtype, type_name):
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame({"value": pd.Series([13, None, 79], dtype=dtype)})
    original = source.copy(deep=True)
    ctx = InsertContext("fake_table", ["value"], [get_from_name(type_name)], source)

    expected = [13, None, 79] if type_name.startswith("Nullable(") else [13, 0, 79]
    assert list(ctx._block_columns[0]) == expected
    pd.testing.assert_frame_equal(source, original)


@pytest.mark.parametrize("type_name", ["Int128", "UInt128", "Int256", "UInt256"])
def test_convert_pandas_wide_integer_keeps_missing(type_name):
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame({"value": pd.Series([13, None, 79], dtype="Int64")})
    ch_type = get_from_name(type_name)
    ctx = InsertContext("fake_table", ["value"], [ch_type], source)

    assert list(ctx._block_columns[0]) == [13, None, 79]


@pytest.mark.parametrize("missing", [None, float("nan"), Decimal("NaN")])
def test_convert_pandas_float_first_object_integer_missing(missing):
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame({"value": pd.Series([13.0, missing, 79.0], dtype=object)})
    original = source.copy(deep=True)
    ctx = InsertContext("fake_table", ["value"], [get_from_name("Int32")], source)

    assert list(ctx._block_columns[0]) == [13.0, 0, 79.0]
    pd.testing.assert_frame_equal(source, original)


def test_convert_pandas_nan_first_object_integer_missing():
    pd = pytest.importorskip("pandas")
    source = pd.DataFrame({"value": pd.Series([float("nan"), None, 79.25], dtype=object)})
    ch_type = get_from_name("Int32")
    ctx = InsertContext("fake_table", ["value"], [ch_type], source)
    dest = bytearray()
    ch_type.write_column(ctx._block_columns[0], dest, ctx)

    assert dest == bytes.fromhex("00000000 00000000 4f000000")


@pytest.mark.parametrize("values", [[13, None, 79], [None, 13.0, 79.0], [13.0, "pd.NA", 79.0]])
def test_convert_pandas_object_integer_keeps_rejected_missing(values):
    pd = pytest.importorskip("pandas")
    values = [pd.NA if value == "pd.NA" else value for value in values]
    source = pd.DataFrame({"value": pd.Series(values, dtype=object)})
    ch_type = get_from_name("Int32")
    with pytest.raises((DataError, TypeError)):
        ctx = InsertContext("fake_table", ["value"], [ch_type], source)
        ch_type.write_column(ctx._block_columns[0], bytearray(), ctx)
