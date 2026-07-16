import datetime

from clickhouse_connect.datatypes.registry import get_from_name
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
