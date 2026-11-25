from unittest.mock import Mock

import pytest

from clickhouse_connect.driver.exceptions import StreamClosedError
from clickhouse_connect.driver.npquery import NumpyResult
from clickhouse_connect.driver.types import Closable

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")


def _reference_frame(blocks, column_names):
    columns = []
    for column in zip(*blocks):
        series = [pd.Series(piece) for piece in column if len(piece) > 0]
        if series:
            columns.append(pd.concat(series, ignore_index=True))
    return pd.DataFrame(dict(zip(column_names, columns)))


@pytest.mark.parametrize(
    "pieces",
    [
        pytest.param([np.array([13, 79], dtype=dtype), np.array([], dtype=dtype), np.array([101], dtype=dtype)], id=dtype)
        for dtype in ["uint16", "float64", "datetime64[D]", "timedelta64[s]", "U6", "object"]
    ]
    + [
        pytest.param(
            [np.array([None, None], dtype=object), np.array([], dtype=object), np.array(["user_1", "user_2"], dtype=object)],
            id="object_strings_after_nulls",
        ),
        pytest.param([np.array([True, False]), np.array([], dtype=bool), np.array([None, True], dtype=object)], id="mixed_bool"),
        pytest.param(
            [pd.array([13, pd.NA], dtype="UInt64"), pd.array([], dtype="UInt64"), pd.array([79], dtype="UInt64")],
            id="nullable_uint64",
        ),
        pytest.param(
            [pd.array(["user_1", pd.NA], dtype="string"), pd.array([], dtype="string"), pd.array(["user_2"], dtype="string")],
            id="string",
        ),
        pytest.param(
            [
                pd.array(["2026-01-13", None], dtype="datetime64[ns, Asia/Tokyo]"),
                pd.array([], dtype="datetime64[ns, Asia/Tokyo]"),
                pd.array(["2026-01-19"], dtype="datetime64[ns, Asia/Tokyo]"),
            ],
            id="datetime_array",
        ),
        pytest.param(
            [
                pd.DatetimeIndex(["2026-01-13", None], tz="Asia/Tokyo"),
                pd.DatetimeIndex([], tz="Asia/Tokyo"),
                pd.DatetimeIndex(["2026-01-19"], tz="Asia/Tokyo"),
            ],
            id="datetime_index",
        ),
        pytest.param([["user_1"], [], [None, None], ["user_2"]], id="lists"),
        pytest.param([np.array([13, 79], dtype="uint16")], id="single_block"),
        pytest.param([pd.array([13, pd.NA], dtype="UInt64")], id="single_extension_block"),
        pytest.param([np.array([], dtype="uint16"), np.array([], dtype="uint16")], id="all_empty"),
        pytest.param([], id="no_blocks"),
    ],
)
def test_close_df_matches_series_concat(pieces):
    blocks = [[piece] for piece in pieces]
    expected = _reference_frame(blocks, ("value",)) if any(len(piece) for piece in pieces) else pd.DataFrame(columns=["value"])
    source = Mock(spec=Closable)
    result = NumpyResult(block_gen=(block for block in blocks), column_names=("value",), source=source)

    pd.testing.assert_frame_equal(result.df_result, expected, check_exact=True)
    assert result.df_result is result._df_result
    source.close.assert_called_once_with()
    with pytest.raises(StreamClosedError):
        result.close_df()


def test_close_df_without_metadata():
    pd.testing.assert_frame_equal(NumpyResult().df_result, pd.DataFrame(), check_exact=True)


def test_close_df_many_blocks():
    column_names = tuple(f"c{index}" for index in range(50))
    blocks = [
        [np.array([], dtype="uint16") for _ in column_names],
        *[[np.array([index], dtype="uint16") for _ in column_names] for index in range(300)],
    ]
    expected = _reference_frame(blocks, column_names)
    frame = NumpyResult(block_gen=(block for block in blocks), column_names=column_names).df_result
    pd.testing.assert_frame_equal(frame, expected, check_exact=True)
