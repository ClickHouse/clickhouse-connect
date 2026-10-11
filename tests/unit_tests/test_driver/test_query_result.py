from unittest.mock import Mock

import pytest

from clickhouse_connect.driver.exceptions import StreamClosedError
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.types import Closable


@pytest.mark.parametrize("column_oriented", [False, True], ids=["row_oriented", "column_oriented"])
@pytest.mark.parametrize("columns_first", [False, True], ids=["rows_first", "columns_first"])
@pytest.mark.parametrize(
    "blocks, expected_columns, expected_rows",
    [
        ([], [[], []], []),
        ([[[], []]], [[], []], []),
        ([[[13], ["user_1"]]], [[13], ["user_1"]], [(13, "user_1")]),
        (
            [[[13], ["user_1"]], [[], []], [[79, 101], ["user_2", None]]],
            [[13, 79, 101], ["user_1", "user_2", None]],
            [(13, "user_1"), (79, "user_2"), (101, None)],
        ),
    ],
    ids=["no_blocks", "empty_block", "single_row", "multiple_blocks"],
)
def test_query_result_views_in_either_order(column_oriented, columns_first, blocks, expected_columns, expected_rows):
    source = Mock(spec=Closable)
    result = QueryResult(
        block_gen=(block for block in blocks),
        column_names=("id", "name"),
        column_oriented=column_oriented,
        source=source,
    )

    if columns_first:
        assert result.result_columns == expected_columns
        assert result.result_rows == expected_rows
    else:
        assert result.result_rows == expected_rows
        assert result.result_columns == expected_columns

    assert result.result_rows == expected_rows
    assert result.result_columns == expected_columns
    assert result.result_set == (expected_columns if column_oriented else expected_rows)
    assert result.row_count == len(expected_rows)
    expected_first_row = expected_rows[0] if expected_rows else None
    if column_oriented and expected_first_row is not None:
        expected_first_row = list(expected_first_row)
    assert result.first_row == expected_first_row
    expected_named = [{"id": row[0], "name": row[1]} for row in expected_rows]
    assert result.first_item == (expected_named[0] if expected_named else None)
    assert list(result.named_results()) == expected_named
    with pytest.raises(StreamClosedError):
        _ = result.column_block_stream
    result.close()
    source.close.assert_called_once_with()


@pytest.mark.parametrize("view", ["result_rows", "result_columns"])
def test_query_result_closed_before_materialization(view):
    result = QueryResult(block_gen=(block for block in [[[13]]]), column_names=("id",))
    result.close()

    with pytest.raises(StreamClosedError):
        getattr(result, view)
