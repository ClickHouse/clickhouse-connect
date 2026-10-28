import asyncio
import gc
import sys
import weakref
from concurrent.futures import ThreadPoolExecutor
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


def _counting_executor(monkeypatch) -> list:
    loop = asyncio.get_running_loop()
    run_in_executor = loop.run_in_executor
    calls = []

    def counted(*args):
        calls.append(args)
        return run_in_executor(*args)

    monkeypatch.setattr(loop, "run_in_executor", counted)
    return calls


@pytest.mark.asyncio
async def test_async_rows_stream_reads_whole_blocks(monkeypatch):
    blocks = [[[13, 79], ["user_1", "user_2"]], [[], []], [[101, 113, 127], [None, "user_4", "user_5"]]]
    with QueryResult(block_gen=(block for block in blocks), column_names=("id", "name")).rows_stream as stream:
        sync_rows = list(stream)

    executor_calls = _counting_executor(monkeypatch)
    async_rows = []
    async with QueryResult(block_gen=(block for block in blocks), column_names=("id", "name")).rows_stream as stream:
        async for row in stream:
            async_rows.append(row)

    assert async_rows == sync_rows == [(13, "user_1"), (79, "user_2"), (101, None), (113, "user_4"), (127, "user_5")]
    # One executor call per block plus the one that finds the end, not one per row
    assert len(executor_calls) == len(blocks) + 1


@pytest.mark.asyncio
async def test_async_rows_stream_early_exit_stops_reading_blocks(monkeypatch):
    blocks_read = []

    def block_gen():
        for block in ([[13, 79], ["user_1", "user_2"]], [[101], ["user_3"]]):
            blocks_read.append(block)
            yield block

    source = Mock(spec=Closable)
    executor_calls = _counting_executor(monkeypatch)
    async with QueryResult(block_gen=block_gen(), column_names=("id", "name"), source=source).rows_stream as stream:
        async for row in stream:
            assert row == (13, "user_1")
            break

    assert len(blocks_read) == 1
    assert len(executor_calls) == 1
    source.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("use_async", [False, True], ids=["sync", "async"])
async def test_rows_stream_early_exit_finalizes_block_generator(use_async):
    finalized = []

    def block_gen():
        try:
            yield [[13, 79], ["user_1", "user_2"]]
            yield [[101], ["user_3"]]
        finally:
            finalized.append(True)

    stream_context = QueryResult(block_gen=block_gen(), column_names=("id", "name")).rows_stream
    if use_async:
        async with stream_context as stream:
            async for _ in stream:
                break
    else:
        with stream_context as stream:
            for _ in stream:
                break

    assert finalized == [True]


@pytest.mark.asyncio
@pytest.mark.skipif(sys.implementation.name != "cpython", reason="Requires immediate reference-count cleanup")
@pytest.mark.parametrize("mode", ["unentered", "sync", "async"])
async def test_abandoned_rows_stream_releases_source_without_gc(mode, monkeypatch):
    class Source(Closable):
        def close(self):
            pass

    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        source = Source()
        result = QueryResult(block_gen=(block for block in [[[13, 79]]]), column_names=("id",), source=source)
        source_ref = weakref.ref(source)
        result_ref = weakref.ref(result)
        stream = result.rows_stream
        if mode == "sync":
            stream.__enter__()
            assert next(stream) == (13,)
        elif mode == "async":
            loop = asyncio.get_running_loop()
            run_in_executor = loop.run_in_executor
            # Join the worker before checking references; future completion can precede argument cleanup.
            with ThreadPoolExecutor(max_workers=1) as executor, monkeypatch.context() as patch:
                patch.setattr(loop, "run_in_executor", lambda _, func, *args: run_in_executor(executor, func, *args))
                await stream.__aenter__()
                assert await anext(stream) == (13,)

        del stream, result, source
        assert result_ref() is None
        assert source_ref() is None
    finally:
        if gc_was_enabled:
            gc.enable()


@pytest.mark.asyncio
async def test_async_rows_stream_error_after_first_block_closes_source():
    def block_gen():
        yield [[13, 79], ["user_1", "user_2"]]
        raise ValueError("block decode failed")

    source = Mock(spec=Closable)
    rows = []
    with pytest.raises(ValueError, match="block decode failed"):
        async with QueryResult(block_gen=block_gen(), column_names=("id", "name"), source=source).rows_stream as stream:
            async for row in stream:
                rows.append(row)

    assert rows == [(13, "user_1"), (79, "user_2")]
    source.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_prefix", [0, 1, 8, 9])
@pytest.mark.parametrize("async_count", [1, 8, 17])
async def test_rows_stream_mixed_iteration_shares_cursor(sync_prefix, async_count):
    blocks = [[list(range(start, start + 8))] for start in range(0, 160, 8)]
    blocks.insert(2, [[]])
    async with QueryResult(block_gen=(block for block in blocks), column_names=("id",)).rows_stream as stream:
        rows = [next(stream) for _ in range(sync_prefix)]
        rows.extend([await anext(stream) for _ in range(async_count)])
        rows.extend(stream)
        with pytest.raises(StopAsyncIteration):
            await anext(stream)

    assert rows == [(value,) for value in range(160)]


@pytest.mark.asyncio
@pytest.mark.parametrize("consumers", [2, 5])
async def test_async_rows_stream_concurrent_consumers_share_cursor(monkeypatch, consumers):
    blocks = [[list(range(start, start + 8))] for start in range(0, 160, 8)]
    blocks.insert(2, [[]])
    loop = asyncio.get_running_loop()
    run_in_executor = loop.run_in_executor
    rows = []

    async def consume(stream):
        async for row in stream:
            rows.append(row)
            await asyncio.sleep(0)

    # Serialize the generator itself so this isolates lost buffered rows, not generator re-entry.
    with ThreadPoolExecutor(max_workers=1) as executor:
        monkeypatch.setattr(loop, "run_in_executor", lambda _, func, *args: run_in_executor(executor, func, *args))
        async with QueryResult(block_gen=(block for block in blocks), column_names=("id",)).rows_stream as stream:
            await asyncio.wait_for(asyncio.gather(*(consume(stream) for _ in range(consumers))), timeout=5)

    assert sorted(rows) == [(value,) for value in range(160)]
