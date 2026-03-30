import asyncio
import time
from typing import Callable

import pytest

from clickhouse_connect import get_async_client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError, ProgrammingError
from clickhouse_connect.driver.options import pd, arrow  # pylint: disable=no-name-in-module
from tests.integration_tests.conftest import make_client_config

# pylint: disable=protected-access


@pytest.mark.asyncio
async def test_concurrent_queries(test_config):
    """Verify multiple queries execute concurrently (not sequentially)."""
    async with await get_async_client(**make_client_config(test_config, autogenerate_session_id=False)) as client:
        queries = [client.query(f"SELECT {i}, sleep(0.1)") for i in range(10)]

        start = time.time()
        results = await asyncio.gather(*queries)
        elapsed = time.time() - start

        assert elapsed < 0.5, f"Took {elapsed}s, queries appear to run sequentially"
        assert len(results) == 10

        for i, result in enumerate(results):
            assert result.row_count == 1
            first_row = result.result_rows[0]
            assert first_row[0] == i


@pytest.mark.asyncio
async def test_stream_cancellation(test_config):
    """Test that early exit from async iteration doesn't leak resources."""
    async with await get_async_client(**make_client_config(test_config)) as client:
        stream = await client.query_rows_stream("SELECT number FROM numbers(100000)", settings={"max_block_size": 1000})

        count = 0
        async with stream:
            async for _ in stream:
                count += 1
                if count >= 10:
                    break

        assert count == 10

        result = await client.query("SELECT 1")
        assert result.result_rows[0][0] == 1


@pytest.mark.asyncio
async def test_concurrent_streams(test_config):
    """Verify multiple streams can run in parallel."""
    async with await get_async_client(**make_client_config(test_config, autogenerate_session_id=False)) as client:

        async def consume_stream(stream_id: int):
            stream = await client.query_rows_stream(
                f"SELECT number FROM numbers(1000) WHERE number % 3 = {stream_id}", settings={"max_block_size": 100}
            )
            total = 0
            async with stream:
                async for row in stream:
                    total += row[0]
            return total

        start = time.time()
        results = await asyncio.gather(consume_stream(0), consume_stream(1), consume_stream(2))
        elapsed = time.time() - start

        assert len(results) == 3
        assert all(r > 0 for r in results)
        assert elapsed < 5.0


@pytest.mark.asyncio
async def test_context_manager_cleanup(test_config):
    """Test proper resource cleanup on context manager exit."""
    client = await get_async_client(**make_client_config(test_config))

    assert client._initialized is True
    assert client._session is not None

    async with client:
        result = await client.query("SELECT 1")
        assert result.result_rows[0][0] == 1

    assert client._session is None or client._session.closed

    with pytest.raises((RuntimeError, OperationalError)):
        await client.query("SELECT 1")


@pytest.mark.asyncio
async def test_session_concurrency_protection(test_config):
    """Test that concurrent queries in the same session are blocked."""
    async with await get_async_client(**make_client_config(test_config, session_id="test_concurrent_session")) as client:

        async def long_query():
            return await client.query("SELECT sleep(0.5), 1")

        async def quick_query():
            await asyncio.sleep(0.1)
            return await client.query("SELECT 1")

        # This can raise either:
        #  - ProgrammingError (client-side detection - best effort)
        #  - DatabaseError code 373 (server-side SESSION_IS_LOCKED - when client check is too slow)
        # Both are valid ways to detect the concurrent session violation.
        with pytest.raises((ProgrammingError, DatabaseError)) as exc_info:
            await asyncio.gather(long_query(), quick_query())

        # Verify it's the right kind of error (concurrent session access)
        error_msg = str(exc_info.value).lower()
        assert ("concurrent" in error_msg or "session" in error_msg or "locked" in error_msg), \
            f"Expected session concurrency error, got: {exc_info.value}"


@pytest.mark.asyncio
async def test_timeout_handling(test_config):
    """Test that async timeout exceptions propagate correctly."""
    async with await get_async_client(
        **make_client_config(test_config),
        send_receive_timeout=1,  # 1 second timeout
        autogenerate_session_id=False,  # No session to avoid session locking after timeout
    ) as client:
        # This query should timeout (sleep 2 seconds with 1 second timeout)
        with pytest.raises((asyncio.TimeoutError, OperationalError)):
            await client.query("SELECT sleep(2)")

        # Client should remain functional after timeout
        result = await client.query("SELECT 1")
        assert result.result_rows[0][0] == 1


@pytest.mark.asyncio
async def test_connection_pool_reuse(test_config):
    """Verify connection pooling works correctly under load."""
    async with await get_async_client(
        **make_client_config(test_config),
        connector_limit=10,  # Limit pool size
        connector_limit_per_host=5,
        autogenerate_session_id=False,
    ) as client:
        # Run more queries in parallel than pool size
        queries = [client.query(f"SELECT {i}") for i in range(50)]

        start = time.time()
        results = await asyncio.gather(*queries)
        elapsed = time.time() - start

        assert len(results) == 50
        for i, result in enumerate(results):
            assert result.result_rows[0][0] == i

        assert elapsed < 10.0


@pytest.mark.asyncio
async def test_concurrent_inserts(test_config, table_context: Callable):
    """Test multiple inserts can run in parallel."""
    with table_context("test_concurrent_inserts", ["id UInt32", "value String"]) as ctx:
        async with await get_async_client(**make_client_config(test_config, autogenerate_session_id=False)) as client:

            async def insert_batch(start_id: int, count: int):
                data = [[start_id + i, f"value_{start_id + i}"] for i in range(count)]
                await client.insert(ctx.table, data, settings={"wait_for_async_insert": 1})

            await asyncio.gather(
                insert_batch(0, 10),
                insert_batch(100, 10),
                insert_batch(200, 10),
                insert_batch(300, 10),
                insert_batch(400, 10),
            )

            result = await client.query(f"SELECT count() FROM {ctx.table}")
            assert result.result_rows[0][0] == 50


@pytest.mark.asyncio
async def test_error_isolation(test_config):
    """Test that one failing query doesn't break other concurrent queries."""
    async with await get_async_client(**make_client_config(test_config, autogenerate_session_id=False)) as client:

        async def good_query(n: int):
            return await client.query(f"SELECT {n}")

        async def bad_query():
            return await client.query("SELECT invalid_syntax_here!!!")

        results = await asyncio.gather(good_query(1), bad_query(), good_query(2), bad_query(), good_query(3), return_exceptions=True)

        assert results[0].result_rows[0][0] == 1
        assert results[2].result_rows[0][0] == 2
        assert results[4].result_rows[0][0] == 3

        assert isinstance(results[1], Exception)
        assert isinstance(results[3], Exception)


@pytest.mark.asyncio
async def test_streaming_early_termination(test_config):
    """Verify streaming can be terminated early without issues."""
    async with await get_async_client(**make_client_config(test_config, autogenerate_session_id=False)) as client:
        stream = await client.query_rows_stream("SELECT number, repeat('x', 10000) FROM numbers(100000)", settings={"max_block_size": 1000})

        count = 0
        async with stream:
            async for _ in stream:
                count += 1
                if count >= 1000:
                    break  # Early termination

        assert count == 1000

        # Client should still be functional after early termination
        result = await client.query("SELECT 1")
        assert result.result_rows[0][0] == 1

        stream2 = await client.query_rows_stream("SELECT number FROM numbers(100)", settings={"max_block_size": 10})

        count2 = 0
        async with stream2:
            async for _ in stream2:
                count2 += 1

        assert count2 == 100


@pytest.mark.asyncio
async def test_regular_query_streams_then_materializes(test_config):
    """Verify regular query() uses streaming internally but materializes result."""
    async with await get_async_client(**make_client_config(test_config)) as client:
        result = await client.query("SELECT number FROM numbers(10000)")

        assert len(result.result_rows) == 10000
        assert result.result_rows[0][0] == 0
        assert result.result_rows[-1][0] == 9999

        expected_numbers = list(range(10000))
        actual_numbers = [row[0] for row in result.result_rows]
        assert actual_numbers == expected_numbers


@pytest.mark.asyncio
@pytest.mark.skipif(pd is None or arrow is None, reason="Pandas and PyArrow required")
async def test_async_query_df_arrow(test_config, table_context: Callable):
    """Test Arrow dtype backend for async query results."""
    with table_context("test_async_df_arrow_q", ["ui8 UInt8", "f32 Float32", "s String"]):
        async with await get_async_client(**make_client_config(test_config)) as client:
            await client.insert("test_async_df_arrow_q", [[1, 3.14, "val_1"], [2, 2.71, "val_2"]])
            result_df = await client.query_df_arrow("SELECT * FROM test_async_df_arrow_q")
            for dt in list(result_df.dtypes):
                assert isinstance(dt, pd.ArrowDtype)


@pytest.mark.asyncio
@pytest.mark.skipif(pd is None or arrow is None, reason="Pandas and PyArrow required")
async def test_async_query_df_arrow_stream(test_config, table_context: Callable):
    """Test streaming Arrow DataFrames via async client."""
    with table_context("test_async_df_arrow_s", ["id UInt32", "val String"]):
        async with await get_async_client(**make_client_config(test_config)) as client:
            await client.insert("test_async_df_arrow_s", [[i, f"v_{i}"] for i in range(100)])
            stream = await client.query_df_arrow_stream(
                "SELECT * FROM test_async_df_arrow_s",
                settings={"max_block_size": 25},
            )
            total_rows = 0
            async with stream:
                async for df_block in stream:
                    total_rows += len(df_block)
                    for dt in list(df_block.dtypes):
                        assert isinstance(dt, pd.ArrowDtype)
            assert total_rows == 100


@pytest.mark.asyncio
@pytest.mark.skipif(pd is None or arrow is None, reason="Pandas and PyArrow required")
async def test_async_insert_df_arrow(test_config, table_context: Callable):
    """Test inserting a PyArrow-backed DataFrame via async client."""
    with table_context("test_async_df_arrow_i", ["i64 Int64", "ni64 Nullable(Int64)", "str String"]):
        async with await get_async_client(**make_client_config(test_config)) as client:
            data = [[78, pd.NA, "a"], [51, 421, "b"]]
            df = pd.DataFrame(data, columns=["i64", "ni64", "str"])
            df = df.convert_dtypes(dtype_backend="pyarrow")
            await client.insert_df_arrow("test_async_df_arrow_i", df)
            res = await client.query("SELECT * FROM test_async_df_arrow_i ORDER BY i64")
            assert res.result_rows == [(51, 421, "b"), (78, None, "a")]
