import pytest

from clickhouse_connect.driver.asyncio import AsyncClient

"""
AsyncClient tests that verify that the wrapper for each method is working correctly.
"""


@pytest.mark.asyncio
async def test_client_settings(test_async_client: AsyncClient):
    key = 'prefer_column_name_to_alias'
    value = '1'
    test_async_client.set_client_setting(key, value)
    assert test_async_client.get_client_setting(key) == value


@pytest.mark.asyncio
async def test_min_version(test_async_client: AsyncClient):
    assert test_async_client.min_version('19') is True
    assert test_async_client.min_version('22.4') is True
    assert test_async_client.min_version('99999') is False


@pytest.mark.asyncio
async def test_query(test_async_client: AsyncClient):
    result = await test_async_client.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0
    assert result.row_count > 0
    assert result.first_item == next(result.named_results())


@pytest.mark.asyncio
async def test_query_column_block_stream(test_async_client: AsyncClient):
    random_string = 'randomStringUTF8(50)'
    block_stream = await test_async_client.query_column_block_stream(
        f'SELECT number, {random_string} FROM numbers(10000)',
        settings={'max_block_size': 4000})
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            total += sum(block[0])
    assert total == 49995000
    assert block_count > 1


@pytest.mark.asyncio
async def test_query_row_block_stream(test_async_client: AsyncClient):
    random_string = 'randomStringUTF8(50)'
    block_stream = await test_async_client.query_row_block_stream(
        f'SELECT number, {random_string} FROM numbers(10000)',
        settings={'max_block_size': 4000})
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            for row in block:
                total += row[0]
    assert total == 49995000
    assert block_count > 1


@pytest.mark.asyncio
async def test_query_rows_stream(test_async_client: AsyncClient):
    row_stream = await test_async_client.query_rows_stream('SELECT number FROM numbers(10000)')
    total = 0
    with row_stream:
        for row in row_stream:
            total += row[0]
    assert total == 49995000


@pytest.mark.asyncio
async def test_raw_query(test_async_client: AsyncClient):
    result = await test_async_client.raw_query('SELECT 42')
    assert result == b'42\n'


@pytest.mark.asyncio
async def test_raw_stream(test_async_client: AsyncClient):
    stream = await test_async_client.raw_stream('SELECT 42')
    result = b''
    with stream:
        for chunk in stream:
            result += chunk
    assert result == b'42\n'


@pytest.mark.asyncio
async def test_query_np(test_async_client: AsyncClient):
    import numpy as np
    result = await test_async_client.query_np('SELECT number FROM numbers(5)')
    assert isinstance(result, np.ndarray)
    assert list(result) == [[0], [1], [2], [3], [4]]


@pytest.mark.asyncio
async def test_query_np_stream(test_async_client: AsyncClient):
    import numpy as np
    stream = await test_async_client.query_np_stream('SELECT number FROM numbers(5)')
    result = np.array([])
    with stream:
        for block in stream:
            result = np.append(result, block)
    assert list(result) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_df(test_async_client: AsyncClient):
    import pandas as pd
    result = await test_async_client.query_df('SELECT number FROM numbers(5)')
    assert isinstance(result, pd.DataFrame)
    assert list(result['number']) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_df_stream(test_async_client: AsyncClient):
    stream = await test_async_client.query_df_stream('SELECT number FROM numbers(5)')
    result = list()
    with stream:
        for block in stream:
            result.append(list(block['number']))
    assert result == [[0, 1, 2, 3, 4]]


@pytest.mark.asyncio
async def test_create_query_context(test_async_client: AsyncClient):
    query_context = test_async_client.create_query_context(
        query=f'SELECT {{k: Int32}}',
        parameters={'k': 42},
        column_oriented=True)
    result = await test_async_client.query(context=query_context)
    assert result.row_count == 1
    assert result.result_set == [[42]]


@pytest.mark.asyncio
async def test_query_arrow(test_async_client: AsyncClient):
    import pyarrow as pa
    result = await test_async_client.query_arrow('SELECT number FROM numbers(5)')
    assert isinstance(result, pa.Table)
    assert list(result[0].to_pylist()) == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_query_arrow_stream(test_async_client: AsyncClient):
    import pyarrow as pa
    stream = await test_async_client.query_arrow_stream('SELECT number FROM numbers(5)')
    result = list()
    with stream:
        for block in stream:
            result.append(block[0].to_pylist())
    assert result == [[0, 1, 2, 3, 4]]


@pytest.mark.asyncio
async def test_command(test_async_client: AsyncClient):
    version = await test_async_client.command('SELECT version()')
    assert int(version.split('.')[0]) >= 19


@pytest.mark.asyncio
async def test_ping(test_async_client: AsyncClient):
    assert await test_async_client.ping() is True
