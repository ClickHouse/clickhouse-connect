"""Public Rust output and resource contracts, also run against isolated installed wheels."""

import asyncio
import gc
import importlib.util
import os
import sys
from pathlib import Path
from threading import Event, Lock

import pytest

import clickhouse_connect
from clickhouse_connect.driver import ctypes
from clickhouse_connect.driver.exceptions import NotSupportedError, ProgrammingError, StreamFailureError
from clickhouse_connect.driver.httputil import ResponseSource
from clickhouse_connect.driver.rustcodec import _rust_query_ineligible_reason, _RustNativeTransform
from clickhouse_connect.driver.streaming import StreamingResponseSource

if os.environ.get("CLICKHOUSE_CONNECT_TEST_INSTALLED_PROFILE"):
    importlib.import_module("_ch_core")
else:
    pytest.importorskip("_ch_core")

_PROFILES = {
    "bare": (False, False, False),
    "numpy": (True, False, False),
    "pandas2": (True, True, False),
    "pandas3": (True, True, False),
    "pandas2-arrow": (True, True, True),
    "pandas3-arrow": (True, True, True),
}
_NUMERIC = "toUInt16(number + 13) AS n, toUInt16(number + 79) AS m"
_MIXED = (
    "toUInt16(number + 13) AS n, CAST(if(number % 3 = 1, NULL, number / 2) AS Nullable(Float32)) AS f, "
    "CAST(if(number % 3 = 1, NULL, fromUnixTimestamp64Nano(1700000000123456789 + toInt64(number))) "
    "AS Nullable(DateTime64(9))) AS dt, "
    "if(number % 3 = 0, unhex('ff'), if(number % 3 = 1, '', 'é')) AS s, "
    "tuple(toInt32(number), toString(number)) AS t"
)


@pytest.fixture(scope="module", autouse=True)
def installed_profile():
    """Fail if an installed dependency job imports the checkout or the wrong extras."""
    profile = os.environ.get("CLICKHOUSE_CONNECT_TEST_INSTALLED_PROFILE")
    if not profile:
        yield
        return
    import _ch_core

    assert profile in _PROFILES
    for module in (clickhouse_connect, _ch_core):
        assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
    assert _ch_core.COLUMN_BUFFER_API_VERSION >= 1
    expected = dict(zip(("numpy", "pandas", "pyarrow"), _PROFILES[profile]))
    expected["nanoarrow"] = False
    for package, present in expected.items():
        assert (importlib.util.find_spec(package) is not None) == present, package
    backend = "clickhouse_connect.driverc" if os.environ.get("CLICKHOUSE_CONNECT_USE_C", "1") == "1" else "clickhouse_connect.driver"
    assert ctypes.RespBuffCls.__module__ == f"{backend}.buffer"
    assert ctypes.data_conv.__name__ == f"{backend}.dataconv"
    if expected["numpy"]:
        assert ctypes.numpy_conv.__name__ == f"{backend}.npconv"
    if expected["pandas"]:
        import pandas

        assert pandas.__version__.startswith(profile[6])
    yield
    for package, present in expected.items():
        if not present:
            assert package not in sys.modules


@pytest.fixture
def strict_client(client_factory, monkeypatch):
    """Trace data queries separately from the driver's Python metadata queries."""
    original = _RustNativeTransform.parse_response
    data_queries = []
    lock = Lock()

    def parse(transform, source, context):
        if not context.internal:
            assert transform.strict
            assert _rust_query_ineligible_reason(context) is None
            with lock:
                data_queries.append(context.query)
        return original(transform, source, context)

    monkeypatch.setattr(_RustNativeTransform, "parse_response", parse)
    client = client_factory(native_codec="rust_strict")
    yield client, data_queries


@pytest.fixture
def transport_responses(monkeypatch):
    responses = []

    def capture(original):
        def initialize(source, response, *args, **kwargs):
            original(source, response, *args, **kwargs)
            responses.append(response)

        return initialize

    for cls in (ResponseSource, StreamingResponseSource):
        monkeypatch.setattr(cls, "__init__", capture(cls.__init__))
    return responses


def test_row_outputs_without_optional_imports(strict_client, call, consume_stream):
    client, queries = strict_client
    packages = ("numpy", "pandas", "pyarrow", "nanoarrow")
    before = {name for name in packages if name in sys.modules}
    query = "SELECT number + 13 AS n, toString(number) AS s FROM numbers(13)"
    expected = [(number + 13, str(number)) for number in range(13)]
    assert call(client.query, query).result_rows == expected
    rows = []
    consume_stream(call(client.query_row_block_stream, query, settings={"max_block_size": 3}), rows.extend)
    assert rows == expected
    blocks = []
    consume_stream(call(client.query_column_block_stream, query, settings={"max_block_size": 3}), blocks.append)
    assert [row for block in blocks for row in zip(*block)] == expected
    assert queries == [query] * 3
    assert {name for name in packages if name in sys.modules} == before


@pytest.mark.parametrize("projection", [_NUMERIC, _MIXED], ids=["numeric", "mixed"])
@pytest.mark.parametrize("block_size", [3, 79])
def test_numpy_output_contract(strict_client, client_factory, call, consume_stream, projection, block_size):
    np = pytest.importorskip("numpy")
    client, queries = strict_client
    python = client_factory(native_codec="python")
    query = f"SELECT {projection} FROM numbers(13)"
    settings = {"max_block_size": block_size, "max_threads": 1}
    expected = call(python.query_np, query, settings=settings)
    result = call(client.query_np, query, settings=settings)
    context_result = call(client.query, query, use_numpy=True, settings=settings)
    chunks = []
    consume_stream(call(client.query_np_stream, query, settings=settings), chunks.append)
    for output in (result, context_result.np_result, np.concatenate(chunks)):
        assert output.dtype == expected.dtype
        assert output.shape == expected.shape
        np.testing.assert_equal(output, expected)
        assert output.flags.writeable
        if projection == _NUMERIC:
            assert output.dtype == np.dtype("uint16")
            assert output.dtype.byteorder == "="
            assert output.shape == (13, 2)
            np.testing.assert_array_equal(output, [[number + 13, number + 79] for number in range(13)])
        else:
            assert output.shape == (13,)
            assert output.dtype.names == ("n", "f", "dt", "s", "t")
            assert output["n"].tolist() == list(range(13, 26))
            assert output["s"].tolist() == ["ff", "", "é"] * 4 + ["ff"]
            assert output["t"].tolist() == [(number, str(number)) for number in range(13)]
            for number, value in enumerate(output["dt"]):
                if number % 3 == 1:
                    assert value is None
                else:
                    assert isinstance(value, np.datetime64)
                    assert value.dtype == np.dtype("datetime64[ns]")
                    assert value.astype("int64") == 1700000000123456789 + number
    assert len(chunks) == (5 if block_size == 3 else 1)
    for chunk in chunks:
        assert chunk.flags.writeable
        chunk[0] = chunk[-1]
    assert queries == [query] * 3


@pytest.mark.parametrize("extended", [False, True])
@pytest.mark.parametrize("storage", ["python", "pyarrow"])
@pytest.mark.parametrize("block_size", [3, 79])
def test_dataframe_output_contract(strict_client, client_factory, call, consume_stream, extended, storage, block_size):
    pd = pytest.importorskip("pandas")
    if storage == "pyarrow":
        pytest.importorskip("pyarrow")
    client, queries = strict_client
    python = client_factory(native_codec="python")
    query = f"SELECT {_MIXED} FROM numbers(13)"
    kwargs = {"settings": {"max_block_size": block_size, "max_threads": 1}, "use_extended_dtypes": extended}
    with pd.option_context("mode.string_storage", storage):
        expected = call(python.query_df, query, **kwargs)
        result = call(client.query_df, query, **kwargs)
        pieces = []
        consume_stream(call(client.query_df_stream, query, **kwargs), pieces.append)
        pd.testing.assert_frame_equal(result, expected)
        pd.testing.assert_frame_equal(pd.concat(pieces, ignore_index=True), expected)
        assert result["dt"].iloc[0].value == 1700000000123456789
        assert pd.isna(result["dt"].iloc[1])
        if extended:
            assert result["s"].dtype.storage == storage
        for frame in (result, *pieces):
            frame.iat[0, 0] = 79
            assert frame.iat[0, 0] == 79
    assert queries == [query] * 2


@pytest.mark.parametrize("method", ["query_np", "query_df"])
def test_empty_and_error_contract(strict_client, client_factory, call, consume_stream, method):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas") if method == "query_df" else None
    client, queries = strict_client
    python = client_factory(native_codec="python")
    query = "SELECT toUInt16(number) AS n FROM numbers(0)"
    expected = call(getattr(python, method), query)
    output = call(getattr(client, method), query)
    if pd is None:
        np.testing.assert_equal(output, expected)
        assert output.dtype == expected.dtype
    else:
        pd.testing.assert_frame_equal(output, expected)
    pieces = []
    consume_stream(call(getattr(client, f"{method}_stream"), query), pieces.append)
    assert not pieces
    assert queries == [query] * 2
    bad = "SELECT toDateTime64(number, 1) AS dt FROM numbers(3)"
    for target in (python, client):
        with pytest.raises(ProgrammingError):
            call(getattr(target, method), bad)
    assert call(client.query, "SELECT 13").first_item == {"13": 13}


def test_missing_optional_dependency_errors(strict_client, call):
    client, _ = strict_client
    for package, method in (("numpy", "query_np"), ("pandas", "query_df"), ("pyarrow", "query_arrow")):
        if importlib.util.find_spec(package) is None:
            with pytest.raises(NotSupportedError, match="package is not installed"):
                call(getattr(client, method), "SELECT 13")


@pytest.mark.parametrize("method", ["query_df", "query_df_stream"])
def test_explicit_arrow_storage_dependency(strict_client, call, consume_stream, method):
    pd = pytest.importorskip("pandas")
    if importlib.util.find_spec("pyarrow") is not None:
        pytest.skip("Requires PyArrow to be absent")
    client, _ = strict_client
    query = "SELECT 'user_1' AS s"
    with pd.option_context("mode.string_storage", "pyarrow"):
        with pytest.raises(ImportError, match="pyarrow"):
            call(getattr(client, method), query, use_extended_dtypes=True)
    with pd.option_context("mode.string_storage", "python"):
        result = call(client.query_df, query, use_extended_dtypes=True)
        assert result["s"].tolist() == ["user_1"]
        assert result["s"].dtype.storage == "python"
        pieces = []
        consume_stream(call(client.query_df_stream, query, use_extended_dtypes=True), pieces.append)
        pd.testing.assert_frame_equal(pieces[0], result)


def test_arrow_wire_api_remains_optional(strict_client, call):
    pytest.importorskip("pyarrow")
    client, queries = strict_client
    table = call(client.query_arrow, "SELECT toUInt16(13) AS n, 'user_1' AS s")
    assert table.to_pydict() == {"n": [13], "s": ["user_1"]}
    assert str(table.schema.field("n").type) == "uint16"
    assert not queries
    assert call(client.query, "SELECT 79").first_row == (79,)


@pytest.mark.parametrize("method", ["query_row_block_stream", "query_np_stream", "query_df_stream"])
def test_stream_close_retains_output(strict_client, call, client_mode, transport_responses, method):
    if method != "query_row_block_stream":
        pytest.importorskip("numpy")
    if method == "query_df_stream":
        pytest.importorskip("pandas")
    client, queries = strict_client
    query = "SELECT number AS n FROM numbers(20000000)"
    stream = call(getattr(client, method), query, settings={"max_block_size": 8192, "max_threads": 1})
    source = stream.source.source
    response = transport_responses[-1]

    def retained(piece):
        return piece.iloc[1:4] if method == "query_df_stream" else piece[1:4]

    if client_mode == "sync":
        with stream:
            output = retained(next(stream))
    else:

        async def first(active_stream):
            async with active_stream:
                return retained(await active_stream.__anext__())

        output = call(first, stream)
    assert source.source is None
    assert source._thread is None or not source._thread.is_alive()
    if client_mode == "async":
        call(asyncio.sleep, 0)
        assert response.closed
    else:
        assert response.closed
    del stream
    gc.collect()
    if method == "query_df_stream":
        assert output["n"].tolist() == [1, 2, 3]
        output.iat[0, 0] = 79
    elif method == "query_np_stream":
        assert output[:, 0].tolist() == [1, 2, 3]
        output[0, 0] = 79
    else:
        assert output == [(1,), (2,), (3,)]
    assert queries == [query]
    assert call(client.query, "SELECT 79").first_row == (79,)


@pytest.mark.parametrize("method", ["query_row_block_stream", "query_np_stream", "query_df_stream"])
def test_late_error_releases_stream(strict_client, call, consume_stream, method):
    if method != "query_row_block_stream":
        pytest.importorskip("numpy")
    if method == "query_df_stream":
        pytest.importorskip("pandas")
    client, _ = strict_client
    query = "SELECT number, throwIf(number >= 1000000) FROM numbers(2000000)"
    stream = call(getattr(client, method), query, settings={"max_block_size": 8192, "max_threads": 1})
    source = stream.source.source
    pieces = []
    with pytest.raises(StreamFailureError):
        consume_stream(stream, lambda piece: pieces.append(len(piece)))
    assert pieces
    assert source.source is None
    assert source._thread is None or not source._thread.is_alive()
    assert call(client.query, "SELECT 79").first_row == (79,)


@pytest.mark.parametrize("method", ["query_np_stream", "query_df_stream"])
def test_async_cancellation_releases_stream(strict_client, call, client_mode, transport_responses, method):
    if client_mode != "async":
        pytest.skip("Cancellation uses the async consumer")
    pytest.importorskip("numpy")
    if method == "query_df_stream":
        pytest.importorskip("pandas")
    client, _ = strict_client

    async def cancel():
        stream = await getattr(client, method)("SELECT number AS n FROM numbers(20000000)", settings={"max_block_size": 8192})
        source = stream.source.source
        response = transport_responses[-1]
        received = asyncio.Event()
        pieces = []

        async def consume():
            async with stream:
                pieces.append(await stream.__anext__())
                received.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(consume())
        try:
            await asyncio.wait_for(received.wait(), 10)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            await asyncio.sleep(0)
            assert source.source is None
            assert source._thread is None or not source._thread.is_alive()
            assert response.closed
            assert len(pieces[0]) > 0
        finally:
            task.cancel()
            await stream.__aexit__(None, None, None)
        assert (await client.query("SELECT 13")).first_row == (13,)

    call(cancel)


@pytest.mark.parametrize("method", ["query_np_stream", "query_df_stream"])
def test_async_pending_read_cancellation_releases_worker(strict_client, call, client_mode, transport_responses, method):
    if client_mode != "async":
        pytest.skip("Cancellation uses the async consumer")
    np = pytest.importorskip("numpy")
    if method == "query_df_stream":
        pytest.importorskip("pandas")
    client, _ = strict_client
    query = "SELECT number AS n, repeat('x', 512) AS s, sleepEachRow(0.0001) AS d FROM numbers(20000)"
    consumer_done = Event()

    async def cancel_pending():
        stream = await getattr(client, method)(query, settings={"max_block_size": 1024, "max_threads": 1})
        source = stream.source.source
        response = transport_responses[-1]
        original = stream.gen

        def observed():
            try:
                yield from original
            finally:
                consumer_done.set()

        stream.gen = observed()
        try:
            async with stream:
                first = await stream.__anext__()
                while source._thread is None:
                    await stream.__anext__()
                for _ in range(50):
                    try:
                        await asyncio.wait_for(stream.__anext__(), 0.02)
                    except asyncio.TimeoutError:
                        break
                else:
                    pytest.fail("no pending read was cancelled")
            assert await asyncio.to_thread(consumer_done.wait, 2)
            assert source.source is None
            assert source._thread is None or not source._thread.is_alive()
            assert response.closed
            values = first["n"].to_numpy() if method == "query_df_stream" else first["n"]
            np.testing.assert_array_equal(values, np.arange(len(values)))
            assert (await client.query("SELECT 13")).first_row == (13,)
        finally:
            if not consumer_done.is_set():
                source.close()
                source.queue.put_nowait(("eof", None))
                await asyncio.to_thread(consumer_done.wait, 2)

    call(cancel_pending)
