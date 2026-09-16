"""Driver adapters over the binding's private column buffers."""

import gc
import sys
from types import SimpleNamespace

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import rustnumpy
from clickhouse_connect.driver.npquery import NumpyResult
from clickhouse_connect.driver.query import QueryContext

_NUMERIC_CASES = [
    ("Int8", "int8", [-128, -13, 0, 127]),
    ("Int16", "int16", [-32768, -79, 0, 32767]),
    ("Int32", "int32", [-(2**31), -79, 0, 2**31 - 1]),
    ("Int64", "int64", [-(2**63), -79, 0, 2**63 - 1]),
    ("UInt8", "uint8", [0, 13, 79, 255]),
    ("UInt16", "uint16", [0, 13, 79, 65535]),
    ("UInt32", "uint32", [0, 13, 79, 2**32 - 1]),
    ("UInt64", "uint64", [0, 13, 79, 2**64 - 1]),
    ("Float32", "float32", [-0.0, 1.25, float("inf"), float("nan")]),
    ("Float64", "float64", [-0.0, -79.5, float("-inf"), float("nan")]),
    ("Bool", "bool", [False, True, False, True]),
    ("Boolean", "bool", [False, True, False, True]),
]


@pytest.fixture(name="core")
def core_fixture():
    pytest.importorskip("numpy")
    return pytest.importorskip("_ch_core")


def _batch(core, type_names, columns):
    names = [f"c{index}" for index in range(len(columns))]
    wire = core.encode_native_block(names, type_names, columns, len(columns[0]), None)
    return core.ColBatch.decode_native(wire)


@pytest.mark.parametrize("type_name,dtype,values", _NUMERIC_CASES)
@pytest.mark.parametrize("wrapper", ["{}", "SimpleAggregateFunction(anyLast, {})"])
def test_numeric_buffers_without_arrow(core, monkeypatch, type_name, dtype, values, wrapper):
    np = pytest.importorskip("numpy")
    declared = wrapper.format(type_name)
    batch = _batch(core, [declared], [values])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)

    (result,) = rustnumpy._convert_block(batch, [converter])
    np.testing.assert_array_equal(result, np.array(values, dtype=dtype))
    assert result.dtype == np.dtype(dtype)
    assert result.dtype.byteorder == np.dtype(dtype).byteorder
    if dtype.startswith("float"):
        assert np.signbit(result[0])
    if dtype != "bool":
        (descriptor,) = batch.column_buffers(0)
        source = np.frombuffer(descriptor.values, dtype=dtype)
        assert np.shares_memory(result, source)
        assert not result.flags.writeable
        del source, descriptor
    del batch
    gc.collect()
    np.testing.assert_array_equal(result, np.array(values, dtype=dtype))


@pytest.mark.parametrize("type_name,dtype,values", _NUMERIC_CASES)
def test_numeric_buffers_empty_and_unequal_chunks(core, type_name, dtype, values):
    np = pytest.importorskip("numpy")
    converter = rustnumpy._build_converter(get_from_name(type_name), QueryContext(use_numpy=True))
    empty = _batch(core, [type_name], [[]])
    assert empty.column_buffers(0) == []
    result = converter(None, empty, 0)
    assert result.shape == (0,) and result.dtype == np.dtype(dtype)

    decoder = core.StreamDecoder()
    batches = []
    for chunk in ([], values[:1], [], values[1:], []):
        wire = core.encode_native_block(["c0"], [type_name], [chunk], len(chunk), None)
        batches.extend(decoder.feed(wire))
    assert [part.column_buffers(0)[0].length for part in batches] == [0, 1, 0, len(values) - 1, 0]
    for part in (batches[0], batches[2], batches[4]):
        result = converter(None, part, 0)
        assert result.shape == (0,) and result.dtype == np.dtype(dtype)
    batch = core.ColBatch.from_batches(batches)
    assert [column.length for column in batch.column_buffers(0)] == [1, len(values) - 1]
    result = converter(None, batch, 0)
    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array(values, dtype=dtype))


@pytest.mark.parametrize("rows", [0, 1, 7, 8, 9, 63, 64, 65])
def test_bool_buffers_bit_boundaries(core, rows):
    np = pytest.importorskip("numpy")
    values = [index % 3 != 0 for index in range(rows)]
    wire = core.encode_native_block(["c0"], ["Bool"], [values], rows, None)
    (batch,) = core.StreamDecoder().feed(wire)
    converter = rustnumpy._build_converter(get_from_name("Bool"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)
    assert result.dtype == np.dtype("bool")
    np.testing.assert_array_equal(result, values)


@pytest.mark.parametrize("byteorder,prefix", [("little", "<"), ("big", ">")])
def test_buffer_values_respect_descriptor_byteorder(byteorder, prefix):
    np = pytest.importorskip("numpy")
    source = np.array([-79, 13], dtype=f"{prefix}i4")
    column = SimpleNamespace(kind="int32", byteorder=byteorder, length=2, values=source.tobytes())
    result = rustnumpy._buffer_values(column)
    assert result.dtype == source.dtype
    assert result.dtype.byteorder == ("=" if byteorder == sys.byteorder else prefix)
    np.testing.assert_array_equal(result, [-79, 13])


def test_buffer_values_reject_unknown_kind():
    with pytest.raises(NotImplementedError, match="Unsupported column buffer kind"):
        rustnumpy._buffer_values(SimpleNamespace(kind="future_kind"))


@pytest.mark.parametrize(
    "type_name,values,dtype,expected",
    [
        ("Int32", [13, None, 79], "float64", [13.0, float("nan"), 79.0]),
        ("UInt64", [13, None, 79], "float64", [13.0, float("nan"), 79.0]),
        ("Float32", [1.25, None, -79.5], "float32", [1.25, float("nan"), -79.5]),
        ("Bool", [True, None, False], "object", [True, None, False]),
    ],
)
def test_nullable_simple_aggregate_keeps_arrow_numeric_semantics(core, type_name, values, dtype, expected):
    np = pytest.importorskip("numpy")
    pytest.importorskip("pyarrow")
    declared = f"Nullable(SimpleAggregateFunction(anyLast, {type_name}))"
    batch = _batch(core, [declared], [values])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is True
    (result,) = rustnumpy._convert_block(batch, [converter])
    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array(expected, dtype=dtype))


@pytest.mark.parametrize("as_pandas", [False, True])
def test_mixed_buffer_arrow_and_object_converters(core, as_pandas):
    np = pytest.importorskip("numpy")
    pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    type_names = ["UInt64", "Bool", "Date", "String", "Nullable(Int32)", "Enum8('low'=13, 'high'=79)", "IPv4"]
    columns = [[13, 79], [False, True], [0, 13], ["user_1", "user_2"], [None, 13], ["low", "high"], ["127.0.0.1", "127.0.0.2"]]
    batch = _batch(core, type_names, columns)
    types = [get_from_name(name) for name in type_names]
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=as_pandas)
    converters = rustnumpy._build_converters(types, context)
    assert [converter.needs_arrow for converter in converters] == [False, False, True, as_pandas, as_pandas, False, False]
    result = rustnumpy._convert_block(batch, converters)
    np.testing.assert_array_equal(result[0], np.array([13, 79], dtype="uint64"))
    np.testing.assert_array_equal(result[1], [False, True])
    days = np.array([0, 13], dtype="datetime64[D]")
    np.testing.assert_array_equal(result[2], days.astype("datetime64[s]") if as_pandas else days)
    assert list(result[3]) == columns[3]
    assert list(result[4]) == [pd.NA if as_pandas else None, 13]
    assert list(result[5]) == columns[5]
    assert [str(value) for value in result[6]] == columns[6]


@pytest.mark.parametrize("block_count", [1, 2])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("as_pandas", [False, True])
@pytest.mark.parametrize("structured", [False, True])
def test_numeric_result_boundary_is_writable(core, block_count, streaming, as_pandas, structured):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    type_names = ["Int32", "Bool" if structured else "Int32"]
    types = [get_from_name(name) for name in type_names]
    context = QueryContext(use_numpy=True, as_pandas=as_pandas)
    converters = rustnumpy._build_converters(types, context)

    def blocks():
        for values in ([13, 79, -3],) if block_count == 1 else ([13], [79, -3]):
            second = [value > 0 for value in values] if structured else values
            batch = _batch(core, type_names, [values, second])
            yield rustnumpy._convert_block(batch, converters)

    result = NumpyResult(blocks(), ("c0", "c1"), tuple(types), [typ.np_type for typ in types])
    if streaming:
        with result.df_stream if as_pandas else result.np_stream as stream:
            pieces = list(stream)
        output = pd.concat(pieces, ignore_index=True) if as_pandas else np.concatenate(pieces)
        for piece in pieces:
            if as_pandas:
                piece.iloc[0, 0] = -7
            else:
                assert piece.flags.writeable
    else:
        output = result.df_result if as_pandas else result.np_result
    if as_pandas:
        assert output["c0"].dtype == np.dtype("int32")
        assert output["c0"].dtype.byteorder == "="
        assert output["c1"].dtype == np.dtype("bool" if structured else "int32")
        assert list(output["c0"]) == [13, 79, -3]
        output.iloc[0, 0] = -7
    else:
        assert output.flags.writeable
        if structured:
            assert output.dtype == np.dtype([("c0", "int32"), ("c1", "bool")])
            np.testing.assert_array_equal(output["c0"], [13, 79, -3])
            output["c0"][0] = -7
        else:
            assert output.shape == (3, 2) and output.dtype == np.dtype("int32")
            np.testing.assert_array_equal(output[:, 0], [13, 79, -3])
            output[0, 0] = -7
