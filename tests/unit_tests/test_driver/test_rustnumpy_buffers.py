"""Driver adapters over the binding's private column buffers."""

import gc
import sys
from contextlib import nullcontext
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
def test_nullable_simple_aggregate_keeps_arrow_numeric_semantics(core, monkeypatch, type_name, values, dtype, expected):
    np = pytest.importorskip("numpy")
    declared = f"Nullable(SimpleAggregateFunction(anyLast, {type_name}))"
    batch = _batch(core, [declared], [values])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
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
    assert [converter.needs_arrow for converter in converters] == [False, False, True, as_pandas, False, False, False]
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
    pieces = []
    if streaming:
        with result.df_stream if as_pandas else result.np_stream as stream:
            pieces = list(stream)
        output = pd.concat(pieces, ignore_index=True) if as_pandas else np.concatenate(pieces)
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
    for piece in pieces:
        if as_pandas:
            piece.iloc[0, 0] = -7
        else:
            assert piece.flags.writeable


@pytest.mark.parametrize("type_name,dtype,values", _NUMERIC_CASES[:10])
@pytest.mark.parametrize("wrapper", ["Nullable({})", "SimpleAggregateFunction(anyLast, Nullable({}))"])
@pytest.mark.parametrize("nulls", ["none", "some", "all"])
def test_nullable_numeric_buffers_without_arrow(core, monkeypatch, type_name, dtype, values, wrapper, nulls):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    values = list(values)
    if nulls == "some":
        values[1] = None
    elif nulls == "all":
        values = [None] * len(values)
    declared = wrapper.format(type_name)
    batch = _batch(core, [declared], [values])
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    converter = rustnumpy._build_converter(get_from_name(declared), context)
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    if dtype.startswith("float"):
        assert result.dtype == np.dtype("float64") and result.dtype.byteorder == "="
        np.testing.assert_array_equal(result, np.array(values, dtype="float64"))
        if nulls != "all":
            assert np.signbit(result[0])
    else:
        expected = pd.array(values, dtype=type_name)
        pd.testing.assert_extension_array_equal(result, expected)
        (descriptor,) = batch.column_buffers(0)
        source = np.frombuffer(descriptor.values, dtype=dtype)
        assert np.shares_memory(result.to_numpy(dtype=dtype, na_value=0, copy=False), source) is (nulls == "none")
        del source, descriptor
    del batch
    gc.collect()
    assert len(result) == len(values)
    if not dtype.startswith("float"):
        pd.testing.assert_extension_array_equal(result, expected)


@pytest.mark.parametrize("type_name,dtype,values", _NUMERIC_CASES[:10])
def test_nullable_numeric_buffers_empty_and_unequal_chunks(core, monkeypatch, type_name, dtype, values):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    declared = f"Nullable({type_name})"
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    chunks = [[], values[:1], [], [None, *values[1:]], []]
    decoder = core.StreamDecoder()
    batches = []
    for chunk in chunks:
        wire = core.encode_native_block(["c0"], [declared], [chunk], len(chunk), None)
        batches.extend(decoder.feed(wire))
    expected_dtype = np.dtype("float64") if dtype.startswith("float") else pd.api.types.pandas_dtype(type_name)
    for batch in (_batch(core, [declared], [[]]), batches[0], batches[2], batches[4]):
        result = converter(None, batch, 0)
        assert len(result) == 0 and result.dtype == expected_dtype
    result = converter(None, core.ColBatch.from_batches(batches), 0)
    expected_values = [values[0], None, *values[1:]]
    assert result.dtype == expected_dtype
    if dtype.startswith("float"):
        np.testing.assert_array_equal(result, np.array(expected_values, dtype="float64"))
    else:
        pd.testing.assert_extension_array_equal(result, pd.array(expected_values, dtype=type_name))


@pytest.mark.parametrize("rows", [0, 1, 7, 8, 9, 63, 64, 65])
@pytest.mark.parametrize("nulls", ["none", "some", "all"])
def test_buffer_null_mask_bit_boundaries(core, rows, nulls):
    np = pytest.importorskip("numpy")
    expected = np.array([nulls == "all" or (nulls == "some" and index % 3 == 0) for index in range(rows)], dtype="bool")
    values = [None if missing else 13 for missing in expected]
    wire = core.encode_native_block(["c0"], ["Nullable(Int16)"], [values], rows, None)
    (batch,) = core.StreamDecoder().feed(wire)
    (descriptor,) = batch.column_buffers(0)
    mask = rustnumpy._buffer_null_mask(descriptor)
    np.testing.assert_array_equal(mask, expected)
    assert mask.dtype == np.dtype("bool") and mask.flags.writeable
    mask[:] = False
    np.testing.assert_array_equal(rustnumpy._buffer_null_mask(descriptor), expected)


def test_buffer_null_mask_all_valid_and_tail_bits():
    np = pytest.importorskip("numpy")
    for validity in (None, b"\xff\xff"):
        descriptor = SimpleNamespace(validity=validity, length=9)
        np.testing.assert_array_equal(rustnumpy._buffer_null_mask(descriptor), np.zeros(9, dtype="bool"))


@pytest.mark.parametrize("byteorder,prefix", [("little", "<"), ("big", ">")])
def test_nullable_integer_buffers_normalize_byteorder(byteorder, prefix):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    values = np.array([2**64 - 1, 0, 2**63 + 13], dtype=f"{prefix}u8")
    descriptor = SimpleNamespace(kind="uint64", byteorder=byteorder, length=3, values=values.tobytes(), validity=b"\x05")
    batch = SimpleNamespace(column_buffers=lambda _index: [descriptor])
    result = rustnumpy._make_nullable_int_convert(pd.UInt64Dtype())(None, batch, 0)
    pd.testing.assert_extension_array_equal(result, pd.array([2**64 - 1, None, 2**63 + 13], dtype="UInt64"))


@pytest.mark.parametrize("type_name", ["Int32", "Float32", "Bool"])
@pytest.mark.parametrize("as_pandas,extended", [(False, False), (True, False), (True, True)])
def test_nullable_numeric_preserves_object_routes(core, monkeypatch, type_name, as_pandas, extended):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    declared = f"Nullable({type_name})"
    values = [None, True, False] if type_name == "Bool" else [None, 13, 79]
    batch = _batch(core, [declared], [values])
    ch_type = get_from_name(declared)
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=extended)
    converter = rustnumpy._build_converter(ch_type, context)
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    expected = ch_type._finalize_column(values, context)
    if isinstance(result, pd.api.extensions.ExtensionArray):
        pd.testing.assert_extension_array_equal(result, expected)
    elif type_name == "Float32" and extended:
        np.testing.assert_array_equal(result, np.array(values, dtype="float64"))
    else:
        np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("type_name,dtype,values", [_NUMERIC_CASES[index] for index in (3, 7, 8, 10)])
@pytest.mark.parametrize(
    "wrapper",
    [
        "Nullable(SimpleAggregateFunction(anyLast, {}))",
        "SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Nullable({})))",
    ],
)
@pytest.mark.parametrize("has_null", [False, True])
def test_numeric_alias_buffers_preserve_chunk_promotion(core, monkeypatch, type_name, dtype, values, wrapper, has_null):
    np = pytest.importorskip("numpy")
    declared = wrapper.format(type_name)
    chunks = [values[:1], [None if has_null else values[1], *values[2:]]]
    batch = core.ColBatch.from_batches([_batch(core, [declared], [chunk]) for chunk in chunks])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    expected_dtype = ("object" if dtype == "bool" else "float64") if has_null and not dtype.startswith("float") else dtype
    expected_values = chunks[0] + chunks[1]
    expected = np.array(expected_values, dtype=expected_dtype)
    assert result.dtype == np.dtype(expected_dtype)
    assert result.dtype.byteorder == np.dtype(expected_dtype).byteorder
    np.testing.assert_array_equal(result, expected)


def test_nested_nullable_integer_alias_preserves_dtype_error():
    pytest.importorskip("pandas")
    declared = "SimpleAggregateFunction(anyLast, Nullable(SimpleAggregateFunction(anyLast, Int32)))"
    with pytest.raises(TypeError, match="data type 'SimpleAggregateFunction' not understood"):
        rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))


@pytest.mark.parametrize("block_count", [1, 2])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("extended", [False, True])
def test_nullable_numeric_dataframe_boundary(core, monkeypatch, block_count, streaming, extended):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    type_names = ["Nullable(UInt64)", "Nullable(Float32)", "Nullable(Bool)", "Array(Int32)"]
    types = [get_from_name(name) for name in type_names]
    columns = [[2**64 - 1, None, 2**63 + 13], [-0.0, None, float("nan")], [True, None, False], [[13], [], [79]]]
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=extended)
    converters = rustnumpy._build_converters(types, context)
    monkeypatch.setattr(rustnumpy.options, "arrow", None)

    def blocks():
        for rows in (slice(None),) if block_count == 1 else (slice(0, 1), slice(1, None)):
            batch = _batch(core, type_names, [column[rows] for column in columns])
            yield rustnumpy._convert_block(batch, converters)

    result = NumpyResult(blocks(), ("c0", "c1", "c2", "c3"), tuple(types), [typ.np_type for typ in types])
    pieces = []
    if streaming:
        with result.df_stream as stream:
            pieces = list(stream)
        frame = pd.concat(pieces, ignore_index=True)
    else:
        frame = result.df_result
    if extended:
        pd.testing.assert_extension_array_equal(frame["c0"].array, pd.array(columns[0], dtype="UInt64"))
        assert frame["c1"].dtype == np.dtype("float64")
    else:
        expected = pd.concat(
            [pd.Series(columns[0][rows]) for rows in ((slice(None),) if block_count == 1 else (slice(0, 1), slice(1, None)))],
            ignore_index=True,
        )
        pd.testing.assert_series_equal(frame["c0"], expected, check_names=False)
    assert np.signbit(frame["c1"].iloc[0])
    assert list(frame["c2"]) == columns[2]
    assert list(frame["c3"]) == columns[3]
    frame.iloc[0, 0] = 79
    frame.iloc[1, 1] = 13.5
    frame.iloc[1, 2] = True
    for piece in pieces:
        piece.iloc[0, 0] = 79


_BFLOAT16_WORDS = [0, 0x8000, 1, 0x007F, 0x0080, 0x3F8C, 0xBF8C, 0x7F7F, 0xFF7F, 0x7F80, 0xFF80, 0x7F81, 0x7FC1, 0xFFFF]
_INTERVAL_UNITS = ["Nanosecond", "Microsecond", "Millisecond", "Second", "Minute", "Hour", "Day", "Week", "Month", "Quarter", "Year"]


def _bfloat16_wire(core, declared, words, nulls):
    np = pytest.importorskip("numpy")
    values = [None if missing else 0.0 for missing in nulls]
    wire = core.encode_native_block(["c0"], [declared], [values], len(words), None)
    return wire[: -2 * len(words)] + np.array(words, dtype="<u2").tobytes() if words else wire


@pytest.mark.parametrize(
    "wrapper",
    [
        "{}",
        "Nullable({})",
        "SimpleAggregateFunction(anyLast, {})",
        "SimpleAggregateFunction(anyLast, Nullable({}))",
        "Nullable(SimpleAggregateFunction(anyLast, {}))",
    ],
)
@pytest.mark.parametrize("as_pandas,extended", [(False, False), (True, False), (True, True)])
@pytest.mark.parametrize("nulls", ["none", "some", "all"])
def test_bfloat16_buffers_exact_words(core, monkeypatch, wrapper, as_pandas, extended, nulls):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    declared = wrapper.format("BFloat16")
    missing = ["Nullable" in declared and (nulls == "all" or nulls == "some" and index % 3 == 2) for index in range(len(_BFLOAT16_WORDS))]
    batch = core.ColBatch.decode_native(_bfloat16_wire(core, declared, _BFLOAT16_WORDS, missing))
    converter = rustnumpy._build_converter(
        get_from_name(declared), QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=extended)
    )
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    expected = (np.array(_BFLOAT16_WORDS, dtype="uint32") << np.uint32(16)).view("float32")
    expected[missing] = np.nan
    extension = extended and wrapper in ("Nullable({})", "SimpleAggregateFunction(anyLast, Nullable({}))")
    if extension:
        pd.testing.assert_extension_array_equal(result, pd.array(expected, dtype="Float32"))
        np.testing.assert_array_equal(result.isna(), np.isnan(expected))
    else:
        assert result.dtype == np.dtype("float32") and result.dtype.byteorder == "="
        np.testing.assert_array_equal(result.view("uint32"), expected.view("uint32"))
    del batch
    gc.collect()
    if extension:
        pd.testing.assert_extension_array_equal(result, pd.array(expected, dtype="Float32"))
    else:
        np.testing.assert_array_equal(result.view("uint32"), expected.view("uint32"))


@pytest.mark.parametrize("distinguish_nan", [False, True])
@pytest.mark.parametrize("streaming", [False, True])
def test_bfloat16_extended_preserves_pandas_nan_option(core, distinguish_nan, streaming):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    has_option = hasattr(getattr(pd.options, "future", None), "distinguish_nan_and_na")
    if distinguish_nan and not has_option:
        pytest.skip("pandas doesn't expose distinguish_nan_and_na")
    declared = "Nullable(BFloat16)"
    ch_type = get_from_name(declared)
    batch = core.ColBatch.decode_native(_bfloat16_wire(core, declared, [0x4150, 0x7FC1, 0, 0x8000], [False, False, True, False]))
    converter = rustnumpy._build_converter(ch_type, QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    option_context = pd.option_context("future.distinguish_nan_and_na", distinguish_nan) if has_option else nullcontext()
    with option_context:
        values = converter(None, batch, 0)
        expected_mask = [False, not distinguish_nan, not distinguish_nan, False]
        assert values.dtype == pd.Float32Dtype()
        np.testing.assert_array_equal(values.isna(), expected_mask)

        def blocks():
            yield [converter(None, batch, 0)]

        result = NumpyResult(blocks(), ("c0",), (ch_type,), ("float32",))
        if streaming:
            with result.df_stream as stream:
                (frame,) = list(stream)
        else:
            frame = result.df_result
        assert frame["c0"].dtype == pd.Float32Dtype()
        assert frame["c0"].count() == (4 if distinguish_nan else 2)
        np.testing.assert_array_equal(frame["c0"].isna(), expected_mask)
        assert frame["c0"].iloc[0] == np.float32(13)
        assert np.signbit(frame["c0"].iloc[3])


@pytest.mark.parametrize("word,signaling", [(0x7F81, True), (0xFF81, True), (0x7FC1, False), (0xFFC1, False)])
@pytest.mark.parametrize("distinguish_nan", [False, True])
def test_bfloat16_extended_nan_ufunc_behavior(core, word, signaling, distinguish_nan):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    has_option = hasattr(getattr(pd.options, "future", None), "distinguish_nan_and_na")
    if distinguish_nan and not has_option:
        pytest.skip("pandas doesn't expose distinguish_nan_and_na")
    declared = "Nullable(BFloat16)"
    ch_type = get_from_name(declared)
    batch = core.ColBatch.decode_native(_bfloat16_wire(core, declared, [0x4150, word, 0], [False, False, True]))
    converter = rustnumpy._build_converter(ch_type, QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    option_context = pd.option_context("future.distinguish_nan_and_na", distinguish_nan) if has_option else nullcontext()
    with option_context:

        def blocks():
            yield [converter(None, batch, 0)]

        frame = NumpyResult(blocks(), ("c0",), (ch_type,), ("float32",)).df_result
        with np.errstate(invalid="raise"):
            if distinguish_nan and signaling:
                with pytest.raises(FloatingPointError, match="invalid value encountered in sqrt"):
                    np.sqrt(frame["c0"])
            else:
                result = np.sqrt(frame["c0"])
                assert result.dtype == pd.Float32Dtype()
                np.testing.assert_array_equal(result.isna(), [False, not distinguish_nan, not distinguish_nan])


@pytest.mark.parametrize("unit", _INTERVAL_UNITS)
@pytest.mark.parametrize(
    "wrapper", ["{}", "Nullable({})", "SimpleAggregateFunction(anyLast, {})", "SimpleAggregateFunction(anyLast, Nullable({}))"]
)
def test_interval_buffers_signed_counts(core, monkeypatch, unit, wrapper):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    declared = wrapper.format(f"Interval{unit}")
    nullable = "Nullable" in declared
    values = [-(2**63), None if nullable else -13, 0, 2**63 - 1]
    batch = _batch(core, [declared], [values])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    if nullable:
        pd.testing.assert_extension_array_equal(result, pd.array(values, dtype="Int64"))
    else:
        assert result.dtype == np.dtype("int64") and result.dtype.byteorder == "="
        np.testing.assert_array_equal(result, values)
        (descriptor,) = batch.column_buffers(0)
        assert np.shares_memory(result, np.frombuffer(descriptor.values, dtype="int64"))
        assert not result.flags.writeable
        del descriptor
    del batch
    gc.collect()
    assert list(result)[-1] == 2**63 - 1


@pytest.mark.parametrize(
    "declared,dtype",
    [("BFloat16", "float32"), ("Nullable(BFloat16)", "Float32"), ("IntervalYear", "int64"), ("Nullable(IntervalSecond)", "Int64")],
)
def test_bfloat16_interval_buffers_empty_and_chunks(core, monkeypatch, declared, dtype):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    expected_dtype = pd.api.types.pandas_dtype(dtype)
    decoder = core.StreamDecoder()
    batches = []
    chunks = [[], [13], [], [None if "Nullable" in declared else 0, 79], []]
    for chunk in chunks:
        wire = core.encode_native_block(["c0"], [declared], [chunk], len(chunk), None)
        batches.extend(decoder.feed(wire))
    for batch in [_batch(core, [declared], [[]]), batches[0], batches[2], batches[4]]:
        output = converter(None, batch, 0)
        assert len(output) == 0 and output.dtype == expected_dtype
    output = converter(None, core.ColBatch.from_batches(batches), 0)
    values = chunks[1] + chunks[3]
    if "Nullable" in declared:
        pd.testing.assert_extension_array_equal(output, pd.array(values, dtype=dtype))
    else:
        np.testing.assert_array_equal(output, np.array(values, dtype=dtype))
    assert output.dtype == expected_dtype


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("multiple", [False, True])
def test_bfloat16_nested_alias_keeps_binary_output(core, monkeypatch, nullable, multiple):
    np = pytest.importorskip("numpy")
    leaf = "Nullable(BFloat16)" if nullable else "BFloat16"
    declared = f"SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, {leaf}))"
    nulls = [nullable and index % 3 == 2 for index in range(len(_BFLOAT16_WORDS))]
    rows = [slice(0, 1), slice(1, None)] if multiple else [slice(None)]
    batches = [core.ColBatch.decode_native(_bfloat16_wire(core, declared, _BFLOAT16_WORDS[row], nulls[row])) for row in rows]
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(core.ColBatch.from_batches(batches), [converter])
    assert result.dtype == np.dtype(object)
    assert list(result) == [None if missing else word.to_bytes(2, "little") for word, missing in zip(_BFLOAT16_WORDS, nulls)]


@pytest.mark.parametrize("words,missing", [([0x3131, 0x3937], [False, False]), ([0, 0], [True, True]), ([0x4150], [False])])
def test_bfloat16_nested_nullable_alias_preserves_float_cast(core, monkeypatch, words, missing):
    np = pytest.importorskip("numpy")
    pytest.importorskip("pandas")
    declared = "SimpleAggregateFunction(anyLast, Nullable(SimpleAggregateFunction(anyLast, BFloat16)))"
    batch = core.ColBatch.decode_native(_bfloat16_wire(core, declared, words, missing))
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    if words == [0x4150]:
        with pytest.raises(ValueError, match="could not convert string to float"):
            rustnumpy._convert_block(batch, [converter])
    else:
        (result,) = rustnumpy._convert_block(batch, [converter])
        expected = np.array([None if null else word.to_bytes(2, "little") for word, null in zip(words, missing)], dtype="float64")
        np.testing.assert_array_equal(result, expected)
        assert result.dtype == np.dtype("float64")


@pytest.mark.parametrize("unit", _INTERVAL_UNITS)
@pytest.mark.parametrize("nullable", [False, True])
def test_interval_nested_alias_keeps_arrow_units(core, monkeypatch, unit, nullable):
    np = pytest.importorskip("numpy")
    leaf = f"Interval{unit}"
    if nullable:
        leaf = f"Nullable({leaf})"
    declared = f"SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, {leaf}))"
    values = [-(2**63), None if nullable else 0, 2**63 - 1]
    batch = core.ColBatch.from_batches([_batch(core, [declared], [[values[0]]]), _batch(core, [declared], [values[1:]])])
    converter = rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))
    assert converter.needs_arrow is False
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    (result,) = rustnumpy._convert_block(batch, [converter])
    duration_unit = {"Nanosecond": "ns", "Microsecond": "us", "Millisecond": "ms", "Second": "s"}.get(unit)
    dtype = f"timedelta64[{duration_unit}]" if duration_unit else "float64" if nullable else "int64"
    expected = np.array(values, dtype=dtype)
    np.testing.assert_array_equal(result, expected)
    assert result.dtype == np.dtype(dtype) and result.dtype.byteorder == "="


@pytest.mark.parametrize("block_count", [1, 2])
@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("as_pandas,extended", [(False, False), (True, False), (True, True)])
def test_bfloat16_interval_result_boundary(core, monkeypatch, block_count, streaming, as_pandas, extended):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    names = ["BFloat16", "Nullable(BFloat16)", "IntervalNanosecond", "Nullable(IntervalYear)"]
    types = [get_from_name(name) for name in names]
    columns = [[-0.0, 1.5, 79.0], [None, -0.0, float("nan")], [-(2**63), 0, 2**63 - 1], [None, -13, 79]]
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=extended)
    converters = rustnumpy._build_converters(types, context)
    monkeypatch.setattr(rustnumpy.options, "arrow", None)

    def blocks():
        for rows in [slice(None)] if block_count == 1 else [slice(0, 1), slice(1, None)]:
            yield rustnumpy._convert_block(_batch(core, names, [column[rows] for column in columns]), converters)

    result = NumpyResult(blocks(), ("bf", "nbf", "iv", "niv"), tuple(types), ["float32", "float32", "int64", object])
    pieces = []
    if streaming:
        with result.df_stream if as_pandas else result.np_stream as stream:
            pieces = list(stream)
        output = pd.concat(pieces, ignore_index=True) if as_pandas else np.concatenate(pieces)
    else:
        output = result.df_result if as_pandas else result.np_result
    if as_pandas:
        assert output["bf"].dtype == np.dtype("float32")
        assert output["iv"].dtype == np.dtype("int64")
        assert np.signbit(output["bf"].iloc[0])
        assert output["iv"].tolist() == columns[2]
        if extended:
            pd.testing.assert_extension_array_equal(output["nbf"].array, pd.array(columns[1], dtype="Float32"))
            pd.testing.assert_extension_array_equal(output["niv"].array, pd.array(columns[3], dtype="Int64"))
        else:
            assert output["nbf"].dtype == np.dtype("float32")
            np.testing.assert_array_equal(output["nbf"].to_numpy(), np.array(columns[1], dtype="float32"))
        for frame in [output, *pieces]:
            frame.iloc[0, 0] = 13.5
            frame.iloc[0, 2] = 79
    else:
        assert output.dtype == np.dtype([("bf", "float32"), ("nbf", "float32"), ("iv", "int64"), ("niv", object)])
        assert list(output["iv"]) == columns[2]
        assert list(output["niv"]) == columns[3]
        np.testing.assert_array_equal(output["nbf"].astype("float32"), np.array(columns[1], dtype="float32"))
        for block in [output, *pieces]:
            assert block.flags.writeable
            block["iv"][0] = 79
