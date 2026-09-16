"""Private scalar buffers need no Arrow Python package."""

import gc
import math
import struct
import sys
import weakref

import pytest
from helpers import _INTERVAL_TYPES, _bfloat16_bytes, _ch_core, build_native_block, build_native_block_from_bodies

NUMERIC_CASES = [
    ("Int8", "b", [-128, -13, 0, 127]),
    ("Int16", "h", [-32768, -79, 0, 32767]),
    ("Int32", "i", [-(2**31), -79, 0, 2**31 - 1]),
    ("Int64", "q", [-(2**63), -79, 0, 2**63 - 1]),
    ("UInt8", "B", [0, 13, 79, 255]),
    ("UInt16", "H", [0, 13, 79, 65535]),
    ("UInt32", "I", [0, 13, 79, 2**32 - 1]),
    ("UInt64", "Q", [0, 13, 79, 2**64 - 1]),
    ("Float32", "f", [-0.0, 1.25, -79.5, math.inf]),
    ("Float64", "d", [-0.0, 1.25, -79.5, math.inf]),
]

SCALAR_CASES = [
    ("Bool", "bool_bitmap", None, [True, False, True]),
    ("BFloat16", "bfloat16", None, [1.25, -0.0, -79.5]),
    ("Date", "uint16", "H", [0, 13, 65535]),
    ("Date32", "int32", "i", [-(2**31), 13, 2**31 - 1]),
    ("DateTime", "uint32", "I", [0, 13, 2**32 - 1]),
    ("DateTime('America/New_York')", "uint32", "I", [0, 13, 2**32 - 1]),
    ("Time", "int32", "i", [-(2**31), 0, 2**31 - 1]),
    *[(f"DateTime64({precision}, 'UTC')", "int64", "q", [-(2**63), -13, 2**63 - 1]) for precision in (0, 3, 6, 9)],
    *[(f"Time64({precision})", "int64", "q", [-(2**63), 0, 2**63 - 1]) for precision in (0, 3, 6, 9)],
    *[(type_name, "int64", "q", [-(2**63), 13, 2**63 - 1]) for type_name in _INTERVAL_TYPES],
]

SCALAR_WRAPPERS = [
    "{}",
    "Nullable({})",
    "SimpleAggregateFunction(anyLast, {})",
    "SimpleAggregateFunction(anyLast, Nullable({}))",
    "Nullable(SimpleAggregateFunction(anyLast, {}))",
    "SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Nullable({})))",
]


@pytest.mark.parametrize("type_name,kind,format_code,source", SCALAR_CASES)
@pytest.mark.parametrize("wrapper", SCALAR_WRAPPERS)
def test_scalar_buffers(type_name, kind, format_code, source, wrapper):
    declared_type = wrapper.format(type_name)
    values = [source[0], None, *source[1:]] if "Nullable" in wrapper else source
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", declared_type, values)]))
    (column,) = batch.column_buffers(0)
    view = memoryview(column.values)
    assert column.kind == kind
    assert column.length == len(values)
    assert column.null_count == values.count(None)
    assert column.offsets is None and column.child is None
    assert view.readonly and view.format == "B" and view.itemsize == 1
    expected = [0 if value is None else value for value in values]
    if kind == "bool_bitmap":
        assert column.itemsize == 0 and column.byteorder == "not-applicable"
        assert len(view) == (len(values) + 7) // 8
        expected_bits = sum(1 << index for index, value in enumerate(expected) if value)
        assert int.from_bytes(view, "little") & ((1 << len(values)) - 1) == expected_bits
    elif kind == "bfloat16":
        assert column.itemsize == 2 and column.byteorder == "little"
        assert view.tobytes() == b"".join(_bfloat16_bytes(value) for value in expected)
    else:
        assert column.itemsize == struct.calcsize(f"={format_code}")
        assert column.byteorder == sys.byteorder
        assert view.tobytes() == struct.pack(f"={len(values)}{format_code}", *expected)
    if "Nullable" in wrapper:
        assert bytes(column.validity) == b"\x0d"
    else:
        assert column.validity is None


@pytest.mark.parametrize("type_name,kind,format_code,source", SCALAR_CASES)
@pytest.mark.parametrize("nullable", [False, True])
def test_empty_scalar_buffers(type_name, kind, format_code, source, nullable):
    declared_type = f"Nullable({type_name})" if nullable else type_name
    data = build_native_block([("v", declared_type, [])])
    assert _ch_core.ColBatch.decode_native(data).column_buffers(0) == []
    (batch,) = _ch_core.StreamDecoder().feed(data)
    (column,) = batch.column_buffers(0)
    assert column.kind == kind
    assert column.length == column.null_count == 0
    assert bytes(column.values) == b""
    if nullable:
        assert column.validity is not None and bytes(column.validity) == b""
    else:
        assert column.validity is None


@pytest.mark.parametrize("rows", [0, 1, 7, 8, 9, 64, 65])
@pytest.mark.parametrize(
    "type_name,pattern",
    [
        ("Bool", [False]),
        ("Bool", [True]),
        ("Bool", [True, False, False]),
        ("Nullable(Bool)", [False]),
        ("Nullable(Bool)", [True]),
        ("Nullable(Bool)", [None]),
        ("Nullable(Bool)", [True, None, False]),
    ],
)
def test_boolean_value_and_validity_boundaries(rows, type_name, pattern):
    values = [pattern[index % len(pattern)] for index in range(rows)]
    data = build_native_block([("v", type_name, values)])
    (batch,) = _ch_core.StreamDecoder().feed(data)
    (column,) = batch.column_buffers(0)
    bits = (1 << rows) - 1
    assert len(memoryview(column.values)) == (rows + 7) // 8
    assert int.from_bytes(column.values, "little") & bits == sum(1 << index for index, value in enumerate(values) if value)
    assert column.null_count == values.count(None)
    if type_name == "Bool":
        assert column.validity is None
    else:
        assert len(memoryview(column.validity)) == (rows + 7) // 8
        assert int.from_bytes(column.validity, "little") & bits == sum(
            1 << index for index, value in enumerate(values) if value is not None
        )


def test_bfloat16_raw_words_are_unchanged():
    # Preserve signed zero, infinities, NaN payloads and subnormals as raw words.
    words = [0x0000, 0x8000, 0x7F80, 0xFF80, 0x7FC1, 0x7FA5, 0x0001, 0x8001]
    raw = struct.pack(f"<{len(words)}H", *words)
    batch = _ch_core.ColBatch.decode_native(build_native_block_from_bodies([("v", "BFloat16", raw)], len(words)))
    assert bytes(batch.column_buffers(0)[0].values) == raw


@pytest.mark.parametrize("type_name,format_code,values", NUMERIC_CASES)
@pytest.mark.parametrize("nullable", [False, True])
def test_numeric_buffers(type_name, format_code, values, nullable):
    source = values + [None, values[1], None, None, values[2]] if nullable else values
    declared_type = f"Nullable({type_name})" if nullable else type_name
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", declared_type, source)]))
    (column,) = batch.column_buffers(0)
    view = memoryview(column.values)

    assert _ch_core.COLUMN_BUFFER_API_VERSION == 1
    assert column.kind == type_name.lower()
    assert column.itemsize == struct.calcsize(f"={format_code}")
    assert column.byteorder == sys.byteorder
    assert column.length == len(source)
    assert column.null_count == (3 if nullable else 0)
    assert view.readonly and view.c_contiguous
    assert view.format == "B" and view.itemsize == 1
    expected = [0 if value is None else value for value in source]
    assert view.tobytes() == struct.pack(f"={len(source)}{format_code}", *expected)
    if nullable:
        validity = memoryview(column.validity)
        assert validity.readonly
        assert validity.tobytes() == b"\x2f\x01"
    else:
        assert column.validity is None
    with pytest.raises(TypeError):
        view[0] = 0
    with pytest.raises(AttributeError):
        column.length = 13
    with pytest.raises(TypeError):
        type(column)()


@pytest.mark.parametrize("rows", [7, 8, 9, 64, 65])
@pytest.mark.parametrize("pattern", [[None], [13], [13, None, 79]])
def test_nullable_bitmap_boundaries(rows, pattern):
    values = [pattern[index % len(pattern)] for index in range(rows)]
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", "Nullable(Int32)", values)]))
    (column,) = batch.column_buffers(0)
    assert column.null_count == sum(value is None for value in values)
    validity = memoryview(column.validity)
    assert len(validity) == (rows + 7) // 8
    expected = sum(1 << index for index, value in enumerate(values) if value is not None)
    assert int.from_bytes(validity, "little") & ((1 << rows) - 1) == expected


@pytest.mark.parametrize(
    "type_name,format_code,values",
    NUMERIC_CASES + [(type_name, format_code, values) for type_name, _, format_code, values in SCALAR_CASES if format_code],
)
@pytest.mark.parametrize("intake", ["decode_native", "StreamDecoder", "BlockDecoder", "from_batches"])
def test_numpy_shared_readonly_allocation_and_lifetime(type_name, format_code, values, intake):
    np = pytest.importorskip("numpy")
    data = build_native_block([("v", type_name, values)])
    if intake == "decode_native":
        producer = None
        batch = _ch_core.ColBatch.decode_native(data)
    elif intake == "StreamDecoder":
        producer = _ch_core.StreamDecoder()
        (batch,) = producer.feed(data)
    elif intake == "BlockDecoder":
        producer = _ch_core.BlockDecoder(data)
        batch = next(producer)
    else:
        producer = [_ch_core.ColBatch.decode_native(data)]
        batch = _ch_core.ColBatch.from_batches(producer)
    (column,) = batch.column_buffers(0)
    owner_ref = weakref.ref(column.values)
    array = np.frombuffer(column.values, dtype=f"={format_code}", count=column.length)
    other = np.frombuffer(batch.column_buffers(0)[0].values, dtype=array.dtype)

    assert not array.flags.owndata and not array.flags.writeable
    assert np.shares_memory(array, other)
    assert array.ctypes.data == other.ctypes.data
    with pytest.raises(ValueError):
        array.setflags(write=True)
    with pytest.raises(ValueError):
        array[0] = 13
    tail = array[1:][::2]
    del array, other, column, batch, producer
    gc.collect()
    assert owner_ref() is not None
    np.testing.assert_array_equal(tail, values[1::2])
    del tail
    gc.collect()
    assert owner_ref() is None


@pytest.mark.parametrize(
    "type_name,values,expected_tail",
    [
        ("Int64", [13, None, 79], struct.pack("=q", 79)),
        ("Bool", [True, None, False], b"\x01"),
        ("BFloat16", [13.0, None, -79.5], _bfloat16_bytes(-79.5)),
        ("DateTime64(9)", [13, None, 79], struct.pack("=q", 79)),
        ("IntervalNanosecond", [13, None, 79], struct.pack("=q", 79)),
    ],
)
def test_memoryview_slice_and_validity_outlive_producer(type_name, values, expected_tail):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", f"Nullable({type_name})", values)]))
    (column,) = batch.column_buffers(0)
    values_ref, validity_ref = weakref.ref(column.values), weakref.ref(column.validity)
    view = memoryview(column.values)
    tail = view[-len(expected_tail) :]
    validity = memoryview(column.validity)
    view.release()
    del view, column, batch
    gc.collect()
    assert values_ref() is not None and validity_ref() is not None
    assert tail.tobytes() == expected_tail
    assert validity.tobytes() == b"\x05"
    tail.release()
    validity.release()
    gc.collect()
    assert values_ref() is None and validity_ref() is None


@pytest.mark.parametrize("type_name", ["Int32", "Nullable(Int32)"])
def test_empty_result_and_empty_decoded_chunk(type_name):
    data = build_native_block([("v", type_name, [])])
    batch = _ch_core.ColBatch.decode_native(data)
    assert batch.column_buffers(0) == []
    decoder = _ch_core.StreamDecoder()
    (block,) = decoder.feed(data)
    (column,) = block.column_buffers(0)
    assert column.kind == "int32" and column.itemsize == 4
    assert column.length == column.null_count == 0
    assert bytes(column.values) == b""
    if type_name == "Nullable(Int32)":
        assert column.validity is not None
        assert bytes(column.validity) == b""
    else:
        assert column.validity is None
    np = pytest.importorskip("numpy")
    assert np.frombuffer(column.values, dtype="=i4", count=0).size == 0


@pytest.mark.parametrize(
    "type_name,chunks,expected",
    [
        ("Int64", ([13, 79], [-13]), (struct.pack("=qq", 13, 79), struct.pack("=q", -13))),
        ("Bool", ([True] * 3, [False] * 8 + [True]), (b"\x07", b"\x00\x01")),
        ("BFloat16", ([1.25, -79.5], [13.0]), (_bfloat16_bytes(1.25) + _bfloat16_bytes(-79.5), _bfloat16_bytes(13.0))),
    ],
)
def test_multiple_chunks_stay_separate(type_name, chunks, expected):
    blocks = [build_native_block([("v", type_name, values)]) for values in chunks]
    batch = _ch_core.ColBatch.decode_native(b"".join(blocks))
    first, second = batch.column_buffers(0)
    assert (first.length, second.length) == (len(chunks[0]), len(chunks[1]))
    assert (bytes(first.values), bytes(second.values)) == expected


@pytest.mark.parametrize(
    "type_name,values,validity",
    [
        ("SimpleAggregateFunction(sum, Int64)", [13, 79], None),
        ("SimpleAggregateFunction(anyLast, Nullable(Int64))", [13, None, 79], b"\x05"),
        ("Nullable(SimpleAggregateFunction(anyLast, Int64))", [13, None, 79], b"\x05"),
        ("SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Nullable(Int64)))", [13, None, 79], b"\x05"),
    ],
)
def test_numeric_aliases(type_name, values, validity):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, values)]))
    (column,) = batch.column_buffers(0)
    assert column.kind == "int64"
    assert column.null_count == values.count(None)
    expected = [0 if value is None else value for value in values]
    assert bytes(column.values) == struct.pack(f"={len(values)}q", *expected)
    if validity is None:
        assert column.validity is None
    else:
        assert bytes(column.validity) == validity


@pytest.mark.parametrize(
    "type_name,values",
    [
        ("String", ["user_1"]),
        ("Int128", [13]),
        ("IPv4", [13]),
        ("Decimal(9, 2)", [13]),
        ("Enum8('enabled' = 1)", [1]),
        ("Array(Int32)", [[13]]),
        ("Tuple(Int32)", [(13,)]),
        ("Array(Tuple(Int32))", [[(13,)]]),
        ("Tuple(Time64(9))", [(13,)]),
        ("Array(Tuple(Time64(9)))", [[(13,)]]),
        ("Tuple(Time)", [(13,)]),
        ("Array(Tuple(Time))", [[(13,)]]),
        ("LowCardinality(String)", ["user_1"]),
        ("LowCardinality(Int32)", [13]),
        ("Nullable(String)", [None]),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_unsupported_storage_is_distinct_from_empty(type_name, values, empty):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, [] if empty else values)]))
    assert batch.column_buffers(0) is None


@pytest.mark.parametrize("type_name,value", [("Bool", True), ("BFloat16", 1.25), ("Date", 13), ("DateTime", 13), ("IntervalSecond", 13)])
@pytest.mark.parametrize("wrapper", ["Array({})", "Tuple({})", "Array(Tuple({}))", "LowCardinality({})"])
@pytest.mark.parametrize("empty", [False, True])
def test_nested_scalar_storage_is_unsupported(type_name, value, wrapper, empty):
    if wrapper == "Array(Tuple({}))":
        value = [(value,)]
    elif wrapper in ("Array({})", "Tuple({})"):
        value = [value]
    values = [] if empty else [value]
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", wrapper.format(type_name), values)]))
    assert batch.column_buffers(0) is None


@pytest.mark.parametrize("index,error", [(-1, OverflowError), (1, ValueError), (2**200, OverflowError), ("v", TypeError), (1.0, TypeError)])
def test_invalid_column_index(index, error):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", "Int64", [13])]))
    with pytest.raises(error):
        batch.column_buffers(index)
