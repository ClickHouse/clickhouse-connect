"""Private numeric buffers need no Arrow Python package."""

import gc
import math
import struct
import sys
import weakref

import pytest
from helpers import _ch_core, build_native_block

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


@pytest.mark.parametrize("type_name,format_code,values", NUMERIC_CASES)
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


def test_memoryview_slice_and_validity_outlive_producer():
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", "Nullable(Int64)", [13, None, 79])]))
    (column,) = batch.column_buffers(0)
    values_ref, validity_ref = weakref.ref(column.values), weakref.ref(column.validity)
    view = memoryview(column.values)
    tail = view[16:]
    validity = memoryview(column.validity)
    view.release()
    del view, column, batch
    gc.collect()
    assert values_ref() is not None and validity_ref() is not None
    assert tail.tobytes() == struct.pack("=q", 79)
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


def test_multiple_chunks_stay_separate():
    blocks = [build_native_block([("v", "Int64", values)]) for values in ([13, 79], [-13])]
    batch = _ch_core.ColBatch.decode_native(b"".join(blocks))
    first, second = batch.column_buffers(0)
    assert (first.length, second.length) == (2, 1)
    assert bytes(first.values) == struct.pack("=qq", 13, 79)
    assert bytes(second.values) == struct.pack("=q", -13)


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
        ("Bool", [True]),
        ("Int128", [13]),
        ("Date", [13]),
        ("DateTime", [13]),
        ("IPv4", [13]),
        ("Decimal(9, 2)", [13]),
        ("Enum8('enabled' = 1)", [1]),
        ("BFloat16", [1.25]),
        ("Array(Int32)", [[13]]),
        ("Tuple(Int32)", [(13,)]),
        ("Array(Tuple(Int32))", [[(13,)]]),
        ("LowCardinality(String)", ["user_1"]),
        ("LowCardinality(Int32)", [13]),
        ("Nullable(String)", [None]),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_unsupported_storage_is_distinct_from_empty(type_name, values, empty):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, [] if empty else values)]))
    assert batch.column_buffers(0) is None


@pytest.mark.parametrize("index,error", [(-1, OverflowError), (1, ValueError), (2**200, OverflowError), ("v", TypeError), (1.0, TypeError)])
def test_invalid_column_index(index, error):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", "Int64", [13])]))
    with pytest.raises(error):
        batch.column_buffers(index)
