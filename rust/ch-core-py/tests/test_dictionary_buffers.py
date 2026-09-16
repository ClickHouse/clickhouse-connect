"""Per-chunk index and dictionary buffers for LowCardinality(Time)."""

import gc
import struct
import sys
import weakref

import pytest
from helpers import _ch_core, _encode_varint, _encode_varint_string, build_native_block


def _check_descriptor(column, rows, nullable, expected=None):
    assert column.kind == "dictionary" and column.itemsize == 4
    assert column.byteorder == sys.byteorder and column.length == len(rows)
    assert column.null_count == rows.count(None) and column.offsets is None
    if expected is not None:
        dictionary, indices = expected
    else:
        dictionary = [0] if nullable and rows else []
        slots = {}
        indices = []
        for value in rows:
            if value is None:
                indices.append(0)
            else:
                if value not in slots:
                    slots[value] = len(dictionary)
                    dictionary.append(value)
                indices.append(slots[value])
    values = memoryview(column.values)
    assert values.readonly and values.format == "B"
    assert values.tobytes() == struct.pack(f"={len(indices)}i", *indices)
    if rows:
        with pytest.raises(TypeError):
            values[0] = 0
    if nullable:
        validity = memoryview(column.validity)
        assert validity.readonly and len(validity) == (len(rows) + 7) // 8
        valid_bits = sum(1 << index for index, value in enumerate(rows) if value is not None)
        assert int.from_bytes(validity, "little") & ((1 << len(rows)) - 1) == valid_bits
    else:
        assert column.validity is None
    child = column.child
    assert child.kind == "int32" and child.itemsize == 4 and child.byteorder == sys.byteorder
    assert child.length == len(dictionary) and child.null_count == 0
    assert child.validity is None and child.offsets is None and child.child is None
    assert memoryview(child.values).readonly
    assert bytes(child.values) == struct.pack(f"={len(dictionary)}i", *dictionary)


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("values", [[13, -79, 13, 0], [-(2**31), 0, 2**31 - 1], [0, 0, 0]])
def test_dictionary_time_buffers(nullable, values):
    rows = [None, *values, None] if nullable else values
    type_name = "LowCardinality(Nullable(Time))" if nullable else "LowCardinality(Time)"
    batch = _ch_core.ColBatch.decode_native(build_native_block([("k", "Int64", [79] * len(rows)), ("v", type_name, rows)]))
    (column,) = batch.column_buffers(1)
    _check_descriptor(column, rows, nullable)
    with pytest.raises(AttributeError):
        column.child = None


@pytest.mark.parametrize("rows", [1, 7, 8, 9, 64, 65])
@pytest.mark.parametrize("nulls", ["none", "mixed", "all"])
def test_dictionary_validity_boundaries(rows, nulls):
    values = [None if nulls == "all" or (nulls == "mixed" and index % 3 == 0) else index % 5 for index in range(rows)]
    data = build_native_block([("v", "LowCardinality(Nullable(Time))", values)])
    (batch,) = _ch_core.StreamDecoder().feed(data)
    _check_descriptor(batch.column_buffers(0)[0], values, True)


@pytest.mark.parametrize("nullable", [False, True])
def test_dictionary_schema_only_and_empty_chunks(nullable):
    type_name = "LowCardinality(Nullable(Time))" if nullable else "LowCardinality(Time)"
    data = build_native_block([("v", type_name, [])])
    assert _ch_core.ColBatch.decode_native(data).column_buffers(0) == []
    for batch in [*_ch_core.StreamDecoder().feed(data), *list(_ch_core.BlockDecoder(data))]:
        (column,) = batch.column_buffers(0)
        _check_descriptor(column, [], nullable)


@pytest.mark.parametrize(
    "type_name,nullable",
    [
        ("SimpleAggregateFunction(anyLast, LowCardinality(Time))", False),
        ("LowCardinality(SimpleAggregateFunction(anyLast, Time))", False),
        ("SimpleAggregateFunction(anyLast, LowCardinality(Nullable(Time)))", True),
        ("LowCardinality(SimpleAggregateFunction(anyLast, Nullable(Time)))", True),
        ("LowCardinality(Nullable(SimpleAggregateFunction(anyLast, Time)))", True),
        ("LowCardinality(SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Nullable(Time))))", True),
        ("LowCardinality(Nullable(SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Time))))", True),
        ("SimpleAggregateFunction(anyLast, LowCardinality(SimpleAggregateFunction(anyLast, Nullable(Time))))", True),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_dictionary_aliases(type_name, nullable, empty):
    rows = [] if empty else [None if nullable else 13, 0, -79, 0]
    (batch,) = _ch_core.StreamDecoder().feed(build_native_block([("v", type_name, rows)]))
    _check_descriptor(batch.column_buffers(0)[0], rows, nullable)


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("index_tag,index_format", list(enumerate("BHIQ")))
def test_dictionary_wire_index_widths_and_sentinel(nullable, index_tag, index_format):
    type_name = "LowCardinality(Nullable(Time))" if nullable else "LowCardinality(Time)"
    dictionary, indices = [-79, 0, 13], [0, 2, 1, 0]
    rows = [None if nullable and index == 0 else dictionary[index] for index in indices]
    header = _encode_varint(1) + _encode_varint(len(rows)) + _encode_varint_string("v") + _encode_varint_string(type_name)
    data = header + struct.pack("<QQQ3iQ", 1, 0x600 | index_tag, len(dictionary), *dictionary, len(rows))
    data += struct.pack(f"<{len(rows)}{index_format}", *indices)
    batch = _ch_core.ColBatch.decode_native(data)
    _check_descriptor(batch.column_buffers(0)[0], rows, nullable, (dictionary, indices))


@pytest.mark.parametrize("nullable", [False, True])
def test_dictionary_chunks_keep_local_order_and_content(nullable):
    type_name = "LowCardinality(Nullable(Time))" if nullable else "LowCardinality(Time)"
    chunks = [[None if nullable else 13, 0, -79, 13], [79, 13], [13, 79, 0]] + ([[None, None]] if nullable else [])
    blocks = [build_native_block([("v", type_name, rows)]) for rows in chunks]
    buffered = _ch_core.ColBatch.decode_native(b"".join(blocks))
    merged = _ch_core.ColBatch.from_batches([_ch_core.ColBatch.decode_native(block) for block in blocks])
    for batch in (buffered, merged):
        columns = batch.column_buffers(0)
        assert len(columns) == len(chunks)
        for column, rows in zip(columns, chunks):
            _check_descriptor(column, rows, nullable)
    streamed = _ch_core.StreamDecoder().feed(blocks[0] + build_native_block([("v", type_name, [])]) + blocks[1])
    for batch, rows in zip(streamed, [chunks[0], [], chunks[1]]):
        _check_descriptor(batch.column_buffers(0)[0], rows, nullable)
    assert len(streamed) == 3


_BUFFER_PATHS = {"values": "=i4", "validity": "u1", "child.values": "=i4"}


def _select(column, path):
    for name in path.split("."):
        column = getattr(column, name)
    return column


@pytest.mark.parametrize("path,dtype", _BUFFER_PATHS.items())
@pytest.mark.parametrize("intake", ["decode_native", "StreamDecoder", "BlockDecoder", "from_batches"])
def test_dictionary_views_keep_only_their_owner(path, dtype, intake):
    np = pytest.importorskip("numpy")
    data = build_native_block([("v", "LowCardinality(Nullable(Time))", [None, 13, 0, -79, None, 13, 79, 0, None])])
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
    buffer = _select(column, path)
    owner = weakref.ref(buffer)
    siblings = [weakref.ref(_select(column, other)) for other in _BUFFER_PATHS if other != path]
    values = np.frombuffer(buffer, dtype=dtype)
    other = np.frombuffer(_select(batch.column_buffers(0)[0], path), dtype=dtype)
    assert values.ctypes.data == other.ctypes.data and np.shares_memory(values, other)
    with pytest.raises(ValueError):
        values.setflags(write=True)
    tail = values[::2]
    expected = tail.copy()
    del values, other, buffer, column, batch, producer
    gc.collect()
    assert owner() is not None
    assert all(sibling() is None for sibling in siblings)
    np.testing.assert_array_equal(tail, expected)
    del tail
    gc.collect()
    assert owner() is None


@pytest.mark.parametrize(
    "type_name,rows",
    [
        ("LowCardinality(Int32)", [13]),
        ("LowCardinality(String)", ["user_1"]),
        ("LowCardinality(Nullable(Date))", [None, 13]),
        ("LowCardinality(SimpleAggregateFunction(anyLast, DateTime))", [13]),
        ("Array(LowCardinality(Time))", [[13]]),
        ("Tuple(LowCardinality(Nullable(Time)))", [(None,)]),
        ("Map(String, LowCardinality(Time))", [{"user_1": 13}]),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_other_dictionary_storage_is_unsupported(type_name, rows, empty):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, [] if empty else rows)]))
    assert batch.column_buffers(0) is None
