"""Per-chunk offsets and leaf buffers for Array chains of Time/Time64."""

import gc
import struct
import sys
import weakref

import pytest
from helpers import _ch_core, build_native_block


def _array_type(leaf, depth):
    for _ in range(depth):
        leaf = f"Array({leaf})"
    return leaf


def _rows(depth, nullable):
    rows = [[], [13, None if nullable else 0, -79], [], [0]]
    for _ in range(depth - 1):
        rows = [[], rows, [[]]]
    return rows


def _check_descriptor(column, rows, depth, format_code, nullable):
    assert column.length == len(rows)
    if depth:
        assert column.kind == "array" and column.itemsize == 0
        assert column.byteorder == "not-applicable" and column.null_count == 0
        assert column.values is None and column.validity is None
        offsets, values = [0], []
        for row in rows:
            values.extend(row)
            offsets.append(len(values))
        view = memoryview(column.offsets)
        assert view.readonly and view.format == "B"
        assert view.tobytes() == struct.pack(f"={len(offsets)}q", *offsets)
        with pytest.raises(TypeError):
            view[0] = 0
        _check_descriptor(column.child, values, depth - 1, format_code, nullable)
        return
    assert column.kind == ("int32" if format_code == "i" else "int64")
    assert column.itemsize == struct.calcsize(f"={format_code}")
    assert column.byteorder == sys.byteorder
    assert column.offsets is None and column.child is None
    assert memoryview(column.values).readonly
    values = [0 if value is None else value for value in rows]
    assert bytes(column.values) == struct.pack(f"={len(values)}{format_code}", *values)
    assert column.null_count == rows.count(None)
    if nullable:
        validity = memoryview(column.validity)
        assert validity.readonly and len(validity) == (len(rows) + 7) // 8
        expected = sum(1 << index for index, value in enumerate(rows) if value is not None)
        assert int.from_bytes(validity, "little") & ((1 << len(rows)) - 1) == expected
    else:
        assert column.validity is None


@pytest.mark.parametrize("leaf,format_code", [("Time", "i"), *[(f"Time64({precision})", "q") for precision in (0, 3, 6, 9)]])
@pytest.mark.parametrize("depth", [1, 2, 3])
@pytest.mark.parametrize("nullable", [False, True])
def test_array_time_buffers(leaf, format_code, depth, nullable):
    type_name = _array_type(f"Nullable({leaf})" if nullable else leaf, depth)
    rows = _rows(depth, nullable)
    batch = _ch_core.ColBatch.decode_native(build_native_block([("k", "Int64", [79] * len(rows)), ("v", type_name, rows)]))
    (column,) = batch.column_buffers(1)
    _check_descriptor(column, rows, depth, format_code, nullable)
    with pytest.raises(AttributeError):
        column.child = None
    with pytest.raises(AttributeError):
        column.offsets = None


@pytest.mark.parametrize("leaf,format_code", [("Time", "i"), ("Time64(9)", "q")])
@pytest.mark.parametrize("depth", [1, 2, 3])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("rows", [[], [[], []]])
def test_array_empty_storage(leaf, format_code, depth, nullable, rows):
    type_name = _array_type(f"Nullable({leaf})" if nullable else leaf, depth)
    data = build_native_block([("v", type_name, rows)])
    buffered = _ch_core.ColBatch.decode_native(data)
    if not rows:
        assert buffered.column_buffers(0) == []
    (streamed,) = _ch_core.StreamDecoder().feed(data)
    for batch in ([buffered] if rows else []) + [streamed]:
        _check_descriptor(batch.column_buffers(0)[0], rows, depth, format_code, nullable)


@pytest.mark.parametrize(
    "type_name,depth,nullable,format_code",
    [
        ("SimpleAggregateFunction(anyLast, Array(Time64(9)))", 1, False, "q"),
        ("Array(SimpleAggregateFunction(anyLast, Time64(9)))", 1, False, "q"),
        ("Array(SimpleAggregateFunction(anyLast, Nullable(Time64(9))))", 1, True, "q"),
        ("Array(Nullable(SimpleAggregateFunction(anyLast, Time64(9))))", 1, True, "q"),
        ("Array(SimpleAggregateFunction(anyLast, Nullable(Time)))", 1, True, "i"),
        ("SimpleAggregateFunction(anyLast, Array(SimpleAggregateFunction(anyLast, Time)))", 1, False, "i"),
        ("Array(SimpleAggregateFunction(anyLast, Array(Nullable(Time64(9)))))", 2, True, "q"),
        ("SimpleAggregateFunction(anyLast, Array(Array(Nullable(Time64(9)))))", 2, True, "q"),
    ],
)
def test_array_aliases(type_name, depth, nullable, format_code):
    rows = _rows(depth, nullable)
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, rows)]))
    _check_descriptor(batch.column_buffers(0)[0], rows, depth, format_code, nullable)


@pytest.mark.parametrize("leaf,format_code,bits", [("Time", "i", 31), ("Time64(9)", "q", 63)])
@pytest.mark.parametrize("layout", ["extremes", "all_nulls"])
def test_array_leaf_extremes_and_all_nulls(leaf, format_code, bits, layout):
    rows = [[-(2**bits), 2**bits - 1]] if layout == "extremes" else [[None] * 9, [], [None]]
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", f"Array(Nullable({leaf}))", rows)]))
    _check_descriptor(batch.column_buffers(0)[0], rows, 1, format_code, True)


@pytest.mark.parametrize("depth", [1, 2])
def test_array_chunks_keep_local_offsets(depth):
    type_name = _array_type("Nullable(Time64(9))", depth)
    rows = _rows(depth, True)
    chunks = [rows, rows[:1], rows[1:]]
    encoded = [build_native_block([("v", type_name, chunk)]) for chunk in chunks]
    for batch in (
        _ch_core.ColBatch.decode_native(b"".join(encoded)),
        _ch_core.ColBatch.from_batches([_ch_core.ColBatch.decode_native(data) for data in encoded]),
    ):
        descriptors = batch.column_buffers(0)
        assert len(descriptors) == len(chunks)
        for column, chunk in zip(descriptors, chunks):
            _check_descriptor(column, chunk, depth, "q", True)


@pytest.mark.parametrize("depth", [1, 2])
def test_array_stream_keeps_empty_chunk_between_rows(depth):
    type_name = _array_type("Nullable(Time64(9))", depth)
    rows = _rows(depth, True)
    chunks = [rows, [], rows[1:]]
    data = b"".join(build_native_block([("v", type_name, chunk)]) for chunk in chunks)
    batches = _ch_core.StreamDecoder().feed(data)
    assert len(batches) == len(chunks)
    for batch, chunk in zip(batches, chunks):
        (column,) = batch.column_buffers(0)
        _check_descriptor(column, chunk, depth, "q", True)


def _select(column, path):
    for name in path.split("."):
        column = getattr(column, name)
    return column


_BUFFER_PATHS = {"offsets": "=i8", "child.offsets": "=i8", "child.child.values": "=i8", "child.child.validity": "u1"}


@pytest.mark.parametrize("path,dtype", _BUFFER_PATHS.items())
@pytest.mark.parametrize("intake", ["decode_native", "StreamDecoder", "BlockDecoder", "from_batches"])
def test_array_buffer_views_keep_only_their_owner(path, dtype, intake):
    np = pytest.importorskip("numpy")
    data = build_native_block([("v", "Array(Array(Nullable(Time64(9))))", _rows(2, True))])
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
        ("Array(Int32)", [[13]]),
        ("Array(DateTime64(9))", [[13]]),
        ("Array(Array(Int64))", [[[13]]]),
        ("Array(LowCardinality(Time))", [[13]]),
        ("Array(Tuple(Time))", [[(13,)]]),
        ("Tuple(Array(Time))", [([13],)]),
        ("Map(String, Time)", [{"user_1": 13}]),
        ("Array(Map(String, Time))", [[{"user_1": 13}]]),
        ("Nested(t Time)", [[(13,)]]),
        ("SimpleAggregateFunction(anyLast, Array(Int64))", [[13]]),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_unrelated_array_storage_stays_unsupported(type_name, rows, empty):
    batch = _ch_core.ColBatch.decode_native(build_native_block([("v", type_name, [] if empty else rows)]))
    assert batch.column_buffers(0) is None
