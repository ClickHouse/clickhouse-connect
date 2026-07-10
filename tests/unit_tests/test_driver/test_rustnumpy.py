import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import rustnumpy


class _ArrowColumn:
    def __init__(self, values):
        self.values = values
        self.null_count = 0

    def to_numpy(self, *, zero_copy_only):
        assert zero_copy_only is False
        return self.values

    def to_pylist(self):
        return self.values.tolist()


@pytest.mark.parametrize(
    ("type_name", "dtype", "ticks"),
    [
        ("Time", "timedelta64[s]", [-5, 0, 90_000]),
        ("Time64(3)", "timedelta64[ms]", [-5_500, 0, 3_723_123]),
        ("Time64(6)", "timedelta64[us]", [-5_500_000, 1, 3_723_123_456]),
        ("Time64(9)", "timedelta64[ns]", [-5_500_000_000, 1, 3_723_123_456_789]),
    ],
)
def test_time_converter_uses_native_timedelta_dtype(monkeypatch, type_name, dtype, ticks):
    np = pytest.importorskip("numpy")
    ch_type = get_from_name(type_name)
    wire_dtype = "int32" if type_name == "Time" else "int64"
    wire_values = np.array(ticks, dtype=wire_dtype)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: _ArrowColumn(wire_values))

    result = rustnumpy._make_time_convert(ch_type)(None, None, 0)

    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array(ticks, dtype=dtype))
    assert np.shares_memory(result, wire_values) is (type_name != "Time")


@pytest.mark.parametrize(
    ("type_name", "dtype"),
    [
        ("Nullable(Time)", "timedelta64[s]"),
        ("Nullable(Time64(3))", "timedelta64[ms]"),
        ("Nullable(Time64(6))", "timedelta64[us]"),
        ("Nullable(Time64(9))", "timedelta64[ns]"),
    ],
)
def test_nullable_time_converter_uses_nat(monkeypatch, type_name, dtype):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    ch_type = get_from_name(type_name)
    wire_type = pa.int32() if type_name == "Nullable(Time)" else pa.int64()
    wire_values = pa.array([-5, None, 79], type=wire_type)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire_values)

    result = rustnumpy._make_time_convert(ch_type, as_pandas=True)(None, None, 0)

    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array([-5, "NaT", 79], dtype=dtype))


def test_nullable_time64_query_np_preserves_nanosecond_scalars(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    ch_type = get_from_name("Nullable(Time64(9))")
    wire_values = pa.array([-1, None, 1], type=pa.int64())
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire_values)

    result = rustnumpy._make_time_convert(ch_type)(None, None, 0)

    assert isinstance(result, list)
    assert isinstance(result[0], np.timedelta64)
    assert result == [np.timedelta64(-1, "ns"), None, np.timedelta64(1, "ns")]


@pytest.mark.parametrize(
    ("type_name", "rows", "unit"),
    [
        ("Array(Time)", [[], [-5], [13, 79]], "s"),
        ("Array(Time64(3))", [[], [-5], [13, 79]], "ms"),
        ("Array(Time64(6))", [[], [-5], [13, 79]], "us"),
        ("Array(Time64(9))", [[], [-5], [13, 79]], "ns"),
        ("Array(Array(Time))", [[[13], []], [[-5, 79]]], "s"),
    ],
)
def test_array_time_converter_slices_flat_values(monkeypatch, type_name, rows, unit):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    ch_type = get_from_name(type_name)
    depth, leaf = rustnumpy._array_time_leaf(ch_type)
    arrow_type = pa.int32() if unit == "s" else pa.int64()
    for _ in range(depth):
        arrow_type = pa.large_list(arrow_type)
    wire = pa.array(rows, type=arrow_type)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)
    context = type("Context", (), {"as_pandas": False, "use_extended_dtypes": False})()

    result = rustnumpy._make_array_time_convert(leaf, depth, context)(None, None, 0)

    def expect(node):
        if isinstance(node, int):
            return np.timedelta64(node, unit)
        return [expect(value) for value in node]

    assert result == expect(rows)
    assert all(isinstance(row, list) for row in result)


def test_array_nullable_time_converter_null_policy(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    ch_type = get_from_name("Array(Nullable(Time64(9)))")
    depth, leaf = rustnumpy._array_time_leaf(ch_type)
    wire = pa.array([[None, 1], [-1]], type=pa.large_list(pa.int64()))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    np_context = type("Context", (), {"as_pandas": False, "use_extended_dtypes": False})()
    result = rustnumpy._make_array_time_convert(leaf, depth, np_context)(None, None, 0)
    assert result == [[None, np.timedelta64(1, "ns")], [np.timedelta64(-1, "ns")]]

    ext_context = type("Context", (), {"as_pandas": True, "use_extended_dtypes": True})()
    result = rustnumpy._make_array_time_convert(leaf, depth, ext_context)(None, None, 0)
    assert np.isnat(result[0][0])
    assert result[0][0].dtype == np.dtype("timedelta64[ns]")
    assert result[0][1] == np.timedelta64(1, "ns")


def test_low_card_time_converter_decodes_dictionary(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    ch_type = get_from_name("LowCardinality(Nullable(Time))")
    wire = pa.DictionaryArray.from_arrays(pa.array([1, None, 0], type=pa.int32()), pa.array([13, -5], type=pa.int32()))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    result = rustnumpy._make_low_card_time_convert(ch_type, as_pandas=False)(None, None, 0)
    assert result == [np.timedelta64(-5, "s"), None, np.timedelta64(13, "s")]

    values = rustnumpy._make_low_card_time_convert(ch_type, as_pandas=True)(None, None, 0)
    assert values.dtype == np.dtype("timedelta64[s]")
    np.testing.assert_array_equal(values, np.array([-5, "NaT", 13], dtype="timedelta64[s]"))


def test_nested_time64_converter_preserves_nanoseconds():
    np = pytest.importorskip("numpy")
    ch_type = get_from_name("Array(Tuple(Time, Nullable(Time64(9))))")

    class _Batch:
        @staticmethod
        def column_data(_index, *, raw_time_ticks):
            assert raw_time_ticks is True
            return [[(-5, -1), (13, None), (79, 1)]]

    context = type("Context", (), {"as_pandas": False, "use_extended_dtypes": False})()
    result = rustnumpy._make_nested_time_convert(ch_type, context)(None, _Batch(), 0)

    assert result == [
        [
            (np.timedelta64(-5, "s"), np.timedelta64(-1, "ns")),
            (np.timedelta64(13, "s"), None),
            (np.timedelta64(79, "s"), np.timedelta64(1, "ns")),
        ]
    ]
