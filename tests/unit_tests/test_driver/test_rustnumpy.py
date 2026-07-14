from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.string import String
from clickhouse_connect.driver import rustnumpy
from clickhouse_connect.driver.query import QueryContext


class _ArrowColumn:
    def __init__(self, values):
        self.values = values
        self.null_count = 0

    def to_numpy(self, *, zero_copy_only):
        assert zero_copy_only is False
        return self.values

    def to_pylist(self):
        return self.values.tolist()


def test_bfloat16_converter_widens_fixed_binary_words(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    wire = pa.array([b"\x8c\x3f", b"\x8c\xbf", b"\x50\x41"], type=pa.binary(2))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    converter = rustnumpy._build_converter(get_from_name("BFloat16"), QueryContext(use_numpy=True))
    result = converter(None, None, 0)

    assert converter.needs_arrow is True
    assert result.dtype == np.dtype("float32")
    np.testing.assert_array_equal(result, np.array([1.09375, -1.09375, 13.0], dtype="float32"))


def test_bfloat16_converter_honors_arrow_offset_and_nulls(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    source = pa.array([b"\x00\x00", b"\x8c\x3f", None, b"\x8c\xbf"], type=pa.binary(2))
    wire = source.slice(1, 3)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, None, 0)

    assert result.dtype == np.dtype("float32")
    assert result[0] == np.float32(1.09375)
    assert np.isnan(result[1])
    assert result[2] == np.float32(-1.09375)


def test_simple_agg_bfloat16_routes_to_fast_converter(monkeypatch):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    wire = pa.array([b"\x8c\x3f", b"\x50\x41"], type=pa.binary(2))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    converter = rustnumpy._build_converter(get_from_name("SimpleAggregateFunction(anyLast, BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, None, 0)

    assert converter.needs_arrow is True
    assert result.dtype == np.dtype("float32")
    np.testing.assert_array_equal(result, np.array([1.09375, 13.0], dtype="float32"))


def test_bfloat16_converter_empty_and_all_null(monkeypatch):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    wire = pa.array([], type=pa.binary(2))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, None, 0)
    assert result.dtype == np.dtype("float32")
    assert len(result) == 0

    wire = pa.array([None, None], type=pa.binary(2))
    result = converter(None, None, 0)
    assert result.dtype == np.dtype("float32")
    assert np.isnan(result).all()

    extended_context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), extended_context)
    result = converter(None, None, 0)
    assert str(result.dtype) == "Float32"
    assert list(result) == [pd.NA, pd.NA]


def test_nullable_bfloat16_extended_converter_returns_pandas_float32(monkeypatch):
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    wire = pa.array([b"\x8c\x3f", None, b"\x8c\xbf"], type=pa.binary(2))
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)

    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), context)
    result = converter(None, None, 0)

    assert str(result.dtype) == "Float32"
    assert list(result) == [pd.Float32Dtype().type(1.09375), pd.NA, pd.Float32Dtype().type(-1.09375)]


@pytest.mark.parametrize(
    ("type_name", "duration_unit"),
    [
        ("IntervalYear", None),
        ("IntervalSecond", "s"),
        ("IntervalMillisecond", "ms"),
        ("IntervalMicrosecond", "us"),
        ("IntervalNanosecond", "ns"),
    ],
)
def test_interval_converter_returns_raw_int64_counts(monkeypatch, type_name, duration_unit):
    np = pytest.importorskip("numpy")
    pa = pytest.importorskip("pyarrow")
    values = [-13, 0, 79]
    arrow_type = pa.duration(duration_unit) if duration_unit else pa.int64()
    wire = pa.array(values, type=arrow_type)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)

    converter = rustnumpy._build_converter(get_from_name(type_name), QueryContext(use_numpy=True))
    result = converter(None, None, 0)

    assert result.dtype == np.dtype("int64")
    np.testing.assert_array_equal(result, np.array(values, dtype="int64"))


@pytest.mark.parametrize(
    ("type_name", "duration_unit"),
    [
        ("IntervalYear", None),
        ("IntervalDay", None),
        ("IntervalSecond", "s"),
        ("IntervalNanosecond", "ns"),
    ],
)
def test_nullable_interval_extended_dtype_returns_pandas_int64(monkeypatch, type_name, duration_unit):
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    arrow_type = pa.duration(duration_unit) if duration_unit else pa.int64()
    wire = pa.array([-13, None, 79], type=arrow_type)
    monkeypatch.setattr(rustnumpy, "_arrow_column", lambda _table, _index: wire)
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)

    converter = rustnumpy._build_converter(get_from_name(f"Nullable({type_name})"), context)
    result = converter(None, None, 0)

    assert str(result.dtype) == "Int64"
    assert list(result) == [-13, pd.NA, 79]


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

    context = type("Context", (), {"as_pandas": False, "use_extended_dtypes": False, "use_numpy": True})()
    result = rustnumpy._make_nested_time_convert(ch_type, context)(None, _Batch(), 0)

    assert result == [
        [
            (np.timedelta64(-5, "s"), np.timedelta64(-1, "ns")),
            (np.timedelta64(13, "s"), None),
            (np.timedelta64(79, "s"), np.timedelta64(1, "ns")),
        ]
    ]


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        ("Array(Date)", True),
        ("Array(DateTime('UTC'))", False),
        ("Array(DateTime64(3, 'UTC'))", False),
        ("Array(Nullable(DateTime64(3, 'UTC')))", True),
        ("Tuple(Time, Nullable(Int64))", True),
        ("Tuple(String, Nullable(Time))", False),
    ],
)
def test_needs_refinalize_only_gates_transforming_leaves(type_name, expected):
    assert rustnumpy._needs_refinalize(get_from_name(type_name)) is expected


@pytest.mark.parametrize(
    ("type_name", "column"),
    [
        ("Tuple(Nullable(Int64), String)", [(None, "untouched"), (13, "also untouched")]),
        ("Map(String, Nullable(Int64))", [{"untouched": None}, {"also untouched": 13}]),
    ],
)
def test_refinalize_skips_unaffected_sibling_leaves(monkeypatch, type_name, column):
    pd = pytest.importorskip("pandas")

    def fail_finalize(*_args):
        raise AssertionError("unaffected String leaf was finalized")

    monkeypatch.setattr(String, "_finalize_column", fail_finalize)
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)

    result = rustnumpy._refinalize_leaves(get_from_name(type_name), column, context)

    leaf = result[0][0] if isinstance(result[0], tuple) else result[0]["untouched"]
    assert leaf is pd.NA


@pytest.mark.parametrize("type_name", ["DateTime('America/Denver')", "DateTime64(3, 'America/Denver')"])
def test_refinalize_nullable_named_timezone_datetimes(type_name):
    pd = pytest.importorskip("pandas")
    timezone = ZoneInfo("America/Denver")
    value = datetime.fromtimestamp(1, timezone)
    ch_type = get_from_name(f"Array(Nullable({type_name}))")
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)

    result = rustnumpy._refinalize_leaves(ch_type, [[None, value]], context)

    assert result[0][0] is pd.NaT
    assert result[0][1] == pd.Timestamp(value)
