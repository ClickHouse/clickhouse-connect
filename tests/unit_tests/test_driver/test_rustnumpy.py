import sys
from array import array
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from clickhouse_connect.datatypes.dynamic import typed_variant
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.string import String
from clickhouse_connect.datatypes.temporal import Time64
from clickhouse_connect.driver import rustnumpy
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.query import QueryContext

_COMPOUND_JSON_CELL = b"\x1e\x01\x03\x01\x02\x03"
_JSON_VALUE = {"shared": _COMPOUND_JSON_CELL, "typed_raw": b"\x01\x0d", "plain": "user_1"}
_DECODED_JSON_VALUE = {"shared": [1, 2, 3], "typed_raw": b"\x01\x0d", "plain": "user_1"}


@pytest.mark.parametrize(
    ("type_name", "column", "expected"),
    [
        ("JSON", [_JSON_VALUE], [_DECODED_JSON_VALUE]),
        ("Nullable(JSON)", [None, _JSON_VALUE], [None, _DECODED_JSON_VALUE]),
        ("Array(JSON)", [[_JSON_VALUE]], [[_DECODED_JSON_VALUE]]),
        (
            "Tuple(JSON, FixedString(3))",
            [(_JSON_VALUE, b"raw")],
            [(_DECODED_JSON_VALUE, b"raw")],
        ),
        (
            "Tuple(payload JSON, raw FixedString(3))",
            [{"payload": _JSON_VALUE, "raw": b"raw"}],
            [{"payload": _DECODED_JSON_VALUE, "raw": b"raw"}],
        ),
        ("Map(String, JSON)", [{"value": _JSON_VALUE}], [{"value": _DECODED_JSON_VALUE}]),
        (
            "Nested(payload JSON, raw FixedString(3))",
            [[{"payload": _JSON_VALUE, "raw": b"raw"}]],
            [[{"payload": _DECODED_JSON_VALUE, "raw": b"raw"}]],
        ),
        ("Variant(JSON, String)", [_JSON_VALUE, "plain"], [_DECODED_JSON_VALUE, "plain"]),
        ("Variant(Array(JSON), String)", [[_JSON_VALUE], "plain"], [[_DECODED_JSON_VALUE], "plain"]),
        (
            "Variant(JSON, Map(String, FixedString(3)))",
            [typed_variant(_JSON_VALUE, "JSON"), typed_variant({"raw": b"raw"}, "Map(String, FixedString(3))")],
            [typed_variant(_DECODED_JSON_VALUE, "JSON"), typed_variant({"raw": b"raw"}, "Map(String, FixedString(3))")],
        ),
    ],
)
def test_normalize_json_shared_column_compound_matrix(type_name, column, expected):
    result = rustnumpy._normalize_json_shared_column(get_from_name(type_name), column, QueryContext())

    assert result == expected


def test_normalize_json_shared_column_preserves_non_json_container():
    column = array("Q", [13, 79])

    result = rustnumpy._normalize_json_shared_column(get_from_name("UInt64"), column, QueryContext())

    assert result is column


def _scalar_buffer_batch(kind, data, length, validity=None, null_count=0, byteorder="little"):
    column = SimpleNamespace(kind=kind, values=data, length=length, byteorder=byteorder, validity=validity, null_count=null_count)
    return SimpleNamespace(column_buffers=lambda _index: [column])


def test_bfloat16_converter_widens_fixed_binary_words():
    np = pytest.importorskip("numpy")
    batch = _scalar_buffer_batch("bfloat16", b"\x8c\x3f\x8c\xbf\x50\x41", 3)
    converter = rustnumpy._build_converter(get_from_name("BFloat16"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)

    assert converter.needs_arrow is False
    assert result.dtype == np.dtype("float32")
    np.testing.assert_array_equal(result, np.array([1.09375, -1.09375, 13.0], dtype="float32"))


def test_bfloat16_converter_honors_buffer_view_and_nulls():
    np = pytest.importorskip("numpy")
    source = b"\x00\x00\x8c\x3f\x00\x00\x8c\xbf"
    batch = _scalar_buffer_batch("bfloat16", memoryview(source)[2:], 3, b"\x05", 1)
    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)

    assert result.dtype == np.dtype("float32")
    assert result[0] == np.float32(1.09375)
    assert np.isnan(result[1])
    assert result[2] == np.float32(-1.09375)


def test_simple_agg_bfloat16_routes_to_fast_converter():
    np = pytest.importorskip("numpy")
    batch = _scalar_buffer_batch("bfloat16", b"\x8c\x3f\x50\x41", 2)
    converter = rustnumpy._build_converter(get_from_name("SimpleAggregateFunction(anyLast, BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)

    assert converter.needs_arrow is False
    assert result.dtype == np.dtype("float32")
    np.testing.assert_array_equal(result, np.array([1.09375, 13.0], dtype="float32"))


def test_bfloat16_converter_empty_and_all_null():
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    batch = _scalar_buffer_batch("bfloat16", b"", 0)
    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)
    assert result.dtype == np.dtype("float32")
    assert len(result) == 0

    batch = _scalar_buffer_batch("bfloat16", b"\x00" * 4, 2, b"\x00", 2)
    result = converter(None, batch, 0)
    assert result.dtype == np.dtype("float32")
    assert np.isnan(result).all()

    extended_context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), extended_context)
    result = converter(None, batch, 0)
    assert str(result.dtype) == "Float32"
    assert list(result) == [pd.NA, pd.NA]


def test_nullable_bfloat16_extended_converter_returns_pandas_float32():
    pd = pytest.importorskip("pandas")
    batch = _scalar_buffer_batch("bfloat16", b"\x8c\x3f\x00\x00\x8c\xbf", 3, b"\x05", 1)
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    converter = rustnumpy._build_converter(get_from_name("Nullable(BFloat16)"), context)
    result = converter(None, batch, 0)

    assert str(result.dtype) == "Float32"
    assert list(result) == [pd.Float32Dtype().type(1.09375), pd.NA, pd.Float32Dtype().type(-1.09375)]


@pytest.mark.parametrize(
    "type_name", ["IntervalYear", "IntervalSecond", "IntervalMillisecond", "IntervalMicrosecond", "IntervalNanosecond"]
)
def test_interval_converter_returns_raw_int64_counts(type_name):
    np = pytest.importorskip("numpy")
    values = np.array([-13, 0, 79], dtype="int64")
    batch = _scalar_buffer_batch("int64", values.tobytes(), 3, byteorder=sys.byteorder)
    converter = rustnumpy._build_converter(get_from_name(type_name), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)

    assert converter.needs_arrow is False
    assert result.dtype == np.dtype("int64")
    np.testing.assert_array_equal(result, values)


@pytest.mark.parametrize("type_name", ["IntervalYear", "IntervalDay", "IntervalSecond", "IntervalNanosecond"])
def test_nullable_interval_extended_dtype_returns_pandas_int64(type_name):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    batch = _scalar_buffer_batch("int64", np.array([-13, 0, 79], dtype="int64").tobytes(), 3, b"\x05", 1, byteorder=sys.byteorder)
    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    converter = rustnumpy._build_converter(get_from_name(f"Nullable({type_name})"), context)
    result = converter(None, batch, 0)

    assert converter.needs_arrow is False
    assert str(result.dtype) == "Int64"
    assert list(result) == [-13, pd.NA, 79]


@pytest.mark.parametrize(
    ("type_name", "dtype", "ticks"),
    [
        ("Time", "timedelta64[s]", [-5, 0, 90_000]),
        ("Time64(0)", "timedelta64[s]", [-5, 0, 3_723]),
        ("Time64(3)", "timedelta64[ms]", [-5_500, 0, 3_723_123]),
        ("Time64(6)", "timedelta64[us]", [-5_500_000, 1, 3_723_123_456]),
        ("Time64(9)", "timedelta64[ns]", [-5_500_000_000, 1, 3_723_123_456_789]),
    ],
)
def test_time_converter_uses_native_timedelta_dtype(type_name, dtype, ticks):
    np = pytest.importorskip("numpy")
    ch_type = get_from_name(type_name)
    wire_dtype = "int32" if type_name == "Time" else "int64"
    wire_values = np.array(ticks, dtype=wire_dtype)
    batch = _scalar_buffer_batch(wire_dtype, memoryview(wire_values), len(ticks), byteorder=sys.byteorder)

    result = rustnumpy._make_time_convert(ch_type)(None, batch, 0)
    pandas_result = rustnumpy._make_time_convert(ch_type, as_pandas=True)(None, batch, 0)

    # The Python codec keeps the wire unit for non-nullable Time columns in every pandas
    # version, so the pandas exit must not coerce.
    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array(ticks, dtype=dtype))
    assert np.shares_memory(result, wire_values) is (type_name != "Time")
    assert pandas_result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(pandas_result, np.array(ticks, dtype=dtype))


@pytest.mark.parametrize(
    ("type_name", "dtype"),
    [
        ("Nullable(Time)", "timedelta64[s]"),
        ("Nullable(Time64(0))", "timedelta64[s]"),
        ("Nullable(Time64(3))", "timedelta64[ms]"),
        ("Nullable(Time64(6))", "timedelta64[us]"),
        ("Nullable(Time64(9))", "timedelta64[ns]"),
    ],
)
def test_nullable_time_converter_uses_nat(type_name, dtype):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    ch_type = get_from_name(type_name)
    wire_dtype = "int32" if type_name == "Nullable(Time)" else "int64"
    wire_values = np.array([-5, 0, 79], dtype=wire_dtype)
    batch = _scalar_buffer_batch(wire_dtype, memoryview(wire_values), 3, b"\x05", 1, byteorder=sys.byteorder)

    result = rustnumpy._make_time_convert(ch_type, as_pandas=True, use_extended_dtypes=True)(None, batch, 0)
    default_result = rustnumpy._make_time_convert(ch_type, as_pandas=True)(None, batch, 0)

    assert result.dtype == np.dtype(dtype)
    np.testing.assert_array_equal(result, np.array([-5, "NaT", 79], dtype=dtype))
    # The Python codec's nullable path hands the DataFrame constructor an object array, which
    # pandas < 3 infers as timedelta64[ns] and pandas >= 3 keeps at the scalar unit.
    default_dtype = "timedelta64[ns]" if int(pd.__version__.split(".", 1)[0]) < 3 else dtype
    assert default_result.dtype == np.dtype(default_dtype)
    np.testing.assert_array_equal(default_result, np.array([-5, "NaT", 79], dtype=dtype).astype(default_dtype))


def test_nullable_time64_query_np_preserves_nanosecond_scalars():
    np = pytest.importorskip("numpy")
    ch_type = get_from_name("Nullable(Time64(9))")
    wire_values = np.array([-1, 0, 1], dtype="int64")
    batch = _scalar_buffer_batch("int64", memoryview(wire_values), 3, b"\x05", 1, byteorder=sys.byteorder)

    result = rustnumpy._make_time_convert(ch_type)(None, batch, 0)

    assert isinstance(result, list)
    assert isinstance(result[0], np.timedelta64)
    assert result == [np.timedelta64(-1, "ns"), None, np.timedelta64(1, "ns")]


@pytest.mark.parametrize(
    ("type_name", "rows", "unit"),
    [
        ("Array(Time)", [[], [-5], [13, 79]], "s"),
        ("Array(Time64(0))", [[], [-5], [13, 79]], "s"),
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
    arrow_type = pa.int64() if isinstance(leaf, Time64) else pa.int32()
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


@pytest.mark.parametrize(
    "type_name",
    [
        "Time64(2)",
        "Nullable(Time64(2))",
        "Array(Time64(2))",
        "Tuple(Time64(2), Nullable(Time64(2)))",
        "Map(String, Time64(2))",
        "Nested(v Time64(2))",
        "Variant(String, Time64(2))",
        "JSON(`v` Time64(2))",
    ],
)
@pytest.mark.parametrize("as_pandas", [False, True])
def test_time64_unsupported_numpy_scale_raises_programming_error(type_name, as_pandas):
    context = QueryContext(use_numpy=True, as_pandas=as_pandas)

    with pytest.raises(ProgrammingError, match=r"Cannot use .*Time64\(2\).* as a numpy or Pandas datatype"):
        rustnumpy._build_converter(get_from_name(type_name), context)


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


def test_nested_data_type_time64_converter_preserves_units_and_extended_nulls():
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    ch_type = get_from_name("Nested(v Time64(0), n Nullable(Time64(0)), i Nullable(Int64))")

    class _Batch:
        @staticmethod
        def column_data(_index, *, raw_time_ticks):
            assert raw_time_ticks is True
            return [[{"v": -5, "n": None, "i": None}, {"v": 13, "n": 79, "i": 13}]]

    np_context = QueryContext(use_numpy=True)
    result = rustnumpy._make_nested_time_convert(ch_type, np_context)(None, _Batch(), 0)
    assert result == [
        [
            {"v": np.timedelta64(-5, "s"), "n": None, "i": None},
            {"v": np.timedelta64(13, "s"), "n": np.timedelta64(79, "s"), "i": 13},
        ]
    ]

    pd_context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    result = rustnumpy._make_nested_time_convert(ch_type, pd_context)(None, _Batch(), 0)
    assert result[0][0]["v"] == np.timedelta64(-5, "s")
    assert np.isnat(result[0][0]["n"])
    assert result[0][0]["n"].dtype == np.dtype("timedelta64[s]")
    assert result[0][0]["i"] is pd.NA
    assert result[0][1] == {"v": np.timedelta64(13, "s"), "n": np.timedelta64(79, "s"), "i": 13}


def test_json_typed_time64_converter_preserves_dotted_path_units_and_shared_data(monkeypatch):
    np = pytest.importorskip("numpy")
    ch_type = get_from_name("JSON(`v` Time64(0), `nested.t` Nullable(Time64(0)), `items` Array(Time64(0)), `plain` Int64)")
    original_segments = rustnumpy.dynamic_module._json_path_segments
    prepared_paths = []

    def track_segments(path):
        prepared_paths.append(path)
        return original_segments(path)

    monkeypatch.setattr(rustnumpy.dynamic_module, "_json_path_segments", track_segments)

    class _Batch:
        @staticmethod
        def column_data(_index, *, raw_time_ticks):
            assert raw_time_ticks is True
            return [
                {"v": -5, "nested": {"t": None}, "items": [13, 79], "plain": 13, "shared": _COMPOUND_JSON_CELL},
                {"v": 79, "nested": {"t": 13}, "items": [], "plain": 79, "shared": _COMPOUND_JSON_CELL},
            ]

    context = QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)
    result = rustnumpy._make_nested_time_convert(ch_type, context)(None, _Batch(), 0)

    assert result[0]["v"] == np.timedelta64(-5, "s")
    assert np.isnat(result[0]["nested"]["t"])
    assert result[0]["nested"]["t"].dtype == np.dtype("timedelta64[s]")
    assert result[0]["items"] == [np.timedelta64(13, "s"), np.timedelta64(79, "s")]
    assert result[0]["shared"] == [np.uint8(1), np.uint8(2), np.uint8(3)]
    assert all(type(value) is np.uint8 for value in result[0]["shared"])
    assert result[1]["v"] == np.timedelta64(79, "s")
    assert result[1]["nested"]["t"] == np.timedelta64(13, "s")
    assert result[1]["shared"] == [np.uint8(1), np.uint8(2), np.uint8(3)]
    assert sorted(prepared_paths) == ["items", "nested.t", "v"]


@pytest.mark.parametrize(
    ("type_name", "expected"),
    [
        ("Array(Date)", True),
        ("Array(DateTime('UTC'))", False),
        ("Array(DateTime64(3, 'UTC'))", False),
        ("Array(Nullable(DateTime64(3, 'UTC')))", True),
        ("Tuple(Time, Nullable(Int64))", True),
        ("Tuple(String, Nullable(Time))", False),
        ("Nested(t Time64(0), n Nullable(Int64))", True),
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


class _ObjectExitBatch:
    def __init__(self, column):
        self._column = column

    def column_data(self, _index):
        return list(self._column)


@pytest.mark.parametrize("type_name", ["DateTime('America/Denver')", "DateTime64(3, 'America/Denver')"])
def test_object_convert_numpy_keeps_stdlib_aware_datetimes(type_name):
    pytest.importorskip("numpy")
    tz = ZoneInfo("America/Denver")
    values = [None, datetime.fromtimestamp(13, tz), datetime.fromtimestamp(79, tz)]

    converter = rustnumpy._build_converter(get_from_name(f"Nullable({type_name})"), QueryContext(use_numpy=True))
    result = converter(None, _ObjectExitBatch(values), 0)

    assert list(result) == values
    assert type(result[1]) is datetime


@pytest.mark.parametrize("type_name", ["DateTime('America/Denver')", "DateTime64(3, 'America/Denver')"])
def test_object_convert_numpy_aware_datetimes_without_pandas(monkeypatch, type_name):
    pytest.importorskip("numpy")
    monkeypatch.setattr(rustnumpy.options, "pd", None)
    tz = ZoneInfo("America/Denver")
    values = [None, datetime.fromtimestamp(13, tz)]

    converter = rustnumpy._build_converter(get_from_name(f"Nullable({type_name})"), QueryContext(use_numpy=True))
    result = converter(None, _ObjectExitBatch(values), 0)

    assert list(result) == values


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


class _ColBatch:
    """Mock ColBatch exposing the python-object exit."""

    def __init__(self, columns):
        self._columns = columns

    def column_data(self, index):
        return list(self._columns[index])


def _extended_pandas_context():
    return QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)


def test_string_converter_extended_pandas_builds_from_arrow():
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    converter = rustnumpy._build_converter(get_from_name("String"), _extended_pandas_context())
    assert converter.needs_arrow is True
    values = ["a", "", "\u00e9", "b" * 40]
    result = converter(pa.table({"c": pa.array(values)}), _ColBatch([values]), 0)
    pd.testing.assert_extension_array_equal(result, pd.array(values, dtype=pd.StringDtype()))


def test_nullable_string_converter_extended_pandas_uses_na():
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    converter = rustnumpy._build_converter(get_from_name("Nullable(String)"), _extended_pandas_context())
    assert converter.needs_arrow is True
    values = ["a", None, "", "b"]
    result = converter(pa.table({"c": pa.array(values)}), _ColBatch([values]), 0)
    pd.testing.assert_extension_array_equal(result, pd.array(values, dtype=pd.StringDtype()))


def test_string_converter_invalid_utf8_falls_back_to_object_exit():
    pa = pytest.importorskip("pyarrow")
    pd = pytest.importorskip("pandas")
    converter = rustnumpy._build_converter(get_from_name("String"), _extended_pandas_context())
    binary = pa.array([b"\xff", b"ok"], type=pa.binary())
    forged = pa.Array.from_buffers(pa.string(), len(binary), binary.buffers(), null_count=0)
    # The binding renders invalid UTF-8 as hex, so the object exit sees these values.
    result = converter(pa.table({"c": forged}), _ColBatch([["ff", "ok"]]), 0)
    pd.testing.assert_extension_array_equal(result, pd.array(["ff", "ok"], dtype=pd.StringDtype()))


@pytest.mark.parametrize(
    ("type_name", "context"),
    [
        ("String", QueryContext(use_numpy=True)),
        ("String", QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=False)),
        ("LowCardinality(String)", QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)),
        ("FixedString(3)", QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True)),
    ],
    ids=["numpy", "pandas_plain", "low_card", "fixed_string"],
)
def test_string_converter_other_outputs_keep_object_exit(type_name, context):
    assert rustnumpy._build_converter(get_from_name(type_name), context).needs_arrow is False


@pytest.mark.parametrize("type_name", ["DateTime64", "Time64"])
@pytest.mark.parametrize("scale", [1, 2, 4, 5, 7, 8])
@pytest.mark.parametrize("wrapper", ["{}", "SimpleAggregateFunction(anyLast, {})", "Nullable(SimpleAggregateFunction(anyLast, {}))"])
def test_scalar_temporal_buffers_reject_unsupported_precision(type_name, scale, wrapper):
    declared = wrapper.format(f"{type_name}({scale})")
    with pytest.raises(ProgrammingError, match="Cannot use .* as a numpy or Pandas datatype"):
        rustnumpy._build_converter(get_from_name(declared), QueryContext(use_numpy=True))


@pytest.mark.parametrize("scale", [1, 2, 4, 5, 7, 8])
@pytest.mark.parametrize(
    "wrapper",
    [
        "Nullable({})",
        "SimpleAggregateFunction(anyLast, Nullable({}))",
        "SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, Nullable({})))",
        "SimpleAggregateFunction(anyLast, Nullable(SimpleAggregateFunction(anyLast, {})))",
        "SimpleAggregateFunction(anyLast, SimpleAggregateFunction(anyLast, {}))",
    ],
)
@pytest.mark.parametrize("as_pandas,extended", [(False, False), (True, False), (True, True)])
def test_nullable_and_alias_datetime64_reject_unsupported_precision(scale, wrapper, as_pandas, extended):
    ch_type = get_from_name(wrapper.format(f"DateTime64({scale})"))
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=extended)
    with pytest.raises(ProgrammingError, match="Cannot use .* as a numpy or Pandas datatype"):
        rustnumpy._build_converter(ch_type, context)


@pytest.mark.parametrize(
    "type_name,needs_arrow",
    [
        ("Date", False),
        ("DateTime64(9)", False),
        ("Time64(9)", False),
        ("Array(Time64(9))", True),
        ("LowCardinality(Time)", True),
        ("LowCardinality(DateTime)", False),
        ("Tuple(DateTime64(9))", False),
        ("Array(Tuple(Time64(9)))", False),
    ],
)
def test_scalar_temporal_dispatch_boundaries(type_name, needs_arrow):
    converter = rustnumpy._build_converter(get_from_name(type_name), QueryContext(use_numpy=True))
    assert converter.needs_arrow is needs_arrow


@pytest.mark.parametrize("type_name", ["Date", "Date32", "DateTime", "DateTime64(0)", "DateTime64(3)", "DateTime64(6)"])
@pytest.mark.parametrize("wrapper", ["Nullable({})", "SimpleAggregateFunction(anyLast, Nullable({}))"])
@pytest.mark.parametrize("as_pandas,extended", [(False, False), (True, False), (True, True)])
def test_nullable_date_and_timestamp_keep_object_exit(monkeypatch, type_name, wrapper, as_pandas, extended):
    ch_type = get_from_name(wrapper.format(type_name))
    marker = object()
    monkeypatch.setattr(rustnumpy, "_make_object_convert", lambda _type, _context: lambda *_args: marker)
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, use_extended_dtypes=extended)
    converter = rustnumpy._build_converter(ch_type, context)
    assert not converter.needs_arrow
    assert converter(None, None, 0) is marker


@pytest.mark.parametrize("byteorder,prefix", [("little", "<"), ("big", ">")])
@pytest.mark.parametrize(
    "type_name,kind,ticks",
    [
        ("Date", "uint16", [0, 13, 2**16 - 1]),
        ("Date32", "int32", [-(2**31), 13, 2**31 - 1]),
        ("DateTime", "uint32", [0, 13, 2**32 - 1]),
        ("DateTime64(9)", "int64", [-(2**63), -1, 2**63 - 1]),
        ("Time", "int32", [-(2**31), 13, 2**31 - 1]),
        ("Time64(9)", "int64", [-(2**63), -1, 2**63 - 1]),
        ("Nullable(Time64(9))", "int64", [-(2**63), -1, 2**63 - 1]),
    ],
)
def test_scalar_temporal_buffers_byteorder(type_name, kind, ticks, byteorder, prefix):
    np = pytest.importorskip("numpy")
    ch_type = get_from_name(type_name)
    values = np.array(ticks, dtype=np.dtype(kind).newbyteorder(prefix))
    batch = _scalar_buffer_batch(kind, values.tobytes(), len(ticks), byteorder=byteorder)
    converter = rustnumpy._build_converter(ch_type, QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    result = converter(None, batch, 0)
    expected = np.array(ticks, dtype=ch_type.np_type)
    if type_name in ("Date", "Date32"):
        expected = expected.astype("datetime64[s]")
    np.testing.assert_array_equal(result, expected)
    assert result.dtype.byteorder == "="


@pytest.mark.filterwarnings("error::DeprecationWarning")
@pytest.mark.parametrize(
    "declared,kind",
    [(f"Nullable({name})", "int32" if name == "Time" else "int64") for name in ["Time", "Time64(0)", "Time64(3)", "Time64(6)", "Time64(9)"]]
    + [
        (f"Nullable(SimpleAggregateFunction(anyLast, {name}))", kind)
        for name, kind in [("Date", "uint16"), ("Date32", "int32"), ("DateTime", "uint32"), ("Time", "int32")]
        + [(f"DateTime64({scale})", "int64") for scale in (0, 3, 6, 9)]
        + [(f"Time64({scale})", "int64") for scale in (0, 3, 6, 9)]
    ],
)
def test_nullable_temporal_nat_keeps_declared_unit(declared, kind):
    np = pytest.importorskip("numpy")
    values = np.array([13, 0, 79], dtype=kind)
    batch = _scalar_buffer_batch(kind, memoryview(values), 3, b"\x05", 1, byteorder=sys.byteorder)
    ch_type = get_from_name(declared)
    converter = rustnumpy._build_converter(ch_type, QueryContext(use_numpy=True, as_pandas=True, use_extended_dtypes=True))
    result = converter(None, batch, 0)
    expected = np.array([13, "NaT", 79], dtype=ch_type.np_type)
    if expected.dtype == np.dtype("datetime64[D]"):
        expected = expected.astype("datetime64[s]")
    assert result.dtype == expected.dtype
    np.testing.assert_array_equal(result, expected)


@pytest.mark.parametrize("byteorder,prefix", [("little", "<"), ("big", ">")])
def test_nullable_datetime64_numpy_buffer_byteorder(monkeypatch, byteorder, prefix):
    np = pytest.importorskip("numpy")
    values = np.array([-123456789, 0, 1714979289123456789], dtype=f"{prefix}i8")
    batch = _scalar_buffer_batch("int64", values.tobytes(), 3, b"\x05", 1, byteorder=byteorder)
    monkeypatch.setattr(rustnumpy.options, "pd", None)
    monkeypatch.setattr(rustnumpy.options, "arrow", None)
    converter = rustnumpy._build_converter(get_from_name("Nullable(DateTime64(9))"), QueryContext(use_numpy=True))
    result = converter(None, batch, 0)
    assert result == [np.datetime64(-123456789, "ns"), None, np.datetime64(1714979289123456789, "ns")]
    assert result[0].dtype.byteorder == "="
