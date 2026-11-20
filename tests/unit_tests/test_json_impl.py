import json

import pytest

from clickhouse_connect import json_impl


@pytest.mark.parametrize("library", ["python", "orjson", "ujson"])
def test_json_numpy_values(library):
    np = pytest.importorskip("numpy")
    if library != "python":
        pytest.importorskip(library)
    serialize = json_impl._to_json[library]
    plain = {"n": 13, "f": 1.5, "b": False, "s": "user_1", "arr": [13, 79]}
    numpy_values = {"n": np.int64(13), "f": np.float64(1.5), "b": np.bool_(False), "s": np.str_("user_1"), "arr": np.array([13, 79])}
    assert serialize(numpy_values) == serialize(plain)
    with pytest.raises(TypeError):
        serialize(object())


@pytest.mark.parametrize("library", ["python", "orjson", "ujson"])
@pytest.mark.parametrize(
    "value_kind",
    ["datetime_ns", "datetime_s", "timedelta", "datetime_array", "complex", "nat", "bytes_array"],
)
def test_json_numpy_unsupported(library, value_kind):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    if library != "python":
        pytest.importorskip(library)
    value = {
        "datetime_ns": np.datetime64("2024-01-02T03:04:05", "ns"),
        "datetime_s": np.datetime64("2024-01-02T03:04:05", "s"),
        "timedelta": np.timedelta64(13, "ns"),
        "datetime_array": np.array(["2024-01-02"], dtype="datetime64[ns]"),
        "complex": np.complex128(1 + 2j),
        "nat": pd.NaT,
        "bytes_array": np.array([b"user_1"], dtype="S"),
    }[value_kind]
    with pytest.raises(TypeError):
        json_impl._to_json[library]({"v": value})


@pytest.mark.parametrize("library", ["python", "orjson", "ujson"])
def test_json_extended_values(library):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    if library != "python":
        pytest.importorskip(library)
    serialize = json_impl._to_json[library]
    value = {"x": np.longdouble(2.5), "missing": pd.NA}
    assert serialize(value) == b'{"x":2.5,"missing":null}'
    assert serialize(np.array([np.int64(13), pd.NA], dtype=object)) == b"[13,null]"
    assert serialize(np.array(["user_1"], dtype="U")) == b'["user_1"]'


@pytest.mark.parametrize("value", [None, True, 13, -79, 1.5, 'a\n"\\', "\u00e9", [13, None], {"b": False, "a": [79, {}]}])
def test_stdlib_json_bytes(value):
    assert json_impl._pyjson_to_json(value) == json.dumps(value, separators=(",", ":")).encode()


def test_stdlib_json_unsupported():
    with pytest.raises(TypeError, match="Object of type object is not JSON serializable"):
        json_impl._pyjson_to_json(object())


@pytest.mark.parametrize("library", ["python", "orjson", "ujson"])
def test_set_json_library(library):
    if library != "python":
        pytest.importorskip(library)
    previous = json_impl.any_to_json
    try:
        json_impl.set_json_library(library)
        assert json_impl.any_to_json is json_impl._to_json[library]
    finally:
        json_impl.any_to_json = previous
