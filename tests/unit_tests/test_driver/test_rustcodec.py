import logging
import struct
import sys
from datetime import timezone
from zoneinfo import ZoneInfo

import pytest

from clickhouse_connect import common
from clickhouse_connect.common import _native_codec_env_default
from clickhouse_connect.datatypes import registry
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import rustcodec
from clickhouse_connect.driver.exceptions import (
    DataError,
    NotSupportedError,
    ProgrammingError,
    StreamClosedError,
    StreamFailureError,
)
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.rustcodec import (
    _make_native_transform,
    _rust_query_ineligible_reason,
    _RustNativeTransform,
    resolve_native_codec,
)
from clickhouse_connect.driver.transform import NativeTransform
from tests.helpers import TAGGED_EXCEPTION_BODY, TAGGED_EXCEPTION_TAG


class FakeSource:
    def __init__(self, chunks, exception_tag=None):
        self.gen = iter(chunks)
        self.exception_tag = exception_tag
        self.closed = False

    def close(self):
        self.closed = True


class _PresentCore:
    """Stand-in for the compiled module with a compatible binding API."""

    __version__ = "0.1.0"
    BINDING_API_VERSION = rustcodec.REQUIRED_BINDING_API_VERSION


def eligible_ctx(**kwargs) -> QueryContext:
    """Baseline rust-eligible context (mirrors a real UTC-server client)."""
    return QueryContext(apply_server_tz=True, server_tz=timezone.utc, **kwargs)


def _uint64_ctx(data, block_size=None, query_formats=None, column_formats=None) -> InsertContext:
    return InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        data,
        block_size=block_size,
        query_formats=query_formats,
        column_formats=column_formats,
    )


def _json_ctx(data, type_name="JSON") -> InsertContext:
    return InsertContext("fake_table", ["payload"], [get_from_name(type_name)], data)


@pytest.fixture(name="restore_native_codec")
def restore_native_codec_fixture():
    original = common.get_setting("native_codec")
    yield
    common.set_setting("native_codec", original)


@pytest.fixture(name="clean_type_cache")
def clean_type_cache_fixture():
    # Container types bake element insert names at construction and the registry caches instances by
    # name, so types built under a monkeypatched json_serialization_format must not outlive the test.
    snapshot = dict(registry.type_cache)
    yield
    registry.type_cache.clear()
    registry.type_cache.update(snapshot)


@pytest.fixture(name="clean_formats")
def clean_formats_fixture():
    from clickhouse_connect.datatypes.base import ch_read_formats, ch_write_formats

    read_snapshot = dict(ch_read_formats)
    write_snapshot = dict(ch_write_formats)
    yield
    ch_read_formats.clear()
    ch_read_formats.update(read_snapshot)
    ch_write_formats.clear()
    ch_write_formats.update(write_snapshot)


# --- Resolution --------------------------------------------------------------


def test_resolve_native_codec_kwarg_beats_common_setting(restore_native_codec):
    common.set_setting("native_codec", "rust")
    assert resolve_native_codec("python") == "python"
    assert resolve_native_codec(None) == "rust"


@pytest.mark.parametrize(
    ("env_value", "expected"),
    [("rust", "rust"), (" RUST ", "rust")],
)
def test_native_codec_env_default_valid(monkeypatch, env_value, expected):
    monkeypatch.setenv("CLICKHOUSE_CONNECT_NATIVE_CODEC", env_value)
    assert _native_codec_env_default() == expected


def test_native_codec_env_default_unset(monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_CONNECT_NATIVE_CODEC", raising=False)
    assert _native_codec_env_default() == "python"


def test_native_codec_env_default_invalid_warns(monkeypatch, caplog):
    monkeypatch.setenv("CLICKHOUSE_CONNECT_NATIVE_CODEC", "bogus")
    with caplog.at_level(logging.WARNING):
        assert _native_codec_env_default() == "python"
    assert any("CLICKHOUSE_CONNECT_NATIVE_CODEC" in record.getMessage() for record in caplog.records)


def test_resolve_native_codec_invalid_kwarg():
    with pytest.raises(ProgrammingError):
        resolve_native_codec("bogus")


# --- Availability ------------------------------------------------------------


@pytest.fixture(name="reset_version_log")
def reset_version_log_fixture(monkeypatch):
    monkeypatch.setattr(rustcodec, "_versions_logged", False)


@pytest.mark.parametrize("codec", ["rust", "rust_strict"])
def test_resolve_rust_raises_when_module_missing(monkeypatch, codec):
    monkeypatch.setitem(sys.modules, "_ch_core", None)
    with pytest.raises(NotSupportedError) as excinfo:
        resolve_native_codec(codec)
    message = str(excinfo.value)
    assert "compiled _ch_core extension module" in message
    assert 'pip install "clickhouse-connect[rust]"' in message


class _StaleCore:
    __version__ = "0.0.9"
    BINDING_API_VERSION = 0


class _NoApiVersionCore:
    __version__ = "0.0.5"


@pytest.mark.parametrize("core", [_StaleCore, _NoApiVersionCore], ids=["api_zero", "api_missing"])
@pytest.mark.parametrize("codec", ["rust", "rust_strict"])
def test_resolve_rust_raises_when_binding_api_too_old(monkeypatch, codec, core):
    monkeypatch.setitem(sys.modules, "_ch_core", core)
    with pytest.raises(NotSupportedError) as excinfo:
        resolve_native_codec(codec)
    message = str(excinfo.value)
    assert f"clickhouse-connect-core version {core.__version__}" in message
    assert "pip install --upgrade clickhouse-connect-core" in message


@pytest.mark.parametrize("api_version", [1, 2])
def test_resolve_rust_accepts_compatible_binding_api(monkeypatch, reset_version_log, api_version):
    class _Core:
        __version__ = "0.1.0"
        BINDING_API_VERSION = api_version

    monkeypatch.setitem(sys.modules, "_ch_core", _Core)
    assert resolve_native_codec("rust") == "rust"
    assert resolve_native_codec("rust_strict") == "rust_strict"


def test_resolve_rust_logs_versions_once(monkeypatch, caplog, reset_version_log):
    monkeypatch.setitem(sys.modules, "_ch_core", _PresentCore)
    with caplog.at_level(logging.INFO, logger="clickhouse_connect"):
        resolve_native_codec("rust")
        resolve_native_codec("rust_strict")
    records = [r for r in caplog.records if "clickhouse-connect-core" in r.getMessage()]
    assert len(records) == 1
    message = records[0].getMessage()
    assert "native_codec=rust" in message
    assert _PresentCore.__version__ in message
    assert f"binding API {_PresentCore.BINDING_API_VERSION}" in message
    assert common.version() in message


@pytest.mark.parametrize(("codec", "strict"), [("rust", False), ("rust_strict", True)])
def test__make_native_transform_rust_variants(monkeypatch, codec, strict):
    monkeypatch.setitem(sys.modules, "_ch_core", _PresentCore)
    transform = _make_native_transform(codec)
    assert isinstance(transform, _RustNativeTransform)
    assert transform.strict is strict
    assert transform.threaded_insert is True


def test__make_native_transform_python():
    transform = _make_native_transform("python")
    assert isinstance(transform, NativeTransform)
    assert transform.threaded_insert is False


# --- Eligibility -------------------------------------------------------------


def _ctx_response_tz():
    ctx = eligible_ctx()
    ctx.set_response_tz(ZoneInfo("America/New_York"))
    return ctx


@pytest.mark.parametrize(
    ("builder", "reason"),
    [
        pytest.param(lambda: eligible_ctx(query_formats={"Int*": "string"}), "query_formats", id="query_formats"),
        pytest.param(lambda: eligible_ctx(column_formats={"x": "string"}), "column_formats", id="column_formats"),
        pytest.param(lambda: eligible_ctx(use_none=False), "use_none=False", id="use_none"),
        pytest.param(lambda: eligible_ctx(encoding="latin-1"), "custom encoding", id="encoding"),
        pytest.param(lambda: eligible_ctx(query_tz="America/New_York"), "query_tz", id="query_tz"),
        pytest.param(lambda: eligible_ctx(column_tzs={"x": "America/New_York"}), "column_tzs", id="column_tzs"),
        pytest.param(lambda: eligible_ctx(tz_mode="aware"), "tz_mode", id="tz_mode_aware"),
        pytest.param(lambda: eligible_ctx(tz_mode="schema"), "tz_mode", id="tz_mode_schema"),
        pytest.param(_ctx_response_tz, "server timezone header", id="response_tz"),
        pytest.param(
            lambda: QueryContext(apply_server_tz=True, server_tz=ZoneInfo("America/New_York")),
            "ambient timezone",
            id="non_utc_server",
        ),
    ],
)
def test_rust_query_ineligible(builder, reason):
    assert _rust_query_ineligible_reason(builder()) == reason


@pytest.mark.parametrize(
    "builder",
    [
        pytest.param(eligible_ctx, id="baseline"),
        pytest.param(lambda: eligible_ctx(column_oriented=True), id="column_oriented"),
        pytest.param(lambda: eligible_ctx(streaming=True), id="streaming"),
        pytest.param(lambda: eligible_ctx(rename_response_column="remove_prefix"), id="renamer"),
        pytest.param(lambda: eligible_ctx(use_numpy=True), id="use_numpy"),
        pytest.param(lambda: eligible_ctx(use_numpy=True, as_pandas=True), id="as_pandas"),
    ],
)
def test_rust_query_eligible(builder):
    assert _rust_query_ineligible_reason(builder()) is None


def test_rust_query_ineligible_pyarrow_missing(monkeypatch):
    monkeypatch.setattr(rustcodec.options, "arrow", None, raising=False)
    assert _rust_query_ineligible_reason(eligible_ctx(use_numpy=True)) == "pyarrow not installed"
    # Non-numpy queries never touch the Arrow exit, so a missing pyarrow is irrelevant.
    assert _rust_query_ineligible_reason(eligible_ctx()) is None


def test_rust_query_ineligible_global_read_format(clean_formats):
    from clickhouse_connect.datatypes.format import set_read_format

    set_read_format("IPv4", "string")
    assert _rust_query_ineligible_reason(eligible_ctx()) == "global read format override"


# --- Routing -----------------------------------------------------------------


def test_strict_ineligible_raises_and_closes_source():
    src = FakeSource([])
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx(use_none=False))
    assert src.closed is True


def test_strict_global_read_format_raises_and_closes_source(clean_formats):
    from clickhouse_connect.datatypes.format import set_read_format

    set_read_format("IPv4", "string")
    src = FakeSource([])
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert src.closed is True


def test_non_strict_ineligible_delegates_to_python_and_logs_reason(monkeypatch, caplog):
    sentinel = object()
    monkeypatch.setattr(NativeTransform, "parse_response", staticmethod(lambda source, context: sentinel))
    src = FakeSource([])
    with caplog.at_level(logging.INFO, logger="clickhouse_connect"):
        result = _RustNativeTransform(strict=False).parse_response(src, eligible_ctx(use_none=False))
    assert result is sentinel
    assert src.closed is False
    assert "fallback to Python for query: use_none=False" in caplog.text


# --- Insert build (FakeCore) -------------------------------------------------


def test_build_insert_probe_not_implemented_non_strict(monkeypatch):
    calls = []

    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            calls.append(args)
            raise NotImplementedError("unsupported")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    rust_ctx = _uint64_ctx([(13,), (79,)], block_size=1)
    python_ctx = _uint64_ctx([(13,), (79,)], block_size=1)

    rust_out = b"".join(_RustNativeTransform(strict=False).build_insert(rust_ctx))
    python_out = b"".join(NativeTransform.build_insert(python_ctx))

    assert rust_out == python_out
    assert len(calls) == 1  # only the probe ran
    assert rust_ctx.insert_exception is None


def test_build_insert_probe_not_implemented_strict(monkeypatch):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise NotImplementedError("unsupported")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).build_insert(_uint64_ctx([(13,)]))


def test_build_insert_mid_block_failure(monkeypatch):
    calls = {"n": 0}

    class FakeCore:
        @staticmethod
        def encode_native_block(names, type_names, columns, row_count, prefix):
            calls["n"] += 1
            if row_count == 0:
                return b""
            if calls["n"] == 2:
                return b"rust_block"
            raise ValueError("late")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    ctx = _uint64_ctx([(13,), (79,)], block_size=1)

    chunks = list(_RustNativeTransform(strict=False).build_insert(ctx))

    assert chunks == [b"rust_block", b"INTERNAL EXCEPTION WHILE SERIALIZING"]
    # Binding ValueErrors surface as DataError with the binding's message.
    assert isinstance(ctx.insert_exception, DataError)
    assert str(ctx.insert_exception) == "late"
    assert isinstance(ctx.insert_exception.__cause__, ValueError)


def test_build_insert_global_write_format_strict_raises(monkeypatch, clean_formats):
    from clickhouse_connect.datatypes.format import set_write_format

    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run when a write format override is set")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    set_write_format("IPv4", "string")
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).build_insert(_uint64_ctx([(13,)]))


def test_build_insert_global_write_format_non_strict_falls_back(monkeypatch, clean_formats):
    from clickhouse_connect.datatypes.format import set_write_format

    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run when a write format override is set")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    set_write_format("IPv4", "string")
    rust_ctx = _uint64_ctx([(13,), (79,)], block_size=1)
    python_ctx = _uint64_ctx([(13,), (79,)], block_size=1)

    rust_out = b"".join(_RustNativeTransform(strict=False).build_insert(rust_ctx))
    python_out = b"".join(NativeTransform.build_insert(python_ctx))
    assert rust_out == python_out


@pytest.mark.parametrize(
    "type_name",
    [
        "JSON",
        "Nullable(JSON)",
        "Array(JSON)",
        "Tuple(id UInt8, payload JSON)",
        "Map(String, JSON)",
        "Variant(JSON, String)",
        "Nested(x JSON)",
    ],
)
def test_build_insert_legacy_json_strict_raises(monkeypatch, clean_type_cache, type_name):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run for legacy JSON serialization")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    monkeypatch.setattr(rustcodec.dynamic_module, "json_serialization_format", 0)
    with pytest.raises(NotSupportedError, match="legacy JSON serialization"):
        _RustNativeTransform(strict=True).build_insert(_json_ctx([], type_name))


def test_build_insert_legacy_json_non_strict_falls_back(monkeypatch):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run for legacy JSON serialization")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    monkeypatch.setattr(rustcodec.dynamic_module, "json_serialization_format", 0)
    rows = [({"id": 13},), ({"name": "user_1"},)]
    rust_out = b"".join(_RustNativeTransform(strict=False).build_insert(_json_ctx(rows)))
    python_out = b"".join(NativeTransform.build_insert(_json_ctx(rows)))
    assert rust_out == python_out


@pytest.mark.parametrize("strict", [False, True])
def test_build_insert_modern_json_uses_rust(monkeypatch, strict):
    calls = []

    class FakeCore:
        @staticmethod
        def encode_native_block(names, type_names, columns, row_count, prefix):
            calls.append(row_count)
            return b"rust_block"

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    monkeypatch.setattr(rustcodec.dynamic_module, "json_serialization_format", 1)
    rows = [({"id": 13},), ({"name": "user_1"},)]

    chunks = list(_RustNativeTransform(strict=strict).build_insert(_json_ctx(rows)))

    assert chunks == [b"rust_block"]
    assert calls == [0, 2]  # probe then one block


def _qbit_ctx(data, type_name) -> InsertContext:
    return InsertContext("fake_table", ["vec"], [get_from_name(type_name)], data)


@pytest.mark.parametrize("type_name", ["QBit(Float32, 8)", "Array(QBit(Float32, 8))"])
def test_build_insert_legacy_json_flag_qbit(monkeypatch, type_name):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise NotImplementedError("unsupported")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    monkeypatch.setattr(rustcodec.dynamic_module, "json_serialization_format", 0)
    vector = [0.5, 1.5, -2.0, 13.0, 79.0, 0.0, -1.25, 3.75]
    value = vector if type_name.startswith("QBit") else [vector]
    rows = [(value,), (value,)]

    rust_out = b"".join(_RustNativeTransform(strict=False).build_insert(_qbit_ctx(rows, type_name)))
    python_out = b"".join(NativeTransform.build_insert(_qbit_ctx(rows, type_name)))
    assert rust_out == python_out

    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).build_insert(_qbit_ctx(rows, type_name))


_USER_FORMATS = [
    pytest.param({"column_formats": {"absent_col": "string"}}, id="column_format"),
    pytest.param({"query_formats": {"IPv4": "string"}}, id="query_format"),
]


@pytest.mark.parametrize("fmt", _USER_FORMATS)
def test_build_insert_user_format_strict_raises(monkeypatch, fmt):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run when a user format is set")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).build_insert(_uint64_ctx([(13,)], **fmt))


@pytest.mark.parametrize("fmt", _USER_FORMATS)
def test_build_insert_user_format_non_strict_falls_back(monkeypatch, fmt):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise AssertionError("encoder must not run when a user format is set")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    rust_out = b"".join(_RustNativeTransform(strict=False).build_insert(_uint64_ctx([(13,), (79,)], block_size=1, **fmt)))
    python_out = b"".join(NativeTransform.build_insert(_uint64_ctx([(13,), (79,)], block_size=1, **fmt)))
    assert rust_out == python_out


def test_build_insert_datetime_dataframe_routes_rust(monkeypatch):
    pd = pytest.importorskip("pandas")
    calls = []

    class FakeCore:
        @staticmethod
        def encode_native_block(names, type_names, columns, row_count, prefix):
            calls.append((names, type_names, columns, row_count, prefix))
            return b"rust_block"

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    df = pd.DataFrame({"ts": pd.to_datetime(["2020-01-01 00:00:13", "2020-01-01 00:01:19"])})
    ctx = InsertContext("fake_table", ["ts"], [get_from_name("DateTime")], df)

    chunks = list(_RustNativeTransform(strict=True).build_insert(ctx))

    # _convert_pandas injects the "int" hint into column_formats, but the compiled dicts stay empty,
    # so rust_strict still serves the insert instead of raising.
    assert ctx.column_formats == {"ts": "int"}
    assert ctx.col_simple_formats == {}
    assert chunks == [b"rust_block"]
    assert len(calls) == 2  # probe + one block


def test_build_insert_success(monkeypatch):
    calls = []

    class FakeCore:
        @staticmethod
        def encode_native_block(names, type_names, columns, row_count, prefix):
            calls.append((names, type_names, columns, row_count, prefix))
            return b"rust_block"

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    ctx = _uint64_ctx([(13,), (79,)])

    chunks = list(_RustNativeTransform(strict=False).build_insert(ctx))

    assert calls[0][3] == 0  # probe carries row_count 0
    assert chunks == [b"rust_block"]
    assert ctx.insert_exception is None


# --- Decode error taxonomy (fake decoder) ------------------------------------


class _FakeBatch:
    def __init__(self, names, type_names, columns=None, error=None):
        self.column_names = names
        self.column_type_names = type_names
        self._columns = columns
        self._error = error

    def to_python_columns(self, typed_numeric=False):
        del typed_numeric
        if self._error is not None:
            raise self._error
        return self._columns

    def to_python_rows(self):
        return list(zip(*self.to_python_columns()))


def _fake_decoder_core(feed_batches=(), finish_batches=(), feed_error=None):
    class _FakeStreamDecoder:
        def __init__(self, has_block_info=False):
            self._feed = list(feed_batches)
            self._finish = list(finish_batches)

        def feed(self, chunk):
            if feed_error is not None:
                raise feed_error
            out, self._feed = self._feed, []
            return out

        def finish(self):
            return self._finish

    class _FakeCore:
        StreamDecoder = _FakeStreamDecoder

    return _FakeCore


@pytest.mark.parametrize(
    ("error", "exception_tag", "chunk", "expected"),
    [
        (NotImplementedError("Unsupported ClickHouse type 'X'"), None, b"random block bytes", NotSupportedError),
        (ValueError("Malformed payload: bad bytes for column 'x'"), None, b"random block bytes", DataError),
        (
            NotImplementedError("Unsupported ClickHouse type 'X'"),
            None,
            b"prefix Code: 62 DB::Exception boom",
            StreamFailureError,
        ),
        (
            ValueError("Malformed payload: bad bytes for column 'x'"),
            None,
            b"prefix Code: 62 DB::Exception boom",
            StreamFailureError,
        ),
        (
            NotImplementedError("Unsupported ClickHouse type 'X'"),
            "T",
            b"prefix Code: 62 DB::Exception boom",
            NotSupportedError,
        ),
        (
            ValueError("Malformed payload: bad bytes for column 'x'"),
            "T",
            b"prefix Code: 62 DB::Exception boom",
            DataError,
        ),
    ],
    ids=[
        "unsupported_plain",
        "malformed_plain",
        "unsupported_untagged_server_error",
        "malformed_untagged_server_error",
        "unsupported_tagged_no_false_positive",
        "malformed_tagged_no_false_positive",
    ],
)
def test_decode_feed_error_disambiguation(monkeypatch, error, exception_tag, chunk, expected):
    core = _fake_decoder_core(feed_error=error)
    monkeypatch.setitem(sys.modules, "_ch_core", core)
    src = FakeSource([chunk], exception_tag=exception_tag)
    with pytest.raises(expected) as excinfo:
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    if expected is DataError:
        assert str(excinfo.value) == str(error)
    assert src.closed is True


def test_decode_feed_not_implemented_maps_to_not_supported(monkeypatch):
    core = _fake_decoder_core(feed_error=NotImplementedError("python object exit"))
    monkeypatch.setitem(sys.modules, "_ch_core", core)
    src = FakeSource([b"random block bytes"])
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert src.closed is True


def test_decode_later_block_unsupported_closes_source(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], error=NotImplementedError("python object exit"))
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx(streaming=True))
    with pytest.raises(NotSupportedError), result.column_block_stream as stream:
        list(stream)
    assert src.closed is True


def test_decode_early_abandonment_closes_source(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], columns=[[79]])
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx(streaming=True))
    with result.column_block_stream as stream:
        for _ in stream:
            break
    assert src.closed is True


def test_decode_buffered_unsupported_raises(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], error=NotImplementedError("python object exit"))
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert src.closed is True


def test_decode_buffered_malformed_fill_raises_data_error(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], error=ValueError("Malformed payload: bad cell"))
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    with pytest.raises(DataError, match="Malformed payload: bad cell"):
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert src.closed is True


def test_decode_streaming_malformed_fill_raises_data_error(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], error=ValueError("Malformed payload: bad cell"))
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx(streaming=True))
    with pytest.raises(DataError, match="Malformed payload: bad cell"), result.column_block_stream as stream:
        list(stream)
    assert src.closed is True


def test_decode_buffered_result_is_materialized(monkeypatch):
    batch0 = _FakeBatch(["a"], ["Int32"], columns=[[13]])
    batch1 = _FakeBatch(["a"], ["Int32"], columns=[[79]])
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert src.closed is True
    assert result.result_rows == [(13,), (79,)]
    assert result.result_columns == [[13, 79]]
    with pytest.raises(StreamClosedError):
        _ = result.column_block_stream


def test_decode_buffered_column_oriented(monkeypatch):
    batch0 = _FakeBatch(["a", "b"], ["Int32", "Int32"], columns=[[13], [1]])
    batch1 = _FakeBatch(["a", "b"], ["Int32", "Int32"], columns=[[79], [2]])
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch0, batch1]))
    src = FakeSource([b"chunk"])
    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx(column_oriented=True))
    assert result.result_columns == [[13, 79], [1, 2]]
    assert result.result_rows == [(13, 1), (79, 2)]
    # Repeated access in either order stays stable across the multi-batch buffers.
    assert result.result_columns == [[13, 79], [1, 2]]
    assert result.result_rows == [(13, 1), (79, 2)]


def test_decode_buffered_result_columns_uses_direct_column_exit(monkeypatch):
    class ColumnsOnlyBatch(_FakeBatch):
        def to_python_rows(self):
            raise AssertionError("row exit must not run for first result_columns access")

    batch = ColumnsOnlyBatch(["a", "b"], ["Int32", "Int32"], columns=[[13, 79], [1, 2]])
    monkeypatch.setitem(sys.modules, "_ch_core", _fake_decoder_core(feed_batches=[batch]))

    result = _RustNativeTransform(strict=True).parse_response(FakeSource([b"chunk"]), eligible_ctx())

    assert result.result_columns == [[13, 79], [1, 2]]


# --- Decode against real _ch_core --------------------------------------------


@pytest.fixture(name="ch_core")
def ch_core_fixture():
    return pytest.importorskip("_ch_core")


def test_decode_basic(ch_core):
    data = ch_core.encode_native_block(["a", "b"], ["Int32", "String"], [[13, 79], ["user_1", "user_2"]], 2, None)
    chunks = [data[i : i + 8] for i in range(0, len(data), 8)]
    src = FakeSource(chunks)

    result = _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())

    assert result.result_rows == [(13, "user_1"), (79, "user_2")]
    assert result.column_names == ("a", "b")
    assert [t.name for t in result.column_types] == ["Int32", "String"]


def test_decode_empty_stream(ch_core):
    result = _RustNativeTransform(strict=True).parse_response(FakeSource([]), eligible_ctx())
    assert result.result_rows == []


def test_decode_truncated(ch_core):
    data = ch_core.encode_native_block(["a", "b"], ["Int32", "String"], [[13, 79], ["user_1", "user_2"]], 2, None)
    with pytest.raises(StreamFailureError):
        _RustNativeTransform(strict=True).parse_response(FakeSource([data[:-4]]), eligible_ctx())


def test_decode_tagged_exception(ch_core):
    src = FakeSource([TAGGED_EXCEPTION_BODY], exception_tag=TAGGED_EXCEPTION_TAG)
    with pytest.raises(StreamFailureError) as excinfo:
        _RustNativeTransform(strict=True).parse_response(src, eligible_ctx())
    assert str(excinfo.value) == "Big bam occurred right while reading the data"


def _varint_str(value: str) -> bytes:
    encoded = value.encode()
    return bytes([len(encoded)]) + encoded


def _dynamic_shared_block(cells: list) -> bytes:
    """V2 Dynamic block whose only child is SharedVariant."""
    body = bytearray(struct.pack("<Q", 2))
    body.append(0)  # zero named child types
    body.extend(struct.pack("<Q", 0))  # discriminator format
    body.extend(b"\x00" * len(cells))  # all rows in SharedVariant
    for cell in cells:
        body.append(len(cell))
        body.extend(cell)
    return b"\x01" + bytes([len(cells)]) + _varint_str("v") + _varint_str("Dynamic") + bytes(body)


def test_decode_malformed_shared_cell_raises_data_error(ch_core):
    # Int32 descriptor with a truncated payload fails the fill, not the prefix parse.
    corrupt = _dynamic_shared_block([b"\x09\x01\x02"])
    with pytest.raises(DataError, match="SharedVariant"):
        _RustNativeTransform(strict=True).parse_response(FakeSource([corrupt]), eligible_ctx())


def test_decode_malformed_shared_cell_streaming_raises_data_error(ch_core):
    # The corrupt cell is in the second block; the first converts eagerly in parse_response.
    valid = _dynamic_shared_block([b"\x15\x01a"])
    corrupt = _dynamic_shared_block([b"\x09\x01\x02"])
    result = _RustNativeTransform(strict=True).parse_response(FakeSource([valid + corrupt]), eligible_ctx(streaming=True))
    with pytest.raises(DataError, match="SharedVariant"), result.column_block_stream as stream:
        list(stream)


def test_decode_malformed_prefix_raises_data_error(ch_core):
    # Dynamic prefix with an unrecognized discriminator format fails at feed time.
    body = struct.pack("<Q", 2) + b"\x00" + struct.pack("<Q", 7) + b"\x00"
    corrupt = b"\x01\x01" + _varint_str("v") + _varint_str("Dynamic") + body
    with pytest.raises(DataError, match="Invalid Dynamic layout"):
        _RustNativeTransform(strict=True).parse_response(FakeSource([corrupt]), eligible_ctx())


def test_decode_unsupported_type_raises_not_supported(ch_core):
    block = b"\x01\x01" + _varint_str("v") + _varint_str("AggregateFunction(avg, UInt64)") + b"\x00" * 8
    with pytest.raises(NotSupportedError):
        _RustNativeTransform(strict=True).parse_response(FakeSource([block]), eligible_ctx())
