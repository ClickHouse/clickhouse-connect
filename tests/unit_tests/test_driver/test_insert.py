import datetime
import sys

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.transform import NativeTransform, insert_transport_settings, rust_insert_requested, rust_insert_strict
from clickhouse_connect.tools.datagen import fixed_len_ascii_str


def test_block_size():
    data = [(1, (datetime.date(2020, 5, 2), datetime.datetime(2020, 5, 2, 10, 5, 2)))]
    ctx = InsertContext(
        "fake_table",
        ["key", "date_tuple"],
        [
            get_from_name("UInt64"),
            get_from_name("Tuple(Date, DateTime)"),
        ],
        data,
    )
    assert ctx.block_row_count == 262144

    data = [(x, fixed_len_ascii_str(400)) for x in range(5000)]
    ctx = InsertContext(
        "fake_table",
        ["key", "big_str"],
        [
            get_from_name("Int32"),
            get_from_name("String"),
        ],
        data,
    )
    assert ctx.block_row_count == 8192


@pytest.mark.parametrize("setting_value", ["1", "true", "TRUE", " yes ", "on", "force", "only", "strict", True, 1])
def test_rust_insert_requested_truthy_values(setting_value):
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings={"rust_insert": setting_value},
    )

    assert rust_insert_requested(ctx) is True


@pytest.mark.parametrize("setting_value", ["force", "only", "strict", "STRICT"])
def test_rust_insert_strict_values(setting_value):
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings={"rust_insert": setting_value},
    )

    assert rust_insert_strict(ctx) is True


@pytest.mark.parametrize("setting_value", [None, "", "0", "false", "no", "off", False, 2])
def test_rust_insert_ignores_non_truthy_values(setting_value):
    transport_settings = {} if setting_value is None else {"rust_insert": setting_value}
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings=transport_settings,
    )

    assert rust_insert_requested(ctx) is False
    assert rust_insert_strict(ctx) is False


def test_insert_transport_settings_filters_internal_rust_selector():
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings={"rust_insert": "on", "Rust_Insert": "strict", "X-Trace": "user_1"},
    )

    assert insert_transport_settings(ctx) == {"X-Trace": "user_1"}


def test_rust_insert_falls_back_to_python_when_module_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "_ch_core", None)
    rust_ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,), (79,)],
        block_size=1,
        transport_settings={"rust_insert": "on"},
    )
    python_ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,), (79,)],
        block_size=1,
    )

    assert b"".join(NativeTransform.build_insert_rust_or_python(rust_ctx)) == b"".join(NativeTransform.build_insert(python_ctx))
    assert rust_ctx.insert_exception is None


@pytest.mark.parametrize("rust_exception", [NotImplementedError, TypeError, ValueError])
def test_rust_insert_falls_back_to_python_before_first_chunk(monkeypatch, rust_exception):
    calls = 0

    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            nonlocal calls
            calls += 1
            raise rust_exception("unsupported")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    rust_ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings={"rust_insert": "on"},
    )
    python_ctx = InsertContext("fake_table", ["key"], [get_from_name("UInt64")], [(13,)])

    assert b"".join(NativeTransform.build_insert_rust_or_python(rust_ctx)) == b"".join(NativeTransform.build_insert(python_ctx))
    assert rust_ctx.insert_exception is None
    assert calls == 1


def test_rust_insert_multi_block_later_failure_does_not_fallback(monkeypatch):
    calls = 0

    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ValueError("late unsupported")
            return b"rust_1"

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,), (79,)],
        block_size=1,
        transport_settings={"rust_insert": "on"},
    )

    chunks = list(NativeTransform.build_insert_rust_or_python(ctx))

    assert chunks == [b"rust_1", b"INTERNAL EXCEPTION WHILE SERIALIZING"]
    assert calls == 2
    assert isinstance(ctx.insert_exception, DataError)
    assert "non-strict Python fallback" in str(ctx.insert_exception)
    assert "late unsupported" in str(ctx.insert_exception)


def test_strict_rust_insert_does_not_fallback(monkeypatch):
    class FakeCore:
        @staticmethod
        def encode_native_block(*args):
            raise NotImplementedError("unsupported")

    monkeypatch.setitem(sys.modules, "_ch_core", FakeCore)
    ctx = InsertContext(
        "fake_table",
        ["key"],
        [get_from_name("UInt64")],
        [(13,)],
        transport_settings={"rust_insert": "strict"},
    )

    chunks = list(NativeTransform.build_insert_rust_or_python(ctx))

    assert chunks == [b"INTERNAL EXCEPTION WHILE SERIALIZING"]
    assert isinstance(ctx.insert_exception, NotImplementedError)
