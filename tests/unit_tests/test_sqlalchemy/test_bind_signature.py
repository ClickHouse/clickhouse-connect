import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import ARRAY, DateTime, String, literal, select
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.exc import CompileError
from sqlalchemy.types import TypeDecorator

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import _datetime64_bind_signature
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, DateTime64, Nullable, Tuple, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


class _StringDecorator(TypeDecorator):
    impl = String
    cache_ok = True


def _run_script(script):
    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=Path(__file__).resolve().parents[3],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.TimeoutExpired as exc:
        output = "".join(part.decode(errors="replace") if isinstance(part, bytes) else part or "" for part in (exc.stdout, exc.stderr))
        pytest.fail(f"Subprocess timed out after {exc.timeout} seconds.\n{output}")
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("cycle", ["default", "load_dialect_impl", "type_engine", "two-step"])
def test_self_adapting_decorator_terminates(cycle):
    _run_script(f"""
from sqlalchemy import String, Text, TypeDecorator
from sqlalchemy.exc import CompileError
from clickhouse_connect.cc_sqlalchemy.datatypes.base import _datetime64_bind_signature
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

class StringDecorator(TypeDecorator):
    impl = String
    cache_ok = True

class LoadDecorator(StringDecorator):
    cache_ok = True
    def load_dialect_impl(self, dialect):
        return String()

class EngineDecorator(StringDecorator):
    cache_ok = True
    def type_engine(self, dialect):
        return String()

class TextDecorator(TypeDecorator):
    impl = Text
    cache_ok = True

class DecoratedDialect(ClickHouseDialect):
    colspecs = {{
        "default": {{String: StringDecorator}},
        "load_dialect_impl": {{String: LoadDecorator}},
        "type_engine": {{String: EngineDecorator}},
        "two-step": {{String: TextDecorator, Text: StringDecorator}},
    }}[{cycle!r}]

dialect = DecoratedDialect()
assert isinstance(String().dialect_impl(dialect), TypeDecorator)
try:
    _datetime64_bind_signature(String(), dialect)
except CompileError as exc:
    assert "did not resolve after 1024 steps" in str(exc), str(exc)
else:
    raise AssertionError("Recursive type adaptation must raise CompileError")
""")


def test_plain_decorator_still_unwraps():
    assert _datetime64_bind_signature(_StringDecorator(), ClickHouseDialect()) is False


def test_clickhouse_colspecs_are_independent(monkeypatch):
    class ForeignStringDecorator(_StringDecorator):
        cache_ok = True

        def load_dialect_impl(self, dialect):
            return String()

        def process_bind_param(self, value, dialect):
            return f"foreign-{value}"

    monkeypatch.setitem(DefaultDialect.colspecs, String, ForeignStringDecorator)
    dialect = ClickHouseDialect(dbapi=dbapi)
    assert dialect.colspecs is not DefaultDialect.colspecs
    assert not isinstance(String().dialect_impl(dialect), TypeDecorator)
    assert _datetime64_bind_signature(String(), dialect) is False
    assert (
        str(select(literal("folder\\leaf", String())).compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        == "SELECT 'folder\\\\leaf' AS `anon_1`"
    )


class _Wrapper(TypeDecorator):
    impl = String
    cache_ok = True

    def __init__(self, child):
        super().__init__()
        self.impl = child


class _DialectWrapper(_Wrapper):
    cache_ok = True

    def load_dialect_impl(self, dialect):
        return DateTime().with_variant(DateTime64(6), "clickhousedb")


@pytest.mark.parametrize("depth", [3, 40])
@pytest.mark.parametrize(
    ("type_", "expected"),
    [
        (DateTime64(6), True),
        (Array(DateTime64(6)), ("array", (True,))),
        (Tuple(UInt32, DateTime64(6)), ("tuple", (False, True))),
        (Array(Tuple(UInt32, DateTime64(6))), ("array", (("tuple", (False, True)),))),
        (Nullable(DateTime64(6)), True),
        (DateTime().with_variant(DateTime64(6), "clickhousedb"), True),
        (_DialectWrapper(DateTime()), True),
        (ARRAY(_Wrapper(_Wrapper(DateTime64(6))), dimensions=2), ("array", (("array", (True,)),))),
        (String(), False),
    ],
    ids=["scalar", "array", "tuple", "array-tuple", "nullable", "variant", "load-dialect-impl", "generic-array", "string"],
)
def test_repeated_decorator_classes_preserve_bind_signature(type_, expected, depth):
    for _ in range(depth):
        type_ = _Wrapper(type_)

    assert _datetime64_bind_signature(type_, ClickHouseDialect()) == expected


@pytest.mark.parametrize("decorated_variant", [False, True])
def test_custom_hooks_resolve_fresh_variants(decorated_variant):
    class VariantDecorator(TypeDecorator):
        impl = String
        cache_ok = True

        def load_dialect_impl(self, dialect):
            child = _Wrapper(DateTime()) if decorated_variant else DateTime()
            return child.with_variant(DateTime64(6), "clickhousedb")

    assert _datetime64_bind_signature(VariantDecorator(), ClickHouseDialect()) is True


@pytest.mark.parametrize("steps", [1024, 1025])
def test_adaptation_step_limit(steps):
    class ChainDecorator(TypeDecorator):
        impl = String
        cache_ok = True

        def __init__(self, remaining):
            super().__init__()
            self.remaining = remaining

        def dialect_impl(self, dialect):
            return self

        def type_engine(self, dialect):
            return ChainDecorator(self.remaining - 1) if self.remaining > 1 else DateTime64(6)

    if steps == 1024:
        assert _datetime64_bind_signature(ChainDecorator(steps), ClickHouseDialect()) is True
    else:
        with pytest.raises(CompileError, match="did not resolve after 1024 steps"):
            _datetime64_bind_signature(ChainDecorator(steps), ClickHouseDialect())


def test_self_adapting_string_literals_raise_bounded_error():
    script = r"""
from sqlalchemy import String, TypeDecorator, literal, select
from sqlalchemy.exc import CompileError
from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

class StringDecorator(TypeDecorator):
    impl = String
    cache_ok = True

class DecoratedDialect(ClickHouseDialect):
    colspecs = {String: StringDecorator}

class Wrapper(TypeDecorator):
    impl = StringDecorator
    cache_ok = True

value = "folder\\'leaf%tail"
for server_side in (False, True):
    for type_ in (String(), Wrapper()):
        dialect = DecoratedDialect(dbapi=dbapi, server_side_params=server_side)
        try:
            select(literal(value, type_)).compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        except CompileError as exc:
            assert "did not resolve after 1024 steps" in str(exc), str(exc)
        else:
            raise AssertionError("Recursive literal type adaptation must raise CompileError")
"""
    _run_script(script)
