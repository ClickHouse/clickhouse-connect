import runpy
import sys
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).parents[1]
SETUP_PATH = ROOT / "setup.py"


class _FakeExtension:
    def __init__(self, name: str, sources: list[str], **kwargs: Any) -> None:
        self.name = name
        self.sources = sources
        self.optional = kwargs.get("optional", False)


def _run_setup(
    monkeypatch: pytest.MonkeyPatch,
    setup: Callable[..., None],
    *,
    cythonize: Callable[..., list[Any]] | None = None,
    skip_cython: bool = False,
    require_c: bool = False,
) -> None:
    setuptools = ModuleType("setuptools")
    setuptools.find_packages = lambda **_kwargs: []  # type: ignore[attr-defined]
    setuptools.setup = setup  # type: ignore[attr-defined]
    setuptools.Extension = _FakeExtension  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "setuptools", setuptools)

    if cythonize is None:
        monkeypatch.setitem(sys.modules, "Cython", None)
        monkeypatch.setitem(sys.modules, "Cython.Build", None)
    else:
        cython = ModuleType("Cython")
        cython.__version__ = "test"  # type: ignore[attr-defined]
        cython_build = ModuleType("Cython.Build")
        cython_build.cythonize = cythonize  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "Cython", cython)
        monkeypatch.setitem(sys.modules, "Cython.Build", cython_build)

    for env_name, enabled in (
        ("CLICKHOUSE_CONNECT_SKIP_CYTHON", skip_cython),
        ("CLICKHOUSE_CONNECT_REQUIRE_C", require_c),
    ):
        if enabled:
            monkeypatch.setenv(env_name, "1")
        else:
            monkeypatch.delenv(env_name, raising=False)

    monkeypatch.chdir(ROOT)
    runpy.run_path(str(SETUP_PATH), run_name="__main__")


def _passthrough_cythonize(module_list: list[Any], **_kwargs: Any) -> list[Any]:
    return list(module_list)


def test_default_build_marks_extensions_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls: list[dict[str, Any]] = []

    def setup(**kwargs: Any) -> None:
        setup_calls.append(kwargs)

    _run_setup(monkeypatch, setup, cythonize=_passthrough_cythonize)

    assert len(setup_calls) == 1
    ext_modules = setup_calls[0]["ext_modules"]
    assert ext_modules
    assert all(ext.optional for ext in ext_modules)


def test_require_c_marks_extensions_mandatory(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls: list[dict[str, Any]] = []

    def setup(**kwargs: Any) -> None:
        setup_calls.append(kwargs)

    _run_setup(monkeypatch, setup, cythonize=_passthrough_cythonize, require_c=True)

    assert len(setup_calls) == 1
    ext_modules = setup_calls[0]["ext_modules"]
    assert ext_modules
    assert not any(ext.optional for ext in ext_modules)


def test_explicit_skip_cython_builds_without_extensions(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls: list[dict[str, Any]] = []

    def setup(**kwargs: Any) -> None:
        setup_calls.append(kwargs)

    def cythonize(*_args: Any, **_kwargs: Any) -> list[Any]:
        raise AssertionError("Cython must not be used when explicitly skipped")

    _run_setup(monkeypatch, setup, cythonize=cythonize, skip_cython=True)

    assert len(setup_calls) == 1
    assert setup_calls[0]["ext_modules"] == []


def test_skip_and_require_together_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls = 0

    def setup(**_kwargs: Any) -> None:
        nonlocal setup_calls
        setup_calls += 1

    with pytest.raises(RuntimeError, match="mutually exclusive"):
        _run_setup(
            monkeypatch,
            setup,
            cythonize=_passthrough_cythonize,
            skip_cython=True,
            require_c=True,
        )

    assert setup_calls == 0


def test_cython_import_failure_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls = 0

    def setup(**_kwargs: Any) -> None:
        nonlocal setup_calls
        setup_calls += 1

    with pytest.raises(RuntimeError, match="CLICKHOUSE_CONNECT_SKIP_CYTHON=1"):
        _run_setup(monkeypatch, setup)

    assert setup_calls == 0


def test_cythonize_failure_is_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls = 0

    def setup(**_kwargs: Any) -> None:
        nonlocal setup_calls
        setup_calls += 1

    def cythonize(*_args: Any, **_kwargs: Any) -> list[Any]:
        raise ValueError("invalid Cython source")

    with pytest.raises(RuntimeError, match="CLICKHOUSE_CONNECT_SKIP_CYTHON=1"):
        _run_setup(monkeypatch, setup, cythonize=cythonize)

    assert setup_calls == 0


def test_cythonize_failure_is_fatal_in_require_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    setup_calls = 0

    def setup(**_kwargs: Any) -> None:
        nonlocal setup_calls
        setup_calls += 1

    def cythonize(*_args: Any, **_kwargs: Any) -> list[Any]:
        raise ValueError("invalid Cython source")

    with pytest.raises(RuntimeError, match="CLICKHOUSE_CONNECT_SKIP_CYTHON=1"):
        _run_setup(monkeypatch, setup, cythonize=cythonize, require_c=True)

    assert setup_calls == 0


@pytest.mark.parametrize("require_c", [False, True])
def test_unrelated_setup_errors_propagate(monkeypatch: pytest.MonkeyPatch, require_c: bool) -> None:
    class SetupError(Exception):
        pass

    setup_calls = 0

    def setup(**_kwargs: Any) -> None:
        nonlocal setup_calls
        setup_calls += 1
        raise SetupError("broken package metadata")

    with pytest.raises(SetupError, match="broken package metadata"):
        _run_setup(monkeypatch, setup, cythonize=_passthrough_cythonize, require_c=require_c)

    assert setup_calls == 1
