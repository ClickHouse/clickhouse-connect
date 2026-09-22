"""Qualify a core wheel with the portable driver wheel outside the checkout."""

import argparse
import hashlib
import importlib
import importlib.metadata
import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path


def check_install(profile: str, output: Path, allow_source_lz4: bool = False) -> None:
    # These imports must run inside the fresh qualification environment.
    import _ch_core
    from packaging.requirements import Requirement

    import clickhouse_connect
    from clickhouse_connect.driver import rustcodec

    modules = {"driver": clickhouse_connect, "core": _ch_core}
    for name, module in modules.items():
        assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), (name, module.__file__)
    core = importlib.metadata.distribution("clickhouse-connect-core")
    core_specs = [
        requirement
        for requirement in map(Requirement, importlib.metadata.requires("clickhouse-connect") or ())
        if requirement.name == "clickhouse-connect-core"
    ]
    assert core_specs, "clickhouse-connect declares no clickhouse-connect-core requirement"
    for requirement in core_specs:
        assert requirement.specifier.contains(core.version, prereleases=True), (str(requirement), core.version)
    wheel_tags = [line.split(":", 1)[1].strip() for line in (core.read_text("WHEEL") or "").splitlines() if line.startswith("Tag:")]
    assert rustcodec.resolve_native_codec("rust_strict") == "rust_strict"
    assert _ch_core.BINDING_API_VERSION >= rustcodec.REQUIRED_BINDING_API_VERSION
    assert _ch_core.COLUMN_BUFFER_API_VERSION >= rustcodec.REQUIRED_COLUMN_BUFFER_API_VERSION
    for name, expected in (
        ("numpy", profile != "bare"),
        ("pandas", profile in ("pandas", "arrow")),
        ("pyarrow", profile == "arrow"),
        ("nanoarrow", False),
    ):
        assert (importlib.util.find_spec(name) is not None) == expected, (profile, name, expected)
        if expected:
            module = importlib.import_module(name)
            assert Path(module.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()), (name, module.__file__)
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    if free_threaded:
        assert not sys._is_gil_enabled(), "Qualification must run with the GIL disabled"
        # PYTHON_GIL=0 hides a module that re-enables the GIL.
        env = {key: value for key, value in os.environ.items() if key != "PYTHON_GIL"}
        subprocess.run(
            [sys.executable, "-W", "error::RuntimeWarning", "-c", "import sys, _ch_core\nassert not sys._is_gil_enabled()"],
            check=True,
            env=env,
        )
    wire = _ch_core.encode_native_block(["value"], ["Int32"], [[13, 79]], 2, None)
    descriptor = _ch_core.ColBatch.decode_native(wire).column_buffers(0)[0]
    assert descriptor.length == 2 and descriptor.kind == "int32"
    assert memoryview(descriptor.values).readonly
    result = {
        "profile": profile,
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "byteorder": sys.byteorder,
        "free_threaded": free_threaded,
        "source_builds": ["lz4"] if allow_source_lz4 else [],
        "binding_api": _ch_core.BINDING_API_VERSION,
        "column_buffer_api": _ch_core.COLUMN_BUFFER_API_VERSION,
        "core_version": core.version,
        "core_wheel_tags": wheel_tags,
        "module_paths": {name: module.__file__ for name, module in modules.items()},
        "core_files": {
            module.__file__: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
            for name, module in sys.modules.items()
            if name == "_ch_core" or name.startswith("_ch_core.")
        },
        "packages": {dist.metadata["Name"]: dist.version for dist in importlib.metadata.distributions()},
    }
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2), flush=True)


def qualify(args: argparse.Namespace) -> None:
    repo = Path(__file__).resolve().parents[2]
    output = args.output.resolve()
    assert not output.is_relative_to(repo), "Qualification must run outside the checkout"
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, CLICKHOUSE_CONNECT_USE_C="0", TZ="UTC")
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    if sysconfig.get_config_var("Py_GIL_DISABLED"):
        env["PYTHON_GIL"] = "0"

    def run(*command: str) -> None:
        print("+", " ".join(command), flush=True)
        subprocess.run(command, check=True, cwd=output, env=env)

    environment = output / "venv"
    run(args.uv, "venv", "--python", sys.executable, str(environment))
    python = str(environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python"))
    install = (args.uv, "pip", "install", "--python", python, "--only-binary=:all:")
    wheels = sorted(args.core_wheels.resolve().glob("*.whl"))
    assert wheels, "No core wheels supplied"
    driver = args.driver_wheel.resolve()
    assert driver.name.endswith("-py3-none-any.whl"), "Platform qualification uses the portable driver wheel"
    manifest = {wheel.name: hashlib.sha256(wheel.read_bytes()).hexdigest() for wheel in [driver, *wheels]}
    (output / "wheel-sha256.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    run(*install, "--no-deps", "--no-index", "--find-links", str(args.core_wheels.resolve()), "clickhouse-connect-core")
    if args.allow_source_lz4:
        # lz4 has no musllinux or free-threaded wheels. Keep other installs binary-only.
        run(args.uv, "pip", "install", "--python", python, "--no-deps", "--no-binary=lz4", "lz4>=4.4.5")
    run(*install, str(driver), "pytest", "tzdata", "packaging")

    tests = output / "tests"
    tests.mkdir()
    shutil.copy2(repo / "tests/conftest.py", tests / "conftest.py")
    binding = tests / "binding"
    binding.mkdir()
    for name in ("helpers.py", "test_buffers.py", "test_array_buffers.py", "test_dictionary_buffers.py"):
        shutil.copy2(repo / "rust/ch-core-py/tests" / name, binding / name)
    for name in ("test_rustnumpy.py", "test_rustnumpy_buffers.py"):
        shutil.copy2(repo / "tests/unit_tests/test_driver" / name, tests / name)
    checker = output / "qualify_core_wheel.py"
    shutil.copy2(__file__, checker)
    source_flags = ("--allow-source-lz4",) if args.allow_source_lz4 else ()
    for profile, packages in (("bare", ()), ("numpy", ("numpy",)), ("pandas", ("pandas>=2,<4",)), ("arrow", ("pyarrow",))):
        if profile == "arrow" and not args.arrow:
            break
        if packages:
            run(*install, *packages)
        run(python, str(checker), "--check-installed", profile, "--output", str(output / f"{profile}.json"), *source_flags)
        if profile != "bare":
            run(python, "-m", "pytest", str(tests), "-q", f"--junitxml={output / (profile + '.xml')}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-wheels", type=Path)
    parser.add_argument("--driver-wheel", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--uv", default="uv")
    parser.add_argument("--arrow", action="store_true")
    parser.add_argument("--allow-source-lz4", action="store_true", help="Build lz4 from source for musllinux or free-threaded Python")
    parser.add_argument("--check-installed", choices=("bare", "numpy", "pandas", "arrow"))
    args = parser.parse_args()
    if args.check_installed:
        check_install(args.check_installed, args.output, args.allow_source_lz4)
    else:
        if args.core_wheels is None or args.driver_wheel is None:
            parser.error("--core-wheels and --driver-wheel are required")
        qualify(args)


if __name__ == "__main__":
    main()
