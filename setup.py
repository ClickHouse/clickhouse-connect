import os
import re

from setuptools import Extension, find_packages, setup

SKIP_CYTHON_ENV = "CLICKHOUSE_CONNECT_SKIP_CYTHON"
REQUIRE_C_ENV = "CLICKHOUSE_CONNECT_REQUIRE_C"

skip_cython = os.environ.get(SKIP_CYTHON_ENV) == "1"
require_c = os.environ.get(REQUIRE_C_ENV) == "1"

if skip_cython and require_c:
    raise RuntimeError(
        f"{SKIP_CYTHON_ENV}=1 and {REQUIRE_C_ENV}=1 are mutually exclusive. "
        f"Set {SKIP_CYTHON_ENV}=1 for a pure Python build, or {REQUIRE_C_ENV}=1 to require the C extensions."
    )

c_modules: list[Extension] = []
pure_hint = "" if require_c else f" Set {SKIP_CYTHON_ENV}=1 to request a pure Python build explicitly."

if skip_cython:
    print(f"{SKIP_CYTHON_ENV} set, not building C extensions")
else:
    try:
        from Cython import __version__ as cython_version
        from Cython.Build import cythonize
    except ImportError as ex:
        raise RuntimeError(
            f"Cython is required to build the C extensions ({type(ex).__name__}: {ex}). Fix the build environment.{pure_hint}"
        ) from ex

    print(f"Using Cython {cython_version} to build cython modules")
    try:
        c_modules = cythonize(
            [
                Extension(
                    "clickhouse_connect.driverc.*",
                    ["clickhouse_connect/driverc/*.pyx"],
                    optional=not require_c,
                )
            ],
            language_level="3str",
        )
    except Exception as ex:
        raise RuntimeError(
            f"Preparing the Cython extensions failed ({type(ex).__name__}: {ex}). Fix the build environment.{pure_hint}"
        ) from ex

    # `cythonize()` regenerates the extension objects, so reassert the flag that decides whether a
    # compiler or linker failure is fatal (`CLICKHOUSE_CONNECT_REQUIRE_C=1`) or falls back to pure Python.
    for c_module in c_modules:
        c_module.optional = not require_c


def run_setup():
    project_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(project_dir, "README.md"), encoding="utf-8") as read_me:
        long_desc = read_me.read()

    version = "development"
    if os.path.isfile(".dev_version"):
        with open(os.path.join(project_dir, ".dev_version"), encoding="utf-8") as version_file:
            version = version_file.readline()
    else:
        with open(os.path.join(project_dir, "clickhouse_connect", "_version.py"), encoding="utf-8") as version_file:
            file_version = version_file.read().strip()
            match = re.search(r"version\s*=\s*['\"](.+)['\"]", file_version)
            if match is None:
                raise ValueError(f"invalid version {file_version} in clickhouse_connect/_version.py")
            version = match.group(1)

    setup(
        name="clickhouse-connect",
        author="ClickHouse Inc.",
        author_email="clients@clickhouse.com",
        keywords=["clickhouse", "superset", "sqlalchemy", "http", "driver"],
        description="ClickHouse Database Core Driver for Python, Pandas, and Superset",
        version=version,
        long_description=long_desc,
        long_description_content_type="text/markdown",
        url="https://github.com/ClickHouse/clickhouse-connect",
        packages=find_packages(exclude=["tests*"]),
        package_data={"clickhouse_connect": ["py.typed"]},
        python_requires=">=3.10,<3.15",
        license="Apache-2.0",
        install_requires=[
            "certifi",
            "urllib3>=1.26",
            'tzdata; sys_platform == "win32"',
            'backports.zstd>=1.3.0; python_version<"3.14"',
            'lz4; python_version<"3.14"',
            'lz4>=4.4.5; python_version>="3.14"',
        ],
        extras_require={
            "sqlalchemy": ["sqlalchemy>=1.4.40,<3.0"],
            "alembic": ["sqlalchemy>=1.4.40,<3.0", "alembic>=1.18"],
            "numpy": ["numpy"],
            "pandas": ["pandas>=2,<4"],
            "polars": ["polars>=1.0"],
            "arrow": ['pyarrow>=22.0; python_version>="3.14"', 'pyarrow; python_version<"3.14"'],
            "orjson": ["orjson"],
            "tzlocal": ["tzlocal>=4.0"],
            "tzdata": ["tzdata"],
            "async": ["aiohttp>=3.9.0"],
            "chdb": ["chdb>=4.1.7"],
            "rust": ["clickhouse-connect-core>=0.2.0,<0.3"],
        },
        tests_require=["pytest"],
        entry_points={
            "sqlalchemy.dialects": [
                "clickhousedb.connect=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect",
                "clickhousedb=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect",
            ]
        },
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: 3.13",
            "Programming Language :: Python :: 3.14",
        ],
        ext_modules=c_modules,
    )


run_setup()
