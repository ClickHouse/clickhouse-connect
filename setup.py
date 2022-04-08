from distutils.errors import CCompilerError, DistutilsExecError, DistutilsPlatformError
from setuptools import setup, find_packages, Extension

c_modules = []

try:
    from Cython.Build import cythonize
    c_modules = cythonize('clickhouse_connect/driverc/*.pyx')
    C_PKG = None
except ImportError:
    C_PKG = 'clickhouse_connect/driverc'
    c_modules = [Extension('creaders', ['clickhouse_connect/driverc/creaders.c'])]


def run_setup(try_c: bool = True):

    if try_c:
        kwargs = {'ext_modules': c_modules}
        if C_PKG:
            kwargs['ext_package'] = C_PKG
    else:
        kwargs = {}

    setup(
        name='clickhouse-connect',
        version='0.0.5',
        author='ClickHouse Inc.',
        author_email='clickhouse-connect@clickhouse.com',
        packages=find_packages(exclude=['tests*']),
        python_requires='~=3.7',
        install_requires=[
            'requests',
            'pytz'
        ],
        tests_require=[
            'sqlalchemy>1.3.21, <1.4',
            'apache_superset>=1.4.1',
            'pytest',
            'pytest-mock'
        ],
        extras_require={
            'sqlalchemy': ['sqlalchemy>1.3.21, <1.4'],
            'superset': ['apache_superset>=1.4.1', 'sqlalchemy>1.3.21, <1.4'],
            'brotli': ['brotli>=1.09'],
            'numpy': ['numpy'],
            'pandas': ['pandas']
        },
        entry_points={
            'sqlalchemy.dialects': ['clickhousedb.connect=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect',
                                    'clickhousedb=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect'],
            'superset.db_engine_specs': ['clickhousedb=clickhouse_connect.cc_superset.engine:ClickHouseEngineSpec']
        },
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
        ],
        **kwargs
    )

try:
    run_setup()
except (CCompilerError, DistutilsExecError, DistutilsPlatformError):
    print ('Unable to compile C extensions for faster performance, will use pure Python')
    run_setup(False)
