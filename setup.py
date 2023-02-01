import os
from setuptools import setup, find_packages

c_modules = []

try:
    from Cython.Build import cythonize
    from Cython import __version__ as cython_version

    print(f'Using Cython {cython_version}to build cython modules')
    c_modules = cythonize('clickhouse_connect/driverc/*.pyx', language_level='3str')
except ImportError as ex:
    print('Cython Install Failed, Not Building C Extensions: ', ex)
    cythonize = None
except Exception as ex:
    print('Cython Build Failed, Not Building C Extensions: ', ex)
    cythonize = None


def run_setup(try_c: bool = True):
    if try_c:
        kwargs = {
            'ext_modules': c_modules,
        }
    else:
        kwargs = {}

    project_dir = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(project_dir, 'README.md'), encoding='utf-8') as read_me:
        long_desc = read_me.read()

    version_fn = '.dev_version' if os.path.isfile('.dev_version') else 'clickhouse_connect/VERSION'
    with open(os.path.join(project_dir, version_fn), encoding='utf-8') as version_file:
        version = version_file.readline()

    setup(
        name='clickhouse-connect',
        author='ClickHouse Inc.',
        author_email='clients@clickhouse.com',
        keywords=['clickhouse', 'superset', 'sqlalchemy', 'http', 'driver'],
        description='ClickHouse core driver, SqlAlchemy, and Superset libraries',
        version=version,
        long_description=long_desc,
        long_description_content_type='text/markdown',
        package_data={'clickhouse_connect': ['VERSION']},
        url='https://github.com/ClickHouse/clickhouse-connect',
        packages=find_packages(exclude=['tests*']),
        python_requires='~=3.7',
        license='Apache License 2.0',
        install_requires=[
            'certifi',
            'urllib3>=1.26',
            'pytz',
            'zstandard',
            'lz4'
        ],
        extras_require={
            'sqlalchemy': ['sqlalchemy>1.3.21,<1.4'],
            'superset': ['apache_superset>=1.4.1'],
            'numpy': ['numpy'],
            'pandas': ['pandas'],
            'arrow': ['pyarrow'],
            'orjson': ['orjson'],
        },
        entry_points={
            'sqlalchemy.dialects': ['clickhousedb.connect=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect',
                                    'clickhousedb=clickhouse_connect.cc_sqlalchemy.dialect:ClickHouseDialect'],
            'superset.db_engine_specs': ['clickhousedb=clickhouse_connect.cc_superset.engine:ClickHouseEngineSpec']
        },
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
            'Programming Language :: Python :: 3.11'
        ],
        **kwargs
    )


try:
    run_setup()
except (Exception, IOError, SystemExit) as e:
    print(f'Unable to compile C extensions for faster performance due to {e}, will use pure Python')
    run_setup(False)
