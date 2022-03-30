from setuptools import setup, find_packages


setup(
    name='clickhouse-connect',
    version='0.0.4',
    author='ClickHouse Inc.',
    author_email='clickhouse-connect@clickhouse.com',
    packages=find_packages(exclude=['tests*']),
    python_requires="~=3.7",
    install_requires=[
        'requests',
        'pytz'
    ],
    tests_require=[
        'sqlalchemy>1.3.21, <1.4'
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
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ]
)
