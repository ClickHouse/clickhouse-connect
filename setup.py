from setuptools import setup, find_packages

setup(
    name='clickhouse-connect',
    version='0.0.1',
    author='ClickHouse Inc.',
    author_email='clickhouse-connect@clickhouse.com',
    packages=find_packages(exclude=['tests*']),
    python_requires="~=3.7",
    install_requires=[
        'httpx[http2]'
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'clickhouse_connect=clickhouse_connect.sqlalchemy.dialect:ClickHouseDialect'
        ],
        'superset.db_engine_specs': [
            'clickhouse_connect=clickhouse_connect.superset.engine:ClickHouseEngineSpec'
        ]
    },
)
