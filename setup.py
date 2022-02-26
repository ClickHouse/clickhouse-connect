from setuptools import setup

setup(
    name='clickhouse-connect',
    version='0.0.1',
    packages=['clickhouse_connect'],
    python_requires="~=3.7",
    install_requires=[
        'httpx'
    ],
)