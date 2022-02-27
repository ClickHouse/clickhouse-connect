from setuptools import setup, find_packages

setup(
    name='clickhouse-connect',
    version='0.0.1',
    author='ClickHouse Inc.',
    author_email='clickhouse-connect@clickhouse.com',
    packages=find_packages(exclude=['tests']),
    python_requires="~=3.7",
    install_requires=[
        'httpx'
    ],
)
