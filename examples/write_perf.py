#!/usr/bin/env python -u

# pylint: disable=import-error,no-name-in-module
import time
import random
import clickhouse_driver

import clickhouse_connect
from clickhouse_connect.tools.testing import TableContext


inserts = [{'query': 'SELECT trip_id, pickup, dropoff, pickup_longitude, ' +
                     'pickup_latitude FROM taxis ORDER BY trip_id LIMIT 5000000',
            'columns': 'trip_id UInt32, pickup String, dropoff String,' +
                       ' pickup_longitude Float64, pickup_latitude Float64'},
           {'query': 'SELECT number from numbers(5000000)',
            'columns': 'number UInt64'}]

excluded = {}
cc_client = clickhouse_connect.get_client(compress=False)
cd_client = clickhouse_driver.Client(host='localhost')
run_id = random.randint(0, 10000000)


def write_python_columns(ix, insert):
    print('\n\tclickhouse-connect Python Insert (column oriented):')
    data = cc_client.query(insert['query']).result_columns
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        start = time.time()
        cc_client.insert(table, data, ctx.column_names, column_type_names=ctx.column_types, column_oriented=True)
    _print_result(start, len(data[0]))


def write_python_rows(ix, insert):
    print('\n\tclickhouse-connect Python Insert (row oriented):')
    data = cc_client.query(insert['query']).result_rows
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        start = time.time()
        cc_client.insert(table, data, ctx.column_names, column_type_names=ctx.column_types)
    _print_result(start, len(data))


def dr_write_python_columns(ix, insert):
    print('\n\tclickhouse-driver Python Insert (column oriented):')
    data = cd_client.execute(insert['query'], columnar=True)
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        cols = ','.join(ctx.column_names)
        start = time.time()
        cd_client.execute(f'INSERT INTO {table} ({cols}) VALUES', data, columnar=True)
    _print_result(start, len(data[0]))


def dr_write_python_rows(ix, insert):
    print('\n\tclickhouse-driver Python Insert (row oriented):')
    data = cd_client.execute(insert['query'], columnar=False)
    table = f'perf_test_insert_{run_id}_{ix}'
    with test_ctx(table, insert) as ctx:
        cols = ','.join(ctx.column_names)
        start = time.time()
        cd_client.execute(f'INSERT INTO {table} ({cols}) VALUES', data, columnar=False)
    _print_result(start, len(data))


def test_ctx(table, insert):
    return TableContext(cc_client, table, insert['columns'])


def _print_result(start, rows):
    total_time = time.time() - start
    print(f'\t\tTime: {total_time:.4f} sec  rows: {rows}  rows/sec {rows // total_time}')


def main():
    for ix, insert in enumerate(inserts):
        if ix in excluded:
            continue
        print(f"\n{insert['query']}")
        # write_python_columns(ix, insert)
        write_python_rows(ix, insert)
        # dr_write_python_columns(ix, insert)
        dr_write_python_rows(ix, insert)


class CDWrapper:
    def __init__(self, client):
        self._client = client

    def command(self, cmd):
        self._client.execute(cmd)


if __name__ == '__main__':
    main()
