#!/usr/bin/env python -u

"""
This script is for simple timed comparisons of various queries between formats (streaming vs batch, pandas vs Python
native types) based on data loaded into a local clickhouse instance from some ClickHouse Sample Datasets
https://clickhouse.com/docs/en/getting-started/example-datasets/

It includes some basic comparisons with clickhouse-driver.  The clickhouse-driver import and client can be
commented out if clickhouse-driver is not installed

Uncomment the queries and formats to measure before running.

This script is not intended to be rigorous or scientific.  For entertainment purposes only
"""

import time
import clickhouse_driver  # pylint: disable=import-error
import clickhouse_connect


queries = ['SELECT trip_id, pickup, dropoff, pickup_longitude, pickup_latitude FROM taxis',
           'SELECT number from numbers(500000000)',
           'SELECT * FROM datasets.hits_100m_obfuscated',
           #"SELECT * FROM perftest.ontime WHERE FlightDate < '2017-02-18'"
           ]

cc_client = clickhouse_connect.get_client(compress=False)
cd_client = clickhouse_driver.Client(host='localhost')


def read_python_columns(query):
    print('\n\tclickhouse-connect Python Batch (column oriented):')
    start = time.time()
    columns = cc_client.query(query).result_columns
    _print_result(start, len(columns[0]))


def read_python_rows(query):
    print('\n\tclickhouse-connect Python Batch (row oriented):')
    start = time.time()
    rows = cc_client.query(query).result_rows
    _print_result(start, len(rows))


def read_python_stream_columns(query):
    print('\n\tclickhouse-connect Python Stream (column blocks):')
    rows = 0
    start = time.time()
    with cc_client.query_column_block_stream(query) as stream:
        for block in stream:
            rows += len(block[0])
    _print_result(start, rows)


def read_python_stream_rows(query):
    print('\n\tclickhouse-connect Python Stream (row blocks):')
    rows = 0
    start = time.time()
    with cc_client.query_row_block_stream(query) as stream:
        for block in stream:
            rows += len(block)
    _print_result(start, rows)


def read_numpy(query):
    print('\n\tclickhouse connect Numpy Batch:')
    start = time.time()
    arr = cc_client.query_np(query, max_str_len=100)
    _print_result(start, len(arr))


def read_pandas(query):
    print('\n\tclickhouse connect Pandas Batch:')
    start = time.time()
    rows = len(cc_client.query_df(query))
    _print_result(start, rows)


def read_arrow(query):
    print('\n\tclickhouse connect Arrow:')
    start = time.time()
    rows = len(cc_client.query_arrow(query))
    _print_result(start, rows)


def read_pandas_stream(query):
    print('\n\tclickhouse-connect Pandas Stream')
    start = time.time()
    rows = 0
    with cc_client.query_df_stream(query) as stream:
        for data_frame in stream:
            rows += len(data_frame)
    _print_result(start, rows)


def dr_read_python(query):
    print('\n\tclickhouse-driver Python Batch (column oriented):')
    start = time.time()
    result = cd_client.execute(query, columnar=True)
    _print_result(start, len(result[0]))


def dr_read_python_rows(query):
    print('\n\tclickhouse-driver Python Batch (row oriented):')
    start = time.time()
    result = cd_client.execute(query)
    _print_result(start, len(result))


def dr_read_python_stream(query):
    print('\n\tclickhouse-driver Python Stream:')
    start = time.time()
    rows = 0
    for block in cd_client.execute_iter(query):
        rows += len(block)
    _print_result(start, rows)


def dr_read_pandas(query):
    print('\n\tclickhouse-driver Pandas Batch:')
    start = time.time()
    data_frame = cd_client.query_dataframe(query)
    _print_result(start, len(data_frame))


def _print_result(start, rows):
    total_time = time.time() - start
    print(f'\t\tTime: {total_time:.4f} sec  rows: {rows}  rows/sec {rows // total_time}')


def main():
    for query in queries:
        print(f'\n{query}')
        # read_python_columns(query)
        #read_python_rows(query)
        #read_python_stream_rows(query)
        read_python_stream_columns(query)
        read_pandas_stream(query)
        # read_numpy(query)
        #read_pandas(query)
        # read_arrow(query)
        #dr_read_python(query)
        # dr_read_python_rows(query)
        #dr_read_pandas(query)


if __name__ == '__main__':
    main()
