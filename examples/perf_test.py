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
import clickhouse_driver
import clickhouse_connect


queries = ['SELECT trip_id, pickup, dropoff, pickup_longitude, pickup_latitude FROM taxis',
           'SELECT number from numbers(500000000)',
           'SELECT * FROM datasets.hits_100m_obfuscated'
           ]

cc_client = clickhouse_connect.get_client(send_progress=False, query_limit=0, compress=False)
cd_client = clickhouse_driver.Client(host='localhost')


def read_python(query):
    print('\n\tPython Batch:')
    start = time.time()
    rows = cc_client.query(query).result_rows
    print(time.time() - start)
    print(rows)


def read_python_stream(query):
    print('\n\tPython Stream:')
    rows = 0
    start = time.time()
    with cc_client.query(query) as query_result:
        for block in query_result.stream_column_blocks():
            rows += len(block[0])
    print(time.time() - start)
    print(rows)


def read_numpy(query):
    print('\n\tNumpy Batch:')
    start = time.time()
    arr = cc_client.query_np(query, max_str_len=100)
    rows = len(arr)
    print(time.time() - start)
    print(rows)


def read_pandas(query):
    print('\n\tPandas Batch:')
    start = time.time()
    rows = len(cc_client.query_df(query))
    print(time.time() - start)
    print(rows)


def read_pandas_stream(query):
    print('\n\tPandas stream version')
    start = time.time()
    rows = 0
    with cc_client.query_df_stream(query) as stream:
        for data_frame in stream:
            rows += len(data_frame)
    print(time.time() - start)
    print(rows)


def dr_read_python(query):
    print('\n\tclickhouse-driver Python Batch:')
    start = time.time()
    result = cd_client.execute(query)
    print(time.time() - start)
    print(len(result))


def dr_read_python_stream(query):
    print('\n\tclickhouse-driver Python Stream:')
    start = time.time()
    rows = 0
    for block in cd_client.execute_iter(query):
        rows += len(block)
    print(time.time() - start)
    print(rows)


def dr_read_pandas(query):
    print('\n\tclickhouse-driver Pandas:')
    start = time.time()
    data_frame = cd_client.query_dataframe(query)
    print(time.time() - start)
    print(len(data_frame))


def main():
    for query in queries:
        print(f'\n{query}')
        read_python(query)
        read_python_stream(query)
        # read_pandas_streaming(query)
        # read_numpy(query)
        read_pandas(query)
        dr_read_python(query)
        dr_read_pandas(query)


if __name__ == '__main__':
    main()
