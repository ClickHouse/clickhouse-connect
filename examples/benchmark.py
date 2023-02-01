#!/usr/bin/env python3 -u

import datetime
import sys
import time
import uuid
import argparse
from ipaddress import IPv6Address
from typing import List

import clickhouse_connect
from clickhouse_connect.datatypes.format import set_default_formats
from clickhouse_connect.driver.client import Client

columns = {
    'int8': ('Int8', -44),
    'uint16': ('UInt16', 1),
    'int16': ('Int16', -2),
    'uint64': ('UInt64', 32489071615273482),
    'float32': ('Float32', 3.14),
    'str': ('String', 'hello'),
    'fstr': ('FixedString(16)', b'world numkn \nman'),
    'date': ('Date', datetime.date(2022, 3, 18)),
    'datetime': ('DateTime', datetime.datetime.utcnow()),
    'nullint': ('Nullable(Int8)', {None, 77}),
    'nullstr': ('Nullable(String)', {None, 'a_null_str'}),
    'enum': ("Enum16('hello' = 1, 'world' = 2)", 'hello'),
    'array': ('Array(String)', ['q', 'w', 'e', 'r']),
    'narray': ('Array(Array(String))', [['xkcd', 'abs', 'norbert'], ['George', 'John', 'Thomas']]),
    'uuid': ('UUID', uuid.UUID('1d439f79-c57d-5f23-52c6-ffccca93e1a9')),
    'bool': ('Bool', True),
    'ipv4': ('IPv4', '107.34.202.7'),
    'ipv6': ('IPv6', IPv6Address('fe80::f4d4:88ff:fe88:4a64')),
    'tuple': ('Tuple(Nullable(String), UInt64)', ('tuple_string', 7502888)),
    'dec': ('Decimal64(5)', 25774.233),
    'bdec': ('Decimal128(10)', 2503.48877233),
    'uint256': ('UInt256', 1057834823498238884432566),
    'dt64': ('DateTime64(9)', datetime.datetime.now()),
    'dt64d': ("DateTime64(6, 'America/Denver')", datetime.datetime.now()),
    'lcstr': ('LowCardinality(String)', 'A simple string')
}

standard_cols = ['uint16', 'int16', 'float32', 'str', 'fstr', 'date', 'datetime', 'array', 'nullint', 'enum', 'uuid']


def create_table(client: Client, col_names: List[str], rows: int):
    if not col_names:
        col_names = columns.keys()
    col_list = ','.join([f'{col_name} {columns[col_name][0]}' for col_name in sorted(col_names)])
    client.command('DROP TABLE IF EXISTS benchmark_test')
    client.command(f'CREATE TABLE benchmark_test ({col_list}) ENGINE Memory')
    insert_cols = []
    for col_name in sorted(col_names):
        col_def = columns[col_name]
        if isinstance(col_def[1], set):
            choices = tuple(col_def[1])
            cnt = len(choices)
            col = [choices[ix % cnt] for ix in range(rows)]
        else:
            col = [col_def[1]] * rows
        insert_cols.append(col)
    client.insert('benchmark_test', insert_cols, column_oriented=True)


def check_reads(client: Client, tries: int = 50, rows: int = 100000):
    start_time = time.time()
    for _ in range(tries):
        result = client.query(f'SELECT * FROM benchmark_test LIMIT {rows}', column_oriented=True)
        assert result.row_count == rows
    total_time = time.time() - start_time
    avg_time = total_time / tries
    speed = int(1 / avg_time * rows)
    print(f'- Avg time reading {rows} rows from {tries} runs: {avg_time} sec. Total: {total_time}')
    print(f'  Speed: {speed} rows/sec')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tries', help='Total tries for each test', type=int, default=50)
    parser.add_argument('-r', '--rows', help='Total rows in dataset', type=int, default=100000)
    parser.add_argument('-c', '--columns', help='Column types to test', type=str, nargs='+')

    args = parser.parse_args()
    rows = args.rows
    tries = args.tries
    col_names = args.columns
    if col_names:
        if 'all' in col_names:
            col_names = list(columns.keys())
        else:
            invalid = set(col_names).difference(set(columns.keys()))
            if invalid:
                print(' ,'.join(invalid) + ' columns not found')
                sys.exit()
    else:
        col_names = standard_cols
    client = clickhouse_connect.get_client(compress=False)

    set_default_formats('IP*', 'native', '*Int64', 'native')
    create_table(client, col_names, rows)
    check_reads(client, tries, rows)


if __name__ == '__main__':
    main()
