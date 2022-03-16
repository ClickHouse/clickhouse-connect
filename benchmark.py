import datetime
import time
import uuid
import argparse
from typing import List

import clickhouse_connect
from clickhouse_connect.driver import BaseDriver


columns = {
    'uint16': ('UInt16', 1),
    'int16': ('Int16', -2),
    'float32': ('Float32', 3.14),
    'str': ('String', 'hello'),
    'fstr': ('FixedString(16)', b"world world \nman"),
    'date': ('Date', datetime.date.today()),
    'datetime': ('DateTime', datetime.datetime.utcnow()),
    'nullable': ('Nullable(Int8)', None),
    'enum': ("Enum16('hello' = 1, 'world' = 2)", 'hello'),
    'array': ('Array(String)', ['q', 'w', 'e', 'r']),
    'uuid': ('UUID', uuid.UUID('1d439f79-c57d-5f23-52c6-ffccca93e1a9')),
    'bool': ('Bool', True),
    'ipv4': ('IPv4', '107.34.202.7')
}

standard_cols = ['uint16', 'int16', 'float32', 'str', 'fstr', 'date', 'datetime', 'nullable', 'enum', 'array', 'uuid']


def create_table(client: BaseDriver, col_names: List[str], rows: int):
    if not col_names:
        col_names = columns.keys()
    col_list = ','.join([f'{cn} {columns[cn][0]}' for cn in sorted(col_names)])
    client.command("DROP TABLE IF EXISTS benchmark_test")
    client.command(f'CREATE TABLE benchmark_test ({col_list}) ENGINE Memory')
    row_data = [columns[cn][1] for cn in sorted(col_names)]
    client.insert('benchmark_test', '*', (row_data,) * rows)


def check_reads(client: BaseDriver, tries: int = 100, rows: int = 10000):
    start_time = time.time()
    for _ in range(tries):
        result = client.query("SELECT * FROM benchmark_test")
        assert len(result.result_set) == rows
    total_time = time.time() - start_time
    avg_time = total_time / tries
    speed = int(1 / avg_time * rows)
    print(
        f"- Avg time reading {rows} rows from {tries} runs: {avg_time} sec. Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--tries', help="Total tries for each test", type=int, default=50)
    parser.add_argument('-r', '--rows', help="Total rows in dataset", type=int, default=10000)
    parser.add_argument('-c', '--columns', help="Column types to test", type=str, nargs='+')

    args = parser.parse_args()
    rows = args.rows
    tries = args.tries
    col_names = args.columns
    if col_names:
        invalid = set(col_names).difference(set(columns.keys()))
        if invalid:
            print(' ,'.join(invalid) + ' columns not found')
            quit()
    else:
        col_names = standard_cols
    client = clickhouse_connect.client(compress=False)
    create_table(client, col_names, rows )
    check_reads(client, tries, rows)


if __name__ == '__main__':
    main()
