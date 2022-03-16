import datetime
import time
import uuid

import clickhouse_connect

row_data = (1, 2, 3.14, "hello", b"world world \nman", datetime.date.today(), datetime.datetime.utcnow(),
            "hello", None, ["q", "w", "e", "r"], uuid.UUID('1d439f79-c57d-5f23-52c6-ffccca93e1a9'))



def create_table(rows: int = 50000):
    client.command("DROP TABLE IF EXISTS benchmark_test")
    client.command("CREATE TABLE benchmark_test ("
                   "a UInt16,"
                   "b Int16,"
                   "c Float32,"
                   "d String,"
                   "e FixedString(16),"
                   "f Date,"
                   "g DateTime,"
                   "h Enum16('hello' = 1, 'world' = 2),"
                   "j Nullable(Int8),"
                   "k Array(String),"
                   "u UUID"
                   ") ENGINE = Memory"
                   )
    client.insert('benchmark_test', '*', (row_data,) * rows)


def check_reads(retries: int = 100, rows: int = 10000):
    start_time = time.time()
    for _ in range(retries):
        result = client.query("SELECT * FROM benchmark_test")
        assert len(result.result_set) == rows
    total_time = time.time() - start_time
    avg_time = total_time / retries
    speed = int(1 / avg_time * rows)
    print(
        f"- Avg time reading {rows} rows from {retries} runs: {avg_time} sec. Total: {total_time}"
    )
    print(f"  Speed: {speed} rows/sec")


if __name__ == '__main__':
    client = clickhouse_connect.client(compress=False)
    create_table(10000)
    check_reads(50, 10000)
