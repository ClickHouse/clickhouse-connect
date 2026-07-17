#!/usr/bin/env python3

import clickhouse_connect

client = clickhouse_connect.get_client(host="localhost", username="default", password="password")

client.command("DROP TABLE IF EXISTS new_table")
client.command("CREATE TABLE new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key")

row1 = [1000, "String Value 1000", 5.233]
row2 = [2000, "String Value 2000", -107.04]

client.insert("new_table", [row1, row2], column_names=["key", "value", "metric"])

result = client.query("SELECT max(key), avg(metric) FROM new_table")
print(result.result_rows)
