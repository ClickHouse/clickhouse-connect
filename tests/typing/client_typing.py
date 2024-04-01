import io

from clickhouse_connect.driver import Client
from typing import assert_type, cast, Union

# Test Client.raw_query overloads:

client = cast(Client, None)

result = client.raw_query("")
assert_type(result, bytes)

result = client.raw_query("", stream=False)
assert_type(result, bytes)

result = client.raw_query("", stream=True)
assert_type(result, io.IOBase)

stream = cast(bool, None)
result = client.raw_query("", stream=stream)
assert_type(result, Union[bytes, io.IOBase])
