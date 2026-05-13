# Migrating from clickhouse-connect 0.x to 1.0

This guide covers the breaking changes between the 0.15.x series and 1.0.0. Each section shows the 0.x usage and the equivalent 1.0 replacement.

If you only use the default settings and `clickhouse_connect.get_client(...)` / `get_async_client(...)`, most code keeps working — read the [Python version](#python-version), [pandas version](#pandas-version), and [`pytz` → `zoneinfo`](#pytz--zoneinfo) sections to check your environment.

The 0.15.x series remains available on PyPI for users who cannot upgrade.

---

## Python version

Python 3.9 is no longer supported. The minimum supported version is now Python 3.10.

If you need to stay on Python 3.9, pin to the 0.15.x series:

```text
clickhouse-connect>=0.15,<0.16
```

## pandas version

pandas 1.x is no longer supported. The minimum supported version is now pandas 2.0.

The first time `clickhouse-connect` touches pandas (e.g. `query_df`, `insert_df`, or any other DataFrame-returning method) with pandas < 2.0 installed, a `NotSupportedError` is raised. Non-pandas usage is unaffected — importing `clickhouse_connect` itself does not trigger the check.

```text
pandas>=2.0
```

## `pytz` → `zoneinfo`

`clickhouse-connect` no longer depends on `pytz`. Timezone handling now uses the standard library `zoneinfo` module.

- **Windows**: `tzdata` is pulled in automatically as a dependency.
- **Slim Linux containers** (e.g. `python:3.x-slim`, distroless) without a system tzdb: install the `tzdata` extra.

```bash
pip install clickhouse-connect[tzdata]
```

Unknown timezone strings (from `query_tz`, `column_tzs`, or the server) now raise `zoneinfo.ZoneInfoNotFoundError` internally instead of `pytz.exceptions.UnknownTimeZoneError`. User-visible errors are still surfaced as `ProgrammingError`, but if you were catching the `pytz` exception directly, update the handler:

```python
# Before (0.x)
import pytz
try:
    client.query(..., query_tz="Bogus/Zone")
except pytz.exceptions.UnknownTimeZoneError:
    ...

# After (1.0)
import zoneinfo
try:
    client.query(..., query_tz="Bogus/Zone")
except zoneinfo.ZoneInfoNotFoundError:
    ...
```

## `utc_tz_aware` → `tz_mode`

The deprecated `utc_tz_aware` parameter has been removed. Use `tz_mode` instead.

| 0.x                      | 1.0                   |
| ------------------------ | --------------------- |
| `utc_tz_aware=False`     | `tz_mode="naive_utc"` (default) |
| `utc_tz_aware=True`      | `tz_mode="aware"`     |
| (no equivalent)          | `tz_mode="schema"`    |

```python
# Before (0.x)
client = clickhouse_connect.get_client(host="...", utc_tz_aware=True)

# After (1.0)
client = clickhouse_connect.get_client(host="...", tz_mode="aware")
```

## `apply_server_timezone` → `tz_source`

The deprecated `apply_server_timezone` parameter has been removed. Use `tz_source` instead.

| 0.x                            | 1.0                  |
| ------------------------------ | -------------------- |
| `apply_server_timezone=True`   | `tz_source="server"` |
| `apply_server_timezone=False`  | `tz_source="local"`  |
| (no equivalent)                | `tz_source="auto"` (default) |

```python
# Before (0.x)
client = clickhouse_connect.get_client(host="...", apply_server_timezone=True)

# After (1.0)
client = clickhouse_connect.get_client(host="...", tz_source="server")
```

## `preserve_pandas_datetime_resolution` removed

The `preserve_pandas_datetime_resolution` common setting has been removed. Datetime columns now always return their natural resolution:

- `DateTime` → `datetime64[s]`
- `DateTime64(3)` → `datetime64[ms]`
- `DateTime64(6)` → `datetime64[us]`
- `DateTime64(9)` → `datetime64[ns]`

In 0.x, the default coerced everything to `datetime64[ns]`. If you have downstream code that assumes nanosecond resolution, cast explicitly:

```python
# After (1.0): cast back to ns if needed
df["ts"] = df["ts"].astype("datetime64[ns]")
```

## Async client: executor-based client removed

The legacy executor-based async client has been removed. The native aiohttp-based client is now the only async implementation.

Removed:
- `AsyncClient(client=...)` constructor pattern
- `executor_threads` parameter
- `executor` parameter
- `pool_mgr` parameter on the async path
- `clickhouse_connect.driver.aiohttp_client` module (the class moved to `clickhouse_connect.driver.AsyncClient`)

`aiohttp` remains an optional dependency. Install the `async` extra:

```bash
pip install clickhouse-connect[async]
```

```python
# Before (0.x)
from clickhouse_connect.driver.asyncclient import AsyncClient
sync_client = clickhouse_connect.get_client(host="...")
async_client = AsyncClient(client=sync_client, executor_threads=8)

# After (1.0)
import clickhouse_connect
async_client = await clickhouse_connect.get_async_client(host="...")
```

If you were importing `AiohttpAsyncClient` directly, it has been renamed to `AsyncClient`:

```python
# Before (0.x)
from clickhouse_connect.driver.aiohttp_client import AiohttpAsyncClient

# After (1.0)
from clickhouse_connect.driver import AsyncClient
```
