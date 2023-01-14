# ClickHouse Connect ChangeLog

## 0.5.0, 2023-01-14

### WARNING -- Breaking Change -- Removing get_client Arbitrary Keyword Arguments
The clickhouse_connect `get_client` method (which proxies the driver.Client constructor) previously accepted arbitrary
keyword arguments that were interpreted as ClickHouse server settings sent with every request.  To be consistent with
other client methods, `get_client` now accepts an optional `settings` Dict[str, Any] argument that should be used instead
to set ClickHouse server settings.

### WARNING -- Breaking Change -- HttpClient argument http_adapter replaced with pool_mgr
The driver.HttpClient constructor previously accepted the optional keyword argument `http_adapter`, which could be used to
pass a custom `requests.adapter.HttpAdapter` to the client.  ClickHouse Connect no longer uses the `requests` library (see
Dependency Changes below).  Instead, the HttpClient constructor now accepts an optional `pool_mgr` keyword argument which
can be used to set a custom `urllib.poolmanager.PoolManager` for the client.  In most cases the default PoolManager is
all that is needed, but multiple PoolManagers may be required for advanced server/proxy applications with many client instances.

### Dependency Changes 
* ClickHouse Connect no longer requires the popular `requests` library.  The `requests` library is built on
[urllib3](https://pypi.org/project/urllib3/), but ClickHouse Connect was utilizing very little of the added functionality.
Requests also has very restricted access to the `urllib3` streaming API, which made adding additional compression methods
difficult.  Accordingly, the project now interfacdes to `urllib3` directly.  This should not change the public API (except as
noted in the warning above), but the HttpClient internals have changed to use the lower level library.
* ClickHouse Connect now requires the [zstandard](https://pypi.org/project/zstandard/) and [lz4](https://pypi.org/project/lz4/)
binding libraries to support zstd and lz4 compression.  ClickHouse itself uses these compression algorithms extensively and
is optimized to work with them, so ClickHouse Connect now takes advantages of them when compression is desired.

### New Features
* The core client `query` method now supports streaming.  The returned `QueryResult` object has new streaming methods:
  * `stream_column_blocks` - returns a generator of smaller result sets matching the ClickHouse blocks returned by the native interface.
  * `stream_row_blocks` - returns a generator of smaller result sets matching the ClickHouse blocks returned by the native interface,
but "pivoted" to return data rows.
  * `stream_rows` - returns a generator that returns a row of data with each iteration.  
These methods should be used within a `with` context to ensure the stream is properly closed when done.  In addition, two new properties
`result_columns` and `result_rows` have been added to `QueryResult`.  Referencing either of these properties will consume the stream
and return the full dataset.  Note that these properties should be used instead of the ambiguous `result_set`, which returns
the data oriented based on the `column_oriented` boolean property.  With the addition of `result_rows` and `result_columns` the
`result_set` property and the `column_oriented` property are unnecessary and may be removed in a future release.
* More compression methods.  As noted above, ClickHouse Connect now supports `zstd` and `lz4` compression, as well as brotli (`br`),
if the brotli library is installed.  If the client `compress` method is set to `True` (the default), ClickHouse Connect will request compression
from the ClickHouse server in the order `lz4,zstd,br,gzip,deflate`, and will compress inserts to ClickHouse using `lz4`.  Otherwise,
the client `compress` argument can be set to any of `lz4`, `zstd`, `br`, or `gzip`, and the specific compression method will be
used for both queries and inserts.  While `gzip` is available, it doesn't perform as well as the other options and should normally not
be used.

### Performance Improvements
* More data conversions for query data have been ported to optimized C/Cython code.  Rough benchmarks suggest that this improves
query performance approximately 20% for standard data types.
* Using the new streaming API to process data in blocks significantly improves performance for large datasets (largely because Python has to
allocate significantly less memory and do much less internal data copying otherwise required to build and hold the full dataset).  For datasets
of a million rows or more, streaming can improve query performance 2x or more.

### Bug Fixes
* As mentioned, ClickHouse `gzip` performance is poor compared to `lz4` and `zstd`.  Using those compression methods by default
avoids the major performance degradation seen in https://github.com/ClickHouse/clickhouse-connect/issues/89.
* Passing SqlAlchemy query parameters to the driver.Client constructor was broken by changes in release 0.4.8.
https://github.com/ClickHouse/clickhouse-connect/issues/94. This has been fixed.

## 0.4.8, 2023-01-02
### New Features
* [Documentation](https://clickhouse.com/docs/en/integrations/language-clients/python/intro) has been expanded to cover recent updates.
* File upload support.  The new `driver.tools` module adds the function `insert_file` to simplify
directly inserting data files into a table.  See the [test file](https://github.com/ClickHouse/clickhouse-connect/blob/main/tests/integration_tests/test_tools.py) 
for examples.  This closes https://github.com/ClickHouse/clickhouse-connect/issues/41.
* Added support for server side [http query parameters](https://clickhouse.com/docs/en/interfaces/http/#cli-queries-with-parameters) 
For queries that contain bindings of the form `{<name>:<datatype>}`, the client will automatically convert the query* method
`parameters` dictionary to the appropriate http query parameters.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/49.
* The main `clickhouse_connect.get_client` command will now accept a standard Python `dsn` argument and extract host, port,
user, password, and settings (query parameters) from the dsn.  Note that values for other keyword parameters will take
precedence over values extracted from the dsn.
* The QueryResult object now contains convenience properties for the `first_item`, `first_row`, and `row_count` in the result.

## 0.4.7, 2022-12-05

### Bug Fixes
* JSON inserts with the ujson failed, this has been fixed.  https://github.com/ClickHouse/clickhouse-connect/issues/84

### New Features
* The JSON/Object datatype now supports writes using JSON strings as well as Python native types

## 0.4.6, 2022-11-29

### Bug Fixes
* Fixed a major settings issue with connecting to a readonly database (introduced in v0.4.4)
* Fix for broken database setup dialog with recent Superset versions using SQLAlchemy 1.4

## 0.4.5, 2022-11-24

### Bug Fixes
* Common settings were stored in an immutable named tuple and could not be changed.  This is fixed.
* Fixed issue where the query_arrow method would not use the client database

## 0.4.4, 2022-11-22

### Bug Fixes
* Ignore all "transport settings" when validating settings.  This should fix https://github.com/ClickHouse/clickhouse-connect/issues/80 
for older ClickHouse versions


## 0.4.3, 2022-11-22

### New Features
* The get_client method now accepts a http_adapter parameter to allow sharing a requests.HTTPAdapter (and its associated
connection pool) across multiple clients.
* The VERSION file is now included in every package installation.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/76

## 0.4.2, 2022-11-22

### New Features
* Global/common configuration options are now available in the `clickhouse_connect.common` module.  The available settings are:
  * `autogenerate_session_id`  [bool]  Whether to generate a UUID1 session id used for every client request.  Defaults to True. Disabling this can facilitate client sharing and load balancing in some use cases.
  * `dict_parameter_format` [str]  Options are 'json' and 'map'.  This controls whether parameterized queries convert a Python dictionary to JSON or ClickHouse Map syntax.  Default to `json` for insert into Object('json') columns.
  * `invalid_setting_action` [str]  Options are 'send' and 'drop'.  Client Connect normally validates and drops (with a warning any settings that aren't recognized by the Server or are readonly).
Changing this setting to 'send' will include such settings with the request anyway -- which will normally result in an error being returned.
* The `clickhouse_connect.get_client` method now accepts a `settings` dictionary argument for consistency with other client methods.

### Bug Fixes
* Fixed insert of Pandas Dataframes for Timestamp columns with timezones  https://github.com/ClickHouse/clickhouse-connect/issues/77
* Fixed exception when inserting a Pandas Dataframes with NaType values into ClickHouse Float column (see known issue)

### Known Issue
When inserting Pandas DataFrame values into a ClickHouse `Nullable(Float*)` column, a Float NaN value will be converted to a ClickHouse NULL.
This is a side effect of a Pandas issue where `df.replace` cannot distinguish between NaT and NaN values:  https://github.com/pandas-dev/pandas/issues/29024

## 0.4.1, 2022-11-14

### Bug Fixes
* Numpy array read and write compatibility has been refined and performance has been improved.  This fixes https://github.com/ClickHouse/clickhouse-connect/issues/69
* Pandas Timestamp objects are now correctly handled for all supported ClickHouse Date* types.  This fixes https://github.com/ClickHouse/clickhouse-connect/issues/68
* SQLAlchemy datatypes are now correctly mapped to the underlying ClickHouse type regardless of case.  This fixes an issue with migrating Superset datasets and queries from
clickhouse-sqlalchemy to clickhouse-connect.  Thanks to [Eugene Torap](https://github.com/EugeneTorap)


## 0.4.0, 2022-11-07

### New Features
* The settings, table information, and insert progress used for client inserts has been centralized in a new reusable InsertContext object.  Client insert methods can now accept such objects to simplify code and reduce overhead
* Query results can now be returned in a column oriented format.  This is useful to efficiently construct other objects (like Pandas dataframes) that use column storage internally
* The transformation of Pandas data to Python types now bypasses Numpy.  As a result compatibility for ClickHouse date, integer, and NULL types has been significantly improved

### Bug Fixes
* An insert using chunked transfer encode could fail in progress during serialization to ClickHouse native format.  This would "hang" the request after throwing the exception, leading to ClickHouse reporting
"concurrent session" errors.  This has been fixed.
* Pandas DataFrame inserts into tables with a "large" integer column would throw an exception.  This has been fixed.
* Pandas DataFrame inserts with NaT/NA/nan values would fail, even if inserted into Nullable column types.  This has been fixed.

### Known Issues
* Numpy inserts into large integer columns are not supported.  https://github.com/ClickHouse/clickhouse-connect/issues/69
* Insert of Pandas timestamps with nanosecond precision will lose the nanosecond value.  https://github.com/ClickHouse/clickhouse-connect/issues/68


## 0.3.8, 2022-11-03

### Bug Fixes
* Fix read compression typo


## 0.3.7, 2022-11-03

### New Features
* Insert performance and memory usage for large inserts has been significantly improved
  * Insert blocks now use chunked transfer encoding (by sending a generator instead of a bytearray to the requests POST method)
  * If the client is initialized with compress = True, gzip compression is now enabled for inserts
* Pandas DataFrame inserts have been optimized by keep the data in columnar format during the entire insert process

### Bug Fixes
* Fix inserts for date and datetime columns from Pandas dataframes.
* Fix serialization issues for Decimal128 and Decimal256 types



## 0.3.6, 2022-11-02

### Bug Fixes
* Update QueryContext.updated_copy method to preserve settings, parameters, etc.  https://github.com/ClickHouse/clickhouse-connect/issues/65



## 0.3.5, 2022-10-28

### New Features
* Build Python 3.11 Wheels


## 0.3.4, 2022-10-26

### Bug fixes
* Correctly handle insert into JSON/Object('json') column via SQLAlchemy
* Fix some incompatibilities with SQLAlchemy 1.4


## 0.3.3, 2022-10-21

### Bug fix
* Fix 'SHOW CREATE' issue.  https://github.com/ClickHouse/clickhouse-connect/issues/61


## 0.3.2, 2022-10-20

### Bug fix
* "Queries" that do not return data results (like DDL and SET queries) are now automatically treated as commands.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/59

### New Features
* A UUID session_id is now generated by default if `session_id` is not specified in `clickhouse_connect.get_client`
* Test infrastructure has been simplified and test configuration has moved from pytest options to environment files

## 0.3.1, 2022-10-19

### Bug Fixes
* UInt64 types were incorrectly returned as signed Python ints even outside of Superset.  This has been fixed
* Superset Engine Spec will now format (U)Int256 and (U)Int128 types as strings to avoid throwing a conversion exception

## 0.3.0, 2022-10-15

### Breaking changes
* The row_binary option for ClickHouse serialization has been removed.  The performance is significantly lower than Native format and maintaining the option added complexity with no corresponding benefit

### Bug Fixes
* The Database Connection dialog was broken in the latest Superset development builds.  This has been fixed
* IPv6 Addresses fixed for default Superset configuration

## 0.2.10, 2022-09-28

### Bug Fixes
* Add single retry for HTTP RemoteDisconnected errors from the ClickHouse Server.  This prevents exception spam when requests (in particular inserts) are sent at approximately the same time as the ClickHouse server closes a keep alive connection.

## 0.2.9, 2022-09-24

### Bug Fixes
* Fix incorrect validation errors in the Superset connection dialog


## 0.2.8, 2022-09-21

### New Features
* This release updates the build process to include binary wheels for the majority of platforms, include MacOS M1 and Linux Aarch64.  This should also fix installation errors on lightweight platforms without build tools.
* Builds are now included for Python 3.11

### Known issues
* Docker images built on MacOS directly from source do not correctly build the C extensions for Linux.  However, installing the official wheels from PyPI should work correctly.

## 0.2.7, 2022-09-10

### New Features
* The HTTP client now raises an OperationalError instead of a DatabaseError when the HTTP status code is 429 (too many requests), 503 (service unavailable), or 504 (gateway timeout) to make it easier to determine if it is a retryable exception
* Add `query_retries` client parameter (default 2) for "retryable" HTTP queries.  Does not apply to "commands" like DDL or to inserts

## 0.2.6, 2022-09-08

### Bug Fixes
* Fixed an SQLAlchemy dialect issue with SQLAlchemy 1.4 that would cause problems in the most recent Superset version

## 0.2.5, 2022-08-30

### Bug Fixes
* Fixed an issue where DBAPI cursors returned an invalid description object for columns.  This would cause `'property' object has no attribute 'startswith'` errors for some SqlAlchemy and SuperSet queries.  
* Fixed an issue where datetime parameters would not be correctly rendered as ClickHouse compatible strings

### New Features
* The "parameters" object passed to client query methods can now be a sequence instead of a dictionary, for compatibility with query strings that contain simple format unnamed format directives, such as `'SELECT * FROM table WHERE value = %s'`

## 0.2.4, 2022-08-19

### Bug Fixes
* The wait_end_of_query parameter/setting was incorrectly being stripped.  This is fixed

## 0.2.3, 2022-08-14

### Bug Fixes
* Fix encoding insert of multibyte characters

### New Features
* Improve identifier handling/quoting for Clickhouse column, table, and database names
* Add client arrow_insert method to directly insert a PyArrow Table insert ClickHouse using Arrow format


## 0.2.2, 2022-08-06

### Bug Fixes
* Fix issue when query_limit set to 0


## 0.2.1, 2022-08-04

### Bug Fixes
* Fix SQL comment problems in DBAPI cursor

## 0.2.0, 2022-08-04

### New Features

* Support (experimental) JSON/Object datatype.  ClickHouse Connect will take advantage of the fast orjson library if available.  Note that inserts for JSON columns require ClickHouse server version 22.6.1 or later
* Standardize read format handling and allow specifying a return data format per column or per query.
* Added convenience min_version method to client to see if the server is at least the requested level
* Increase default HTTP timeout to 300 seconds to match ClickHouse server default

### Bug Fixes
* Fixed multiple issues with SQL comments that would cause some queries to fail
* Fixed problem with SQLAlchemy literal binds that would cause an error in Superset filters
* Fixed issue with parameterized queries
* Named Tuples were not supported and would result in throwing an exception.  This has been fixed.
* The client query_arrow function would return incomplete results if the query result exceeded the ClickHouse max_block_size.  This has been fixed.  As part of the fix query_arrow method returns a PyArrow Table object.  While this is a breaking change in the API it should be easy to work around.


## 0.1.6, 2022-07-06

### New Features

* Support Nested data types.

### Bug Fixes

* Fix issue with native reads of Nullable(LowCardinality) numeric and date types.
* Empty inserts will now just log a debug message instead of throwing an IndexError.
