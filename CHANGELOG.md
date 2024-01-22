# ClickHouse Connect ChangeLog

### WARNING -- Python 3.7 EOL
Official support for Python 3.7 ended on June 27, 2023.  As of `clickhouse-connect` v0.7.0, Python 3.7 is no
longer supported and binary wheels are not published for versions 0.7.0 and later 

### WARNING -- Superset Compatibility
ClickHouse Connect has been included as an official Apache Superset database connector starting with release 2.1.0.
However, if you need compatibility with older versions of Superset, you may need clickhouse-connect
v0.5.25, which dynamically loads the EngineSpec from the clickhouse-connect project.

## 0.7.0, 2024-01-22
### Breaking Change
- Python 3.7 builds are no longer part of the wheels deployed to PyPI

### Bug Fix
- Due to a change in default ClickHouse settings, inserts with "named" Tuple types no longer worked with ClickHouse
version 24.1 and later.  This has been fixed.

### Improvements
- Some types of security and other proxies require additional query parameters on any call to ClickHouse server behind
such a proxy.  Because the HTTPClient makes certain initialization queries to ClickHouse before any query parameters
are set, it was difficult or impossible to create a Client successfully.  You can now modify the HTTPClient class level
properties `params` and `valid_transport_settings` before calling `get_client` so that such "special" query parameters will be
included even on initialization queries.  Thanks to [Aleksey Astafiev](https://github.com/aastafiev) for highlighting
the problem and contributing a PR.
- In some cases the user make want to disable urllib3 timeout settings `connect_timeout` and `send_receive_timeout` by
setting them to none.  The same PR from Aleksey Astafiev now allows setting to values to `None`
- Update to Cython 3.0.8


## 0.6.23, 2023-12-15
### Bug Fix
- Add missing Nothing SQLAlchemy datatype, which fixes some edge case Superset queries.
Thanks to [elchyn-cheliabiyeu](https://github.com/elchyn-cheliabiyeu) for the PR!

### Improvement
- Avoid concatenation of empty dataframes during `query_df` due to Pandas future warning.  Thanks to [Dylan Modesitt](https://github.com/DylanModesitt)
for the PR!

## 0.6.22, 2023-12-01
### Improvements
- Fix typo in log message for bad inserts.  Thanks to [Stas](https://github.com/reijnnn) for the fix.
- Allow non ClickHouse Cloud tests to run on community Pull Requests
- Update to Cython 3.0.6

### Bug Fix
- `ATTACH` queries were not be correctly processed as "commands".  Thanks to [Aleksei Palshin](https://github.com/alekseipalshin)
for the PR!


## 0.6.21, 2023-11-23
### Improvements
- Added support for Point type.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/151.  Thanks to
[Dhruvit Maniya](https://github.com/Dhruvit96) for the PR!
- Upgraded to Cython 3.0.5
- Change exception handling in C API to stop spamming stderr

## 0.6.20, 2023-11-09
### Bug Fix
- Fixed an issue where client side binding of datetimes with timezones would produce the incorrect time string if
timezones differed between the client and ClickHouse server.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/268

## 0.6.19, 2023-11-07
### Bug Fixes
- In some circumstances it was possible to insert a `None` value into a non-Nullable String column.  As this could mask
invalid input data, any attempt to insert None into a non-Nullable String or LowCardinality(String) will now throw
a DataError
- Reading a named Tuple column where the Tuple element names contained spaces would fail. In particular this would
cause expected failures reading the experimental JSON column type with spaces in the keys.  This has been fixed.  Closes
https://github.com/ClickHouse/clickhouse-connect/issues/265.  Note that handling spaces in column types is tricky and
fragile in several respects, so the best approach remains to use simple column names without spaces.

## 0.6.18, 2023-10-25
### Bug Fixes
- Reduce the estimated insert block size from 16-32MB to 1-2MB for large inserts.  The large data transfers could cause
"write timeout" errors in the Python code or "empty query" responses from ClickHouse over HTTPS connections.
Should fix https://github.com/ClickHouse/clickhouse-connect/issues/258
- Ensure that the internal client _progress_interval is positive even if a very small `send_receive_timeout` value is specified.
Closes https://github.com/ClickHouse/clickhouse-connect/issues/259.  Note that a very short `send_receive_timeout` is not recommended.

## 0.6.17, 2023-10-21
### Bug Fix
- Fix "negative" Date32 (before 1970-01-01) values for numpy and Pandas queries.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/254

## 0.6.16, 2023-10-18
### Bug Fix
- Remove bad private import to fix C Linkage.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/252

## 0.6.15, 2023-10-16
### Improvement
- Added Python 3.12 wheels and CI tests.  Note that PyArrow is not yet available for 3.12, but should be soon.  See https://github.com/apache/arrow/issues/37880
- The main `clickhouse-connect.get_client` method now displays type hints and ignores non-keyword arguments.  Thanks to
[Avery Fischer](https://github.com/biggerfisch) for the usability improvement!
- Log messages regarding C optimization availability and JSON library selection have been change from INFO to DEBUG.  Closes
https://github.com/ClickHouse/clickhouse-connect/issues/249

## 0.6.14, 2023-09-22
### Bug Fixes
- Fixed insert error when inserting a zero length string into a FixedString column.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/244
- Removed unnecessary validate_entrypoints import from top level package __init__ that was breaking Python 3.7.  Note that Python 3.7 is EOL
and will no longer be supported as of January 1, 2024.

## 0.6.13, 2023-09-20
### Bug Fix
- Fixed an issue with the automatic retry of "connection reset errors".  This should prevent exceptions when the
ClickHouse server closes a Keep Alive connection while a new request is in flight.

### Improvement
- Improved support for typing tools by adding a `py.typed` file.  Thanks to [Avery Fischer](https://github.com/biggerfisch)
for the contribution.

## 0.6.12, 2023-08-30
### Bug Fix
- Nested empty Maps would return an IndexError when queried.  https://github.com/ClickHouse/clickhouse-connect/issues/239.  Thanks
to [Ashton Hudson](https://github.com/CaptainCuddleCube) for the report and the fix

## 0.6.11, 2023-08-30
### Bug fixes
- Inserts using Pandas 2.1 would fail due to a removed method in the Pandas library.  There is now a workaround/fix for
this.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/234
- Inserts into a FixedString column that were not the expected size could cause corrupt insert blocks and mysterious errors
from the ClickHouse server.  Validation has been added so that more meaningful error messages are generated if a fixed string
value is an invalid size.  A reminder that strings which are "too short" for a FixedString column will be padded with 0 bytes, while
strings that are "too long" will generate an exception during the insert.

## 0.6.10, 2023-08-27
### Improvement
- Add support and tests for the `Object(Nullable('json'))` type, which is sometimes detected by schema inference.

## 0.6.9, 2023-08-21
### Improvements
- Logging and exception handling for failed insert transformations has been reworked.  If an exception is thrown when attempting to
convert Python, Pandas, or Numpy data into ClickHouse Native format, the column name and type will be logged, as well as a
stack trace of actual exception (note this may be in the C/Cython code, so the exception data may still be difficult to interpret).
This partially addresses https://github.com/ClickHouse/clickhouse-connect/issues/229.  Unfortunately determining data errors on a row level in
addition to the column level is not practical in most cases without seriously impacting performance.
- Version information has been moved from a top level `VERSION` to a Python `__version__` file in the package.  This removes the Python 3.7 dependency
on importlib_metadata.
- Cython `.pyx`, and `.pxd` files are now included in the PyPI source distribution to improve compatibility with 3rd party build tools.

## 0.6.8, 2023-07-18
### Bug Fix
- Fixed client `raw_insert` method when a compression method specified.  https://github.com/ClickHouse/clickhouse-connect/issues/223

### Improvement
- Add compression parameter to the clickhouse `tools.insert_file` method.  '.gz' and '.gzip' extensions are automatically
recognized.  

## 0.6.7, 2023-07-18
### Bug Fixes
- Fixed an issue for older versions of ClickHouse where the server would send an initial block of 0 rows for larger queries.
This would break some queries with LowCardinality columns.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/221 
- Fixed the`compression` alias for the `compress` client setting in SQLAlchemy/Superset DSN urls.

### Improvements
- Upgraded to Cython 3.0.0 final release!
- Reversed the internal variable names of keys and indexes for low cardinality columns to be consistent with the ClickHouse server nomenclature.

## 0.6.6, 2023-07-07
### Bug Fix
- Inserting into an Enum column from a Pandas DataFrame with integer values only inserted 0 values.  This is fixed.
https://github.com/ClickHouse/clickhouse-connect/issues/219

## 0.6.5, 2023-07-06
### Bug Fixes
- The Client min_version method now ignores unrecognized "text" elements.  This could cause issues for unofficial
ClickHouse releases. Thanks to [Diego Nieto](https://github.com/lesandie) for the fix!
- In most cases insert query is now sent as part of the POST body instead of as a query parameter.  This fixes
https://github.com/ClickHouse/clickhouse-connect/issues/213.  Note that this does not happen for direct file inserts
using the `driver.tools` module, since these rely on an unmodified buffered input stream to efficiently upload files.
In that case the actual insert query will still be passed as a query parameter.
- All datetime objects returned from a query will now be timezone aware.  This fixes https://github.com/ClickHouse/clickhouse-connect/issues/210.
There remains one exception to this -- if the calculated timezone and the local timezone are both UTC, then naive timezones
will be used to improve performance in such "all UTC" environments.
- Inserting Python dictionaries into a ClickHouse "named" Tuple column now works correctly.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/215.
Note that using dictionaries for inserts will be noticeably slower than inserting the equivalent Python tuple value
(with elements in the correct order)

### Improvements
- Client error messages used to be cut off at 240 characters to avoid creating huge log files.  This value is now
configurable using the `common.max_error_size` setting.  Use `0` for this setting to get the full ClickHouse
error message.  In addition, the default has been changed to `1024` to capture more SQL errors without needing to
modify the global setting value.  Thanks to [Ramlah Aziz](https://github.com/RamlahAziz) for the update!
- All Client insert methods now return a simple QuerySummary object, which includes properties `written_rows`,
`written_bytes`, and `query_id` calculated from ClickHouse HTTP response headers.  A QuerySummary object is also
returned from the Client `command` method if the command does not return other data. Closes https://github.com/ClickHouse/clickhouse-connect/issues/216
- Version determination no longer indirectly depends on the setuptools `pkg_resources` package.  This also
avoids some indirect dependency problems.  Thanks to [cwegener](https://github.com/cwegener) for the PR!

## 0.6.4, 2023-06-22
### Bug Fixes
- Quote database name when retrieving tables via SQLAlchemy.  Fixes the Superset issue https://github.com/apache/superset/issues/24372
for recent versions of Superset using clickhouse-connect
- Don't rely on the ClickHouse currentDatabase() function to set an explicit database parameter.  This should not change functionality
when no database is specified in Client creation since ClickHouse will use the user's default database in that situation regardless.
Fixes https://github.com/ClickHouse/clickhouse-connect/issues/207

## 0.6.3, 2023-06-16
### Bug Fix
- Inserts into decimal columns first convert the source value to a Python Decimal to work around floating point
rounding issues.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/203
- DateTime64 values were broken for dates before 01-01-1970.  This is fixed.  https://github.com/ClickHouse/clickhouse-connect/issues/204

## 0.6.2, 2023-06-10
### Improvements
- Cython version upgraded to 3.0.0b3
- Inserts for string columns are now C optimized (approximately 2x faster)

### Bug Fix
- Very long running queries could break because ClickHouse returned too many progress headers.  Thanks to
[Ivan](https://github.com/istrebitel-1) for the fix

## 0.6.1, 2023-06-06
### Improvements
Minor documentation clean up regarding Superset compatibility

## 0.6.0, 2023-06-05
### Bug Fixes
- Use uuid4 instead of uuid1 for generating client level session_ids, as well as use a new urllib3 PoolManager
when multiprocessing mode is detected.  This should fix https://github.com/ClickHouse/clickhouse-connect/issues/194.
Thanks to [Guillaume Matheron](https://github.com/guillaumematheron) for filing the issue and digging into details.
The underlying problem is that the Python uuid1() is not guaranteed to be unique in a `forked` multiprocessing environment.
- Change log warning to debug message if numpy is not available for C bindings.  This check is harmless if numpy
is not installed and should not have produced a warning.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/195

### Improvements
- Cython version upgraded to 3.0.0b2
- The block size (number of rows) for chunked/streaming inserts is now dynamically determined based on sample of
the insert data.  This allows more efficient streaming of large inserts and significantly improves insert performance
in some circumstances.
- Pivoting row based data to native columns for inserts has been optimized in C.  This improves insert performance
for large inserts of row oriented data.

## 0.5.25, 2023-05-23
### Bug Fix
- The client will now validate that the `client_protocol_version` query parameter is actually received and used by the ClickHouse
server before assuming that data returned confirms to the expected protocol version.  This fixes an incompatibility with the
current versions of CHProxy (and possibly other proxies that restrict the query parameters passed to the ClickHouse Server).
Note that other features that require the use of query parameters (such as server side bound query parameters) may also fail
because of this behavior in CHProxy.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/191

## 0.5.24, 2023-05-11
### Bug Fixes
- The client `command` method now accepts ClickHouse "external data."  Closes https://github.com/ClickHouse/clickhouse-connect/issues/186
- Arrays of Python date and datetime objects are now correctly formatted when use as server side parameters.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/188
- Fixed inserts of SimpleAggregateFunction columns with a LowCardinality type parameter.  https://github.com/ClickHouse/clickhouse-connect/issues/187

## 0.5.23, 2023-05-03
### Bug Fixes
- SQLAlchemy table reflection threw an exception for `SimpleAggregateFunction` columns.  This has been fixed.
https://github.com/ClickHouse/clickhouse-connect/issues/180
- The client no longer logs an invalid warning for query types that did not return a timezone header.
https://github.com/ClickHouse/clickhouse-connect/issues/181
- Querying `SimpleAggregateFunction` columns with a LowCardinality type parameter was broken.  This has been fixed.
https://github.com/ClickHouse/clickhouse-connect/issues/182
- The `query_arrow` method now correctly accepts the external_data parameter.  https://github.com/ClickHouse/clickhouse-connect/issues/183
- The `query_arrow` method has been fixed for read only queries/settings.  https://github.com/ClickHouse/clickhouse-connect/issues/184

### New Feature
- A common setting `max_connection_age` has been added, which will ensure that HTTP connections are not reused forever (this
can help with certain load balancing issues.  It defaults to 10 minutes

## 0.5.22, 2023-04-27
### Bug Fix
- There was a critical issue when using zstd compression (the default) with urllib3 version 2.0+.  This has been fixed.

## 0.5.21, 2023-04-26
### Bug Fix
- Logging "Unexpected Http Driver Exception" only as WARNING instead of ERROR. Use the raised OperationalError if you depend on this.  Thanks to
[Alexandro Sandre](https://github.com/alexandrosandre) for the fix.
- The `wait_end_of_query` setting is no longer automatically sent with inserts.  This caused unnecessary buffering on the ClickHouse server file system, especially
in the case of many small inserts.  It can still be added using the `settings` dictionary of the client `*insert` methods if needed for some reason.
- The query setting `use_na_values` has been renamed to `use_extended_dtypes` and now applies to all extended/special Pandas dtypes (except the Pandas Timestamp type).
Set this to `False` to limit  the dtypes returned in Pandas dataframes to the "basic" numpy types.  (Note that this will force the use of numpy object arrays
for most "nullable types")  This should allow creating "basic" dataframes for greater compatibility. Closes https://github.com/ClickHouse/clickhouse-connect/issues/172.  

## 0.5.20, 2023-04-06
### Bug Fixes
- Fix Pandas dataframe inserts where the Dataframe index does not match the data values (after, for example, creating a new DataFrame from
a subset of the original.)   https://github.com/ClickHouse/clickhouse-connect/issues/167  Thanks to [Georgi Peev](https://github.com/georgipeev) for
the report and suggested fix, and his continued stress testing of Pandas functionality.
- Compression and other control settings were not properly sent with the request if the corresponding setting was not enabled on the server.
Many thanks to [Alexander Khmelevskiy](https://github.com/khmelevskiy) for the extended investigation and subsequent fix.  https://github.com/ClickHouse/clickhouse-connect/issues/157


## 0.5.19, 2023-04-05
### Bug Fixes
- Fix quoting and escaping of array literals in server parameters.  See [#159](https://github.com/ClickHouse/clickhouse-connect/issues/159).  Big thanks to
[Joachim Jablon](https://github.com/ewjoachim) for the report and the fix.
- Pandas and numpy Date values were incorrect for values after 2050.  This has been fixed.  https://github.com/ClickHouse/clickhouse-connect/issues/164
- Fixed server side parameter binding of the NULL value for Nullable types
- Added support for `no_proxy`/`NO_PROXY` environment variable.  Also added support for lower case `http_proxy` and `https_proxy` variables.  Note that
lower case versions have precedence over upper case versions.  Fixes https://github.com/ClickHouse/clickhouse-connect/issues/163

## 0.5.18, 2023-03-30
### Performance Improvement
- The server timezone will not be applied (and Python datetime types will be timezone naive) if the client and server timezones match
and the `get_client` apply_server_timezone parameter is True (the default).  This improves performance where client and server
have the same (non-UTC) timezone.  To override this behavior and always apply a server timezone to the result, use `apply_server_timezone='always'`.
This should fix https://github.com/ClickHouse/clickhouse-connect/issues/157


## 0.5.17, 2023-03-26
### Timezone Improvements
- The client `query_df` and `query_df_stream` methods now accept `query_tz` and `column_tzs` parameters like other
`query*` methods.
- A new boolean parameter `apply_server_timezone` has been added to the main `get_client` method.  Setting this
parameter to `True` (the default) will apply the server timezone (if not UTC) to values returned by the client `query*`
methods.  The previous behavior would always return timezone naive, UTC based Python and Pandas `datetime` objects for
ClickHouse DateTime and DateTime64 columns without a defined timezone.  To revert to the previous behavior, set the
`apply_server_timezone` parameter to `False`.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/152
- The timezone logic applied to query results has been simplified and now uses the following order of precedence:
  - Use the column timezone for the column if it is specified using the `column_tzs` parameter
  - Use the column timezone for the column if specified in the ClickHouse column definition (only works for ClickHouse versions 23.2 and later)
  - Use the query timezone for the query if it is set using the `query_tz` parameter
  - Use the "response" timezone for the query as read from the `X-ClickHouse-Timezone` header if different from the server timezone.  This closes https://github.com/ClickHouse/clickhouse-connect/issues/138.
  - Use the ClickHouse server timezone (if the client parameter `apply_server_timezone` is `True`)
- Note if the detected timezone according to the above precedence is UTC, `clickhouse-connect` will always return a naive datetime object with no timezone information

### New Feature
- ClickHouse external data is now support for all client `query` methods.  To send external data, construct a `driver.external.ExternalData` object and
send it as the `external_data` parameter in the appropriate query method.  See the [ClickHouse documentation](https://clickhouse.com/docs/en/engines/table-engines/special/external-data)
for additional details.  There are also examples in the [test file ](https://github.com/ClickHouse/clickhouse-connect/blob/main/tests/integration_tests/test_external_data.py).
Closes https://github.com/ClickHouse/clickhouse-connect/issues/98


## 0.5.16, 2023-03-15
### Bug Fix
- Creating a client would fail if for some reason the user did not have access to the `system.settings` table.  Thanks
to [Filipp Balakin](https://github.com/Barsoomx) for the fix.

### Improvements
- String columns now accept values of bytes-like objects (bytes/bytearray/etc.) for inserts (as with other inserts, all
values for the inserted column should be the same types, either a bytes-like object or `str`).  A corresponding `bytes`
read format has been enabled for String columns as well.  Thanks to [Tim Nooran](https://github.com/TimNooren) for opening
the issue and providing unit tests.  https://github.com/ClickHouse/clickhouse-connect/issues/148
- Cython version upgraded to 3.0.0b1


## 0.5.15, 2023-03-10
### Bug Fix
- Remove unnecessary addition of the client database to the table name for inserts. Fixes
https://github.com/ClickHouse/clickhouse-connect/issues/145

### Improvement
- The driver should now work for older versions of ClickHouse back to 19.16.  Note that older versions are not
officially tested or supported (like the main ClickHouse database, we officially support the last three monthly ClickHouse
releases and the last two LTS ClickHouse releases).  For versions prior to 19.17, you may want change the new `readonly`
`clickhouse_connect.common` setting to '1' to allow sending ClickHouse settings with individual queries (if the user has
write permissions).  Thanks to [Aleksey Astafiev](https://github.com/aastafiev) for this contribution and for
updating the tests to run with these legacy versions!


## 0.5.14, 2023-03-02
### Bug Fix
- Remove direct pandas import that caused an unrecoverable error when pandas was not installed.
https://github.com/ClickHouse/clickhouse-connect/issues/139


## 0.5.13, 2023-02-27

### Improvements
- By default, reading Pandas Dataframes with query_df and query_df_stream now sets a new QueryContext property
of `use_pandas_na` to `True`.  When `use_pandas_na` is True, clickhouse_connect will attempt to use Pandas "missing"
values, such as pandas.NaT and pandas.NA, for ClickHouse NULLs (in Nullable columns only), and use the associated
extended Pandas dtype.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/132
- There are new low level optimizations for reading some Nullable columns, and writing Pandas dataframes

### Bug Fixes
- Timezone information from ClickHouse DateTime columns with a timezone was lost.  There was a workaround implemented
for this issue in v0.5.8 that allowed assigned timezones to the query or columns on the client side.  ClickHouse now
support sending this timezone data with the column, but only in server versions 23.2 and later.  If such a version is
detected, clickhouse-connect will return timezone aware DateTime values without a workaround.  Fixes
https://github.com/ClickHouse/clickhouse-connect/issues/120
- For certain queries, an incorrect, non-zero "zero value" would be returned for queries where `use_none` was set
to `False`.  All NULL values are now properly converted.
- Timezone data was lost when a DateTime64 column with a timezone was converted to a Pandas DataFrame.  This has been
fixed.  https://github.com/ClickHouse/clickhouse-connect/issues/136
- send_progress headers were not being correctly requested, which could result in unexpected timeouts for long-running
queries.  This has been fixed.


## 0.5.12, 2023-02-16
### Improvement
- A new keyword parameter `server_host_name` is now recognized by the `clickhouse_connect.get_client` method.  This identifies
the "real" ClickHouse server hostname that should be used for HTTPS/TLS certificate validation, in cases where access to
the server is through an ssh tunnel or other proxy with a different hostname.  For examples of how to use the new parameter,
see the updated file https://github.com/ClickHouse/clickhouse-connect/blob/main/examples/ssh_tunnels.py.

### Bug fix
- The `database` element of a DSN was not recognized when present in the `dsn` parameter of `clickhouse_connect.get_client`.
This has been fixed.


## 0.5.11, 2023-02-15

### Bug Fix
- Referencing the QueryResult `named_results` property after other properties such as `row_count` would incorrectly
raise a StreamClosedError.  Thanks to [Stas](https://github.com/reijnnn) for the fix.

### Improvement
- A better error message is returned when trying to read a "non-standard" DateTime64 column function for a numpy array
or Pandas DataFrame.  "non-standard" means a DateTime64 precision not conforming to seconds, milliseconds, microseconds,
or nanoseconds (0, 3, 6, or 9 respectively).  These DateTime64 types are not supported for numpy or Pandas because there is
no corresponding standard numpy datetime64 type and conversion would be unacceptably slow (supported numpy types are 
`datetime64[s]`, `datetime64[ms]`, `datetime64[us]`, and `datetime64[ns]`).  A workaround is to cast the DateTime64 type
to a supported type, i.e. `SELECT toDateTime64(col_name, 3)` for a millisecond column.
- The base configuration required for a urllib PoolManager has been broken out into its own help method,
`clickhouse_connect.driver.http_util.get_pool_manager_options`.  This makes it simpler to configure a SOCKSProxyManager
as in the new example file https://github.com/ClickHouse/clickhouse-connect/blob/main/examples/ssh_tunnels.py


## 0.5.10, 2023-02-13

### Improvement
- Reading Nullable(String) columns has been optimized and should be approximately 2x faster.  (This does yet not include
LowCardinality(Nullable(String)) columns.)
- Extraction of ClickHouse error messages included in the HTTP Response has been improved

### Bug Fixes
- When reading native Python integer columns, the `use_none=False` query parameter would not be respected,
and ClickHouse NULLS would be returned as None instead of 0.  `use_none=False` should now work correctly for
Nullable(*Int*) columns
- Starting with release 0.5.0, HTTP Connection pools were not always cleanly closed on exit.  This has been fixed.


## 0.5.9, 2023-02-11

### Bug Fixes
- Large query results using `zstd` compression incorrectly buffered all incoming data at the start of the query,
consuming an excessive amount of memory. This has been fixed. https://github.com/ClickHouse/clickhouse-connect/issues/122
Big thanks to [Denny Crane](https://github.com/den-crane) for his detailed investigation of the problem.  Note that
this affected large queries using the default `compress=True` client setting, as ClickHouse would prefer `zstd` compression
in those cases.
- Fixed an issue where a small query_limit would break client initialization due to an incomplete read of the `system.settings`
table.  https://github.com/ClickHouse/clickhouse-connect/issues/123

### Improvement
- Stream error handling has been improved so exceptions thrown while consuming a stream should be correctly propagated.
This includes unexpected stream closures by the ClickHouse server.  Errors inserted into the HTTP response by ClickHouse
during a query should also be reported as part of a StreamFailureError

## 0.5.8, 2023-02-10

### Bug Fix
- Return empty dataframe instead of empty list when no records returned from `query_df` method  Fixes
https://github.com/ClickHouse/clickhouse-connect/issues/118

### Default parameter change
- The client `query_limit` now defaults to 0 (unlimited rows returned), since the previous default of 5000 was unintuitive
and led to confusion when limited results were returned.

### New Feature
- Allow client side control of datetime.datetime timezones for query results.  The client `query` methods for native
Python results now accept two new parameters: `query_tz` is the timezone to be assigned for any DateTime or DateTime64
objects in the results, while timezones can be set per column using the `column_tzs` dictionary of column names to
timezones.  See the [test file](https://github.com/ClickHouse/clickhouse-connect/blob/main/tests/integration_tests/test_timezones.py)
for simple examples.  This is a workaround for https://github.com/ClickHouse/clickhouse-connect/issues/120 and the
underlying ClickHouse issue https://github.com/ClickHouse/ClickHouse/issues/40397  Note that this issue only affects DateTime
columns, not DateTime64, although the query context parameters will override the returned DateTime64 timezone as well.

## 0.5.7, 2023-02-01

### Bug Fix
- Http proxies did not work after removing the requests library. https://github.com/ClickHouse/clickhouse-connect/issues/114.
This should be fixed.  Note that socks proxies are still not supported directly, but can be added by creating a correctly
configured urllib3 SOCKSProxyManager and using it as the `pool_mgr` argument to teh `clickhouse_connect.create_client` method.


## 0.5.6, 2023-02-01

### Bug Fix
- Dataframe inserts would incorrectly modify null-like elements of the inserted dataframe.  https://github.com/ClickHouse/clickhouse-connect/issues/112.
This should be fixed

## 0.5.5, 2023-02-01

### Bug Fix
- Queries of LowCardinality columns using pandas or numpy query methods would result in an exception.  https://github.com/ClickHouse/clickhouse-connect/issues/108
This has been fixed.


## 0.5.4, 2023-01-31

### New Features
* Several streaming query methods have been added to the core ClickHouse Connect client.  Each of these methods returns a StreamContext object, which must be used as a Python `with` Context to stream data (this ensures the underlying
streaming response is properly closed/consumed.)  For simple examples, see the basic [tests](https://github.com/ClickHouse/clickhouse-connect/blob/main/tests/integration_tests/test_streaming.py).
  * `query_column_block_stream` -- returns a generator of blocks in column oriented (Native) format.  Fastest method for retrieving data in native Python format
  * `query_row_block_stream` -- returns a generator of blocks in row oriented format.  Used for processing data in a "batch" of rows at time while limiting memory usage
  * `query_rows_stream` -- returns a convenience generator to process rows one at a time (data is still loaded in ClickHouse blocks to preserve memory)
  * `query_np_stream` -- returns a generator where each ClickHouse data block is transformed into a Numpy array
  * `query_df_stream` -- returns a generator where each ClickHouse data block is transformed into a Pandas Dataframe
* The `client_name` is now reported in a standardized way to ClickHouse (as the `http_user_agent`).  For better tracking of your
Python application, use the new `product_name` common setting or set `client_name` `get_client` parameter to identify your product
as `<your-product-name>/<product-version>`.

### Performance Improvements
* C/Cython optimizations for transforming ClickHouse data to Python types have been improved, and additional datatypes have been
optimized in Cython.  The performance increase over the previous 0.5.x version is approximately 10% for "normal" read queries.
* Transformation of Numpy arrays and Pandas Dataframes has been completely rewritten to avoid an intermediate conversion to
Python types.  As a result, querying in Numpy format, and especially Pandas format, has been **significantly** improved -- from 2x
for small datasets to 5x or more for very large Pandas DataFrames (even without streaming).  Queries including Numpy datetime64 or
Pandas Timestamp objects have particularly benefited from the new implementation.

### Bug Fixes
* The default `maxsize` for concurrent HTTP connections to a single host was accidentally dropped in the 0.5.x release.  It
has been restored to 8 for better performance when using multiple client objects.
* A single low level retry has been restored for HTTP connections on ConnectionReset or RemoteDisconnected exceptions.  This
should reduce connection errors related to ClickHouse closing expired KeepAlive connections.

### Internal Changes
* As noted above, streaming, contexts and exception handling have been tightened up to avoid leaving HTTP responses open
when querying streams.
* Previous versions used `threading.local()` variables to store context information during query processing.  The architecture
has been changed to pass the relevant Query or Insert Context to transformation methods instead of relying on thread local
variables.  This is significantly safer in an environment where multiple queries can conceivably be open at the same on the
same thread (for example, if using async functions).
* Per query formatting logic has moved from `ClickHouseType` to the `QueryContext`.
* `ClickHouseType` methods have been renamed to remove outdated references to `native` format (everything is native now)
* Upgraded Cython Build to 3.0.11alpha release

## 0.5.3, 2023-01-23

### Bug Fix
* Correctly return QueryResult object when created as a context using a `with` statement.  This fixes examples and
the preferred context syntax for processing query results.  Thanks to [John McCann Cunniff Jr](https://github.com/wabscale)

## 0.5.2, 2023-01-17

### Bug fix
* Fix issue where client database is set to None (this normally only happens when deleting the initial database)

## 0.5.1, 2023-01-16

### Bug fix
* Fix ping check in http client.  Closes https://github.com/ClickHouse/clickhouse-connect/issues/96.

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
