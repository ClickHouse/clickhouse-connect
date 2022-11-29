# ClickHouse Connect ChangeLog

### Deprecation Warning -- Removing get_client Arbitrary Keyword Arguments 
* The clickhouse_connect `get_client` method (which proxies the driver.Client constructor) currently accepts arbitrary
keyword arguments that are interpreted as ClickHouse server settings sent with every request.  To be consistent with
other client methods, `get_client` now accepts an optional `settings` Dict[str, Any] argument that should be used instead
to set ClickHouse server settings.  The use of `**kwargs` for this purpose is deprecated and will be removed in a future
release.

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
