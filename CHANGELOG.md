## ClickHouse Connect ChangeLog

### Release 0.3.1 2022-10-19

#### Bug Fixes
* UInt64 types were incorrectly returned as signed Python ints even outside of Superset.  This has been fixed
* Superset Engine Spec will now format (U)Int256 and (U)Int128 types as strings to avoid throwing a conversion exception

### Release 0.3.0 2022-10-15

#### Row Binary Removed
The row_binary option for ClickHouse serialization has been removed.  The performance is significantly lower than Native format and maintaining the option added complexity with no corresponding benefit

#### Bug Fixes
* The Database Connection dialog was broken in the latest Superset development builds.  This has been fixed
* IPv6 Addresses fixed for default Superset configuration

### Release 0.2.10 2022-09-28

#### Bug Fix
* Add single retry for HTTP RemoteDisconnected errors from the ClickHouse Server.  This prevents exception spam when requests (in particular inserts) are sent at approximately the same time as the ClickHouse server closes a keep alive connection.

### Release 0.2.9 2022-09-24

#### Bug Fix
* Fix incorrect validation errors in the Superset connection dialog


### Release 0.2.8 2022-09-21

#### Improvements
* This release updates the build process to include binary wheels for the majority of platforms, include MacOS M1 and Linux Aarch64.  This should also fix installation errors on lightweight platforms without build tools.
* Builds are now included for Python 3.11

#### Note on Docker Builds
* Docker images built on MacOS directly from source do not correctly build the C extensions for Linux.  However, installing the official wheels from PyPI should work correctly.

### Release 0.2.7 2022-09-10

#### Improvements
* The HTTP client now raises an OperationalError instead of a DatabaseError when the HTTP status code is 429 (too many requests), 503 (service unavailable), or 504 (gateway timeout) to make it easier to determine if it is a retryable exception
* Add `query_retries` client parameter (default 2) for "retryable" HTTP queries.  Does not apply to "commands" like DDL or to inserts

### Release 0.2.6 2022-09-08

#### Bug Fix
* Fixed an SQLAlchemy dialect issue with SQLAlchemy 1.4 that would cause problems in the most recent Superset version

### Release 0.2.5 2022-08-30

#### Bug Fix
* Fixed an issue where DBAPI cursors returned an invalid description object for columns.  This would cause `'property' object has no attribute 'startswith'` errors for some SqlAlchemy and SuperSet queries.  
* Fixed an issue where datetime parameters would not be correctly rendered as ClickHouse compatible strings

#### Improvement
* The "parameters" object passed to client query methods can now be a sequence instead of a dictionary, for compatibility with query strings that contain simple format unnamed format directives, such as `'SELECT * FROM table WHERE value = %s'`

### Release 0.2.4, 2022-08-19

#### Bug Fix
* The wait_end_of_query parameter/setting was incorrectly being stripped.  This is fixed

### Release 0.2.3, 2022-08-14

#### Bug Fix
* Fix encoding insert of multibyte characters

#### Improvements
* Improve identifier handling/quoting for Clickhouse column, table, and database names
* Add client arrow_insert method to directly insert a PyArrow Table insert ClickHouse using Arrow format


### Release 0.2.2, 2022-08-06

#### Bug Fix
* Fix issue when query_limit set to 0


### Release 0.2.1, 2022-08-04

#### Bug Fix
* Fix SQL comment problems in DBAPI cursor

### Release 0.2.0, 2022-08-04

#### Improvements

* Support (experimental) JSON/Object datatype.  ClickHouse Connect will take advantage of the fast orjson library if available.  Note that inserts for JSON columns require ClickHouse server version 22.6.1 or later
* Standardize read format handling and allow specifying a return data format per column or per query.
* Added convenience min_version method to client to see if the server is at least the requested level
* Increase default HTTP timeout to 300 seconds to match ClickHouse server default

#### Bug Fixes
* Fixed multiple issues with SQL comments that would cause some queries to fail
* Fixed problem with SQLAlchemy literal binds that would cause an error in Superset filters
* Fixed issue with parameterized queries
* Named Tuples were not supported and would result in throwing an exception.  This has been fixed.
* The client query_arrow function would return incomplete results if the query result exceeded the ClickHouse max_block_size.  This has been fixed.  As part of the fix query_arrow method returns a PyArrow Table object.  While this is a breaking change in the API it should be easy to work around.


### Release 0.1.6, 2022-07-06

#### Improvements

* Support Nested data types.

#### Bug Fixes

* Fix issue with native reads of Nullable(LowCardinality) numeric and date types.
* Empty inserts will now just log a debug message instead of throwing an IndexError.