## ClickHouse Connect ChangeLog

### Release 0.2.1, 2022-08-04

#### Bug Fix
* Fix SQL comment problems in DBAPI cursor

### Release 0.2.0, 2022-08-04

#### Deprecation warning

* In the next release the row_binary option for ClickHouse serialization will be removed.  The performance is significantly lower than Native format and maintaining the option add complexity with no corresponding benefit

#### Improvements

* Support (experimental) JSON/Object datatype.  ClickHouse Connect will take advantage of the fast orjson library if available.  Note that inserts for JSON columns require ClickHouse server version 22.6.1 or later
* Standardize read format handling and allow specifying a return data format per column or per query.
* Added convenience min_version method to client to see if the server is at least the requested level
* Increase default HTTP timeout to 300 seconds to match ClickHouse server default

#### Bug Fixes
* Fixed multiple issues with SQL comments that would cause some queries to fail
* Fixed problem with SQLAlchemy literal binds that would cause an error in Superset filters
* Fixed issue with parameter
* Named Tuples were not supported and would result in throwing an exception.  This has been fixed.
* The client query_arrow function would return incomplete results if the query result exceeded the ClickHouse max_block_size.  This has been fixed.  As part of the fix query_arrow method returns a PyArrow Table object.  While this is a breaking change in the API it should be easy to work around.


### Release 0.1.6, 2022-07-06

#### Improvements

* Support Nested data types.

#### Bug Fixes

* Fix issue with native reads of Nullable(LowCardinality) numeric and date types.
* Empty inserts will now just log a debug message instead of throwing an IndexError.