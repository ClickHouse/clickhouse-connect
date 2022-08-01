## ClickHouse Connect ChangeLog

### Release 0.1.7, 2022-07-28

#### Improvements

* Support (experimental) JSON/Object datatype.  ClickHouse Connect will take advantage of the fast orjson library if available.
* Standardize read format handling and allow setting a return data format per column or per query.

#### Bug Fixes
* Named Tuples were not supported and would result in throwing an exception.  This has been fixed.
* The client query_arrow function would return incomplete results if the query result exceeded the ClickHouse max_block_size.  This has been fixed.  As part of the fix query_arrow method returns a PyArrow Table object.  While this is a breaking change in the API it should be easy to work around.


### Release 0.1.6, 2022-07-06

#### Improvements

* Support Nested data types.

#### Bug Fixes

* Fix issue with native reads of Nullable(LowCardinality) numeric and date types.
* Empty inserts will now just log a debug message instead of throwing an IndexError.