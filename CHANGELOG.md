## ClickHouse Connect ChangeLog

### Release 0.1.6, 2022-07-06

#### Improvements

* Support Nested data types

#### Bug Fixes

* Fix issue with native reads of Nullable(LowCardinality) numeric and date types
* Empty inserts will now just log a debug message instead of throwing an IndexError