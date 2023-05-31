## ClickHouse Connect

A suite of Python packages for connecting Python to ClickHouse:
* Pandas DataFrames
* Numpy Arrays
* PyArrow Tables
* SQLAlchemy 1.3 and 1.4 (limited feature set)


### Warning -- ZStd errors with versions 0.5.21 and below
Versions prior to 0.5.22 are not compatible with urllib3 version 2+ when using zstd compression.  If you encounter
such errors please upgrade to clickhouse-connect 0.5.22+ or downgrade your urllib3 version to 1.x

### Superset Compatibility
Starting with v0.60.0, clickhouse-connect no longer includes a Superset EngineSpec.  Instead, the relevant EngineSpec
has been moved to the core Apache Superset project as of Superset v2.1.0.  If you have issues connecting to earlier
versions of Superset, please use clickhouse-connect v0.5.25.


### Complete Documentation
The documentation for ClickHouse Connect has moved to
[ClickHouse Docs](https://clickhouse.com/docs/en/integrations/language-clients/python/intro) 


### Installation

```
pip install clickhouse-connect
```

ClickHouse Connect requires Python 3.7 or higher.  
