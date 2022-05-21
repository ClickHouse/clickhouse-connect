## ClickHouse Connect

A suite of Python packages for connecting Python to ClickHouse, initially
supporting Apache Superset using a minimal read only SQLAlchemy dialect.  Uses the
ClickHouse HTTP interface.

### Getting Started

Basic query:

```
import clickhouse_connect
client = clickhouse_connect.get_client(host='play.clickhouse.com', port=443, username='play')
query_result = client.query('SELECT * FROM system.tables')
print (query_result.result_set) 
```


Simple 'command' that does not return a result set.

```
import clickhouse_connect
client = clickhouse_connect.get_client()
client.command ('CREATE TABLE test_table (key UInt16, value String) ENGINE Memory')
```

Bulk insert of a matrix of rows and columns.

```
data = [[100, 'value1'], [200, 'value2']]
client.insert('test_table', data)
print(client.query('SELECT * FROM test_table').result_set)
->  [(100, 'value1'), (200, 'value2')]
```

### Minimal SQLAlchemy Support

On installation ClickHouse Connect registers the `clickhousedb` SQLAlchemy Dialect entry point. This
dialect supports basic table reflection for table columns and datatypes, and command and query
execution using DB API 2.0 cursors.  Most ClickHouse datatypes have full query/cursor support.

ClickHouse Connect does not yet implement the full SQLAlchemy API for DDL (Data Definition Language)
or ORM (Object Relational Mapping).  These features are in development.

### Superset Support

On installation ClickHouse Connect registers the `clickhousedb` Superset Database Engine Spec entry
point. Using the `clickhousedb` SQLAlchemy dialect, the engine spec supports complete data exploration
and Superset SQL Lab functionality with all standard ClickHouse data types.  ClickHouse Enum, UUID,
and IP Address datatypes are treated as strings.  For compatibility with Superset Pandas dataframes,
unsigned UInt64 data types are interpreted as signed Int64 values.  ClickHouse CSV Upload via SuperSet 
is not yet implemented.

### Optional Features

SQLAlchemy and Superset require the corresponding SQLAlchemy and Apache Superset packages to be
included in your Python installation.  ClickHouse connect also includes C/Cython extensions for
improved performance reading String and FixedString datatypes.  These extensions will be installed
automatically by setup.py if a C compiler is available.

Query results can be returned as either a numpy array or a pandas DataFrame if the numpy and
pandas libraries are available.  Use the client methods `query_np` and `query_df` respectively.


### Tests

The `tests` directory contains a standard `pytest` test suite.  Integration tests require docker to be 
installed and run against the current `clickhouse/clickhouse-server` image.  The test suite includes
"fuzz" testing for reading/writing all supported datatypes with randomly generated data.  To run the
full test suite all libraries supporting optional features (Superset and SQLAlchemy) should be available.
To install the C/Cython libaries required for testing in the project, run `python setup.py build_ext --inplace`.  

## Main Client Interface

Interaction with the ClickHouse server is done through a clickhouse_connect Client instance.
At this point only an HTTP based Client is supported.

### HTTP Client constructor/initialization parameters

All parameters can be passed as keyword arguments.

* `interface:str` _https_ or _http_  
  Defaults to _https_ if a recognized secure port (443 or 8443), otherwise _http_
* `host:str` ClickHouse server hostname or ip address  
  Defaults to _localhost_
* `port:int` ClickHouse server port number  
  Defaults to 8123 or 8443 (if the interface is _https_)
* `username:str` ClickHouse user 
* `password:str` ClickHouse password
* `database:str` Default database to use for the client.  
  Defaults to the default database for the ClickHouse user
* `compress:bool` Accept compressed data from the ClickHouse server.  
  Defaults to _True_
* `format:str` _native_ (ClickHouse Native) or _rb_ (ClickHouse Row Binary)  
  Native format is preferred for performance reasons
* `query_limit:int` LIMIT value added to all queries.  
  Defaults to 5,000 rows.  Unlimited queries are not supported to prevent crashing the driver
* `ca_cert:str`  Private/Self-Signed Certificate Authority TLS certificate to validate ClickHouse server
* `client_cert:str`  File path to Client Certificate for mutual TLS authentication
* `client_cert_key:str` File path to Client Certificate private key for mutual TLS authentication
  



