## ClickHouse Connect

A suite of Python packages for connecting Python to ClickHouse, initially
supporting Apache Superset using a minimal read only SQLAlchemy dialect.  Uses the
ClickHouse HTTP interface.


### Installation

```
pip install clickhouse-connect
```

ClickHouse Connect requires Python 3.7 or higher.  The `cython` package must be installed prior to installing 
`clickhouse_connect` to build and install the optional  Cython/C extensions used for improving read and write
performance using the ClickHouse Native format. After installing cython if desired, clone this repository and
run `python setup.py install`from the project directory.

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
and Superset SQL Lab functionality with all standard ClickHouse data types.  See
[Connecting Superset](./docs/superset.md) for complete instructions.  

ClickHouse Enum, UUID, and IP Address datatypes are treated as strings.  For compatibility with Superset Pandas dataframes,
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
At this point only an HTTP(s) based Client is supported.

### HTTP Client constructor/initialization parameters

Create a ClickHouse client using the `clickhouse_connect.driver.create_client(...)` function or
`clickhouse_connect.get_client(...)` wrapper.  All parameters are optional:

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
  Defaults to 5,000 rows.  Setting query_limit=0 will return unlimited results, at the risk of running out of memory
* `connect_timeout:int` HTTP connection timeout in seconds.  Default 10 seconds.
* `send_receive_timeout:int` HTTP read timeout in seconds.  Default 300 seconds.
* `client_name:str` HTTP User-Agent header.  Defaults to `clickhouse-connect`
* `verify:bool` For HTTPS connections, validate the ClickHouse server TLS certificate, including
  matching hostname, expiration, and signed by a trusted Certificate Authority. Defaults to True.
* `ca_cert:str`  File path to private/Self-Signed Certificate Authority TLS certificate to validate ClickHouse server
* `client_cert:str`  File path to Client Certificate for mutual TLS authentication (including any intermediate certificates)
* `client_cert_key:str` File path to Client Certificate private key for mutual TLS authentication.
This parameter is not required if the client_cert is a .pem file containing both the certificate(s) and 
private key

Any remaining keyword parameters are interpreted as 'setting' parameters to send to the ClickHouse server
with every query/request
### Querying data

Use the client `query` method to retrieve a QueryResult from ClickHouse.  Parameters:

* `query:str`  Required.  ClickHouse query statement (either `SELECT`, `DESCRIBE`, or other system commands that result a result set.)
* `parameters:Dict[str, Any]` Optional.  Dictionary of `str` keys with values of any Python scalar datatype.  These
values will be replaced in the query string using Python %s type formatting
* `settings:Dict[str, str]` Optional.  Dictionary of `str` keys and `str` values for ClickHouse query settings
* `use_none:bool`  Optional, defaults to true.  Use the Python None type for ClickHouse NULLS.  Otherwise
  the QueryResult will include 0/empty values for ClickHouse nulls.  This is useful for populating
  Numpy arrays or Pandas dataframes, which do not accept nulls

The `query` method results a QueryResult object with the following fields:

* `result_set:Sequence[Sequence]`  A sequence of rows of column values containing the Python native data types from the query.
* `column_names:list` A list of column names for the rows in the `result_set`
* `column_types:list` A list of ClickHouseType objects for each column
* `query_id:str`  The ClickHouse query id.  Note that this can  be specified for ClickHouse by using the settings
  parameter, e.g. `client.query('SELECT * FROM system.tables', settings={'query_id': 'test_query_id'})`
  Otherwise ClickHouse will assign a random UUID as the query id.
* `summary:Dict[str, Any]`  The final contents of the X-ClickHouse-Summary header.  Note that this is empty unless the
  setting `send_progress_in_http_headers` is enabled.

#### Numpy and Pandas queries

If Numpy is installed, the driver can return a complete Numpy array by using the client `query_np` method instead of
`query`.  The parameters for `query` and `query_np` are the same, except `query_np` does not need or accept the `use_none`
argument as Numpy arrays do not support Python None.

If Pandas is installed as well as Numpy, a populated pandas DataFrame will be returned by the client `query_df` method
instead of `query`.  The three parameters (`query`, `parameters`, and `settings`) are identical to the parameters
for `query_np`.

#### Datatype options for queries

There are some convenience methods in the `clickhouse_connect.driver` package that control the format
of some ClickHouse datatypes.  These are included in part to improve Superset compatibility.

* `fixed_string_format`  Format for FixedString datatypes.  Options are _bytes_ and _string_, defaults to Python
  byte objects.  Set to _string_ when the SuperSet packages are initialized for datasets that used
  FixedString objects as actual strings.
* `big_int_format` Format for U/Int128 and U/Int256 datatypes.  Options are _int_ and _string_, defaults to int
  datatypes.  Set to _string_ when SupersetSet packages are initialized because SuperSet dataframes do
  not handle Python integers larger than 64 bits.
* `uint64_format`  Format for UInt64 ClickHouse types.  Options are _signed_ and _unsigned_, defaults to `_unsigned`.
  Set to _signed_ when SuperSet packages are initialized because SuperSet dataframes do not handle
  unsigned 64 bit integers.
* `uuid_format`  Format for UUID ClickHouse types.  Options are _uuid_ and _string_, defaults to Python UUID
  datatypes.  Set to _string_ when SuperSet packages are initialized because SuperSet dataframes do not
  provide special handling for UUID types
* `ip_format`  Format for IPv4/IPv6 ClickHouse types.  Options are `ip` and `string`, default to Python IP4Address/
  IPv6Address types.  Set to _string_ for compatibility with SuperSet dataframes

### Inserting data

Use the client `insert` method to insert data into a ClickHouse table.  Parameters:

* `table:str`  Required.  ClickHouse table name to insert data into.  This can be either the full `database.table` or
  just the table name.  In the latter case the database is determined by either the database parameter or the
  default database for the client/connection
* `data:Iterable:[Iterable[Any]]`  Required.  The matrix of rows and columns of native Python datatypes to insert.
* `column_names:Union[str, Iterable[str]]`  Required.  Either a single column name or list of columns.  If `*`, the
  driver will retrieve the list of columns from the ClickHouse Server in `position` order, which is fragile and
  not recommended.  Column names should be in the same order as columns in the `data` collection.
* `column_types:[Iterable[ClickHouseType]]`  Optional.  List of driver ClickHouseType objects that match the
  `column_names` parameter.  If not specified (and `column_type_names` is not specified), column types will be retrieved from the ClickHouse server using
  the `DESCRIBE TABLE`.
* `column_type_names:[Iterable[str]]`  Optional.  List of column type names as required/returned by the ClickHouse
  server.  These can be used to populate the `column_types` parameter without calling the ClickHouse Server.
* `column_oriented:bool`  Default _False_.  If _True_ the `data` parameter is processed as a sequence of equal length
  columns, instead of a list of rows.  This eliminates the need to "pivot" the matrix when using the Native data format.
* `settings:Dict[str, str]`  Optional.  Dictionary of ClickHouse settings to be applied to the insert query.

#### Notes on data inserts

The client `insert_df` can be used to insert a Pandas DataFrame, assuming the column names in the DataFrame match the
ClickHouse table column names.  Note that a Numpy array can be passed directly  as the `data` parameter to the primary
`insert` method so there is no separate `insert_np` method.

For column types that can be different native Python types (for example, UUIDs or IP Addresses), the driver will assume
that the data type for the whole column matches the first non "None" value in the column and process insert data
accordingly.  So if the first data value for insert into a ClickHouse UUID column is a string, the driver will assume
all data values in that insert column are strings.

### DDL and other "simple" SQL statements

The client `command` method can be used for ClickHouse commands/queries that return a single result or row of results
values.  In this case the result is returned as a single row TabSeparated values and are cast to a single string,
int, or list of string values.  The `command` method parameters are:

* `cmd:str`  Required.  ClickHouse SQL command/query.
* `parameters:Dict[str, Any]` Optional.  Dictionary of `str` keys with values of any Python scalar datatype.  These
values will be replaced in the query string using Python %s type formatting
* `use_database:bool` Optional, defaults to True.  Use the client default database (as set when the client is created
  or the user's default database) when sending the query.  This is set to False in order to determine the
  users default database on connection.
* `settings:Dict[str, Any]`  Optional.  Dictionary of ClickHouse settings to be applied to the insert query.
