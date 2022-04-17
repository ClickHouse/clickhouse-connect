## ClickHouse Connect

A suite of Python packages for connecting Python to ClickHouse, initially
supporting Apache Superset using a minimal read only SQLAlchemy dialect.  Uses the
ClickHouse HTTP interface.

### Getting Started

Interaction with ClickHouse occurs through a client object:

```
import clickhouse_connect
client = clickhouse_connect.client(host='play.clickhouse.com', port=443, username='play')
query_result = client.query('SHOW TABLES')
print (query_result.result_set) 
```

Other significant client keyword parameters include the user `password`, and the default `database` 
used for queries.  With no parameters, the client will connect to localhost on port 8123 with
no username or password.

In addition to queries, the client can execute 'commands' that do not return a result set.

```
import clickhouse_connect
client = clickhouse_connect.client()
client.command ('CREATE TABLE test_table (key UInt16, value String) ENGINE Memory')
```

Bulk insert of a matrix of rows and columns uses the client `insert` method.

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



